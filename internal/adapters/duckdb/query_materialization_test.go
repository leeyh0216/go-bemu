package duckdb

// Public protocol sources for these transaction tests:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
//   - https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L315-L331

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestQueryDestinationWriteDispositionsAreAtomic(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, schema := newQueryMaterializationFixture(t, ctx)

	appendResult, err := warehouse.MaterializeQuery(ctx, queryMaterializationRequest(
		"SELECT id, payload FROM `test-project.analytics.source` WHERE id = 2",
		domain.WriteAppend, true, schema,
	))
	if err != nil {
		t.Fatal(err)
	}
	if appendResult.DestinationCreated || len(appendResult.QueryResult.Rows) != 1 {
		t.Fatalf("unexpected append materialization result: %#v", appendResult)
	}
	assertQueryDestinationRows(t, ctx, warehouse, []int64{1, 2})

	_, err = warehouse.MaterializeQuery(ctx, queryMaterializationRequest(
		"SELECT id, payload FROM `test-project.analytics.source` WHERE id = 3",
		domain.WriteEmpty, true, schema,
	))
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("WRITE_EMPTY error = %v, want conflict", err)
	}
	assertQueryDestinationRows(t, ctx, warehouse, []int64{1, 2})

	_, err = warehouse.MaterializeQuery(ctx, queryMaterializationRequest(
		"SELECT id, payload FROM `test-project.analytics.source` WHERE id >= 2",
		domain.WriteTruncate, true, schema,
	))
	if err != nil {
		t.Fatal(err)
	}
	assertQueryDestinationRows(t, ctx, warehouse, []int64{2, 3})
}

func TestQueryDestinationSchemaMismatchPreservesExistingRows(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, schema := newQueryMaterializationFixture(t, ctx)

	_, err := warehouse.MaterializeQuery(ctx, queryMaterializationRequest(
		"SELECT CAST(id AS VARCHAR) AS id, payload FROM `test-project.analytics.source`",
		domain.WriteTruncate, true, schema,
	))
	if !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("schema drift error = %v, want precondition", err)
	}
	assertQueryDestinationRows(t, ctx, warehouse, []int64{1})
}

func TestQueryDestinationNotNullFailureRollsBackTruncate(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, schema := newQueryMaterializationFixture(t, ctx)

	_, err := warehouse.MaterializeQuery(ctx, queryMaterializationRequest(
		"SELECT CAST(NULL AS BIGINT) AS id, CAST('new' AS VARCHAR) AS payload",
		domain.WriteTruncate, true, schema,
	))
	if err == nil {
		t.Fatal("WRITE_TRUNCATE with NULL for REQUIRED id unexpectedly succeeded")
	}
	assertQueryDestinationRows(t, ctx, warehouse, []int64{1})
}

func TestQueryDestinationCreateIfNeededUsesOneCTASTransaction(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, _ := newQueryMaterializationFixture(t, ctx)
	request := queryMaterializationRequest(
		"SELECT id, payload FROM `test-project.analytics.source` ORDER BY id",
		domain.WriteEmpty, false, nil,
	)
	request.Destination.TableID = "created_by_query"
	result, err := warehouse.MaterializeQuery(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DestinationCreated || len(result.QueryResult.Rows) != 3 {
		t.Fatalf("unexpected CTAS result: %#v", result)
	}
	stored, err := warehouse.Query(ctx, ports.QueryRequest{SQL: "SELECT count(*) FROM `test-project.analytics.created_by_query`"})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Rows) != 1 || stored.Rows[0][0] != int64(3) {
		t.Fatalf("created destination count = %#v", stored.Rows)
	}
}

func TestQueryDestinationNormalizesAnonymousAggregateColumn(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, _ := newQueryMaterializationFixture(t, ctx)
	request := queryMaterializationRequest(
		"SELECT COUNT(*) FROM `test-project.analytics.source`",
		domain.WriteEmpty, false, nil,
	)
	request.Destination.TableID = "count_result"
	result, err := warehouse.MaterializeQuery(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.QueryResult.Columns) != 1 || result.QueryResult.Columns[0].Name != "f0_" {
		t.Fatalf("normalized aggregate schema = %#v", result.QueryResult.Columns)
	}
	stored, err := warehouse.Query(ctx, ports.QueryRequest{
		SQL: "SELECT f0_ FROM `test-project.analytics.count_result`",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Rows) != 1 || stored.Rows[0][0] != int64(3) {
		t.Fatalf("normalized aggregate destination rows = %#v", stored.Rows)
	}
}

func TestQueryMaterializationCarriesRecursiveDecimalSchemaAndValues(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, _ := newQueryMaterializationFixture(t, ctx)
	precision10, scale2 := int64(10), int64(2)
	schema := []domain.Field{
		{Name: "amount", Type: "BIGNUMERIC"},
		{Name: "details", Type: "STRUCT", Fields: []domain.Field{{Name: "nested_amount", Type: "BIGNUMERIC"}}},
		{Name: "amounts", Type: "NUMERIC", Mode: "REPEATED", Precision: &precision10, Scale: &scale2},
	}
	if err := warehouse.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "decimal_source", Schema: schema}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO "bq_746573742d70726f6a656374_616e616c7974696373"."decimal_source" VALUES
		(12345678901234567890.123456789012345678, {'nested_amount': 1.000000000000000001}, [12.34, 56.78])`); err != nil {
		t.Fatal(err)
	}
	request := queryMaterializationRequest(
		"SELECT amount, details, amounts FROM `test-project.analytics.decimal_source`",
		domain.WriteEmpty, false, nil,
	)
	request.Destination.TableID = "decimal_result"
	result, err := warehouse.MaterializeQuery(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	fields := result.QueryResult.Columns
	if len(fields) != 3 || fields[0].Type != "BIGNUMERIC" || fields[0].Precision == nil || *fields[0].Precision != 38 || *fields[0].Scale != 18 ||
		fields[1].Type != "RECORD" || fields[1].Fields[0].Type != "BIGNUMERIC" || fields[2].Mode != "REPEATED" || fields[2].Type != "NUMERIC" {
		t.Fatalf("recursive query schema = %#v", fields)
	}
	row := result.QueryResult.Rows[0]
	details, ok := row[1].(map[string]any)
	if row[0] != "12345678901234567890.123456789012345678" || !ok || details["nested_amount"] != "1.000000000000000001" {
		t.Fatalf("canonical decimal query row = %#v", row)
	}
}

func TestExistingDestinationRestoresAmbiguousBigNumericIdentity(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, _ := newQueryMaterializationFixture(t, ctx)
	precision, scale := int64(10), int64(2)
	schema := []domain.Field{{Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale}}
	for _, tableID := range []string{"ambiguous_source", "ambiguous_destination"} {
		if err := warehouse.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: tableID, Schema: schema}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO "bq_746573742d70726f6a656374_616e616c7974696373"."ambiguous_source" VALUES (12345678.90)`); err != nil {
		t.Fatal(err)
	}
	request := queryMaterializationRequest(
		"SELECT amount FROM `test-project.analytics.ambiguous_source`", domain.WriteAppend, true, schema,
	)
	request.Destination.TableID = "ambiguous_destination"
	result, err := warehouse.MaterializeQuery(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	field := result.QueryResult.Columns[0]
	if field.Type != "BIGNUMERIC" || field.Precision == nil || *field.Precision != 10 || field.Scale == nil || *field.Scale != 2 {
		t.Fatalf("destination metadata did not restore BIGNUMERIC identity: %#v", field)
	}
}

func TestQuerySchemaInfersBigNumericWhenNumericRangeCannotRepresentDecimal(t *testing.T) {
	field, err := parseDuckDBResultType("DECIMAL(38,2)")
	if err != nil {
		t.Fatal(err)
	}
	if field.Type != "BIGNUMERIC" || field.Precision == nil || *field.Precision != 38 || field.Scale == nil || *field.Scale != 2 {
		t.Fatalf("DECIMAL(38,2) query field = %#v, want unambiguous BIGNUMERIC", field)
	}
	ambiguous, err := parseDuckDBResultType("DECIMAL(10,2)")
	if err != nil {
		t.Fatal(err)
	}
	if ambiguous.Type != "NUMERIC" {
		t.Fatalf("DECIMAL(10,2) query field = %#v, want conservative NUMERIC", ambiguous)
	}
}

func TestQueryMaterializationLogsShapeAndDigestWithoutRawSQLOrRows(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, _ := newQueryMaterializationFixture(t, ctx)
	var logs bytes.Buffer
	previous := slog.Default()
	observability.Configure(false)
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		observability.Configure(false)
	})

	const marker = "raw-query-and-row-marker-7f42"
	request := queryMaterializationRequest("SELECT '"+marker+"' AS payload", domain.WriteEmpty, false, nil)
	request.Destination.TableID = "safe_logs"
	if _, err := warehouse.MaterializeQuery(ctx, request); err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	if strings.Contains(output, marker) {
		t.Fatalf("raw SQL/result value leaked into safe logs: %s", output)
	}
	for _, field := range []string{"query_bytes", "query_digest", "schema_fingerprint", "model_version", "transaction_mode"} {
		if !strings.Contains(output, field) {
			t.Fatalf("safe materialization log is missing %q: %s", field, output)
		}
	}
}

func newQueryMaterializationFixture(t *testing.T, ctx context.Context) (*Warehouse, []domain.Field) {
	t.Helper()
	if ctx.Err() != nil {
		t.Fatalf("query test context already ended: %v", ctx.Err())
	}
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "analytics"); err != nil {
		t.Fatal(err)
	}
	schema := []domain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "payload", Type: "STRING", Mode: "NULLABLE"},
	}
	for _, tableID := range []string{"source", "destination"} {
		if err := warehouse.CreateTable(ctx, domain.Table{
			ProjectID: "test-project", DatasetID: "analytics", ID: tableID, Schema: schema,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{
		SQL: "INSERT INTO `test-project.analytics.source` VALUES (1, 'one'), (2, 'two'), (3, 'three')",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{
		SQL: "INSERT INTO `test-project.analytics.destination` VALUES (1, 'old')",
	}); err != nil {
		t.Fatal(err)
	}
	return warehouse, schema
}

func queryMaterializationRequest(sql string, disposition domain.WriteDisposition, exists bool, schema []domain.Field) ports.QueryMaterializationRequest {
	return ports.QueryMaterializationRequest{
		Query: ports.QueryRequest{ProjectID: "test-project", SQL: sql},
		Destination: domain.TableReference{
			ProjectID: "test-project", DatasetID: "analytics", TableID: "destination",
		},
		DestinationExists: exists, DestinationSchema: schema,
		WriteDisposition: disposition, CreateDisposition: domain.CreateIfNeeded,
	}
}

func assertQueryDestinationRows(t *testing.T, ctx context.Context, warehouse *Warehouse, want []int64) {
	t.Helper()
	if ctx.Err() != nil {
		t.Fatalf("query test context ended: %v", ctx.Err())
	}
	result, err := warehouse.Query(ctx, ports.QueryRequest{
		SQL: "SELECT id FROM `test-project.analytics.destination` ORDER BY id",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("destination rows = %#v, want IDs %v", result.Rows, want)
	}
	for index, id := range want {
		if result.Rows[index][0] != id {
			t.Fatalf("destination row %d = %#v, want id=%d", index, result.Rows[index], id)
		}
	}
}
