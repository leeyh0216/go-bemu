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
	schema := []domain.Field{
		{Name: "amount", Type: "BIGNUMERIC"},
		{Name: "details", Type: "STRUCT", Fields: []domain.Field{{Name: "nested_amount", Type: "BIGNUMERIC"}}},
		{Name: "amounts", Type: "BIGNUMERIC", Mode: "REPEATED"},
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
		fields[1].Type != "RECORD" || fields[1].Fields[0].Type != "BIGNUMERIC" || fields[2].Mode != "REPEATED" || fields[2].Type != "BIGNUMERIC" {
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
	schema := []domain.Field{{Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale, RoundingMode: domain.RoundingModeHalfEven}}
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
	if field.Type != "BIGNUMERIC" || field.Precision == nil || *field.Precision != 10 || field.Scale == nil || *field.Scale != 2 || field.RoundingMode != domain.RoundingModeHalfEven {
		t.Fatalf("destination metadata did not restore BIGNUMERIC identity: %#v", field)
	}
}

func TestQueryDestinationTreatsStructAndRecordAliasesBidirectionally(t *testing.T) {
	precision, scale := int64(12), int64(4)
	field := func(outerType, nestedType string) domain.Field {
		return domain.Field{Name: "payload", Type: outerType, Fields: []domain.Field{{
			Name: "items", Type: nestedType, Mode: "REPEATED", Fields: []domain.Field{{
				Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale, RoundingMode: domain.RoundingModeHalfEven,
			}},
		}}}
	}
	for _, testCase := range []struct {
		name        string
		output      domain.Field
		destination domain.Field
	}{
		{name: "STRUCT output to RECORD destination", output: field("STRUCT", "RECORD"), destination: field("RECORD", "STRUCT")},
		{name: "RECORD output to STRUCT destination", output: field("RECORD", "STRUCT"), destination: field("STRUCT", "RECORD")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			compatible, requiresRounding := queryDestinationFieldsCompatible(testCase.output, testCase.destination)
			if !compatible || requiresRounding {
				t.Fatalf("alias compatibility = (%t, %t), want (true, false)", compatible, requiresRounding)
			}
		})
	}
}

func TestExistingQueryDestinationPreservesNestedRepeatedBigNumericAcrossStructAliases(t *testing.T) {
	for _, testCase := range []struct {
		name                      string
		disposition               domain.WriteDisposition
		sourceOuter, sourceNested string
		destOuter, destNested     string
		wantLabels                []string
		wantAmounts               []string
	}{
		{
			name: "append STRUCT source to RECORD destination", disposition: domain.WriteAppend,
			sourceOuter: "STRUCT", sourceNested: "RECORD", destOuter: "RECORD", destNested: "STRUCT",
			wantLabels: []string{"old", "source"}, wantAmounts: []string{"1.2300", "12345678.9012"},
		},
		{
			name: "truncate RECORD source to STRUCT destination", disposition: domain.WriteTruncate,
			sourceOuter: "RECORD", sourceNested: "STRUCT", destOuter: "STRUCT", destNested: "RECORD",
			wantLabels: []string{"source"}, wantAmounts: []string{"12345678.9012"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := duckDBQueryTestContext(t)
			defer cancel()
			warehouse, _ := newQueryMaterializationFixture(t, ctx)
			precision, scale := int64(12), int64(4)
			sourceSchema := nestedRepeatedDecimalSchema(testCase.sourceOuter, testCase.sourceNested, &precision, &scale)
			destinationSchema := nestedRepeatedDecimalSchema(testCase.destOuter, testCase.destNested, &precision, &scale)
			for tableID, schema := range map[string][]domain.Field{
				"alias_source": sourceSchema, "alias_destination": destinationSchema,
			} {
				if err := warehouse.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: tableID, Schema: schema}); err != nil {
					t.Fatal(err)
				}
			}
			physical := quoteIdentifier(physicalSchema("test-project", "analytics"))
			if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO `+physical+`."alias_source" VALUES
				({'items': [{'amount': 12345678.9012, 'label': 'source'}]})`); err != nil {
				t.Fatal(err)
			}
			if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO `+physical+`."alias_destination" VALUES
				({'items': [{'amount': 1.2300, 'label': 'old'}]})`); err != nil {
				t.Fatal(err)
			}

			request := queryMaterializationRequest(
				"SELECT payload FROM `test-project.analytics.alias_source`", testCase.disposition, true, destinationSchema,
			)
			request.Destination.TableID = "alias_destination"
			result, err := warehouse.MaterializeQuery(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			assertNestedDecimalSchemaAndValue(t, result, testCase.destOuter, testCase.destNested, precision, scale)
			assertStoredNestedDecimalRows(t, ctx, warehouse, testCase.wantLabels, testCase.wantAmounts)
		})
	}
}

func TestExistingQueryDestinationRejectsTrueNestedMismatchWithoutMutation(t *testing.T) {
	for _, disposition := range []domain.WriteDisposition{domain.WriteAppend, domain.WriteTruncate} {
		t.Run(string(disposition), func(t *testing.T) {
			ctx, cancel := duckDBQueryTestContext(t)
			defer cancel()
			warehouse, _ := newQueryMaterializationFixture(t, ctx)
			precision, scale := int64(12), int64(4)
			destinationSchema := nestedRepeatedDecimalSchema("STRUCT", "RECORD", &precision, &scale)
			sourceSchema := domain.CloneFields(destinationSchema)
			sourceSchema[0].Fields[0].Fields[0] = domain.Field{Name: "amount", Type: "STRING"}
			for tableID, schema := range map[string][]domain.Field{
				"mismatch_source": sourceSchema, "mismatch_destination": destinationSchema,
			} {
				if err := warehouse.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: tableID, Schema: schema}); err != nil {
					t.Fatal(err)
				}
			}
			physical := quoteIdentifier(physicalSchema("test-project", "analytics"))
			if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO `+physical+`."mismatch_source" VALUES
				({'items': [{'amount': 'not-a-decimal', 'label': 'source'}]})`); err != nil {
				t.Fatal(err)
			}
			if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO `+physical+`."mismatch_destination" VALUES
				({'items': [{'amount': 9.9900, 'label': 'old'}]})`); err != nil {
				t.Fatal(err)
			}

			request := queryMaterializationRequest(
				"SELECT payload FROM `test-project.analytics.mismatch_source`", disposition, true, destinationSchema,
			)
			request.Destination.TableID = "mismatch_destination"
			_, err := warehouse.MaterializeQuery(ctx, request)
			if !errors.Is(err, domain.ErrPrecondition) || !strings.Contains(err.Error(), domain.CapabilityQueryDestinationExactSchemaV1) {
				t.Fatalf("nested mismatch error = %v", err)
			}
			assertStoredNestedDecimalRowsForTable(t, ctx, warehouse, "mismatch_destination", []string{"old"}, []string{"9.9900"})
		})
	}
}

func TestQueryDestinationRejectsDecimalRoundingBeforeMutation(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, _ := newQueryMaterializationFixture(t, ctx)
	sourcePrecision, sourceScale := int64(6), int64(3)
	destinationPrecision, destinationScale := int64(5), int64(2)
	sourceSchema := []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: &sourcePrecision, Scale: &sourceScale}}
	destinationSchema := []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: &destinationPrecision, Scale: &destinationScale}}
	for tableID, schema := range map[string][]domain.Field{"rounding_source": sourceSchema, "rounding_destination": destinationSchema} {
		if err := warehouse.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: tableID, Schema: schema}); err != nil {
			t.Fatal(err)
		}
	}
	physical := quoteIdentifier(physicalSchema("test-project", "analytics"))
	if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO `+physical+`."rounding_source" VALUES (1.025)`); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO `+physical+`."rounding_destination" VALUES (9.99)`); err != nil {
		t.Fatal(err)
	}
	request := queryMaterializationRequest("SELECT amount FROM `test-project.analytics.rounding_source`", domain.WriteTruncate, true, destinationSchema)
	request.Destination.TableID = "rounding_destination"
	_, err := warehouse.MaterializeQuery(ctx, request)
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.CapabilityQueryDecimalRoundingV1) {
		t.Fatalf("query decimal rounding error = %v", err)
	}
	var amount string
	if err := warehouse.db.QueryRowContext(ctx, `SELECT CAST(amount AS VARCHAR) FROM `+physical+`."rounding_destination"`).Scan(&amount); err != nil {
		t.Fatal(err)
	}
	if amount != "9.99" {
		t.Fatalf("rejected query rounding changed destination to %q", amount)
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
	_, err = parseDuckDBResultType("DECIMAL(10,2)")
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.GapQueryDecimalLineageV1) {
		t.Fatalf("ambiguous DECIMAL error = %v, want fail-closed lineage capability", err)
	}
}

func TestQueryMaterializationLogsShapeAndDigestWithoutRawSQLOrRows(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, _ := newQueryMaterializationFixture(t, ctx)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
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

func nestedRepeatedDecimalSchema(outerType, nestedType string, precision, scale *int64) []domain.Field {
	return []domain.Field{{Name: "payload", Type: outerType, Fields: []domain.Field{{
		Name: "items", Type: nestedType, Mode: "REPEATED", Fields: []domain.Field{
			{Name: "amount", Type: "BIGNUMERIC", Precision: precision, Scale: scale, RoundingMode: domain.RoundingModeHalfEven},
			{Name: "label", Type: "STRING"},
		},
	}}}}
}

func assertNestedDecimalSchemaAndValue(t *testing.T, result ports.QueryMaterializationResult, outerType, nestedType string, precision, scale int64) {
	t.Helper()
	if len(result.QueryResult.Columns) != 1 {
		t.Fatalf("query schema = %#v", result.QueryResult.Columns)
	}
	outer := result.QueryResult.Columns[0]
	if outer.Type != outerType || len(outer.Fields) != 1 {
		t.Fatalf("outer query field = %#v", outer)
	}
	nested := outer.Fields[0]
	if nested.Type != nestedType || nested.Mode != "REPEATED" || len(nested.Fields) != 2 {
		t.Fatalf("nested query field = %#v", nested)
	}
	amount := nested.Fields[0]
	if amount.Type != "BIGNUMERIC" || amount.Precision == nil || *amount.Precision != precision || amount.Scale == nil || *amount.Scale != scale || amount.RoundingMode != domain.RoundingModeHalfEven {
		t.Fatalf("nested decimal identity = %#v", amount)
	}
	if len(result.QueryResult.Rows) != 1 {
		t.Fatalf("query rows = %#v", result.QueryResult.Rows)
	}
	payload, ok := result.QueryResult.Rows[0][0].(map[string]any)
	if !ok {
		t.Fatalf("query payload = %#v", result.QueryResult.Rows[0][0])
	}
	items, ok := payload["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("query items = %#v", payload["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok || item["amount"] != "12345678.9012" || item["label"] != "source" {
		t.Fatalf("canonical nested query value = %#v", items[0])
	}
}

func assertStoredNestedDecimalRows(t *testing.T, ctx context.Context, warehouse *Warehouse, labels, amounts []string) {
	t.Helper()
	assertStoredNestedDecimalRowsForTable(t, ctx, warehouse, "alias_destination", labels, amounts)
}

func assertStoredNestedDecimalRowsForTable(t *testing.T, ctx context.Context, warehouse *Warehouse, tableID string, labels, amounts []string) {
	t.Helper()
	physical := quoteIdentifier(physicalSchema("test-project", "analytics"))
	rows, err := warehouse.db.QueryContext(ctx, `SELECT payload.items[1].label, CAST(payload.items[1].amount AS VARCHAR) FROM `+physical+`.`+quoteIdentifier(tableID)+` ORDER BY payload.items[1].label`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var gotLabels, gotAmounts []string
	for rows.Next() {
		var label, amount string
		if err := rows.Scan(&label, &amount); err != nil {
			t.Fatal(err)
		}
		gotLabels = append(gotLabels, label)
		gotAmounts = append(gotAmounts, amount)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(gotLabels, "\x00") != strings.Join(labels, "\x00") || strings.Join(gotAmounts, "\x00") != strings.Join(amounts, "\x00") {
		t.Fatalf("stored nested rows = labels %v amounts %v, want labels %v amounts %v", gotLabels, gotAmounts, labels, amounts)
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
