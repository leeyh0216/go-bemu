package duckdb

// Exact fixture source and partition semantics:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L905
// BigQuery ARRAY_AGG IGNORE NULLS:
// https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	v0442 "github.com/leeyh0216/go-bemu/internal/adapters/sparkbigquery/v0442"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const (
	testDynamicTargetAlias = "__target_0123456789abcdef0123456789abcdef"
	testDynamicSourceAlias = "__source_fedcba9876543210fedcba9876543210"
)

func TestSparkDynamicTimePartitionOverwriteAnalysisIsSourcePinnedAndFailClosed(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	analyzer, err := v0442.NewAnalyzer(warehouse)
	if err != nil {
		t.Fatal(err)
	}

	statement := sparkDynamicTimeOverwriteFixture(
		"test-project.analytics.destination", "test-project.analytics.temporary",
		"partition_date", "DATE_TRUNC", "DAY", []string{"id", "partition_date", "payload"},
	)
	analysis, err := analyzer.AnalyzeQuery(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: statement})
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis.ReferencedTables) != 2 || len(analysis.MutationTargets) != 1 ||
		analysis.MutationTargets[0].TableID != "destination" || analysis.ProducesRows {
		t.Fatalf("dynamic overwrite analysis = %#v", analysis)
	}
	operation, matched, err := analyzer.AnalyzeQueryOperation(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: statement})
	if err != nil || !matched || operation.ProfileID() != v0442.DynamicTimeOverwriteProfile ||
		operation.PartitionField() != "partition_date" || operation.Granularity() != "DAY" {
		t.Fatalf("dynamic overwrite operation = %#v, matched=%t err=%v", operation, matched, err)
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: statement}); !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.GapQueryScriptsUnsupportedV1) ||
		strings.Contains(err.Error(), v0442.DynamicTimeOverwriteProfile) {
		t.Fatalf("generic DuckDB path recognized a connector profile: %v", err)
	}

	driftedAlias := strings.Replace(statement, testDynamicTargetAlias, "__target_not-a-connector-uuid", 1)
	driftedAlias = strings.ReplaceAll(driftedAlias, "analytics.temporary", "analytics.sensitive_payload_marker")
	logs.Reset()
	_, err = analyzer.AnalyzeQuery(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: driftedAlias})
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), v0442.DynamicTimeOverwriteProfile) ||
		!strings.Contains(err.Error(), domain.GapQueryScriptsUnsupportedV1) {
		t.Fatalf("UUID alias drift error = %v", err)
	}
	logged := logs.String()
	for _, required := range []string{
		"boundary.reject", "connector-dynamic-partition-overwrite", v0442.DynamicTimeOverwriteProfile,
		"capability", domain.CapabilitySparkDynamicTimePartitionOverwriteV1,
		"gap", domain.GapQueryScriptsUnsupportedV1, "token_index", "expected_shape", "query_digest", "fix_hint",
	} {
		if !strings.Contains(logged, required) {
			t.Fatalf("profile drift log lacks %q: %s", required, logged)
		}
	}
	if strings.Contains(logged, "sensitive_payload_marker") || strings.Contains(logged, driftedAlias) {
		t.Fatalf("profile drift log exposed SQL payload: %s", logged)
	}
	onePartSource := strings.ReplaceAll(statement, "test-project.analytics.temporary", "temporary")
	if _, err := analyzer.AnalyzeQuery(ctx, ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics", SQL: onePartSource,
	}); !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), v0442.DynamicTimeOverwriteProfile) {
		t.Fatalf("one-part connector source drift error = %v", err)
	}

	generalScript := "DECLARE ordinary_value DEFAULT 1; SELECT ordinary_value"
	for name, analyze := range map[string]func() error{
		"analyzer": func() error { _, err := analyzer.AnalyzeQuery(ctx, ports.QueryRequest{SQL: generalScript}); return err },
		"engine":   func() error { _, err := warehouse.Query(ctx, ports.QueryRequest{SQL: generalScript}); return err },
	} {
		t.Run(name+" rejects general script", func(t *testing.T) {
			err := analyze()
			if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.GapQueryScriptsUnsupportedV1) {
				t.Fatalf("general script error = %v", err)
			}
		})
	}

	rangeScript := sparkDynamicRangeOverwriteFixture()
	_, err = analyzer.AnalyzeQuery(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: rangeScript})
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.GapSparkDynamicRangePartitionOverwriteV1) ||
		!strings.Contains(err.Error(), v0442.DynamicRangeOverwriteProfile) {
		t.Fatalf("range template error = %v", err)
	}
}

func TestConnectorOverwriteBackendErrorsDoNotExposePhysicalCause(t *testing.T) {
	const secretPhysicalDetail = `Catalog Error: Table "secret_physical_table" does not exist`
	raw := errors.New(secretPhysicalDetail)
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "query", err: classifyDynamicOverwriteQueryError("execute overwrite", raw), want: domain.ErrInvalidQuery},
		{name: "backend", err: classifyDynamicOverwriteBackendError("commit overwrite", raw), want: domain.ErrBackend},
	} {
		t.Run(test.name, func(t *testing.T) {
			if !errors.Is(test.err, test.want) {
				t.Fatalf("classified error = %v, want %v", test.err, test.want)
			}
			if errors.Is(test.err, raw) || strings.Contains(test.err.Error(), secretPhysicalDetail) ||
				strings.Contains(test.err.Error(), "secret_physical_table") {
				t.Fatalf("classified error exposed physical cause: %v", test.err)
			}
		})
	}
	if got := classifyDynamicOverwriteQueryError("execute overwrite", context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("cancellation classification = %v", got)
	}
	if got := classifyDynamicOverwriteBackendError("commit overwrite", context.DeadlineExceeded); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("deadline classification = %v", got)
	}
	wrappedCancellation := fmt.Errorf("%s: %w", secretPhysicalDetail, context.Canceled)
	if got := classifyDynamicOverwriteQueryError("execute overwrite", wrappedCancellation); got != context.Canceled ||
		strings.Contains(got.Error(), "secret_physical_table") {
		t.Fatalf("wrapped cancellation classification = %v", got)
	}
}

func TestSparkDynamicTimePartitionOverwriteValidatesCanonicalFieldTypeAndGranularity(t *testing.T) {
	tests := []struct {
		name                string
		fieldType           string
		partitionFunction   string
		granularity         string
		metadataGranularity string
		wantErr             bool
	}{
		{name: "DATE DAY", fieldType: "DATE", partitionFunction: "DATE_TRUNC", granularity: "DAY", metadataGranularity: "DAY"},
		{name: "TIMESTAMP HOUR", fieldType: "TIMESTAMP", partitionFunction: "TIMESTAMP_TRUNC", granularity: "HOUR", metadataGranularity: "HOUR"},
		{name: "DATETIME MONTH", fieldType: "DATETIME", partitionFunction: "TIMESTAMP_TRUNC", granularity: "MONTH", metadataGranularity: "MONTH"},
		{name: "DATE rejects HOUR", fieldType: "DATE", partitionFunction: "DATE_TRUNC", granularity: "HOUR", metadataGranularity: "HOUR", wantErr: true},
		{name: "TIMESTAMP rejects DATE function", fieldType: "TIMESTAMP", partitionFunction: "DATE_TRUNC", granularity: "DAY", metadataGranularity: "DAY", wantErr: true},
		{name: "metadata granularity drift", fieldType: "DATE", partitionFunction: "DATE_TRUNC", granularity: "DAY", metadataGranularity: "MONTH", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement := sparkDynamicTimeOverwriteFixture(
				"test-project.analytics.destination", "test-project.analytics.temporary",
				"event_time", test.partitionFunction, test.granularity, []string{"id", "event_time", "payload"},
			)
			operation, matched, err := parseSparkDynamicTimeOverwrite(ports.QueryRequest{ProjectID: "test-project", SQL: statement})
			if err != nil || !matched {
				t.Fatalf("parse operation: matched=%t err=%v", matched, err)
			}
			table := dynamicOverwriteCanonicalTable("destination", "event_time", test.fieldType, test.metadataGranularity)
			_, err = validateDynamicTimeOverwriteDestination(operation, table)
			if test.wantErr && err == nil {
				t.Fatal("expected canonical partition validation to fail")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("canonical partition validation: %v", err)
			}
		})
	}

	statement := sparkDynamicTimeOverwriteFixture(
		"test-project.analytics.destination", "test-project.analytics.temporary",
		"event_time", "DATE_TRUNC", "DAY", []string{"id", "event_time", "payload"},
	)
	operation, _, err := parseSparkDynamicTimeOverwrite(ports.QueryRequest{ProjectID: "test-project", SQL: statement})
	if err != nil {
		t.Fatal(err)
	}
	rangeDestination := dynamicOverwriteCanonicalTable("destination", "event_time", "DATE", "DAY")
	rangeDestination.RangePartitioning = &domain.RangePartitioning{Field: "id", Range: domain.Range{Start: 0, End: 100, Interval: 10}}
	if _, err := validateDynamicTimeOverwriteDestination(operation, rangeDestination); !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.GapSparkDynamicRangePartitionOverwriteV1) {
		t.Fatalf("range metadata error = %v", err)
	}
}

func TestSparkDynamicTimePartitionOverwriteRejectsSourceSchemaImplicitCasts(t *testing.T) {
	statement := sparkDynamicTimeOverwriteFixture(
		"test-project.analytics.destination", "test-project.analytics.temporary",
		"event_time", "DATE_TRUNC", "DAY", []string{"id", "event_time", "payload"},
	)
	operation, matched, err := parseSparkDynamicTimeOverwrite(ports.QueryRequest{ProjectID: "test-project", SQL: statement})
	if err != nil || !matched {
		t.Fatalf("parse operation: matched=%t err=%v", matched, err)
	}
	destination := dynamicOverwriteCanonicalTable("destination", "event_time", "DATE", "DAY")

	tests := []struct {
		name    string
		mutate  func(*domain.Table)
		wantErr bool
	}{
		{
			name: "canonical aliases need no BigQuery type conversion",
			mutate: func(source *domain.Table) {
				source.Schema[0].Type = "INTEGER"
			},
		},
		{
			name: "FLOAT64 to INT64 is rejected before DuckDB coercion",
			mutate: func(source *domain.Table) {
				source.Schema[0].Type = "FLOAT64"
			},
			wantErr: true,
		},
		{
			name: "STRING to INT64 is rejected before DuckDB coercion",
			mutate: func(source *domain.Table) {
				source.Schema[0].Type = "STRING"
			},
			wantErr: true,
		},
		{
			name: "source partition type drift is rejected",
			mutate: func(source *domain.Table) {
				source.Schema[1].Type = "TIMESTAMP"
			},
			wantErr: true,
		},
		{
			name: "source mode drift is rejected",
			mutate: func(source *domain.Table) {
				source.Schema[2].Mode = "REQUIRED"
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := dynamicOverwriteCanonicalTable("temporary", "event_time", "DATE", "DAY")
			test.mutate(&source)
			_, err := validateDynamicTimeOverwriteTables(operation, destination, source)
			if test.wantErr {
				if !errors.Is(err, domain.ErrInvalidQuery) {
					t.Fatalf("schema drift error = %v, want ErrInvalidQuery", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("canonical alias validation: %v", err)
			}
		})
	}
}

func TestSparkDynamicTimePartitionOverwriteIsAtomicAndIgnoresNullTouchedPartition(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "analytics"); err != nil {
		t.Fatal(err)
	}
	target := dynamicOverwriteCanonicalTable("destination", "partition_date", "DATE", "DAY")
	for _, table := range []domain.Table{
		target,
		dynamicOverwriteCanonicalTable("temporary", "partition_date", "DATE", "DAY"),
		{
			ProjectID: "test-project", DatasetID: "analytics", ID: "invalid_temporary",
			Schema: []domain.Field{
				{Name: "id", Type: "STRING"},
				{Name: "partition_date", Type: "DATE"},
				{Name: "payload", Type: "STRING"},
			},
		},
	} {
		if err := warehouse.CreateTable(ctx, table); err != nil {
			t.Fatal(err)
		}
	}
	query := func(statement string) domain.QueryResult {
		t.Helper()
		result, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: statement})
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		return result
	}
	query("INSERT INTO `test-project.analytics.destination` VALUES " +
		"(1, DATE '2026-01-01', 'old-day-one'), (2, DATE '2026-01-02', 'keep'), " +
		"(3, DATE '2026-01-03', 'old-day-three'), (4, NULL, 'old-null')")
	query("INSERT INTO `test-project.analytics.temporary` VALUES " +
		"(5, DATE '2026-01-01', 'new-one-a'), (6, DATE '2026-01-01', 'new-one-b'), " +
		"(7, DATE '2026-01-03', 'new-three'), (8, NULL, 'new-null')")

	statement := sparkDynamicTimeOverwriteFixture(
		"test-project.analytics.destination", "test-project.analytics.temporary",
		"partition_date", "DATE_TRUNC", "DAY", []string{"id", "partition_date", "payload"},
	)
	request := ports.QueryRequest{ProjectID: "test-project", SQL: statement}
	operation, matched, err := analyzeV0442QueryOperation(ctx, warehouse, request)
	if err != nil || !matched {
		t.Fatalf("analyze operation: matched=%t err=%v", matched, err)
	}
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	if _, err := warehouse.ExecuteQueryOperation(ctx, request, operation, target, dynamicOverwriteCanonicalTable("temporary", "partition_date", "DATE", "DAY")); err != nil {
		t.Fatal(err)
	}
	logged := logs.String()
	for _, required := range []string{
		"begin_dynamic_overwrite_transaction", "delete_dynamic_partitions", "insert_dynamic_partition_source",
		"commit_dynamic_overwrite_transaction", "destination_reference_fingerprint", "source_reference_fingerprint",
		"destination_schema_fingerprint", "source_schema_fingerprint", "affected_rows", `"tx_state":"committed"`,
	} {
		if !strings.Contains(logged, required) {
			t.Fatalf("dynamic transaction log lacks %q: %s", required, logged)
		}
	}
	for _, raw := range []string{"test-project", "analytics", `"destination"`, `"temporary"`, "partition_date", "new-one-a"} {
		if strings.Contains(logged, raw) {
			t.Fatalf("dynamic transaction log exposed raw value %q: %s", raw, logged)
		}
	}
	assertDynamicOverwriteIDs(t, query, []int64{2, 4, 5, 6, 7, 8})

	query("INSERT INTO `test-project.analytics.invalid_temporary` VALUES " +
		"('not-an-int', DATE '2026-01-02', 'must-rollback')")
	invalidStatement := sparkDynamicTimeOverwriteFixture(
		"test-project.analytics.destination", "test-project.analytics.invalid_temporary",
		"partition_date", "DATE_TRUNC", "DAY", []string{"id", "partition_date", "payload"},
	)
	invalidRequest := ports.QueryRequest{ProjectID: "test-project", SQL: invalidStatement}
	invalidOperation, matched, err := analyzeV0442QueryOperation(ctx, warehouse, invalidRequest)
	if err != nil || !matched {
		t.Fatalf("analyze invalid operation: matched=%t err=%v", matched, err)
	}
	if _, err := warehouse.ExecuteQueryOperation(ctx, invalidRequest, invalidOperation, target, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "invalid_temporary",
		Schema: []domain.Field{{Name: "id", Type: "STRING"}, {Name: "partition_date", Type: "DATE"}, {Name: "payload", Type: "STRING"}},
	}); !errors.Is(err, domain.ErrInvalidQuery) {
		t.Fatalf("canonical source type drift = %v, want ErrInvalidQuery", err)
	}

	// Simulate physical/catalog adapter drift after canonical validation so the
	// insert fails after the delete. This pins rollback state/logging without
	// weakening the normal pre-transaction schema gate above.
	logs.Reset()
	if _, err := warehouse.ExecuteQueryOperation(ctx, invalidRequest, invalidOperation, target,
		dynamicOverwriteCanonicalTable("invalid_temporary", "partition_date", "DATE", "DAY")); !errors.Is(err, domain.ErrInvalidQuery) {
		t.Fatalf("physical source drift = %v, want ErrInvalidQuery", err)
	}
	rollbackLog := logs.String()
	for _, required := range []string{"rollback_dynamic_overwrite_transaction", `"tx_state":"rolled_back"`} {
		if !strings.Contains(rollbackLog, required) {
			t.Fatalf("rollback log lacks %q: %s", required, rollbackLog)
		}
	}
	assertDynamicOverwriteIDs(t, query, []int64{2, 4, 5, 6, 7, 8})
}

func TestSparkDynamicTimePartitionOverwriteExecutesTimestampAndDatetimeGranularities(t *testing.T) {
	tests := []struct {
		name         string
		fieldType    string
		granularity  string
		targetValues string
		sourceValues string
	}{
		{
			name: "TIMESTAMP uses UTC hour boundaries", fieldType: "TIMESTAMP", granularity: "HOUR",
			targetValues: "(1, TIMESTAMPTZ '2026-01-01 01:10:00+00', 'replace'), " +
				"(2, TIMESTAMPTZ '2026-01-01 02:00:00+00', 'keep')",
			sourceValues: "(3, TIMESTAMPTZ '2026-01-01 01:45:00+00', 'new')",
		},
		{
			name: "DATETIME uses month boundaries", fieldType: "DATETIME", granularity: "MONTH",
			targetValues: "(1, TIMESTAMP '2026-01-10 01:00:00', 'replace'), " +
				"(2, TIMESTAMP '2026-02-01 00:00:00', 'keep')",
			sourceValues: "(3, TIMESTAMP '2026-01-20 12:00:00', 'new')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := duckDBQueryTestContext(t)
			defer cancel()
			warehouse, err := New("")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = warehouse.Close() })
			if err := warehouse.CreateDataset(ctx, "test-project", "analytics"); err != nil {
				t.Fatal(err)
			}
			target := dynamicOverwriteCanonicalTable("destination", "event_time", test.fieldType, test.granularity)
			for _, table := range []domain.Table{
				target,
				dynamicOverwriteCanonicalTable("temporary", "event_time", test.fieldType, test.granularity),
			} {
				if err := warehouse.CreateTable(ctx, table); err != nil {
					t.Fatal(err)
				}
			}
			query := func(statement string) domain.QueryResult {
				t.Helper()
				result, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: statement})
				if err != nil {
					t.Fatalf("query failed: %v", err)
				}
				return result
			}
			query("INSERT INTO `test-project.analytics.destination` VALUES " + test.targetValues)
			query("INSERT INTO `test-project.analytics.temporary` VALUES " + test.sourceValues)
			statement := sparkDynamicTimeOverwriteFixture(
				"test-project.analytics.destination", "test-project.analytics.temporary",
				"event_time", "TIMESTAMP_TRUNC", test.granularity, []string{"id", "event_time", "payload"},
			)
			request := ports.QueryRequest{ProjectID: "test-project", SQL: statement}
			operation, matched, err := analyzeV0442QueryOperation(ctx, warehouse, request)
			if err != nil || !matched {
				t.Fatalf("analyze operation: matched=%t err=%v", matched, err)
			}
			if _, err := warehouse.ExecuteQueryOperation(ctx, request, operation, target, dynamicOverwriteCanonicalTable("temporary", "event_time", test.fieldType, test.granularity)); err != nil {
				t.Fatal(err)
			}
			assertDynamicOverwriteIDs(t, query, []int64{2, 3})
		})
	}
}

func assertDynamicOverwriteIDs(t *testing.T, query func(string) domain.QueryResult, want []int64) {
	t.Helper()
	result := query("SELECT id FROM `test-project.analytics.destination` ORDER BY id")
	got := make([]int64, len(result.Rows))
	for index, row := range result.Rows {
		got[index] = row[0].(int64)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("destination IDs = %v, want %v", got, want)
	}
}

func dynamicOverwriteCanonicalTable(tableID, partitionField, fieldType, granularity string) domain.Table {
	return domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: tableID,
		Schema: []domain.Field{
			{Name: "id", Type: "INT64"},
			{Name: partitionField, Type: fieldType},
			{Name: "payload", Type: "STRING"},
		},
		TimePartitioning: &domain.TimePartitioning{Type: granularity, Field: partitionField},
	}
}

func sparkDynamicTimeOverwriteFixture(destination, source, partitionField, function, granularity string, fields []string) string {
	destinationFields := "`" + strings.Join(fields, "`,`") + "`"
	sourceFields := make([]string, len(fields))
	for index, field := range fields {
		sourceFields[index] = fmt.Sprintf("`%s`.`%s`", testDynamicSourceAlias, field)
	}
	return fmt.Sprintf(
		"DECLARE partitions_to_delete DEFAULT (SELECT ARRAY_AGG(DISTINCT(%s(`%s`, %s)) IGNORE NULLS) FROM `%s`); \n"+
			"MERGE `%s` AS `%s`\n"+
			"USING `%s` AS `%s`\n"+
			"ON FALSE\n"+
			"WHEN NOT MATCHED BY SOURCE AND (TRUE) AND %s(`%s`.`%s`, %s) IN UNNEST(partitions_to_delete) THEN DELETE\n"+
			"WHEN NOT MATCHED BY TARGET THEN\n"+
			"INSERT(%s) VALUES(%s)",
		function, partitionField, granularity, source,
		destination, testDynamicTargetAlias, source, testDynamicSourceAlias,
		function, testDynamicTargetAlias, partitionField, granularity,
		destinationFields, strings.Join(sourceFields, ","),
	)
}

func sparkDynamicRangeOverwriteFixture() string {
	return "DECLARE partitions_to_delete DEFAULT " +
		"(SELECT ARRAY_AGG(DISTINCT(IFNULL(IF(partition_id >= 100, 0, RANGE_BUCKET(partition_id, GENERATE_ARRAY(0, 100, 10))), -1)) IGNORE NULLS) " +
		"FROM `test-project.analytics.temporary`); \n" +
		"MERGE `test-project.analytics.destination` AS `" + testDynamicTargetAlias + "`\n" +
		"USING `test-project.analytics.temporary` AS `" + testDynamicSourceAlias + "`\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED BY SOURCE AND (`" + testDynamicTargetAlias + "`.`partition_id` IS NULL OR `" + testDynamicTargetAlias + "`.`partition_id` >= -9223372036854775808) " +
		"AND IFNULL(IF(`" + testDynamicTargetAlias + "`.`partition_id` >= 100, 0, RANGE_BUCKET(`" + testDynamicTargetAlias + "`.`partition_id`, GENERATE_ARRAY(0, 100, 10))), -1) " +
		"IN UNNEST(partitions_to_delete) THEN DELETE\n" +
		"WHEN NOT MATCHED BY TARGET THEN\n" +
		"INSERT(`id`,`partition_id`,`payload`) VALUES(`" + testDynamicSourceAlias + "`.`id`,`" + testDynamicSourceAlias + "`.`partition_id`,`" + testDynamicSourceAlias + "`.`payload`)"
}

type unreachableConnectorFallback struct{}

func (unreachableConnectorFallback) AnalyzeQuery(context.Context, ports.QueryRequest) (ports.QueryAnalysis, error) {
	return ports.QueryAnalysis{}, errors.New("unexpected generic query fallback")
}

func parseSparkDynamicTimeOverwrite(request ports.QueryRequest) (ports.QueryOperation, bool, error) {
	analyzer, err := v0442.NewAnalyzer(unreachableConnectorFallback{})
	if err != nil {
		return ports.QueryOperation{}, false, err
	}
	return analyzer.AnalyzeQueryOperation(context.Background(), request)
}

func analyzeV0442QueryOperation(
	ctx context.Context,
	fallback ports.QueryAnalyzer,
	request ports.QueryRequest,
) (ports.QueryOperation, bool, error) {
	analyzer, err := v0442.NewAnalyzer(fallback)
	if err != nil {
		return ports.QueryOperation{}, false, err
	}
	return analyzer.AnalyzeQueryOperation(ctx, request)
}
