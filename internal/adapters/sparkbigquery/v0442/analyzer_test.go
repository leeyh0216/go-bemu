package v0442

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestAnalyzerRecognizesPinnedStaticProfileAndVerifiesRequest(t *testing.T) {
	fallback := &recordingFallback{}
	analyzer, err := NewAnalyzer(fallback)
	if err != nil {
		t.Fatal(err)
	}
	if err := analyzer.WithGoogleSQLGateway(connectorGateway(t)); err != nil {
		t.Fatal(err)
	}
	request := ports.QueryRequest{
		ProjectID: "test-project", DefaultProjectID: "data-project", DefaultDataset: "analytics",
		SQL: staticOverwriteFixture("test-project.analytics.destination", "test-project.analytics.temporary"),
	}
	operation, matched, err := analyzer.AnalyzeQueryOperation(t.Context(), request)
	if err != nil || !matched {
		t.Fatalf("static analysis matched=%t err=%v", matched, err)
	}
	if operation.Kind() != ports.QueryOperationSparkStaticOverwrite || operation.ProfileID() != StaticOverwriteProfile ||
		operation.Destination().TableID != "destination" || operation.Source().TableID != "temporary" {
		t.Fatalf("static semantic operation = %#v", operation)
	}
	if err := analyzer.VerifyQueryOperation(request, operation); err != nil {
		t.Fatalf("verify pinned operation: %v", err)
	}
	forged, err := ports.NewQueryOperation(ports.QueryOperationDescriptor{
		Kind: ports.QueryOperationSparkStaticOverwrite, ProfileID: StaticOverwriteProfile,
		Destination: domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "other_destination"},
		Source:      operation.Source(), Request: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := analyzer.VerifyQueryOperation(request, forged); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("forged semantic payload verification = %v", err)
	}
	changed := request
	changed.DefaultDataset = "changed_dataset"
	if err := analyzer.VerifyQueryOperation(changed, operation); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("changed request verification = %v", err)
	}
	if SourceCommit != "719817782a214b8ca72be520870013a3e0253d92" {
		t.Fatalf("source commit = %q", SourceCommit)
	}

	drifted := strings.Replace(request.SQL, "WHEN NOT MATCHED BY SOURCE", "WHEN MATCHED", 1)
	_, matched, err = analyzer.AnalyzeQueryOperation(t.Context(), ports.QueryRequest{ProjectID: "test-project", SQL: drifted})
	if err == nil || !matched || !strings.Contains(err.Error(), StaticOverwriteProfile) {
		t.Fatalf("profile drift matched=%t err=%v", matched, err)
	}
}

func TestAnalyzerBuildsDynamicOperationAndDelegatesOrdinaryQueries(t *testing.T) {
	fallback := &recordingFallback{analysis: ports.QueryAnalysis{ProducesRows: true}}
	analyzer, err := NewAnalyzer(fallback)
	if err != nil {
		t.Fatal(err)
	}
	if err := analyzer.WithGoogleSQLGateway(connectorGateway(t)); err != nil {
		t.Fatal(err)
	}
	request := ports.QueryRequest{
		ProjectID: "test-project",
		SQL: dynamicOverwriteFixture(
			"test-project.analytics.destination", "test-project.analytics.temporary",
			"event_time", "DATE_TRUNC", "DAY", []string{"id", "event_time", "payload"},
		),
	}
	operation, matched, err := analyzer.AnalyzeQueryOperation(t.Context(), request)
	if err != nil || !matched {
		t.Fatalf("dynamic analysis matched=%t err=%v", matched, err)
	}
	if operation.Kind() != ports.QueryOperationSparkDynamicTimeOverwrite ||
		operation.ProfileID() != DynamicTimeOverwriteProfile || operation.PartitionFunction() != "DATE_TRUNC" ||
		operation.PartitionField() != "event_time" || operation.Granularity() != "DAY" ||
		strings.Join(operation.InsertFields(), ",") != "id,event_time,payload" {
		t.Fatalf("dynamic semantic operation = %#v", operation)
	}
	analysis, err := analyzer.AnalyzeQuery(t.Context(), ports.QueryRequest{ProjectID: "test-project", SQL: "SELECT 1"})
	if err != nil || !analysis.ProducesRows || fallback.calls != 1 {
		t.Fatalf("fallback analysis=%#v calls=%d err=%v", analysis, fallback.calls, err)
	}
}

func TestAnalyzerMatchesConnectorProfilesFromAnalyzedGoogleSQL(t *testing.T) {
	analyzer, err := NewAnalyzer(&recordingFallback{})
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name string
		sql  string
		kind ports.QueryOperationKind
	}{
		{name: "static", sql: staticOverwriteFixture("test-project.analytics.destination", "test-project.analytics.temporary"), kind: ports.QueryOperationSparkStaticOverwrite},
		{name: "dynamic", sql: dynamicOverwriteFixture("test-project.analytics.destination", "test-project.analytics.temporary", "event_time", "DATE_TRUNC", "DAY", []string{"id", "event_time", "payload"}), kind: ports.QueryOperationSparkDynamicTimeOverwrite},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := ports.QueryRequest{ProjectID: "test-project", DefaultDataset: "analytics", SQL: testCase.sql}
			gateway := connectorGateway(t)
			statement, err := gateway.Analyze(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			operation, matched, err := analyzer.AnalyzeStatementOperation(t.Context(), statement, request)
			if err != nil || !matched || operation.Kind() != testCase.kind {
				t.Fatalf("analyzed operation = (%#v, %t, %v)", operation, matched, err)
			}
			if err := analyzer.WithGoogleSQLGateway(gateway); err != nil {
				t.Fatal(err)
			}
			operation, matched, err = analyzer.AnalyzeQueryOperation(t.Context(), request)
			if err != nil || !matched || operation.Kind() != testCase.kind {
				t.Fatalf("gateway-backed operation = (%#v, %t, %v)", operation, matched, err)
			}
		})
	}
}

func connectorGateway(t *testing.T) ports.GoogleSQLGateway {
	t.Helper()
	gateway, err := googlesqladapter.NewGateway(connectorAnalyzerCatalog{})
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

type connectorAnalyzerCatalog struct{}

func (connectorAnalyzerCatalog) GoogleSQLCatalogSnapshot(context.Context) (ports.GoogleSQLCatalogSnapshot, error) {
	fields := []domain.Field{{Name: "id", Type: "INT64"}, {Name: "event_time", Type: "TIMESTAMP"}, {Name: "payload", Type: "STRING"}}
	return ports.GoogleSQLCatalogSnapshot{Projects: []ports.GoogleSQLProjectSnapshot{{
		Project: domain.Project{ID: "test-project"},
		Datasets: []ports.GoogleSQLDatasetSnapshot{{
			Dataset: domain.Dataset{ProjectID: "test-project", ID: "analytics"},
			Tables: []domain.Table{
				{ProjectID: "test-project", DatasetID: "analytics", ID: "destination", Type: "TABLE", Schema: fields},
				{ProjectID: "test-project", DatasetID: "analytics", ID: "temporary", Type: "TABLE", Schema: fields},
			},
		}},
	}}}, nil
}

func TestAnalyzerRequiresFallback(t *testing.T) {
	if _, err := NewAnalyzer(nil); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("nil fallback error = %v", err)
	}
	var typedNil *recordingFallback
	if _, err := NewAnalyzer(typedNil); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("typed nil fallback error = %v", err)
	}
}

type recordingFallback struct {
	analysis ports.QueryAnalysis
	calls    int
}

func (fallback *recordingFallback) AnalyzeQuery(context.Context, ports.QueryRequest) (ports.QueryAnalysis, error) {
	fallback.calls++
	return fallback.analysis, nil
}

func staticOverwriteFixture(destination, source string) string {
	return "MERGE `" + destination + "`\n" +
		"USING (SELECT * FROM `" + source + "`)\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED THEN INSERT ROW\n" +
		"WHEN NOT MATCHED BY SOURCE THEN DELETE"
}

func dynamicOverwriteFixture(
	destination, source, partitionField, function, granularity string,
	fields []string,
) string {
	const targetAlias = "__target_0123456789abcdef0123456789abcdef"
	const sourceAlias = "__source_fedcba9876543210fedcba9876543210"
	destinationFields := "`" + strings.Join(fields, "`,`") + "`"
	sourceFields := make([]string, len(fields))
	for index, field := range fields {
		sourceFields[index] = fmt.Sprintf("`%s`.`%s`", sourceAlias, field)
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
		destination, targetAlias, source, sourceAlias,
		function, targetAlias, partitionField, granularity,
		destinationFields, strings.Join(sourceFields, ","),
	)
}
