package googlesql_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	analyzeradapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

func TestAnalyzerResolvesCanonicalRecursiveSchema(t *testing.T) {
	repository := &snapshotReader{snapshot: analyzerSnapshot()}
	gateway := newGateway(t, repository)
	statement, err := gateway.Analyze(t.Context(), ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics",
		SQL: "SELECT id, amount, wide, payload, labels FROM analytics.events",
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if statement.Kind() != queryast.StatementSelect {
		t.Fatalf("kind = %q", statement.Kind())
	}
	if repository.calls != 1 {
		t.Fatalf("snapshot calls = %d, want 1", repository.calls)
	}
	references := statement.ReferencedTables()
	if len(references) != 1 || references[0] != (domain.TableReference{
		ProjectID: "test-project", DatasetID: "analytics", TableID: "events",
	}) {
		t.Fatalf("references = %#v", references)
	}
	relations, err := queryast.Relations(statement.Syntax())
	if err != nil || len(relations) != 1 {
		t.Fatalf("syntax relations = (%#v, %v)", relations, err)
	}
	binding, err := statement.RequireRelationBinding(relations[0].NodeKey())
	if err != nil {
		t.Fatalf("RequireRelationBinding() error = %v", err)
	}
	if reference, physical := binding.Reference(); !physical || reference != references[0] {
		t.Fatalf("relation binding = (%#v, %t)", reference, physical)
	}
	columns := statement.OutputColumns()
	if len(columns) != 5 {
		t.Fatalf("output columns = %#v", columns)
	}
	assertDecimalType(t, columns[1].Type(), semantic.TypeNumeric, false, false, 38, 9, "")
	assertDecimalType(t, columns[2].Type(), semantic.TypeBigNumeric, true, false, 38, 0, domain.RoundingModeHalfEven)
	if columns[3].Type().Kind() != semantic.TypeStruct {
		t.Fatalf("payload type = %q", columns[3].Type().Kind())
	}
	fields := columns[3].Type().Fields()
	if len(fields) != 2 || fields[1].Name() != "scores" || fields[1].Type().Kind() != semantic.TypeArray {
		t.Fatalf("payload fields = %#v", fields)
	}
	element, ok := fields[1].Type().Element()
	if !ok || element.Kind() != semantic.TypeInt64 {
		t.Fatalf("scores element = %#v", element)
	}
	if columns[4].Type().Kind() != semantic.TypeArray {
		t.Fatalf("labels type = %q", columns[4].Type().Kind())
	}
	expressions, err := queryast.Expressions(statement.Syntax())
	if err != nil {
		t.Fatal(err)
	}
	if !statement.ExpressionsComplete() || len(expressions) != 5 {
		t.Fatalf("expression bindings are incomplete")
	}
	amountType, err := statement.RequireExpressionType(expressions[1].NodeKey())
	if err != nil {
		t.Fatalf("RequireExpressionType() error = %v", err)
	}
	assertDecimalType(t, amountType, semantic.TypeNumeric, false, false, 38, 9, "")
}

func TestGatewayUsesOneEntrypointForInsertAndDDL(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	tests := []struct {
		name   string
		sql    string
		kind   queryast.StatementKind
		target bool
	}{
		{name: "insert", sql: "INSERT INTO analytics.events (id) VALUES (1)", kind: queryast.StatementInsert, target: true},
		{name: "create", sql: "CREATE TABLE analytics.created (id INT64, amount NUMERIC(12))", kind: queryast.StatementCreateTable, target: true},
		{name: "alter", sql: "ALTER TABLE analytics.events ADD COLUMN extra STRING", kind: queryast.StatementAlterTable, target: true},
		{name: "drop", sql: "DROP TABLE analytics.events", kind: queryast.StatementDropTable, target: true},
		{name: "truncate", sql: "TRUNCATE TABLE analytics.events", kind: queryast.StatementTruncateTable, target: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := gateway.Analyze(t.Context(), ports.QueryRequest{
				ProjectID: "test-project", DefaultDataset: "analytics", SQL: test.sql,
			})
			if err != nil {
				t.Fatalf("Analyze() error = %v", err)
			}
			if statement.Kind() != test.kind {
				t.Fatalf("kind = %q, want %q", statement.Kind(), test.kind)
			}
			if test.target && len(statement.MutationTargets()) != 1 {
				t.Fatalf("mutation targets = %#v", statement.MutationTargets())
			}
		})
	}
}

func TestAnalyzerRedactsStableResolutionErrors(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	tests := []struct {
		name   string
		sql    string
		secret string
		kind   error
		code   string
	}{
		{name: "table", sql: "SELECT * FROM analytics.customer_secret", secret: "customer_secret", kind: domain.ErrNotFound, code: analyzeradapter.ErrorTableNotFoundV1},
		{name: "column", sql: "SELECT customer_secret FROM analytics.events", secret: "customer_secret", kind: domain.ErrInvalidQuery, code: analyzeradapter.ErrorColumnNotFoundV1},
		{name: "type", sql: "CREATE TABLE analytics.created (value CUSTOMER_SECRET)", secret: "CUSTOMER_SECRET", kind: domain.ErrInvalidQuery, code: analyzeradapter.ErrorTypeNotFoundV1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := gateway.Analyze(t.Context(), ports.QueryRequest{
				ProjectID: "test-project", DefaultDataset: "analytics", SQL: test.sql,
			})
			if !errors.Is(err, test.kind) {
				t.Fatalf("error = %v, want %v", err, test.kind)
			}
			if !strings.Contains(err.Error(), test.code) {
				t.Fatalf("error = %v, want code %s", err, test.code)
			}
			if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.secret)) || strings.Contains(err.Error(), test.sql) {
				t.Fatalf("error leaked submitted content: %v", err)
			}
		})
	}
}

func TestAnalyzerRejectsUnsupportedCanonicalTypesBeforeAnalysis(t *testing.T) {
	tests := []struct {
		name  string
		field domain.Field
		code  string
	}{
		{name: "geography", field: domain.Field{Name: "secret", Type: "GEOGRAPHY", Mode: "NULLABLE"}, code: domain.GapGeographyUnsupportedV1},
		{name: "wide decimal", field: decimalField("secret", "BIGNUMERIC", pointer(39), pointer(18), ""), code: domain.CapabilitySparkDecimal38V1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := analyzerSnapshot()
			snapshot.Projects[0].Datasets[0].Tables[0].Schema = []domain.Field{test.field}
			gateway := newGateway(t, &snapshotReader{snapshot: snapshot})
			_, err := gateway.Analyze(t.Context(), ports.QueryRequest{
				ProjectID: "test-project", DefaultDataset: "analytics", SQL: "SELECT secret FROM analytics.events",
			})
			if !errors.Is(err, domain.ErrUnsupported) {
				t.Fatalf("error = %v", err)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaked field name: %v", err)
			}
			if !strings.Contains(err.Error(), test.code) {
				t.Fatalf("error = %v, want capability %s", err, test.code)
			}
		})
	}
}

func TestGatewayRejectsTypedNilCatalogReader(t *testing.T) {
	var reader *snapshotReader
	_, err := analyzeradapter.NewGateway(reader)
	if !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("NewGateway() error = %v", err)
	}
}

func TestGatewayRejectsScriptsAndCTASBeforeEngine(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	tests := []struct {
		name string
		sql  string
		code string
	}{
		{
			name: "multi statement script",
			sql:  "SELECT id FROM analytics.events; SELECT id FROM analytics.events",
			code: domain.GapQueryScriptsUnsupportedV1,
		},
		{
			name: "create table as select",
			sql:  "CREATE TABLE analytics.secret_copy AS SELECT id FROM analytics.events",
			code: "query.google-sql-ast-v1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := gateway.Analyze(t.Context(), ports.QueryRequest{
				ProjectID: "test-project", DefaultDataset: "analytics", SQL: test.sql,
			})
			if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), test.code) {
				t.Fatalf("Analyze() error = %v", err)
			}
			if strings.Contains(err.Error(), "secret_copy") || strings.Contains(err.Error(), test.sql) {
				t.Fatalf("error leaked submitted content: %v", err)
			}
		})
	}
}

func TestGatewayAssignsBigQueryVisibleAnonymousOutputNames(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	statement, err := gateway.Analyze(t.Context(), ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics", SQL: "SELECT 1, id FROM analytics.events",
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	columns := statement.OutputColumns()
	if len(columns) != 2 || columns[0].Name() != "f0_" || columns[1].Name() != "id" {
		t.Fatalf("output columns = %#v", columns)
	}
}

type snapshotReader struct {
	snapshot ports.GoogleSQLCatalogSnapshot
	calls    int
}

func (reader *snapshotReader) GoogleSQLCatalogSnapshot(context.Context) (ports.GoogleSQLCatalogSnapshot, error) {
	reader.calls++
	return reader.snapshot, nil
}

func newGateway(t *testing.T, reader ports.GoogleSQLCatalogReader) *analyzeradapter.Gateway {
	t.Helper()
	gateway, err := analyzeradapter.NewGateway(reader)
	if err != nil {
		t.Fatalf("NewGateway() error = %v", err)
	}
	return gateway
}

func analyzerSnapshot() ports.GoogleSQLCatalogSnapshot {
	precision := int64(38)
	return ports.GoogleSQLCatalogSnapshot{Projects: []ports.GoogleSQLProjectSnapshot{{
		Project: domain.Project{ID: "test-project"},
		Datasets: []ports.GoogleSQLDatasetSnapshot{{
			Dataset: domain.Dataset{ProjectID: "test-project", ID: "analytics"},
			Tables: []domain.Table{{
				ProjectID: "test-project", DatasetID: "analytics", ID: "events", Type: "TABLE",
				Schema: []domain.Field{
					{Name: "id", Type: "INT64", Mode: "REQUIRED"},
					decimalField("amount", "NUMERIC", nil, nil, ""),
					decimalField("wide", "BIGNUMERIC", &precision, nil, domain.RoundingModeHalfEven),
					{Name: "payload", Type: "STRUCT", Mode: "NULLABLE", Fields: []domain.Field{
						{Name: "name", Type: "STRING", Mode: "NULLABLE"},
						{Name: "scores", Type: "INT64", Mode: "REPEATED"},
					}},
					{Name: "labels", Type: "STRING", Mode: "REPEATED"},
				},
			}},
		}},
	}}}
}

func decimalField(name, typ string, precision, scale *int64, rounding domain.RoundingMode) domain.Field {
	return domain.Field{
		Name: name, Type: typ, Mode: "NULLABLE",
		Precision: precision, Scale: scale, RoundingMode: rounding,
	}
}

func pointer(value int64) *int64 { return &value }

func assertDecimalType(
	t *testing.T,
	typ semantic.Type,
	kind semantic.TypeKind,
	hasPrecision, hasScale bool,
	effectivePrecision, effectiveScale int64,
	rounding domain.RoundingMode,
) {
	t.Helper()
	if typ.Kind() != kind {
		t.Fatalf("type kind = %q, want %q", typ.Kind(), kind)
	}
	if _, present := typ.Precision(); present != hasPrecision {
		t.Fatalf("precision present = %t, want %t", present, hasPrecision)
	}
	if _, present := typ.Scale(); present != hasScale {
		t.Fatalf("scale present = %t, want %t", present, hasScale)
	}
	parameters, ok := typ.EffectiveDecimalParameters()
	if !ok || parameters.Precision != effectivePrecision || parameters.Scale != effectiveScale {
		t.Fatalf("effective parameters = %#v", parameters)
	}
	if typ.RoundingMode() != rounding {
		t.Fatalf("rounding mode = %q, want %q", typ.RoundingMode(), rounding)
	}
}
