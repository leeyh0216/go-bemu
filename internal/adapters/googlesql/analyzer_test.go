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

func TestAnalyzerResolvesPublicBetweenPredicate(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	statement, err := gateway.Analyze(t.Context(), ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics",
		SQL: "SELECT id FROM analytics.events WHERE id BETWEEN 2 AND 4",
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	where := statement.Syntax().(*queryast.SelectStatement).Query().Body().(*queryast.SelectQuery).Where()
	between, ok := where.(*queryast.BetweenExpression)
	if !ok || between.Not() {
		t.Fatalf("WHERE expression = %#v", where)
	}
	expressions, err := queryast.Expressions(statement.Syntax())
	if err != nil {
		t.Fatal(err)
	}
	if len(expressions) != 5 || !statement.ExpressionsComplete() {
		t.Fatalf("BETWEEN bindings incomplete: expressions=%d complete=%t", len(expressions), statement.ExpressionsComplete())
	}
	typ, err := statement.RequireExpressionType(between.NodeKey())
	if err != nil {
		t.Fatal(err)
	}
	if typ.Kind() != semantic.TypeBool {
		t.Fatalf("BETWEEN type = %q, want BOOL", typ.Kind())
	}
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
		{name: "alter type", sql: "ALTER TABLE analytics.events ALTER COLUMN id SET DATA TYPE STRING", kind: queryast.StatementAlterTable, target: true},
		{name: "drop", sql: "DROP TABLE analytics.events", kind: queryast.StatementDropTable, target: true},
		{name: "create view", sql: "CREATE VIEW analytics.event_ids AS SELECT id FROM analytics.events", kind: queryast.StatementCreateView, target: true},
		{name: "drop view", sql: "DROP VIEW analytics.events", kind: queryast.StatementDropView, target: true},
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

func TestGatewayResolvesBacktickQualifiedPaths(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	tests := []struct {
		name string
		sql  string
		kind queryast.StatementKind
	}{
		{name: "select", sql: "SELECT id FROM `test-project.analytics.events`", kind: queryast.StatementSelect},
		{name: "insert", sql: "INSERT INTO `test-project.analytics.events` (id) VALUES (1)", kind: queryast.StatementInsert},
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
				t.Fatalf("kind = %q", statement.Kind())
			}
			references := statement.ReferencedTables()
			if len(references) != 1 || references[0] != (domain.TableReference{
				ProjectID: "test-project", DatasetID: "analytics", TableID: "events",
			}) {
				t.Fatalf("references = %#v", references)
			}
		})
	}
}

func TestGatewayUsesOneEntrypointForUpdateDeleteAndMerge(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	tests := []struct {
		name string
		sql  string
		kind queryast.StatementKind
	}{
		{
			name: "update",
			sql:  "UPDATE analytics.events SET amount = NUMERIC '1.25' WHERE id = 1",
			kind: queryast.StatementUpdate,
		},
		{
			name: "delete",
			sql:  "DELETE FROM analytics.events WHERE id = 1",
			kind: queryast.StatementDelete,
		},
		{
			name: "merge",
			sql:  "MERGE analytics.events AS T USING analytics.events AS S ON T.id = S.id WHEN MATCHED THEN UPDATE SET amount = S.amount",
			kind: queryast.StatementMerge,
		},
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
			if len(statement.MutationTargets()) != 1 {
				t.Fatalf("mutation targets = %#v", statement.MutationTargets())
			}
			relations, err := queryast.Relations(statement.Syntax())
			if err != nil || len(relations) == 0 {
				t.Fatalf("syntax relations = (%#v, %v)", relations, err)
			}
			for _, relation := range relations {
				if _, err := statement.RequireRelationBinding(relation.NodeKey()); err != nil {
					t.Fatalf("RequireRelationBinding() error = %v", err)
				}
			}
		})
	}
}

func TestGatewayAnalyzesDeclareMergeScript(t *testing.T) {
	snapshot := analyzerSnapshot()
	fields := []domain.Field{
		{Name: "id", Type: "INT64", Mode: "NULLABLE"},
		{Name: "partition_date", Type: "DATE", Mode: "NULLABLE"},
		{Name: "payload", Type: "STRING", Mode: "NULLABLE"},
	}
	for _, tableID := range []string{"temporary", "destination"} {
		snapshot.Projects[0].Datasets[0].Tables = append(snapshot.Projects[0].Datasets[0].Tables, domain.Table{
			ProjectID: "test-project", DatasetID: "analytics", ID: tableID, Type: "TABLE",
			Schema: domain.CloneFields(fields),
		})
	}
	gateway := newGateway(t, &snapshotReader{snapshot: snapshot})
	sql := "DECLARE partitions_to_delete DEFAULT " +
		"(SELECT ARRAY_AGG(DISTINCT(date_trunc(`partition_date`, DAY)) IGNORE NULLS) " +
		"FROM `test-project.analytics.temporary`); " +
		"MERGE `test-project.analytics.destination` AS `target` " +
		"USING `test-project.analytics.temporary` AS `source` ON FALSE " +
		"WHEN NOT MATCHED BY SOURCE AND (TRUE) AND date_trunc(`target`.`partition_date`, DAY) " +
		"IN UNNEST(partitions_to_delete) THEN DELETE " +
		"WHEN NOT MATCHED BY TARGET THEN INSERT(`id`,`partition_date`,`payload`) " +
		"VALUES(`source`.`id`,`source`.`partition_date`,`source`.`payload`)"
	statement, err := gateway.Analyze(t.Context(), ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics", SQL: sql,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if statement.Kind() != queryast.StatementScript || len(statement.AnalysisFingerprint()) != 64 {
		t.Fatalf("script analysis identity is invalid")
	}
	targets := statement.MutationTargets()
	if len(targets) != 1 || targets[0].TableID != "destination" {
		t.Fatalf("mutation targets = %#v", targets)
	}
	references := statement.ReferencedTables()
	if len(references) != 2 || references[0].TableID != "destination" || references[1].TableID != "temporary" {
		t.Fatalf("references = %#v", references)
	}
	relations, err := queryast.Relations(statement.Syntax())
	if err != nil || len(relations) != 3 {
		t.Fatalf("relations = (%#v, %v)", relations, err)
	}
	for _, relation := range relations {
		if _, err := statement.RequireRelationBinding(relation.NodeKey()); err != nil {
			t.Fatalf("RequireRelationBinding() error = %v", err)
		}
	}
	script := statement.Syntax().(*queryast.ScriptStatement)
	declaration := script.Statements()[0].(*queryast.DeclareStatement)
	variableType, err := statement.RequireExpressionType(declaration.DefaultValue().NodeKey())
	if err != nil {
		t.Fatalf("RequireExpressionType() error = %v", err)
	}
	element, ok := variableType.Element()
	if variableType.Kind() != semantic.TypeArray || !ok || element.Kind() != semantic.TypeDate {
		t.Fatalf("declared variable type = %#v", variableType)
	}
}

func TestGatewayCarriesDeclaredVariableTypeThroughSetAndMerge(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	sql := "DECLARE match_id INT64 DEFAULT 1; " +
		"SET match_id = match_id + 1; " +
		"MERGE analytics.events AS target USING analytics.events AS source " +
		"ON target.id = match_id WHEN MATCHED THEN DELETE"
	statement, err := gateway.Analyze(t.Context(), ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics", SQL: sql,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if statement.Kind() != queryast.StatementScript || len(statement.MutationTargets()) != 1 {
		t.Fatalf("script statement = %#v", statement)
	}
	script := statement.Syntax().(*queryast.ScriptStatement)
	if len(script.Statements()) != 3 || script.Statements()[1].Kind() != queryast.StatementSet {
		t.Fatalf("script syntax = %#v", script.Statements())
	}
	set := script.Statements()[1].(*queryast.SetStatement)
	typ, err := statement.RequireExpressionType(set.Value().NodeKey())
	if err != nil || typ.Kind() != semantic.TypeInt64 {
		t.Fatalf("SET expression type = (%#v, %v)", typ, err)
	}
	expressions, err := queryast.Expressions(statement.Syntax())
	if err != nil {
		t.Fatalf("Expressions() error = %v", err)
	}
	variableReferences := 0
	for _, expression := range expressions {
		identifier, ok := expression.(*queryast.IdentifierExpression)
		if !ok || len(identifier.Path().Segments()) != 1 || !strings.EqualFold(identifier.Path().Segments()[0], "match_id") {
			continue
		}
		binding, found := statement.SymbolBinding(identifier.NodeKey())
		if !found || binding.Kind() != semantic.SymbolScriptVariable || !strings.EqualFold(binding.Name(), "match_id") {
			t.Fatalf("script variable binding = (%#v, %t)", binding, found)
		}
		variableReferences++
	}
	if variableReferences != 2 {
		t.Fatalf("script variable reference count = %d", variableReferences)
	}
}

func TestGatewayPreservesColumnPrecedenceOverScriptVariables(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	tests := []struct {
		name       string
		sql        string
		symbolName string
		kind       semantic.SymbolBindingKind
		count      int
	}{
		{
			name:       "column shadows variable",
			sql:        "DECLARE id INT64 DEFAULT 10; SELECT id + id FROM analytics.events",
			symbolName: "id",
			kind:       semantic.SymbolColumn,
			count:      2,
		},
		{
			name:       "unshadowed variable",
			sql:        "DECLARE x INT64 DEFAULT 10; SELECT id + x FROM analytics.events",
			symbolName: "x",
			kind:       semantic.SymbolScriptVariable,
			count:      1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := gateway.Analyze(t.Context(), ports.QueryRequest{
				ProjectID: "test-project", DefaultDataset: "analytics", SQL: test.sql,
			})
			if err != nil {
				t.Fatalf("Analyze() error = %v", err)
			}
			expressions, err := queryast.Expressions(statement.Syntax())
			if err != nil {
				t.Fatalf("Expressions() error = %v", err)
			}
			matched := 0
			for _, expression := range expressions {
				identifier, ok := expression.(*queryast.IdentifierExpression)
				if !ok || len(identifier.Path().Segments()) != 1 || !strings.EqualFold(identifier.Path().Segments()[0], test.symbolName) {
					continue
				}
				binding, found := statement.SymbolBinding(identifier.NodeKey())
				if !found || binding.Kind() != test.kind {
					t.Fatalf("symbol binding = (%#v, %t), want %s", binding, found, test.kind)
				}
				matched++
			}
			if matched != test.count {
				t.Fatalf("matching bindings = %d, want %d", matched, test.count)
			}
		})
	}
}

func TestAnalyzerRetainsStableResolutionErrorsAndInput(t *testing.T) {
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
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.secret)) || !strings.Contains(err.Error(), test.sql) {
				t.Fatalf("error omitted submitted content: %v", err)
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
		{name: "wide decimal", field: decimalField("secret", "BIGNUMERIC", pointer(39), pointer(18), ""), code: domain.CapabilityDecimalPrecision38V1},
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
			if !strings.Contains(err.Error(), "secret") {
				t.Fatalf("error omitted field name: %v", err)
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

func TestGatewayAnalyzesGeneralMultiStatementScript(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	statement, err := gateway.Analyze(t.Context(), ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics",
		SQL: "UPDATE analytics.events SET amount = NUMERIC '2.5' WHERE id = 1; " +
			"SELECT id, amount FROM analytics.events WHERE id = 1",
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if statement.Kind() != queryast.StatementScript || len(statement.OutputColumns()) != 2 {
		t.Fatalf("script result contract = kind %s, columns %#v", statement.Kind(), statement.OutputColumns())
	}
	if len(statement.MutationTargets()) != 1 || len(statement.ReferencedTables()) != 1 {
		t.Fatalf("script table bindings = targets %#v, references %#v", statement.MutationTargets(), statement.ReferencedTables())
	}
}

func TestGatewayRejectsCTASBeforeEngine(t *testing.T) {
	gateway := newGateway(t, &snapshotReader{snapshot: analyzerSnapshot()})
	tests := []struct {
		name string
		sql  string
		code string
	}{
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
