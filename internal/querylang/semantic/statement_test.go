package semantic_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

func TestStatementOwnsRecursiveTypesAndCanonicalReferences(t *testing.T) {
	precision, scale := int64(38), int64(18)
	numeric := semantic.TypeDescriptor{
		Kind: semantic.TypeBigNumeric, Precision: &precision, Scale: &scale,
		RoundingMode: domain.RoundingModeHalfEven,
	}
	payload, err := semantic.NewType(semantic.TypeDescriptor{
		Kind: semantic.TypeStruct,
		Fields: []semantic.FieldDescriptor{{
			Name: "amounts",
			Type: semantic.TypeDescriptor{Kind: semantic.TypeArray, Element: &numeric},
		}},
	})
	if err != nil {
		t.Fatalf("NewType() error = %v", err)
	}
	digest := strings.Repeat("a", 64)
	source := mustSource(t, digest, 0, 100)
	tableZ1 := mustTable(t, digest, 1, 2, 0, "z")
	tableA := mustTable(t, digest, 3, 4, 0, "a")
	tableZ2 := mustTable(t, digest, 5, 6, 0, "z")
	deleteZ1, err := ast.NewDeleteStatement(source, tableZ1, nil)
	if err != nil {
		t.Fatalf("NewDeleteStatement() error = %v", err)
	}
	deleteA, err := ast.NewDeleteStatement(source, tableA, nil)
	if err != nil {
		t.Fatalf("NewDeleteStatement() error = %v", err)
	}
	deleteZ2, err := ast.NewDeleteStatement(source, tableZ2, nil)
	if err != nil {
		t.Fatalf("NewDeleteStatement() error = %v", err)
	}
	script, err := ast.NewScriptStatement(source, []ast.Statement{deleteZ1, deleteA, deleteZ2})
	if err != nil {
		t.Fatalf("NewScriptStatement() error = %v", err)
	}
	referenceZ := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "z"}
	referenceA := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "a"}
	descriptor := semantic.StatementDescriptor{
		Syntax:       script,
		ResolvedKind: ast.StatementScript,
		RelationBindings: []semantic.RelationBindingDescriptor{
			{Key: tableZ1.NodeKey(), Kind: semantic.RelationPhysical, Reference: referenceZ},
			{Key: tableA.NodeKey(), Kind: semantic.RelationPhysical, Reference: referenceA},
			{Key: tableZ2.NodeKey(), Kind: semantic.RelationPhysical, Reference: referenceZ},
		},
		OutputColumns: []semantic.ColumnDescriptor{{Name: "payload", Type: payload}},
	}
	statement, err := semantic.NewStatement(descriptor)
	if err != nil {
		t.Fatalf("NewStatement() error = %v", err)
	}

	descriptor.RelationBindings[0].Reference.TableID = "changed"
	references := statement.ReferencedTables()
	if len(references) != 2 || references[0].TableID != "a" || references[1].TableID != "z" {
		t.Fatalf("references = %#v", references)
	}
	if statement.Kind() != ast.StatementScript || statement.Syntax() != script || !statement.RelationsComplete() {
		t.Fatalf("statement contract was not preserved")
	}
	if len(statement.AnalysisFingerprint()) != 64 {
		t.Fatalf("analysis fingerprint = %q", statement.AnalysisFingerprint())
	}
	columns := statement.OutputColumns()
	fields := columns[0].Type().Fields()
	element, ok := fields[0].Type().Element()
	if !ok || element.Kind() != semantic.TypeBigNumeric {
		t.Fatalf("recursive output type = %#v", columns[0].Type())
	}
	if got, present := element.Precision(); !present || got != 38 {
		t.Fatalf("precision = (%d, %t)", got, present)
	}
	if element.RoundingMode() != domain.RoundingModeHalfEven {
		t.Fatalf("rounding mode = %q", element.RoundingMode())
	}
}

func TestStatementRejectsIncompleteRelationAndExpressionBindings(t *testing.T) {
	digest := strings.Repeat("b", 64)
	source := mustSource(t, digest, 0, 20)
	table := mustTable(t, digest, 7, 12, 0, "events")
	key := mustNodeKey(t, digest, 13, 17, "BOOLEAN_LITERAL", 0)
	predicate, err := ast.NewBooleanLiteral(key, true)
	if err != nil {
		t.Fatalf("NewBooleanLiteral() error = %v", err)
	}
	statement, err := ast.NewDeleteStatement(source, table, predicate)
	if err != nil {
		t.Fatalf("NewDeleteStatement() error = %v", err)
	}
	reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}

	_, err = semantic.NewStatement(semantic.StatementDescriptor{Syntax: statement, ResolvedKind: ast.StatementDelete})
	if !errors.Is(err, domain.ErrPrecondition) || !strings.Contains(err.Error(), semantic.ErrorRelationBindingInvalidV1) {
		t.Fatalf("missing relation error = %v", err)
	}

	boolType, err := semantic.NewType(semantic.TypeDescriptor{Kind: semantic.TypeBool})
	if err != nil {
		t.Fatalf("NewType() error = %v", err)
	}
	descriptor := semantic.StatementDescriptor{
		Syntax:       statement,
		ResolvedKind: ast.StatementDelete,
		RelationBindings: []semantic.RelationBindingDescriptor{{
			Key: table.NodeKey(), Kind: semantic.RelationPhysical, Reference: reference,
		}},
		ExpressionsComplete: true,
	}
	_, err = semantic.NewStatement(descriptor)
	if !errors.Is(err, domain.ErrPrecondition) || !strings.Contains(err.Error(), semantic.ErrorExpressionBindingInvalidV1) {
		t.Fatalf("missing expression error = %v", err)
	}

	descriptor.ExpressionTypes = []semantic.ExpressionTypeDescriptor{{Key: key, Type: boolType}}
	analyzed, err := semantic.NewStatement(descriptor)
	if err != nil {
		t.Fatalf("NewStatement() error = %v", err)
	}
	if !analyzed.ExpressionsComplete() {
		t.Fatal("complete expression attestation was lost")
	}
	resolved, err := analyzed.RequireExpressionType(key)
	if err != nil || resolved.Kind() != semantic.TypeBool {
		t.Fatalf("RequireExpressionType() = (%#v, %v)", resolved, err)
	}
	bound, err := analyzed.RequireRelationBinding(table.NodeKey())
	if err != nil {
		t.Fatalf("RequireRelationBinding() error = %v", err)
	}
	if got, ok := bound.Reference(); !ok || got != reference {
		t.Fatalf("binding reference = (%#v, %t)", got, ok)
	}
	descriptor.OutputColumns = []semantic.ColumnDescriptor{{Name: "", Type: boolType}}
	if _, err := semantic.NewStatement(descriptor); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("empty output name error = %v", err)
	}
}

func TestStatementRequiresBindingsForIdentifiersThatMatchDeclaredVariables(t *testing.T) {
	digest := strings.Repeat("c", 64)
	source := mustSource(t, digest, 0, 60)
	identifier, err := ast.NewIdentifier("value")
	if err != nil {
		t.Fatal(err)
	}
	path, err := ast.NewIdentifierPath([]ast.Identifier{identifier})
	if err != nil {
		t.Fatal(err)
	}
	defaultValue, err := ast.NewIntegerLiteral(mustNodeKey(t, digest, 20, 21, "INTEGER_LITERAL", 0), "1")
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := ast.NewDeclareStatement(source, []ast.Identifier{identifier}, nil, defaultValue)
	if err != nil {
		t.Fatal(err)
	}
	variable, err := ast.NewIdentifierExpression(mustNodeKey(t, digest, 30, 35, "IDENTIFIER", 1), path)
	if err != nil {
		t.Fatal(err)
	}
	body, err := ast.NewSelectQuery(false, []ast.SelectItem{mustSelectItem(t, variable)}, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	query, err := ast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	selectStatement, err := ast.NewSelectStatement(source, query)
	if err != nil {
		t.Fatal(err)
	}
	script, err := ast.NewScriptStatement(source, []ast.Statement{declaration, selectStatement})
	if err != nil {
		t.Fatal(err)
	}
	intType, err := semantic.NewType(semantic.TypeDescriptor{Kind: semantic.TypeInt64})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := semantic.StatementDescriptor{
		Syntax: script, ResolvedKind: ast.StatementScript,
		ExpressionTypes: []semantic.ExpressionTypeDescriptor{
			{Key: defaultValue.NodeKey(), Type: intType},
			{Key: variable.NodeKey(), Type: intType},
		},
		ExpressionsComplete: true,
		OutputColumns:       []semantic.ColumnDescriptor{{Name: "value", Type: intType}},
	}
	if _, err := semantic.NewStatement(descriptor); !errors.Is(err, domain.ErrPrecondition) ||
		!strings.Contains(err.Error(), semantic.ErrorSymbolBindingInvalidV1) {
		t.Fatalf("missing symbol binding error = %v", err)
	}
	descriptor.SymbolBindings = []semantic.SymbolBindingDescriptor{{
		Key: variable.NodeKey(), Kind: semantic.SymbolScriptVariable, Name: "value",
	}}
	statement, err := semantic.NewStatement(descriptor)
	if err != nil {
		t.Fatalf("NewStatement() error = %v", err)
	}
	binding, found := statement.SymbolBinding(variable.NodeKey())
	if !found || binding.Kind() != semantic.SymbolScriptVariable || binding.Name() != "value" {
		t.Fatalf("symbol binding = (%#v, %t)", binding, found)
	}
}

func TestDecimalTypePreservesOmissionAndEffectivePolicy(t *testing.T) {
	tests := []struct {
		kind      semantic.TypeKind
		precision int64
		scale     int64
	}{
		{kind: semantic.TypeNumeric, precision: 38, scale: 9},
		{kind: semantic.TypeBigNumeric, precision: 38, scale: 18},
	}
	for _, test := range tests {
		typ, err := semantic.NewType(semantic.TypeDescriptor{Kind: test.kind})
		if err != nil {
			t.Fatalf("NewType(%s) error = %v", test.kind, err)
		}
		if _, present := typ.Precision(); present {
			t.Fatalf("%s precision presence was not preserved", test.kind)
		}
		if _, present := typ.Scale(); present {
			t.Fatalf("%s scale presence was not preserved", test.kind)
		}
		parameters, ok := typ.EffectiveDecimalParameters()
		if !ok || parameters.Precision != test.precision || parameters.Scale != test.scale {
			t.Fatalf("%s effective parameters = %#v", test.kind, parameters)
		}
	}

	precision := int64(12)
	typ, err := semantic.NewType(semantic.TypeDescriptor{Kind: semantic.TypeNumeric, Precision: &precision})
	if err != nil {
		t.Fatalf("NewType(explicit precision) error = %v", err)
	}
	if _, present := typ.Scale(); present {
		t.Fatal("omitted scale became present")
	}
	parameters, _ := typ.EffectiveDecimalParameters()
	if parameters.Scale != 0 {
		t.Fatalf("effective scale = %d, want 0", parameters.Scale)
	}
}

func mustSource(t *testing.T, digest string, start, end int) ast.Source {
	t.Helper()
	span, err := ast.NewSpan(start, end)
	if err != nil {
		t.Fatalf("NewSpan() error = %v", err)
	}
	source, err := ast.NewSource(digest, span)
	if err != nil {
		t.Fatalf("NewSource() error = %v", err)
	}
	return source
}

func mustNodeKey(t *testing.T, digest string, start, end int, kind string, ordinal int) ast.NodeKey {
	t.Helper()
	span, err := ast.NewSpan(start, end)
	if err != nil {
		t.Fatalf("NewSpan() error = %v", err)
	}
	key, err := ast.NewNodeKey(digest, span, kind, ordinal)
	if err != nil {
		t.Fatalf("NewNodeKey() error = %v", err)
	}
	return key
}

func mustTable(t *testing.T, digest string, start, end, ordinal int, tableID string) *ast.TableRelation {
	t.Helper()
	identifier, err := ast.NewIdentifier(tableID)
	if err != nil {
		t.Fatalf("NewIdentifier() error = %v", err)
	}
	path, err := ast.NewIdentifierPath([]ast.Identifier{identifier})
	if err != nil {
		t.Fatalf("NewIdentifierPath() error = %v", err)
	}
	relation, err := ast.NewTableRelation(mustNodeKey(t, digest, start, end, "TABLE", ordinal), path, nil)
	if err != nil {
		t.Fatalf("NewTableRelation() error = %v", err)
	}
	return relation
}

func mustSelectItem(t *testing.T, expression ast.Expression) ast.SelectItem {
	t.Helper()
	item, err := ast.NewSelectItem(expression, nil)
	if err != nil {
		t.Fatalf("NewSelectItem() error = %v", err)
	}
	return item
}
