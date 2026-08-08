package ast_test

import (
	"strings"
	"testing"

	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestStatementOwnsCollectionsAndProvidesStableSemanticFingerprint(t *testing.T) {
	source := mustSource(t, 0, 64)
	target := mustTable(t, 1, "project", "dataset", "events")
	first := mustInteger(t, 2, "1")
	label, err := queryast.NewStringLiteral(mustKey(t, 3, "string", 20, 27), "private-value")
	if err != nil {
		t.Fatal(err)
	}
	row := []queryast.Expression{first, label}
	columns := []queryast.Identifier{mustIdentifier(t, "id"), mustIdentifier(t, "label")}

	statement, err := queryast.NewInsertValuesStatement(source, target, columns, [][]queryast.Expression{row})
	if err != nil {
		t.Fatalf("NewInsertValuesStatement() error = %v", err)
	}
	fingerprint := statement.SemanticFingerprint()
	if len(fingerprint) != 64 || strings.Contains(fingerprint, "private-value") {
		t.Fatalf("semantic fingerprint = %q", fingerprint)
	}

	columns[0] = mustIdentifier(t, "changed")
	row[0] = mustInteger(t, 4, "9")
	returnedRows := statement.Rows()
	returnedRows[0][0] = mustInteger(t, 5, "7")
	if got := statement.Columns()[0].Value(); got != "id" {
		t.Fatalf("column changed through constructor input: %q", got)
	}
	if got := statement.Rows()[0][0].(*queryast.IntegerLiteral).CanonicalValue(); got != "1" {
		t.Fatalf("row changed through returned slice: %q", got)
	}
	if statement.SemanticFingerprint() != fingerprint {
		t.Fatal("immutable statement fingerprint changed")
	}
}

func TestNodeKeysDisambiguateSharedSpansAndRelationsIncludeMutationTargets(t *testing.T) {
	span, err := queryast.NewSpan(7, 24)
	if err != nil {
		t.Fatal(err)
	}
	first, err := queryast.NewNodeKey(testDigest, span, "table-relation", 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := queryast.NewNodeKey(testDigest, span, "table-relation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || first.Fingerprint() == second.Fingerprint() {
		t.Fatal("node ordinal did not disambiguate a shared span")
	}

	target := mustTableWithKey(t, first, "dataset", "target")
	statement, err := queryast.NewInsertValuesStatement(
		mustSource(t, 0, 40), target, nil,
		[][]queryast.Expression{{mustInteger(t, 2, "1")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	relations, err := queryast.Relations(statement)
	if err != nil {
		t.Fatal(err)
	}
	if len(relations) != 1 || relations[0].NodeKey() != first {
		t.Fatalf("Relations() = %#v", relations)
	}
}

func TestNumericTypePreservesOmittedScale(t *testing.T) {
	precision := int64(20)
	typ, err := queryast.NewScalarType(mustKey(t, 1, "scalar-type", 0, 11), queryast.TypeNumeric, &precision, nil)
	if err != nil {
		t.Fatalf("NUMERIC(P) error = %v", err)
	}
	if typ.Precision() == nil || *typ.Precision() != 20 || typ.Scale() != nil {
		t.Fatalf("NUMERIC(P) presence = precision %v scale %v", typ.Precision(), typ.Scale())
	}
	scale := int64(2)
	if _, err := queryast.NewScalarType(mustKey(t, 2, "scalar-type", 0, 11), queryast.TypeNumeric, nil, &scale); err == nil {
		t.Fatal("scale without precision was accepted")
	}
}

func TestNodeConstructorsRejectZeroNodeKey(t *testing.T) {
	if _, err := queryast.NewStarExpression(queryast.NodeKey{}); err == nil {
		t.Fatal("star accepted a zero NodeKey")
	}
	if _, err := queryast.NewNullLiteral(queryast.NodeKey{}); err == nil {
		t.Fatal("NULL accepted a zero NodeKey")
	}
	if _, err := queryast.NewBooleanLiteral(queryast.NodeKey{}, true); err == nil {
		t.Fatal("boolean accepted a zero NodeKey")
	}
	if _, err := queryast.NewFloatLiteral(queryast.NodeKey{}, 1); err == nil {
		t.Fatal("float accepted a zero NodeKey")
	}
	if _, err := queryast.NewDecimalLiteral(queryast.NodeKey{}, queryast.TypeNumeric, "1.25"); err == nil {
		t.Fatal("decimal accepted a zero NodeKey")
	}
	if _, err := queryast.NewStringLiteral(queryast.NodeKey{}, "value"); err == nil {
		t.Fatal("string accepted a zero NodeKey")
	}
	if _, err := queryast.NewScalarType(queryast.NodeKey{}, queryast.TypeInt64, nil, nil); err == nil {
		t.Fatal("scalar type accepted a zero NodeKey")
	}
	if _, err := queryast.NewBetweenExpression(
		queryast.NodeKey{}, mustInteger(t, 11, "1"), mustInteger(t, 12, "2"), mustInteger(t, 13, "3"), false,
	); err == nil {
		t.Fatal("BETWEEN accepted a zero NodeKey")
	}
}

func TestBetweenExpressionOwnsTypedOperandsAndTraversesInSourceOrder(t *testing.T) {
	value := mustInteger(t, 1, "7")
	low := mustInteger(t, 2, "2")
	high := mustInteger(t, 3, "9")
	between, err := queryast.NewBetweenExpression(mustKey(t, 4, "between", 0, 13), value, low, high, true)
	if err != nil {
		t.Fatal(err)
	}
	if between.Kind() != queryast.ExpressionBetween || !between.Not() ||
		between.Value() != value || between.Low() != low || between.High() != high {
		t.Fatalf("BETWEEN expression = %#v", between)
	}
	item, err := queryast.NewSelectItem(between, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := queryast.NewSelectQuery(false, []queryast.SelectItem{item}, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := queryast.NewSelectStatement(mustSource(t, 0, 13), query)
	if err != nil {
		t.Fatal(err)
	}
	expressions, err := queryast.Expressions(statement)
	if err != nil {
		t.Fatal(err)
	}
	if len(expressions) != 4 || expressions[0] != between || expressions[1] != value ||
		expressions[2] != low || expressions[3] != high {
		t.Fatalf("BETWEEN traversal = %#v", expressions)
	}
}

func TestDecimalLiteralPreservesExactCanonicalValueAndType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "+001.2500", want: "1.25"},
		{input: "1.2e3", want: "1200"},
		{input: "1.2e-3", want: "0.0012"},
		{input: "-0.000", want: "0"},
	}
	for index, tt := range tests {
		literal, err := queryast.NewDecimalLiteral(mustKey(t, index, "decimal", 0, 8), queryast.TypeBigNumeric, tt.input)
		if err != nil {
			t.Fatalf("NewDecimalLiteral(%q) error = %v", tt.input, err)
		}
		if literal.Type() != queryast.TypeBigNumeric || literal.CanonicalValue() != tt.want {
			t.Fatalf("NewDecimalLiteral(%q) = (%q, %q)", tt.input, literal.Type(), literal.CanonicalValue())
		}
	}
	if _, err := queryast.NewDecimalLiteral(mustKey(t, 9, "decimal", 0, 8), queryast.TypeFloat64, "1.25"); err == nil {
		t.Fatal("decimal literal accepted FLOAT64 type")
	}
	if _, err := queryast.NewDecimalLiteral(mustKey(t, 10, "decimal", 0, 8), queryast.TypeNumeric, "not-a-number"); err == nil {
		t.Fatal("decimal literal accepted invalid value")
	}
}

func TestStatementVisitorSeesTypedRoot(t *testing.T) {
	item, err := queryast.NewSelectItem(mustInteger(t, 1, "1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := queryast.NewSelectQuery(false, []queryast.SelectItem{item}, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := queryast.NewSelectStatement(mustSource(t, 0, 8), query)
	if err != nil {
		t.Fatal(err)
	}
	visitor := &statementKindVisitor{}
	if err := statement.Accept(visitor); err != nil {
		t.Fatal(err)
	}
	if visitor.kind != queryast.StatementSelect {
		t.Fatalf("visitor kind = %q", visitor.kind)
	}
}

type statementKindVisitor struct{ kind queryast.StatementKind }

func (visitor *statementKindVisitor) VisitScript(*queryast.ScriptStatement) error {
	visitor.kind = queryast.StatementScript
	return nil
}
func (visitor *statementKindVisitor) VisitDeclare(*queryast.DeclareStatement) error {
	visitor.kind = queryast.StatementDeclare
	return nil
}
func (visitor *statementKindVisitor) VisitSet(*queryast.SetStatement) error {
	visitor.kind = queryast.StatementSet
	return nil
}
func (visitor *statementKindVisitor) VisitSelect(*queryast.SelectStatement) error {
	visitor.kind = queryast.StatementSelect
	return nil
}
func (visitor *statementKindVisitor) VisitInsert(*queryast.InsertStatement) error {
	visitor.kind = queryast.StatementInsert
	return nil
}
func (visitor *statementKindVisitor) VisitUpdate(*queryast.UpdateStatement) error {
	visitor.kind = queryast.StatementUpdate
	return nil
}
func (visitor *statementKindVisitor) VisitDelete(*queryast.DeleteStatement) error {
	visitor.kind = queryast.StatementDelete
	return nil
}
func (visitor *statementKindVisitor) VisitMerge(*queryast.MergeStatement) error {
	visitor.kind = queryast.StatementMerge
	return nil
}
func (visitor *statementKindVisitor) VisitCreateTable(*queryast.CreateTableStatement) error {
	visitor.kind = queryast.StatementCreateTable
	return nil
}
func (visitor *statementKindVisitor) VisitDropTable(*queryast.DropTableStatement) error {
	visitor.kind = queryast.StatementDropTable
	return nil
}
func (visitor *statementKindVisitor) VisitAlterTable(*queryast.AlterTableStatement) error {
	visitor.kind = queryast.StatementAlterTable
	return nil
}
func (visitor *statementKindVisitor) VisitTruncateTable(*queryast.TruncateTableStatement) error {
	visitor.kind = queryast.StatementTruncateTable
	return nil
}

func mustSource(t *testing.T, start, end int) queryast.Source {
	t.Helper()
	span, err := queryast.NewSpan(start, end)
	if err != nil {
		t.Fatal(err)
	}
	source, err := queryast.NewSource(testDigest, span)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mustKey(t *testing.T, ordinal int, kind string, start, end int) queryast.NodeKey {
	t.Helper()
	span, err := queryast.NewSpan(start, end)
	if err != nil {
		t.Fatal(err)
	}
	key, err := queryast.NewNodeKey(testDigest, span, kind, ordinal)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mustIdentifier(t *testing.T, value string) queryast.Identifier {
	t.Helper()
	identifier, err := queryast.NewIdentifier(value)
	if err != nil {
		t.Fatal(err)
	}
	return identifier
}

func mustPath(t *testing.T, parts ...string) queryast.IdentifierPath {
	t.Helper()
	identifiers := make([]queryast.Identifier, len(parts))
	for index, part := range parts {
		identifiers[index] = mustIdentifier(t, part)
	}
	path, err := queryast.NewIdentifierPath(identifiers)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func mustTable(t *testing.T, ordinal int, parts ...string) *queryast.TableRelation {
	t.Helper()
	return mustTableWithKey(t, mustKey(t, ordinal, "table-relation", 0, 10), parts...)
}

func mustTableWithKey(t *testing.T, key queryast.NodeKey, parts ...string) *queryast.TableRelation {
	t.Helper()
	relation, err := queryast.NewTableRelation(key, mustPath(t, parts...), nil)
	if err != nil {
		t.Fatal(err)
	}
	return relation
}

func mustInteger(t *testing.T, ordinal int, value string) *queryast.IntegerLiteral {
	t.Helper()
	literal, err := queryast.NewIntegerLiteral(mustKey(t, ordinal, "integer", ordinal, ordinal+1), value)
	if err != nil {
		t.Fatal(err)
	}
	return literal
}
