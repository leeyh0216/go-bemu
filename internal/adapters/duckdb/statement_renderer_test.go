package duckdb

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

const rendererTestDigest = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

func TestLowerDuckDBSelectUsesCanonicalBindingsAndTypedArguments(t *testing.T) {
	t.Parallel()
	fixture := newRendererASTFixture(t)
	alias := fixture.identifier("source")
	table := fixture.table([]string{"unresolved-secret-project", "wrong_dataset", "wrong_table"}, &alias)

	idSelect := fixture.identifierExpression("source", "id")
	idWhere := fixture.identifierExpression("source", "id")
	idOrder := fixture.identifierExpression("source", "id")
	label := fixture.identifierExpression("source", "label")
	deleted := fixture.identifierExpression("source", "deleted_at")
	timestamp := fixture.temporal(queryast.TypeTimestamp, "2026-08-08 12:34:56+09:00")
	structValue := fixture.structLiteral(
		[]string{"id", "label"},
		[]queryast.Expression{fixture.integer("7"), fixture.stringLiteral("bound-struct")},
	)
	intType := fixture.scalarType(queryast.TypeInt64, nil, nil)
	arrayValue := fixture.arrayLiteral(intType, fixture.integer("1"), fixture.integer("2"))

	greater, err := queryast.NewBinaryExpression(fixture.key("binary"), ">=", idWhere, fixture.integer("2"))
	if err != nil {
		t.Fatal(err)
	}
	in, err := queryast.NewInListExpression(
		fixture.key("in"), label, false,
		[]queryast.Expression{fixture.stringLiteral("safe"), fixture.stringLiteral("x'); DROP TABLE private; --")},
	)
	if err != nil {
		t.Fatal(err)
	}
	isNull, err := queryast.NewBinaryExpression(fixture.key("binary"), "IS", deleted, fixture.nullLiteral())
	if err != nil {
		t.Fatal(err)
	}
	and, err := queryast.NewBinaryExpression(fixture.key("binary"), "AND", greater, in)
	if err != nil {
		t.Fatal(err)
	}
	where, err := queryast.NewBinaryExpression(fixture.key("binary"), "OR", and, isNull)
	if err != nil {
		t.Fatal(err)
	}

	items := []queryast.SelectItem{
		fixture.selectItem(idSelect, ""),
		fixture.selectItem(timestamp, "event_time"),
		fixture.selectItem(structValue, "payload"),
		fixture.selectItem(arrayValue, "values"),
	}
	body, err := queryast.NewSelectQuery(false, items, table, where, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	order, err := queryast.NewOrderItem(idOrder, queryast.SortDescending, queryast.NullsLast)
	if err != nil {
		t.Fatal(err)
	}
	limit, offset := int64(10), int64(2)
	query, err := queryast.NewQuery(nil, false, body, []queryast.OrderItem{order}, &limit, &offset)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	reference := domain.TableReference{ProjectID: "data-project", DatasetID: "analytics", TableID: `events"archive`}
	binding := fixture.physicalBinding(reference)

	plan := fixture.lower(statement, map[queryast.NodeKey]duckDBTableBinding{table.NodeKey(): binding})
	physical, err := renderPhysicalTable(reference)
	if err != nil {
		t.Fatal(err)
	}
	wantSQL := `SELECT "source"."id", CAST(? AS TIMESTAMPTZ) AS "event_time", ` +
		`struct_pack("id" := ?, "label" := ?) AS "payload", CAST([?, ?] AS BIGINT[]) AS "values" ` +
		`FROM ` + physical + ` AS "source" WHERE ((("source"."id" >= ?) AND ` +
		`("source"."label" IN (?, ?))) OR ("source"."deleted_at" IS NULL)) ` +
		`ORDER BY "source"."id" DESC NULLS LAST LIMIT 10 OFFSET 2`
	if plan.statementSQL() != wantSQL {
		t.Fatalf("lowered SQL mismatch:\n got: %s\nwant: %s", plan.statementSQL(), wantSQL)
	}
	wantArguments := []any{
		"2026-08-08 12:34:56+09:00", int64(7), "bound-struct", int64(1), int64(2),
		int64(2), "safe", "x'); DROP TABLE private; --",
	}
	if got := plan.bindArguments(); !reflect.DeepEqual(got, wantArguments) {
		t.Fatalf("bind arguments = %#v, want %#v", got, wantArguments)
	}
	if !plan.returnsRows() {
		t.Fatal("SELECT plan must produce rows")
	}
	for _, secret := range []string{"unresolved-secret-project", "wrong_dataset", "wrong_table", "DROP TABLE private"} {
		if strings.Contains(plan.statementSQL(), secret) {
			t.Fatalf("generated SQL leaked unresolved input %q: %s", secret, plan.statementSQL())
		}
	}
}

func TestLowerDuckDBBetweenUsesBoundOperands(t *testing.T) {
	t.Parallel()
	fixture := newRendererASTFixture(t)
	table := fixture.table([]string{"unresolved-project", "unresolved-dataset", "unresolved-table"}, nil)
	between, err := queryast.NewBetweenExpression(
		fixture.key("between"), fixture.identifierExpression("id"),
		fixture.integer("2"), fixture.integer("4"), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	body, err := queryast.NewSelectQuery(
		false, []queryast.SelectItem{fixture.selectItem(fixture.identifierExpression("id"), "")},
		table, between, nil, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	plan := fixture.lower(statement, map[queryast.NodeKey]duckDBTableBinding{
		table.NodeKey(): fixture.physicalBinding(reference),
	})
	physical, err := renderPhysicalTable(reference)
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "id" FROM ` + physical + ` WHERE ("id" BETWEEN ? AND ?)`
	if plan.statementSQL() != want {
		t.Fatalf("BETWEEN plan = %q, want %q", plan.statementSQL(), want)
	}
	if got := plan.bindArguments(); !reflect.DeepEqual(got, []any{int64(2), int64(4)}) {
		t.Fatalf("BETWEEN arguments = %#v", got)
	}
}

func TestLowerDuckDBQuerySupportsCTEUnionAndAggregateModifiers(t *testing.T) {
	t.Parallel()
	fixture := newRendererASTFixture(t)
	first := fixture.selectBody(
		fixture.selectItem(fixture.integer("1"), "id"),
		fixture.selectItem(fixture.nullLiteral(), "value"),
	)
	second := fixture.selectBody(
		fixture.selectItem(fixture.integer("2"), "id"),
		fixture.selectItem(fixture.stringLiteral("two"), "value"),
	)
	union, err := queryast.NewSetOperationQuery(queryast.SetUnion, true, first, second)
	if err != nil {
		t.Fatal(err)
	}
	cteQuery, err := queryast.NewQuery(nil, false, union, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cte, err := queryast.NewCommonTableExpression(fixture.identifier("input_rows"), nil, cteQuery)
	if err != nil {
		t.Fatal(err)
	}
	local := fixture.table([]string{"this-path-is-not-rendered"}, nil)
	star := fixture.star()
	count := fixture.function("COUNT", []queryast.Expression{star}, false, queryast.FunctionNullHandlingDefault)
	value := fixture.identifierExpression("value")
	arrayAgg := fixture.function("ARRAY_AGG", []queryast.Expression{value}, true, queryast.FunctionIgnoreNulls)
	body, err := queryast.NewSelectQuery(
		false,
		[]queryast.SelectItem{fixture.selectItem(count, "rows"), fixture.selectItem(arrayAgg, "values")},
		local, nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	query, err := queryast.NewQuery([]queryast.CommonTableExpression{cte}, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	localBinding, err := newDuckDBLocalTableBinding("input_rows")
	if err != nil {
		t.Fatal(err)
	}
	plan := fixture.lower(statement, map[queryast.NodeKey]duckDBTableBinding{local.NodeKey(): localBinding})
	want := `WITH "input_rows" AS ((SELECT ? AS "id", NULL AS "value") UNION ALL ` +
		`(SELECT ? AS "id", ? AS "value")) SELECT count(*) AS "rows", ` +
		`array_agg(DISTINCT "value") FILTER (WHERE "value" IS NOT NULL) AS "values" FROM "input_rows"`
	if plan.statementSQL() != want {
		t.Fatalf("lowered SQL mismatch:\n got: %s\nwant: %s", plan.statementSQL(), want)
	}
	if got, expected := plan.bindArguments(), []any{int64(1), int64(2), "two"}; !reflect.DeepEqual(got, expected) {
		t.Fatalf("bind arguments = %#v, want %#v", got, expected)
	}
}

func TestLowerDuckDBDMLStatements(t *testing.T) {
	t.Parallel()
	t.Run("insert values", func(t *testing.T) {
		fixture := newRendererASTFixture(t)
		target := fixture.table([]string{"unresolved", "target"}, nil)
		statement, err := queryast.NewInsertValuesStatement(
			fixture.source(), target,
			[]queryast.Identifier{fixture.identifier("id"), fixture.identifier("label")},
			[][]queryast.Expression{
				{fixture.integer("1"), fixture.stringLiteral("one")},
				{fixture.integer("2"), fixture.stringLiteral("two")},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
		plan := fixture.lower(statement, map[queryast.NodeKey]duckDBTableBinding{target.NodeKey(): fixture.physicalBinding(reference)})
		physical, _ := renderPhysicalTable(reference)
		want := `INSERT INTO ` + physical + ` ("id", "label") VALUES (?, ?), (?, ?)`
		if plan.statementSQL() != want || !reflect.DeepEqual(plan.bindArguments(), []any{int64(1), "one", int64(2), "two"}) {
			t.Fatalf("INSERT plan = %q %#v", plan.statementSQL(), plan.bindArguments())
		}
	})

	t.Run("update", func(t *testing.T) {
		fixture := newRendererASTFixture(t)
		alias := fixture.identifier("target")
		target := fixture.table([]string{"unresolved"}, &alias)
		assignment, err := queryast.NewAssignment(fixture.path("label"), fixture.stringLiteral("updated"))
		if err != nil {
			t.Fatal(err)
		}
		where, err := queryast.NewBinaryExpression(
			fixture.key("binary"), "=", fixture.identifierExpression("target", "id"), fixture.integer("7"),
		)
		if err != nil {
			t.Fatal(err)
		}
		statement, err := queryast.NewUpdateStatement(fixture.source(), target, []queryast.Assignment{assignment}, nil, where)
		if err != nil {
			t.Fatal(err)
		}
		reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
		plan := fixture.lower(statement, map[queryast.NodeKey]duckDBTableBinding{target.NodeKey(): fixture.physicalBinding(reference)})
		physical, _ := renderPhysicalTable(reference)
		want := `UPDATE ` + physical + ` AS "target" SET "label" = ? WHERE ("target"."id" = ?)`
		if plan.statementSQL() != want || !reflect.DeepEqual(plan.bindArguments(), []any{"updated", int64(7)}) {
			t.Fatalf("UPDATE plan = %q %#v", plan.statementSQL(), plan.bindArguments())
		}
	})

	t.Run("delete", func(t *testing.T) {
		fixture := newRendererASTFixture(t)
		target := fixture.table([]string{"unresolved"}, nil)
		where, err := queryast.NewInListExpression(
			fixture.key("in"), fixture.identifierExpression("id"), false,
			[]queryast.Expression{fixture.integer("1"), fixture.integer("2")},
		)
		if err != nil {
			t.Fatal(err)
		}
		statement, err := queryast.NewDeleteStatement(fixture.source(), target, where)
		if err != nil {
			t.Fatal(err)
		}
		reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
		plan := fixture.lower(statement, map[queryast.NodeKey]duckDBTableBinding{target.NodeKey(): fixture.physicalBinding(reference)})
		physical, _ := renderPhysicalTable(reference)
		want := `DELETE FROM ` + physical + ` WHERE ("id" IN (?, ?))`
		if plan.statementSQL() != want || !reflect.DeepEqual(plan.bindArguments(), []any{int64(1), int64(2)}) {
			t.Fatalf("DELETE plan = %q %#v", plan.statementSQL(), plan.bindArguments())
		}
	})

	t.Run("static merge", func(t *testing.T) {
		fixture := newRendererASTFixture(t)
		targetAlias := fixture.identifier("target")
		sourceAlias := fixture.identifier("source")
		target := fixture.table([]string{"unresolved-target"}, &targetAlias)
		source := fixture.table([]string{"unresolved-source"}, &sourceAlias)
		insertWhen, err := queryast.NewMergeWhen(
			queryast.MergeNotMatchedByTarget, nil, queryast.NewMergeInsertRowAction(),
		)
		if err != nil {
			t.Fatal(err)
		}
		deleteWhen, err := queryast.NewMergeWhen(
			queryast.MergeNotMatchedBySource, nil, queryast.NewMergeDeleteAction(),
		)
		if err != nil {
			t.Fatal(err)
		}
		statement, err := queryast.NewMergeStatement(
			fixture.source(), target, source, fixture.boolean(false), []queryast.MergeWhen{insertWhen, deleteWhen},
		)
		if err != nil {
			t.Fatal(err)
		}
		targetReference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "destination"}
		sourceReference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "temporary"}
		plan := fixture.lower(statement, map[queryast.NodeKey]duckDBTableBinding{
			target.NodeKey(): fixture.physicalBinding(targetReference),
			source.NodeKey(): fixture.physicalBinding(sourceReference),
		})
		targetPhysical, _ := renderPhysicalTable(targetReference)
		sourcePhysical, _ := renderPhysicalTable(sourceReference)
		want := `MERGE INTO ` + targetPhysical + ` AS "target" USING ` + sourcePhysical + ` AS "source" ` +
			`ON ? WHEN NOT MATCHED THEN INSERT BY NAME WHEN NOT MATCHED BY SOURCE THEN DELETE`
		if plan.statementSQL() != want || !reflect.DeepEqual(plan.bindArguments(), []any{false}) {
			t.Fatalf("MERGE plan = %q %#v", plan.statementSQL(), plan.bindArguments())
		}
	})
}

func TestLowerDuckDBFailsClosedWithoutBindingsOrForUnsupportedFunctions(t *testing.T) {
	t.Parallel()
	t.Run("missing relation binding retains unresolved path", func(t *testing.T) {
		fixture := newRendererASTFixture(t)
		table := fixture.table([]string{"private-project", "private_dataset", "private_table"}, nil)
		body := fixture.selectBody(fixture.selectItem(fixture.star(), ""))
		bodyWithTable, err := queryast.NewSelectQuery(false, body.Items(), table, nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		query, err := queryast.NewQuery(nil, false, bodyWithTable, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		statement, err := queryast.NewSelectStatement(fixture.source(), query)
		if err != nil {
			t.Fatal(err)
		}
		_, err = semantic.NewStatement(semantic.StatementDescriptor{
			Syntax: statement, ResolvedKind: statement.Kind(), ExpressionsComplete: false,
		})
		if !errors.Is(err, domain.ErrPrecondition) {
			t.Fatalf("error = %v, want ErrPrecondition", err)
		}
		for _, secret := range []string{"private-project", "private_dataset", "private_table"} {
			if !strings.Contains(err.Error(), secret) {
				t.Fatalf("binding error omitted unresolved path %q: %v", secret, err)
			}
		}
	})

	t.Run("unknown function never reaches DuckDB", func(t *testing.T) {
		fixture := newRendererASTFixture(t)
		call := fixture.function(
			"READ_CSV", []queryast.Expression{fixture.stringLiteral("/private/secret.csv")},
			false, queryast.FunctionNullHandlingDefault,
		)
		body := fixture.selectBody(fixture.selectItem(call, ""))
		query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		statement, err := queryast.NewSelectStatement(fixture.source(), query)
		if err != nil {
			t.Fatal(err)
		}
		analyzed := fixture.semanticStatement(statement, nil)
		_, err = lowerDuckDBStatement(analyzed)
		if !errors.Is(err, domain.ErrUnsupported) {
			t.Fatalf("error = %v, want ErrUnsupported", err)
		}
		if strings.Contains(err.Error(), "/private/secret.csv") {
			t.Fatalf("unsupported error leaked literal: %v", err)
		}
		if !strings.Contains(err.Error(), "READ_CSV") || !strings.Contains(err.Error(), duckDBGoogleSQLLoweringUnsupportedV1) {
			t.Fatalf("unsupported error omitted function or capability code: %v", err)
		}
	})

	t.Run("unknown operator is reflected", func(t *testing.T) {
		fixture := newRendererASTFixture(t)
		const marker = "SECRET_OPERATOR_MARKER"
		expression, err := queryast.NewBinaryExpression(
			fixture.key("binary"), marker, fixture.integer("1"), fixture.integer("2"),
		)
		if err != nil {
			t.Fatal(err)
		}
		body := fixture.selectBody(fixture.selectItem(expression, ""))
		query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		statement, err := queryast.NewSelectStatement(fixture.source(), query)
		if err != nil {
			t.Fatal(err)
		}
		_, err = lowerDuckDBStatement(fixture.semanticStatement(statement, nil))
		if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), marker) {
			t.Fatalf("operator error omitted diagnostic operator: %v", err)
		}
	})
}

func TestDuckDBTypeVisitorAppliesCanonicalDecimalAndNestedTypePolicy(t *testing.T) {
	t.Parallel()
	fixture := newRendererASTFixture(t)
	renderer := &duckDBStatementRenderer{}
	precision := int64(20)
	amount := fixture.scalarType(queryast.TypeNumeric, &precision, nil)
	label := fixture.scalarType(queryast.TypeString, nil, nil)
	amountField, err := queryast.NewStructTypeField(rendererIdentifierPointer(fixture.identifier("amount")), amount)
	if err != nil {
		t.Fatal(err)
	}
	labelField, err := queryast.NewStructTypeField(rendererIdentifierPointer(fixture.identifier("label")), label)
	if err != nil {
		t.Fatal(err)
	}
	structType, err := queryast.NewStructType(fixture.key("struct-type"), []queryast.StructTypeField{amountField, labelField})
	if err != nil {
		t.Fatal(err)
	}
	arrayType, err := queryast.NewArrayType(fixture.key("array-type"), structType)
	if err != nil {
		t.Fatal(err)
	}
	got, err := renderer.renderType(arrayType)
	if err != nil {
		t.Fatal(err)
	}
	if want := `STRUCT("amount" DECIMAL(20,0), "label" VARCHAR)[]`; got != want {
		t.Fatalf("nested physical type = %q, want %q", got, want)
	}
	bigNumeric := fixture.scalarType(queryast.TypeBigNumeric, nil, nil)
	got, err = renderer.renderType(bigNumeric)
	if err != nil {
		t.Fatal(err)
	}
	if got != "DECIMAL(38,18)" {
		t.Fatalf("default BIGNUMERIC physical type = %q", got)
	}
}

type rendererASTFixture struct {
	t       *testing.T
	ordinal int
}

func newRendererASTFixture(t *testing.T) *rendererASTFixture {
	t.Helper()
	return &rendererASTFixture{t: t}
}

func (fixture *rendererASTFixture) source() queryast.Source {
	fixture.t.Helper()
	span, err := queryast.NewSpan(0, 4096)
	if err != nil {
		fixture.t.Fatal(err)
	}
	source, err := queryast.NewSource(rendererTestDigest, span)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return source
}

func (fixture *rendererASTFixture) key(kind string) queryast.NodeKey {
	fixture.t.Helper()
	fixture.ordinal++
	span, err := queryast.NewSpan(fixture.ordinal, fixture.ordinal+1)
	if err != nil {
		fixture.t.Fatal(err)
	}
	key, err := queryast.NewNodeKey(rendererTestDigest, span, kind, fixture.ordinal)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return key
}

func (fixture *rendererASTFixture) identifier(value string) queryast.Identifier {
	fixture.t.Helper()
	identifier, err := queryast.NewIdentifier(value)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return identifier
}

func (fixture *rendererASTFixture) path(parts ...string) queryast.IdentifierPath {
	fixture.t.Helper()
	identifiers := make([]queryast.Identifier, len(parts))
	for index, part := range parts {
		identifiers[index] = fixture.identifier(part)
	}
	path, err := queryast.NewIdentifierPath(identifiers)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return path
}

func (fixture *rendererASTFixture) table(parts []string, alias *queryast.Identifier) *queryast.TableRelation {
	fixture.t.Helper()
	table, err := queryast.NewTableRelation(fixture.key("table-relation"), fixture.path(parts...), alias)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return table
}

func (fixture *rendererASTFixture) identifierExpression(parts ...string) queryast.Expression {
	fixture.t.Helper()
	expression, err := queryast.NewIdentifierExpression(fixture.key("identifier"), fixture.path(parts...))
	if err != nil {
		fixture.t.Fatal(err)
	}
	return expression
}

func (fixture *rendererASTFixture) integer(value string) queryast.Expression {
	fixture.t.Helper()
	literal, err := queryast.NewIntegerLiteral(fixture.key("integer"), value)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return literal
}

func (fixture *rendererASTFixture) stringLiteral(value string) queryast.Expression {
	fixture.t.Helper()
	literal, err := queryast.NewStringLiteral(fixture.key("string"), value)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return literal
}

func (fixture *rendererASTFixture) decimal(kind queryast.TypeKind, value string) queryast.Expression {
	fixture.t.Helper()
	literal, err := queryast.NewDecimalLiteral(fixture.key("decimal"), kind, value)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return literal
}

func (fixture *rendererASTFixture) temporal(kind queryast.TypeKind, value string) queryast.Expression {
	fixture.t.Helper()
	literal, err := queryast.NewTemporalLiteral(fixture.key("temporal"), kind, value)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return literal
}

func (fixture *rendererASTFixture) boolean(value bool) queryast.Expression {
	fixture.t.Helper()
	literal, err := queryast.NewBooleanLiteral(fixture.key("boolean"), value)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return literal
}

func (fixture *rendererASTFixture) nullLiteral() queryast.Expression {
	fixture.t.Helper()
	literal, err := queryast.NewNullLiteral(fixture.key("null"))
	if err != nil {
		fixture.t.Fatal(err)
	}
	return literal
}

func (fixture *rendererASTFixture) star() queryast.Expression {
	fixture.t.Helper()
	star, err := queryast.NewStarExpression(fixture.key("star"))
	if err != nil {
		fixture.t.Fatal(err)
	}
	return star
}

func (fixture *rendererASTFixture) scalarType(kind queryast.TypeKind, precision, scale *int64) queryast.Type {
	fixture.t.Helper()
	typ, err := queryast.NewScalarType(fixture.key("scalar-type"), kind, precision, scale)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return typ
}

func (fixture *rendererASTFixture) arrayLiteral(elementType queryast.Type, elements ...queryast.Expression) queryast.Expression {
	fixture.t.Helper()
	literal, err := queryast.NewArrayLiteral(fixture.key("array"), elementType, elements)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return literal
}

func (fixture *rendererASTFixture) structLiteral(names []string, values []queryast.Expression) queryast.Expression {
	fixture.t.Helper()
	fields := make([]queryast.StructLiteralField, len(values))
	for index, value := range values {
		var name *queryast.Identifier
		if names != nil {
			identifier := fixture.identifier(names[index])
			name = &identifier
		}
		field, err := queryast.NewStructLiteralField(name, value)
		if err != nil {
			fixture.t.Fatal(err)
		}
		fields[index] = field
	}
	literal, err := queryast.NewStructLiteral(fixture.key("struct"), nil, fields)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return literal
}

func (fixture *rendererASTFixture) function(
	name string,
	arguments []queryast.Expression,
	distinct bool,
	nullHandling queryast.FunctionNullHandling,
) queryast.Expression {
	fixture.t.Helper()
	call, err := queryast.NewFunctionCall(fixture.key("function"), fixture.path(name), arguments, distinct, nullHandling)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return call
}

func (fixture *rendererASTFixture) selectItem(expression queryast.Expression, alias string) queryast.SelectItem {
	fixture.t.Helper()
	var identifier *queryast.Identifier
	if alias != "" {
		value := fixture.identifier(alias)
		identifier = &value
	}
	item, err := queryast.NewSelectItem(expression, identifier)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return item
}

func (fixture *rendererASTFixture) selectBody(items ...queryast.SelectItem) *queryast.SelectQuery {
	fixture.t.Helper()
	body, err := queryast.NewSelectQuery(false, items, nil, nil, nil, nil, nil)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return body
}

func (fixture *rendererASTFixture) physicalBinding(reference domain.TableReference) duckDBTableBinding {
	fixture.t.Helper()
	binding, err := newDuckDBPhysicalTableBinding(reference)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return binding
}

func (fixture *rendererASTFixture) lower(
	statement queryast.Statement,
	bindings map[queryast.NodeKey]duckDBTableBinding,
) duckDBStatementPlan {
	fixture.t.Helper()
	plan, err := lowerDuckDBStatement(fixture.semanticStatement(statement, bindings))
	if err != nil {
		fixture.t.Fatal(err)
	}
	return plan
}

func (fixture *rendererASTFixture) semanticStatement(
	statement queryast.Statement,
	bindings map[queryast.NodeKey]duckDBTableBinding,
) semantic.Statement {
	return fixture.semanticStatementWithOutput(statement, bindings, nil)
}

func (fixture *rendererASTFixture) semanticStatementWithOutput(
	statement queryast.Statement,
	bindings map[queryast.NodeKey]duckDBTableBinding,
	output []semantic.ColumnDescriptor,
) semantic.Statement {
	fixture.t.Helper()
	relations, err := queryast.Relations(statement)
	if err != nil {
		fixture.t.Fatal(err)
	}
	descriptors := make([]semantic.RelationBindingDescriptor, 0, len(relations))
	for _, relation := range relations {
		binding, ok := bindings[relation.NodeKey()]
		if !ok {
			fixture.t.Fatalf("test relation binding is missing node_key=%s", relation.NodeKey().Fingerprint())
		}
		descriptor := semantic.RelationBindingDescriptor{Key: relation.NodeKey()}
		switch binding.kind {
		case duckDBTableBindingPhysical:
			descriptor.Kind, descriptor.Reference = semantic.RelationPhysical, binding.reference
		case duckDBTableBindingLocal:
			descriptor.Kind, descriptor.LocalName = semantic.RelationLocal, binding.localName
		default:
			fixture.t.Fatal("test relation binding kind is invalid")
		}
		descriptors = append(descriptors, descriptor)
	}
	analyzed, err := semantic.NewStatement(semantic.StatementDescriptor{
		Syntax: statement, ResolvedKind: statement.Kind(), RelationBindings: descriptors,
		ExpressionsComplete: false, OutputColumns: output,
	})
	if err != nil {
		fixture.t.Fatal(err)
	}
	return analyzed
}

func rendererIdentifierPointer(identifier queryast.Identifier) *queryast.Identifier {
	return &identifier
}
