package duckdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

func TestExecuteStatementRunsTypedInsertAndSelectPlans(t *testing.T) {
	t.Parallel()
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "source"}

	insertFixture := newRendererASTFixture(t)
	insertTarget := insertFixture.table([]string{"UNRESOLVED_INSERT_MARKER"}, nil)
	insert, err := queryast.NewInsertValuesStatement(
		insertFixture.source(), insertTarget,
		[]queryast.Identifier{insertFixture.identifier("id"), insertFixture.identifier("payload")},
		[][]queryast.Expression{
			{insertFixture.integer("2"), insertFixture.stringLiteral("two")},
			{insertFixture.integer("1"), insertFixture.stringLiteral("one")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	insertStatement := insertFixture.semanticStatement(insert, map[queryast.NodeKey]duckDBTableBinding{
		insertTarget.NodeKey(): insertFixture.physicalBinding(reference),
	})
	insertResult, err := warehouse.ExecuteStatement(ctx, insertStatement)
	if err != nil {
		t.Fatal(err)
	}
	if insertResult.AffectedRows != 2 {
		t.Fatalf("insert affected rows = %d, want 2", insertResult.AffectedRows)
	}

	selectFixture := newRendererASTFixture(t)
	selectTarget := selectFixture.table([]string{"UNRESOLVED_SELECT_MARKER"}, nil)
	id := selectFixture.identifierExpression("id")
	payload := selectFixture.identifierExpression("payload")
	where, err := queryast.NewBinaryExpression(
		selectFixture.key("binary"), ">=", selectFixture.identifierExpression("id"), selectFixture.integer("1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	body, err := queryast.NewSelectQuery(
		false,
		[]queryast.SelectItem{selectFixture.selectItem(id, ""), selectFixture.selectItem(payload, "")},
		selectTarget, where, nil, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	order, err := queryast.NewOrderItem(selectFixture.identifierExpression("id"), queryast.SortAscending, queryast.NullOrderingDefault)
	if err != nil {
		t.Fatal(err)
	}
	query, err := queryast.NewQuery(nil, false, body, []queryast.OrderItem{order}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	selectStatement, err := queryast.NewSelectStatement(selectFixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	analyzedSelect := selectFixture.semanticStatement(selectStatement, map[queryast.NodeKey]duckDBTableBinding{
		selectTarget.NodeKey(): selectFixture.physicalBinding(reference),
	})
	result, err := warehouse.ExecuteStatement(ctx, analyzedSelect)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 || result.Rows[0][0] != int64(1) || result.Rows[0][1] != "one" ||
		result.Rows[1][0] != int64(2) || result.Rows[1][1] != "two" {
		t.Fatalf("typed SELECT result = %#v", result.Rows)
	}
}

func TestExecuteStatementRedactsDuckDBDiagnostics(t *testing.T) {
	t.Parallel()
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	fixture := newRendererASTFixture(t)
	const marker = "SECRET_MISSING_COLUMN_MARKER"
	body := fixture.selectBody(fixture.selectItem(fixture.identifierExpression(marker), ""))
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	_, err = warehouse.ExecuteStatement(ctx, fixture.semanticStatement(statement, nil))
	if !errors.Is(err, domain.ErrBackend) {
		t.Fatalf("execution error = %v, want ErrBackend", err)
	}
	if strings.Contains(err.Error(), marker) || !strings.Contains(err.Error(), duckDBStatementBackendFailureV1) {
		t.Fatalf("backend diagnostic was not safely redacted: %v", err)
	}
}

func TestMaterializeStatementCreatesDestinationFromOneTypedPlan(t *testing.T) {
	t.Parallel()
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	sourceReference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "source"}
	seedStatement := newTypedSourceSeedStatement(t, sourceReference)
	if _, err := warehouse.ExecuteStatement(ctx, seedStatement); err != nil {
		t.Fatal(err)
	}

	fixture := newRendererASTFixture(t)
	source := fixture.table([]string{"UNRESOLVED_SOURCE_MARKER"}, nil)
	where, err := queryast.NewBinaryExpression(
		fixture.key("binary"), "=", fixture.identifierExpression("id"), fixture.integer("2"),
	)
	if err != nil {
		t.Fatal(err)
	}
	body, err := queryast.NewSelectQuery(false, []queryast.SelectItem{
		fixture.selectItem(fixture.identifierExpression("id"), ""),
		fixture.selectItem(fixture.identifierExpression("payload"), ""),
	}, source, where, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	selectStatement, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	analyzed := fixture.semanticStatement(selectStatement, map[queryast.NodeKey]duckDBTableBinding{
		source.NodeKey(): fixture.physicalBinding(sourceReference),
	})
	destinationReference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "created"}
	destination, err := NewStatementDestination(StatementDestinationDescriptor{
		Reference: destinationReference, Exists: false,
		WriteDisposition: domain.WriteEmpty, CreateDisposition: domain.CreateIfNeeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := warehouse.MaterializeStatement(ctx, analyzed, destination)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DestinationCreated || len(result.QueryResult.Rows) != 1 || result.QueryResult.Rows[0][0] != int64(2) {
		t.Fatalf("materialization result = %#v", result)
	}
	physical, err := renderPhysicalTable(destinationReference)
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := warehouse.db.QueryRowContext(ctx, "SELECT count(*) FROM "+physical).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("materialized destination row count = %d, want 1", count)
	}
}

func TestStatementDestinationOwnsCanonicalSchema(t *testing.T) {
	t.Parallel()
	precision := int64(20)
	schema := []domain.Field{{
		Name: "payload", Type: "STRUCT", Mode: "NULLABLE",
		Fields: []domain.Field{{Name: "amount", Type: "NUMERIC", Mode: "NULLABLE", Precision: &precision}},
	}}
	destination, err := NewStatementDestination(StatementDestinationDescriptor{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "destination"},
		Exists:    true, Schema: schema,
		WriteDisposition: domain.WriteAppend, CreateDisposition: domain.CreateNever,
	})
	if err != nil {
		t.Fatal(err)
	}
	schema[0].Fields[0].Name = "mutated"
	precision = 7
	first := destination.Schema()
	first[0].Fields[0].Name = "also-mutated"
	second := destination.Schema()
	if second[0].Fields[0].Name != "amount" || second[0].Fields[0].Precision == nil || *second[0].Fields[0].Precision != 20 {
		t.Fatalf("destination schema was not owned: %#v", second)
	}
}

func newStatementExecutionWarehouse(t *testing.T, ctx context.Context) *Warehouse {
	t.Helper()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "analytics"); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "source",
		Schema: []domain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "payload", Type: "STRING", Mode: "NULLABLE"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return warehouse
}

func newTypedSourceSeedStatement(t *testing.T, reference domain.TableReference) semantic.Statement {
	t.Helper()
	fixture := newRendererASTFixture(t)
	target := fixture.table([]string{"UNRESOLVED_SEED_MARKER"}, nil)
	statement, err := queryast.NewInsertValuesStatement(
		fixture.source(), target,
		[]queryast.Identifier{fixture.identifier("id"), fixture.identifier("payload")},
		[][]queryast.Expression{
			{fixture.integer("1"), fixture.stringLiteral("one")},
			{fixture.integer("2"), fixture.stringLiteral("two")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return fixture.semanticStatement(statement, map[queryast.NodeKey]duckDBTableBinding{
		target.NodeKey(): fixture.physicalBinding(reference),
	})
}
