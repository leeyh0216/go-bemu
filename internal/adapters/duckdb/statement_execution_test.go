package duckdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

type statementExecutionCatalog struct {
	snapshot ports.GoogleSQLCatalogSnapshot
}

func (catalog statementExecutionCatalog) GoogleSQLCatalogSnapshot(context.Context) (ports.GoogleSQLCatalogSnapshot, error) {
	return catalog.snapshot, nil
}

func TestExecuteStatementRunsGoogleSQLScriptWithResolvedVariables(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "source"}
	if _, err := warehouse.ExecuteStatement(ctx, newTypedSourceSeedStatement(t, reference)); err != nil {
		t.Fatal(err)
	}
	gateway, err := googlesqladapter.NewGateway(statementExecutionCatalog{snapshot: statementExecutionSnapshot()})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		sql  string
		want [][]any
	}{
		{
			name: "declare set and select",
			sql:  "DECLARE x INT64 DEFAULT 10; SET x = x + 1; SELECT x AS value",
			want: [][]any{{int64(11)}},
		},
		{
			name: "column shadows variable",
			sql:  "DECLARE id INT64 DEFAULT 10; SELECT id + id AS value FROM analytics.source ORDER BY id",
			want: [][]any{{int64(2)}, {int64(4)}},
		},
		{
			name: "unshadowed variable",
			sql:  "DECLARE x INT64 DEFAULT 10; SELECT id + x AS value FROM analytics.source ORDER BY id",
			want: [][]any{{int64(11)}, {int64(12)}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := gateway.Analyze(ctx, ports.QueryRequest{
				ProjectID: "test-project", DefaultDataset: "analytics", SQL: test.sql,
			})
			if err != nil {
				t.Fatalf("Analyze() error = %v", err)
			}
			result, err := warehouse.ExecuteStatement(ctx, statement)
			if err != nil {
				t.Fatalf("ExecuteStatement() error = %v", err)
			}
			if len(result.Rows) != len(test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
			for index := range test.want {
				if len(result.Rows[index]) != 1 || result.Rows[index][0] != test.want[index][0] {
					t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
				}
			}
		})
	}
}

func TestExecuteStatementRunsGenericDeclareMergeScriptAtomically(t *testing.T) {
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
	fields := []domain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "partition_date", Type: "DATE", Mode: "NULLABLE"},
		{Name: "payload", Type: "STRING", Mode: "NULLABLE"},
	}
	for _, tableID := range []string{"temporary", "destination"} {
		if err := warehouse.CreateTable(ctx, domain.Table{
			ProjectID: "test-project", DatasetID: "analytics", ID: tableID,
			Type: "TABLE", Schema: domain.CloneFields(fields),
		}); err != nil {
			t.Fatal(err)
		}
	}
	schema := quoteIdentifier(physicalSchema("test-project", "analytics"))
	if _, err := warehouse.db.ExecContext(ctx,
		"INSERT INTO "+schema+"."+quoteIdentifier("destination")+" VALUES (1, DATE '2024-01-01', 'old'), (2, DATE '2024-01-02', 'keep')",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx,
		"INSERT INTO "+schema+"."+quoteIdentifier("temporary")+" VALUES (3, DATE '2024-01-01', 'new'), (4, NULL, 'unpartitioned')",
	); err != nil {
		t.Fatal(err)
	}

	snapshot := statementExecutionSnapshot()
	snapshot.Projects[0].Datasets[0].Tables = []domain.Table{
		{ProjectID: "test-project", DatasetID: "analytics", ID: "temporary", Type: "TABLE", Schema: domain.CloneFields(fields)},
		{ProjectID: "test-project", DatasetID: "analytics", ID: "destination", Type: "TABLE", Schema: domain.CloneFields(fields)},
	}
	gateway, err := googlesqladapter.NewGateway(statementExecutionCatalog{snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	googleSQL := "DECLARE partitions_to_delete DEFAULT " +
		"(SELECT ARRAY_AGG(DISTINCT(DATE_TRUNC(partition_date, DAY)) IGNORE NULLS) " +
		"FROM `test-project.analytics.temporary`); " +
		"MERGE `test-project.analytics.destination` AS target " +
		"USING `test-project.analytics.temporary` AS source ON FALSE " +
		"WHEN NOT MATCHED BY SOURCE AND DATE_TRUNC(target.partition_date, DAY) " +
		"IN UNNEST(partitions_to_delete) THEN DELETE " +
		"WHEN NOT MATCHED BY TARGET THEN INSERT(id, partition_date, payload) " +
		"VALUES(source.id, source.partition_date, source.payload)"
	statement, err := gateway.Analyze(ctx, ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics", SQL: googleSQL,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	result, err := warehouse.ExecuteStatement(ctx, statement)
	if err != nil {
		t.Fatalf("ExecuteStatement() error = %v", err)
	}
	if result.AffectedRows != 3 {
		t.Fatalf("affected rows = %d, want 3", result.AffectedRows)
	}
	rows, err := warehouse.db.QueryContext(ctx,
		"SELECT id, payload FROM "+schema+"."+quoteIdentifier("destination")+" ORDER BY id",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var observed []string
	for rows.Next() {
		var id int64
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			t.Fatal(err)
		}
		observed = append(observed, fmt.Sprintf("%d:%s", id, payload))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(observed, ",") != "2:keep,3:new,4:unpartitioned" {
		t.Fatalf("destination rows = %#v", observed)
	}
}

func TestExecuteStatementRunsRangePartitionMergeScript(t *testing.T) {
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
	fields := []domain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "partition_id", Type: "INT64", Mode: "NULLABLE"},
		{Name: "payload", Type: "STRING", Mode: "NULLABLE"},
	}
	for _, tableID := range []string{"temporary", "destination"} {
		if err := warehouse.CreateTable(ctx, domain.Table{
			ProjectID: "test-project", DatasetID: "analytics", ID: tableID,
			Type: "TABLE", Schema: domain.CloneFields(fields),
		}); err != nil {
			t.Fatal(err)
		}
	}
	schema := quoteIdentifier(physicalSchema("test-project", "analytics"))
	if _, err := warehouse.db.ExecContext(ctx,
		"INSERT INTO "+schema+"."+quoteIdentifier("destination")+
			" VALUES (1, 5, 'old'), (2, 15, 'keep'), (5, NULL, 'old-null')",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx,
		"INSERT INTO "+schema+"."+quoteIdentifier("temporary")+
			" VALUES (3, 7, 'new'), (4, NULL, 'new-null')",
	); err != nil {
		t.Fatal(err)
	}

	snapshot := statementExecutionSnapshot()
	snapshot.Projects[0].Datasets[0].Tables = []domain.Table{
		{ProjectID: "test-project", DatasetID: "analytics", ID: "temporary", Type: "TABLE", Schema: domain.CloneFields(fields)},
		{ProjectID: "test-project", DatasetID: "analytics", ID: "destination", Type: "TABLE", Schema: domain.CloneFields(fields)},
	}
	gateway, err := googlesqladapter.NewGateway(statementExecutionCatalog{snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	googleSQL := "DECLARE partitions_to_delete DEFAULT " +
		"(SELECT ARRAY_AGG(DISTINCT(IFNULL(IF(partition_id >= 100, 0, " +
		"RANGE_BUCKET(partition_id, GENERATE_ARRAY(0, 100, 10))), -1)) IGNORE NULLS) " +
		"FROM `test-project.analytics.temporary`); " +
		"MERGE `test-project.analytics.destination` AS target " +
		"USING `test-project.analytics.temporary` AS source ON FALSE " +
		"WHEN NOT MATCHED BY SOURCE AND IFNULL(IF(target.partition_id >= 100, 0, " +
		"RANGE_BUCKET(target.partition_id, GENERATE_ARRAY(0, 100, 10))), -1) " +
		"IN UNNEST(partitions_to_delete) THEN DELETE " +
		"WHEN NOT MATCHED BY TARGET THEN INSERT(id, partition_id, payload) " +
		"VALUES(source.id, source.partition_id, source.payload)"
	statement, err := gateway.Analyze(ctx, ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics", SQL: googleSQL,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	result, err := warehouse.ExecuteStatement(ctx, statement)
	if err != nil {
		t.Fatalf("ExecuteStatement() error = %v", err)
	}
	if result.AffectedRows != 4 {
		t.Fatalf("affected rows = %d, want 4", result.AffectedRows)
	}
	rows, err := warehouse.db.QueryContext(ctx,
		"SELECT id, payload FROM "+schema+"."+quoteIdentifier("destination")+" ORDER BY id",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var observed []string
	for rows.Next() {
		var id int64
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			t.Fatal(err)
		}
		observed = append(observed, fmt.Sprintf("%d:%s", id, payload))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(observed, ",") != "2:keep,3:new,4:new-null" {
		t.Fatalf("destination rows = %#v", observed)
	}
}

func TestExecuteStatementRollsBackGoogleSQLScriptOnLaterFailure(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "source"}
	if _, err := warehouse.ExecuteStatement(ctx, newTypedSourceSeedStatement(t, reference)); err != nil {
		t.Fatal(err)
	}
	gateway, err := googlesqladapter.NewGateway(statementExecutionCatalog{snapshot: statementExecutionSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	statement, err := gateway.Analyze(ctx, ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics",
		SQL: "UPDATE analytics.source SET payload = 'changed' WHERE id = 1; " +
			"SELECT CAST('not-an-integer' AS INT64) AS value",
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if _, err := warehouse.ExecuteStatement(ctx, statement); err == nil {
		t.Fatal("ExecuteStatement() unexpectedly succeeded")
	}
	physical, err := renderPhysicalTable(reference)
	if err != nil {
		t.Fatal(err)
	}
	var payload string
	if err := warehouse.db.QueryRowContext(ctx, "SELECT payload FROM "+physical+" WHERE id = 1").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "one" {
		t.Fatalf("payload after rollback = %q", payload)
	}
}

func TestMaterializeStatementRunsGoogleSQLScriptInOneTransaction(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	source := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "source"}
	if _, err := warehouse.ExecuteStatement(ctx, newTypedSourceSeedStatement(t, source)); err != nil {
		t.Fatal(err)
	}
	gateway, err := googlesqladapter.NewGateway(statementExecutionCatalog{snapshot: statementExecutionSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	statement, err := gateway.Analyze(ctx, ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics",
		SQL: "DECLARE increment INT64 DEFAULT 10; SET increment = increment + 1; " +
			"SELECT id + increment AS value FROM analytics.source ORDER BY id",
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	destinationReference := domain.TableReference{
		ProjectID: "test-project", DatasetID: "analytics", TableID: "script_result",
	}
	destination, err := NewStatementDestination(StatementDestinationDescriptor{
		Reference: destinationReference, Exists: false,
		WriteDisposition: domain.WriteEmpty, CreateDisposition: domain.CreateIfNeeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := warehouse.MaterializeStatement(ctx, statement, destination)
	if err != nil {
		t.Fatalf("MaterializeStatement() error = %v", err)
	}
	if !result.DestinationCreated || len(result.QueryResult.Rows) != 2 ||
		result.QueryResult.Rows[0][0] != int64(12) || result.QueryResult.Rows[1][0] != int64(13) {
		t.Fatalf("materialized script result = %#v", result)
	}
	physical, err := renderPhysicalTable(destinationReference)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	if err := warehouse.db.QueryRowContext(ctx, "SELECT sum(value) FROM "+physical).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 25 {
		t.Fatalf("materialized script total = %d, want 25", total)
	}
}

func TestMaterializeStatementRollsBackScriptPrefixWhenPublicationFails(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	source := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "source"}
	if _, err := warehouse.ExecuteStatement(ctx, newTypedSourceSeedStatement(t, source)); err != nil {
		t.Fatal(err)
	}
	destinationReference := domain.TableReference{
		ProjectID: "test-project", DatasetID: "analytics", TableID: "incompatible_result",
	}
	destinationSchema := []domain.Field{{Name: "payload", Type: "STRING", Mode: "NULLABLE"}}
	if err := warehouse.CreateTable(ctx, domain.Table{
		ProjectID: destinationReference.ProjectID, DatasetID: destinationReference.DatasetID,
		ID: destinationReference.TableID, Type: "TABLE", Schema: domain.CloneFields(destinationSchema),
	}); err != nil {
		t.Fatal(err)
	}
	gateway, err := googlesqladapter.NewGateway(statementExecutionCatalog{snapshot: statementExecutionSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	statement, err := gateway.Analyze(ctx, ports.QueryRequest{
		ProjectID: "test-project", DefaultDataset: "analytics",
		SQL: "UPDATE analytics.source SET payload = 'changed' WHERE id = 1; " +
			"SELECT id AS value FROM analytics.source ORDER BY id",
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	destination, err := NewStatementDestination(StatementDestinationDescriptor{
		Reference: destinationReference, Exists: true, Schema: destinationSchema,
		WriteDisposition: domain.WriteAppend, CreateDisposition: domain.CreateNever,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.MaterializeStatement(ctx, statement, destination); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("MaterializeStatement() error = %v, want ErrPrecondition", err)
	}
	sourcePhysical, err := renderPhysicalTable(source)
	if err != nil {
		t.Fatal(err)
	}
	var payload string
	if err := warehouse.db.QueryRowContext(ctx, "SELECT payload FROM "+sourcePhysical+" WHERE id = 1").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "one" {
		t.Fatalf("source payload after failed publication = %q, want rollback", payload)
	}
	destinationPhysical, err := renderPhysicalTable(destinationReference)
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := warehouse.db.QueryRowContext(ctx, "SELECT count(*) FROM "+destinationPhysical).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("destination rows after failed publication = %d, want 0", count)
	}
}

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
	analyzedSelect := selectFixture.semanticStatementWithOutput(selectStatement, map[queryast.NodeKey]duckDBTableBinding{
		selectTarget.NodeKey(): selectFixture.physicalBinding(reference),
	}, []semantic.ColumnDescriptor{
		semanticScalarColumn(t, "id", semantic.TypeInt64),
		semanticScalarColumn(t, "payload", semantic.TypeString),
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

func TestExecuteStatementRunsBetweenPredicate(t *testing.T) {
	t.Parallel()
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "source"}
	if _, err := warehouse.ExecuteStatement(ctx, newTypedSourceSeedStatement(t, reference)); err != nil {
		t.Fatal(err)
	}

	fixture := newRendererASTFixture(t)
	table := fixture.table([]string{"UNRESOLVED_BETWEEN_MARKER"}, nil)
	between, err := queryast.NewBetweenExpression(
		fixture.key("between"), fixture.identifierExpression("id"),
		fixture.integer("2"), fixture.integer("4"), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	body, err := queryast.NewSelectQuery(false, []queryast.SelectItem{
		fixture.selectItem(fixture.identifierExpression("id"), ""),
	}, table, between, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	statement := fixture.semanticStatementWithOutput(syntax, map[queryast.NodeKey]duckDBTableBinding{
		table.NodeKey(): fixture.physicalBinding(reference),
	}, []semantic.ColumnDescriptor{semanticScalarColumn(t, "id", semantic.TypeInt64)})
	result, err := warehouse.ExecuteStatement(ctx, statement)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(2) {
		t.Fatalf("BETWEEN result = %#v", result.Rows)
	}
}

func TestExecuteStatementRetainsDuckDBDiagnostics(t *testing.T) {
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
	_, err = warehouse.ExecuteStatement(ctx, fixture.semanticStatementWithOutput(statement, nil,
		[]semantic.ColumnDescriptor{semanticScalarColumn(t, marker, semantic.TypeString)},
	))
	if !errors.Is(err, domain.ErrBackend) {
		t.Fatalf("execution error = %v, want ErrBackend", err)
	}
	if !strings.Contains(err.Error(), marker) || !strings.Contains(err.Error(), duckDBStatementBackendFailureV1) {
		t.Fatalf("backend diagnostic lost stable code or original cause: %v", err)
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
	analyzed := fixture.semanticStatementWithOutput(selectStatement, map[queryast.NodeKey]duckDBTableBinding{
		source.NodeKey(): fixture.physicalBinding(sourceReference),
	}, []semantic.ColumnDescriptor{
		semanticScalarColumn(t, "id", semantic.TypeInt64),
		semanticScalarColumn(t, "payload", semantic.TypeString),
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

func TestStatementOutputPreservesNestedRepeatedBigNumericIdentity(t *testing.T) {
	t.Parallel()
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	fixture := newRendererASTFixture(t)

	physicalBigNumeric := fixture.scalarType(queryast.TypeBigNumeric, nil, nil)
	first, err := queryast.NewCastExpression(
		fixture.key("cast"), fixture.stringLiteral("1.000000000000000001"), physicalBigNumeric, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := queryast.NewCastExpression(
		fixture.key("cast"), fixture.stringLiteral("2.000000000000000002"), physicalBigNumeric, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	amounts := fixture.arrayLiteral(physicalBigNumeric, first, second)
	payload := fixture.structLiteral([]string{"amounts"}, []queryast.Expression{amounts})
	body := fixture.selectBody(fixture.selectItem(payload, "payload"))
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	logicalBigNumeric := semantic.TypeDescriptor{
		Kind: semantic.TypeBigNumeric, RoundingMode: domain.RoundingModeHalfEven,
	}
	logicalPayload, err := semantic.NewType(semantic.TypeDescriptor{
		Kind: semantic.TypeStruct,
		Fields: []semantic.FieldDescriptor{{
			Name: "amounts",
			Type: semantic.TypeDescriptor{Kind: semantic.TypeArray, Element: &logicalBigNumeric},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	statement := fixture.semanticStatementWithOutput(syntax, nil, []semantic.ColumnDescriptor{{
		Name: "payload", Type: logicalPayload,
	}})

	executed, err := warehouse.ExecuteStatement(ctx, statement)
	if err != nil {
		t.Fatal(err)
	}
	assertNestedRepeatedBigNumericIdentity(t, executed.Columns)

	destination, err := NewStatementDestination(StatementDestinationDescriptor{
		Reference: domain.TableReference{
			ProjectID: "test-project", DatasetID: "analytics", TableID: "nested_decimal_result",
		},
		Exists: false, WriteDisposition: domain.WriteEmpty, CreateDisposition: domain.CreateIfNeeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := warehouse.MaterializeStatement(ctx, statement, destination)
	if err != nil {
		t.Fatal(err)
	}
	assertNestedRepeatedBigNumericIdentity(t, materialized.QueryResult.Columns)
}

func TestExecuteStatementFailsClosedForAnalyzedOutputMismatch(t *testing.T) {
	t.Parallel()
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	fixture := newRendererASTFixture(t)
	body := fixture.selectBody(fixture.selectItem(fixture.integer("1"), "actual_name"))
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	statement := fixture.semanticStatementWithOutput(syntax, nil, []semantic.ColumnDescriptor{
		semanticScalarColumn(t, "different_name", semantic.TypeInt64),
	})
	_, err = warehouse.ExecuteStatement(ctx, statement)
	if !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("output mismatch error = %v, want ErrPrecondition", err)
	}
	for _, marker := range []string{"actual_name", "different_name"} {
		if !strings.Contains(err.Error(), marker) {
			t.Fatalf("output mismatch omitted identifier %q: %v", marker, err)
		}
	}
}

func TestMaterializeStatementUsesCanonicalAnonymousOutputName(t *testing.T) {
	t.Parallel()
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	fixture := newRendererASTFixture(t)
	body := fixture.selectBody(fixture.selectItem(fixture.integer("7"), ""))
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	statement := fixture.semanticStatementWithOutput(syntax, nil, []semantic.ColumnDescriptor{
		semanticScalarColumn(t, "f0_", semantic.TypeInt64),
	})
	destinationReference := domain.TableReference{
		ProjectID: "test-project", DatasetID: "analytics", TableID: "anonymous_result",
	}
	destination, err := NewStatementDestination(StatementDestinationDescriptor{
		Reference: destinationReference, Exists: false,
		WriteDisposition: domain.WriteEmpty, CreateDisposition: domain.CreateIfNeeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := warehouse.MaterializeStatement(ctx, statement, destination)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.QueryResult.Columns) != 1 || result.QueryResult.Columns[0].Name != "f0_" {
		t.Fatalf("canonical anonymous schema = %#v", result.QueryResult.Columns)
	}
	physical, err := renderPhysicalTable(destinationReference)
	if err != nil {
		t.Fatal(err)
	}
	var value int64
	if err := warehouse.db.QueryRowContext(ctx, "SELECT "+quoteIdentifier("f0_")+" FROM "+physical).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 7 {
		t.Fatalf("canonical anonymous value = %d, want 7", value)
	}
}

func TestExecuteStatementBindsExactNumericAndBigNumericLiterals(t *testing.T) {
	t.Parallel()
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse := newStatementExecutionWarehouse(t, ctx)
	fixture := newRendererASTFixture(t)
	const numeric = "12345678901234567890123456789.123456789"
	const bigNumeric = "12345678901234567890.123456789012345678"
	body := fixture.selectBody(
		fixture.selectItem(fixture.decimal(queryast.TypeNumeric, numeric), "numeric_value"),
		fixture.selectItem(fixture.decimal(queryast.TypeBigNumeric, bigNumeric), "bignumeric_value"),
	)
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := queryast.NewSelectStatement(fixture.source(), query)
	if err != nil {
		t.Fatal(err)
	}
	statement := fixture.semanticStatementWithOutput(syntax, nil, []semantic.ColumnDescriptor{
		semanticScalarColumn(t, "numeric_value", semantic.TypeNumeric),
		semanticScalarColumn(t, "bignumeric_value", semantic.TypeBigNumeric),
	})
	plan, err := lowerDuckDBStatement(statement)
	if err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{numeric, bigNumeric} {
		if strings.Contains(plan.statementSQL(), literal) {
			t.Fatalf("decimal literal leaked into generated SQL: %s", plan.statementSQL())
		}
	}
	if want := `SELECT CAST(? AS DECIMAL(38,9)) AS "numeric_value", CAST(? AS DECIMAL(38,18)) AS "bignumeric_value"`; plan.statementSQL() != want {
		t.Fatalf("decimal plan = %q, want %q", plan.statementSQL(), want)
	}

	result, err := warehouse.ExecuteStatement(ctx, statement)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != numeric || result.Rows[0][1] != bigNumeric {
		t.Fatalf("exact decimal result = %#v", result.Rows)
	}
	if len(result.Columns) != 2 || result.Columns[0].Type != "NUMERIC" || result.Columns[1].Type != "BIGNUMERIC" ||
		result.Columns[0].Precision != nil || result.Columns[0].Scale != nil ||
		result.Columns[1].Precision != nil || result.Columns[1].Scale != nil {
		t.Fatalf("exact decimal schema identity = %#v", result.Columns)
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

func statementExecutionSnapshot() ports.GoogleSQLCatalogSnapshot {
	return ports.GoogleSQLCatalogSnapshot{Projects: []ports.GoogleSQLProjectSnapshot{{
		Project: domain.Project{ID: "test-project"},
		Datasets: []ports.GoogleSQLDatasetSnapshot{{
			Dataset: domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"},
			Tables: []domain.Table{{
				ProjectID: "test-project", DatasetID: "analytics", ID: "source", Type: "TABLE",
				Schema: []domain.Field{
					{Name: "id", Type: "INT64", Mode: "REQUIRED"},
					{Name: "payload", Type: "STRING", Mode: "NULLABLE"},
				},
			}},
		}},
	}}}
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

func semanticScalarColumn(t *testing.T, name string, kind semantic.TypeKind) semantic.ColumnDescriptor {
	t.Helper()
	typ, err := semantic.NewType(semantic.TypeDescriptor{Kind: kind})
	if err != nil {
		t.Fatal(err)
	}
	return semantic.ColumnDescriptor{Name: name, Type: typ}
}

func assertNestedRepeatedBigNumericIdentity(t *testing.T, fields []domain.Field) {
	t.Helper()
	if len(fields) != 1 || fields[0].Name != "payload" || fields[0].Type != "RECORD" ||
		fields[0].Mode != "NULLABLE" || len(fields[0].Fields) != 1 {
		t.Fatalf("nested output schema = %#v", fields)
	}
	amounts := fields[0].Fields[0]
	if amounts.Name != "amounts" || amounts.Type != "BIGNUMERIC" || amounts.Mode != "REPEATED" ||
		amounts.Precision != nil || amounts.Scale != nil || amounts.RoundingMode != domain.RoundingModeHalfEven {
		t.Fatalf("repeated BIGNUMERIC identity = %#v", amounts)
	}
}
