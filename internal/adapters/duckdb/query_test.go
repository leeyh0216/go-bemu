package duckdb

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	v0442 "github.com/leeyh0216/go-bemu/internal/adapters/sparkbigquery/v0442"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestTranslateSQLDistinguishesRelationsFromQuotedIdentifiers(t *testing.T) {
	physical := quoteIdentifier(physicalSchema("p", "d")) + `."t"`
	crossProjectPhysical := quoteIdentifier(physicalSchema("data-project", "d")) + `."t"`
	tests := []struct {
		name    string
		request ports.QueryRequest
		want    string
	}{
		{
			name:    "quoted column and fully qualified relation",
			request: ports.QueryRequest{SQL: "SELECT `col` FROM `p.d.t`"},
			want:    `SELECT "col" FROM ` + physical,
		},
		{
			name: "reserved column and alias",
			request: ports.QueryRequest{
				SQL: "SELECT source.`select` AS `from` FROM `p.d.t` AS source",
			},
			want: `SELECT source."select" AS "from" FROM ` + physical + ` AS source`,
		},
		{
			name: "backticks in string and comments",
			request: ports.QueryRequest{
				SQL: "SELECT 'literal `not_a_table`' AS value FROM `p.d.t` -- `line_comment`\n/* `block_comment` */",
			},
			want: "SELECT 'literal `not_a_table`' AS value FROM " + physical + " -- `line_comment`\n/* `block_comment` */",
		},
		{
			name: "default dataset relation",
			request: ports.QueryRequest{
				ProjectID: "p", DefaultDataset: "d", SQL: "SELECT * FROM `t`",
			},
			want: "SELECT * FROM " + physical,
		},
		{
			name: "cross-project default dataset relation",
			request: ports.QueryRequest{
				ProjectID: "p", DefaultProjectID: "data-project", DefaultDataset: "d", SQL: "SELECT * FROM `t`",
			},
			want: "SELECT * FROM " + crossProjectPhysical,
		},
		{
			name: "quoted CTE is not a physical table",
			request: ports.QueryRequest{
				ProjectID: "p", DefaultDataset: "d",
				SQL: "WITH `input` AS (SELECT 1 AS `select`) SELECT `select` FROM `input`",
			},
			want: `WITH "input" AS (SELECT 1 AS "select") SELECT "select" FROM "input"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := translateSQL(test.request)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("translation mismatch:\n got: %s\nwant: %s", got, test.want)
			}
		})
	}
}

func TestTranslateSQLRejectsMalformedQuotedInput(t *testing.T) {
	for _, sql := range []string{"SELECT `unterminated", "SELECT 'unterminated", "SELECT 1 /* unterminated"} {
		if _, err := translateSQL(ports.QueryRequest{SQL: sql}); err == nil || !strings.Contains(err.Error(), "invalid") {
			t.Fatalf("expected invalid input error for %q, got %v", sql, err)
		}
	}
}

func TestWarehouseCreateInsertSelectAndMerge(t *testing.T) {
	t.Parallel()
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
	for _, tableID := range []string{"inventory", "incoming"} {
		err := warehouse.CreateTable(ctx, domain.Table{
			ProjectID: "test-project", DatasetID: "analytics", ID: tableID,
			Schema: []domain.Field{
				{Name: "id", Type: "INT64", Mode: "REQUIRED"},
				{Name: "name", Type: "STRING"},
				{Name: "quantity", Type: "INT64"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	execute := func(sql string) domain.QueryResult {
		t.Helper()
		result, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: sql})
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		return result
	}
	execute("INSERT INTO `test-project.analytics.inventory` VALUES (1, 'washer', 10), (2, 'dryer', 4)")
	execute("INSERT INTO `test-project.analytics.incoming` VALUES (1, 'washer', 15), (3, 'oven', 7)")
	execute("MERGE INTO `test-project.analytics.inventory` AS target\n" +
		"USING `test-project.analytics.incoming` AS source\n" +
		"ON target.id = source.id\n" +
		"WHEN MATCHED THEN UPDATE SET name = source.name, quantity = source.quantity\n" +
		"WHEN NOT MATCHED THEN INSERT (id, name, quantity) VALUES (source.id, source.name, source.quantity)")

	result := execute("SELECT id, name, quantity FROM `test-project.analytics.inventory` ORDER BY id")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 merged rows, got %#v", result.Rows)
	}
	assertRow := func(index int, id int64, name string, quantity int64) {
		t.Helper()
		row := result.Rows[index]
		if row[0] != id || row[1] != name || row[2] != quantity {
			t.Fatalf("row %d: got %#v", index, row)
		}
	}
	assertRow(0, 1, "washer", 15)
	assertRow(1, 2, "dryer", 4)
	assertRow(2, 3, "oven", 7)
}

func TestWarehouseExecutesReservedQuotedColumnAndAlias(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "items",
		Schema: []domain.Field{{Name: "select", Type: "STRING"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{SQL: "INSERT INTO `test-project.dataset.items` (`select`) VALUES ('value `kept`')"}); err != nil {
		t.Fatal(err)
	}
	result, err := warehouse.Query(ctx, ports.QueryRequest{SQL: "SELECT `select` AS `from` FROM `test-project.dataset.items`"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Columns) != 1 || result.Columns[0].Name != "from" || len(result.Rows) != 1 || result.Rows[0][0] != "value `kept`" {
		t.Fatalf("unexpected quoted identifier result: %#v", result)
	}
}

func TestWarehouseExecutesSparkConnectorStaticOverwriteAtomically(t *testing.T) {
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
	canonicalTable := func(tableID, idType string) domain.Table {
		return domain.Table{
			ProjectID: "test-project", DatasetID: "analytics", ID: tableID,
			Schema: []domain.Field{
				{Name: "id", Type: idType, Mode: "REQUIRED"},
				{Name: "payload", Type: "STRING"},
			},
		}
	}
	destinationTable := canonicalTable("destination", "INT64")
	sourceTable := canonicalTable("temporary", "INT64")
	for _, table := range []domain.Table{destinationTable, sourceTable} {
		if err := warehouse.CreateTable(ctx, table); err != nil {
			t.Fatal(err)
		}
	}
	query := func(sql string) domain.QueryResult {
		t.Helper()
		result, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: sql})
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		return result
	}
	query("INSERT INTO `test-project.analytics.destination` VALUES (1, 'old'), (2, 'remove')")
	query("INSERT INTO `test-project.analytics.temporary` VALUES (3, 'new'), (4, 'replacement')")

	connectorSQL := "MERGE `test-project.analytics.destination`\n" +
		"USING (SELECT * FROM `test-project.analytics.temporary`)\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED THEN INSERT ROW\n" +
		"WHEN NOT MATCHED BY SOURCE THEN DELETE"
	request := ports.QueryRequest{ProjectID: "test-project", SQL: connectorSQL}
	analyzer, err := v0442.NewAnalyzer(warehouse)
	if err != nil {
		t.Fatal(err)
	}
	if err := analyzer.WithGoogleSQLGateway(duckDBConnectorGateway()); err != nil {
		t.Fatal(err)
	}
	operation, matched, err := analyzer.AnalyzeQueryOperation(ctx, request)
	if err != nil || !matched || operation.ProfileID() != v0442.StaticOverwriteProfile {
		t.Fatalf("static overwrite operation=%#v matched=%t err=%v", operation, matched, err)
	}
	if _, err := warehouse.ExecuteQueryOperation(ctx, request, operation, destinationTable, sourceTable); err != nil {
		t.Fatal(err)
	}

	result := query("SELECT id, payload FROM `test-project.analytics.destination` ORDER BY id")
	if len(result.Rows) != 2 || result.Rows[0][0] != int64(3) || result.Rows[1][0] != int64(4) {
		t.Fatalf("static overwrite result = %#v", result.Rows)
	}

	// A failed replacement must not expose the delete half of the MERGE. The
	// connector relies on one query job being atomic when it swaps the temporary
	// direct-write table into the destination.
	invalidSource := canonicalTable("invalid_temporary", "STRING")
	if err := warehouse.CreateTable(ctx, invalidSource); err != nil {
		t.Fatal(err)
	}
	query("INSERT INTO `test-project.analytics.invalid_temporary` VALUES ('not-an-int', 'invalid')")
	invalidOverwrite := strings.Replace(connectorSQL, "analytics.temporary", "analytics.invalid_temporary", 1)
	invalidRequest := ports.QueryRequest{ProjectID: "test-project", SQL: invalidOverwrite}
	invalidOperation, matched, err := analyzer.AnalyzeQueryOperation(ctx, invalidRequest)
	if err != nil || !matched {
		t.Fatalf("analyze incompatible overwrite: matched=%t err=%v", matched, err)
	}
	if _, err := warehouse.ExecuteQueryOperation(
		ctx, invalidRequest, invalidOperation, destinationTable, invalidSource,
	); !errors.Is(err, domain.ErrInvalidQuery) {
		t.Fatal("expected incompatible replacement source to fail")
	}
	result = query("SELECT id, payload FROM `test-project.analytics.destination` ORDER BY id")
	if len(result.Rows) != 2 || result.Rows[0][0] != int64(3) || result.Rows[1][0] != int64(4) {
		t.Fatalf("failed overwrite changed destination: %#v", result.Rows)
	}

	for name, mutate := range map[string]func(*ports.QueryRequest){
		"SQL": func(changed *ports.QueryRequest) {
			changed.SQL = strings.Replace(connectorSQL, "analytics.temporary", "analytics.invalid_temporary", 1)
		},
		"project":         func(changed *ports.QueryRequest) { changed.ProjectID = "other-project" },
		"default project": func(changed *ports.QueryRequest) { changed.DefaultProjectID = "other-project" },
		"default dataset": func(changed *ports.QueryRequest) { changed.DefaultDataset = "other-dataset" },
	} {
		t.Run("reject changed "+name+" binding", func(t *testing.T) {
			changedRequest := request
			mutate(&changedRequest)
			if _, err := warehouse.ExecuteQueryOperation(
				ctx, changedRequest, operation, destinationTable, sourceTable,
			); !errors.Is(err, domain.ErrPrecondition) {
				t.Fatalf("changed request replay error = %v, want precondition", err)
			}
		})
	}
}

func TestSparkConnectorStaticOverwriteRejectsProfileDrift(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	analyzer, err := v0442.NewAnalyzer(warehouse)
	if err != nil {
		t.Fatal(err)
	}
	if err := analyzer.WithGoogleSQLGateway(duckDBConnectorGateway()); err != nil {
		t.Fatal(err)
	}
	statement := "MERGE `test-project.analytics.destination`\n" +
		"USING (SELECT * FROM `test-project.analytics.temporary`)\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED THEN INSERT ROW\n" +
		"WHEN MATCHED THEN DELETE"
	_, matched, err := analyzer.AnalyzeQueryOperation(ctx, ports.QueryRequest{ProjectID: "project-id", SQL: statement})
	if err == nil || !matched || !strings.Contains(err.Error(), v0442.StaticOverwriteProfile) {
		t.Fatalf("profile drift result: matched=%t err=%v", matched, err)
	}
}

func duckDBQueryTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 10 * time.Second
	if configured := os.Getenv("BQEMU_QUERY_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("BQEMU_QUERY_TEST_TIMEOUT must be a positive Go duration: %q", configured)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}
