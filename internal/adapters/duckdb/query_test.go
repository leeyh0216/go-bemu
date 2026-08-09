package duckdb

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

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

func TestLowerQueryParametersBindsNamedAndPositionalTokens(t *testing.T) {
	tests := []struct {
		name, sql  string
		mode       domain.QueryParameterMode
		parameters []domain.QueryParameter
		want       string
		wantArgs   []any
	}{
		{"named", "SELECT @id, '@id' -- @id", domain.QueryParameterNamed, []domain.QueryParameter{{Name: "id", Type: "INT64", Value: "42"}}, "SELECT ?, '@id' -- @id", []any{int64(42)}},
		{"positional", "SELECT ? + ?", domain.QueryParameterPositional, []domain.QueryParameter{{Type: "INT64", Value: "2"}, {Type: "INT64", Value: "3"}}, "SELECT ? + ?", []any{int64(2), int64(3)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, args, err := lowerQueryParameters(test.sql, ports.QueryRequest{ParameterMode: test.mode, QueryParameters: test.parameters})
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want || !reflect.DeepEqual(args, test.wantArgs) {
				t.Fatalf("got %q %#v, want %q %#v", got, args, test.want, test.wantArgs)
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
	if err := warehouse.CreateDataset(ctx, "p", "d"); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.CreateTable(ctx, domain.Table{
		ProjectID: "p", DatasetID: "d", ID: "t",
		Schema: []domain.Field{{Name: "select", Type: "STRING"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{SQL: "INSERT INTO `p.d.t` (`select`) VALUES ('value `kept`')"}); err != nil {
		t.Fatal(err)
	}
	result, err := warehouse.Query(ctx, ports.QueryRequest{SQL: "SELECT `select` AS `from` FROM `p.d.t`"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Columns) != 1 || result.Columns[0].Name != "from" || len(result.Rows) != 1 || result.Rows[0][0] != "value `kept`" {
		t.Fatalf("unexpected quoted identifier result: %#v", result)
	}
}

func TestWarehouseBindsGoogleSQLQueryParameters(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	for _, test := range []struct {
		name    string
		request ports.QueryRequest
		want    []any
	}{
		{
			name: "named parameters are bound without interpolation",
			request: ports.QueryRequest{
				SQL: "SELECT @id AS id, @name AS name", ParameterMode: domain.QueryParameterNamed,
				QueryParameters: []domain.QueryParameter{
					{Name: "id", Type: "INT64", Value: "7"},
					{Name: "name", Type: "STRING", Value: "safe ' value"},
				},
			},
			want: []any{int64(7), "safe ' value"},
		},
		{
			name: "positional parameters are bound in occurrence order",
			request: ports.QueryRequest{
				SQL: "SELECT ? AS enabled, ? AS amount", ParameterMode: domain.QueryParameterPositional,
				QueryParameters: []domain.QueryParameter{
					{Type: "BOOL", Value: "true"}, {Type: "FLOAT64", Value: "2.5"},
				},
			},
			want: []any{true, 2.5},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := warehouse.Query(ctx, test.request)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 || !reflect.DeepEqual(result.Rows[0], test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestLowerQueryParametersRejectsMissingOrUnusedTokensOutsideLiteralsAndComments(t *testing.T) {
	_, _, err := lowerQueryParameters("SELECT '@id' -- @id", ports.QueryRequest{
		ParameterMode:   domain.QueryParameterNamed,
		QueryParameters: []domain.QueryParameter{{Name: "id", Type: "STRING", Value: "value"}},
	})
	if !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "unused") {
		t.Fatalf("literal/comment-only parameter error = %v", err)
	}
	_, _, err = lowerQueryParameters("SELECT @missing", ports.QueryRequest{
		ParameterMode:   domain.QueryParameterNamed,
		QueryParameters: []domain.QueryParameter{{Name: "id", Type: "STRING", Value: "value"}},
	})
	if !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing parameter error = %v", err)
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
	for _, tableID := range []string{"destination", "temporary"} {
		if err := warehouse.CreateTable(ctx, domain.Table{
			ProjectID: "test-project", DatasetID: "analytics", ID: tableID,
			Schema: []domain.Field{
				{Name: "id", Type: "INT64", Mode: "REQUIRED"},
				{Name: "payload", Type: "STRING"},
			},
		}); err != nil {
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
	translated, model, err := translateSQLWithModel(ports.QueryRequest{ProjectID: "test-project", SQL: connectorSQL})
	if err != nil {
		t.Fatal(err)
	}
	if model != sparkStaticOverwriteModel || !strings.Contains(translated, "INSERT BY NAME") {
		t.Fatalf("adapter model=%q SQL=%q", model, translated)
	}
	query(connectorSQL)

	result := query("SELECT id, payload FROM `test-project.analytics.destination` ORDER BY id")
	if len(result.Rows) != 2 || result.Rows[0][0] != int64(3) || result.Rows[1][0] != int64(4) {
		t.Fatalf("static overwrite result = %#v", result.Rows)
	}

	// A failed replacement must not expose the delete half of the MERGE. The
	// connector relies on one query job being atomic when it swaps the temporary
	// direct-write table into the destination.
	if err := warehouse.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "invalid_temporary",
		Schema: []domain.Field{
			{Name: "id", Type: "STRING", Mode: "REQUIRED"},
			{Name: "payload", Type: "STRING"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	query("INSERT INTO `test-project.analytics.invalid_temporary` VALUES ('not-an-int', 'invalid')")
	invalidOverwrite := strings.Replace(connectorSQL, "analytics.temporary", "analytics.invalid_temporary", 1)
	if _, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: invalidOverwrite}); err == nil {
		t.Fatal("expected incompatible replacement source to fail")
	}
	result = query("SELECT id, payload FROM `test-project.analytics.destination` ORDER BY id")
	if len(result.Rows) != 2 || result.Rows[0][0] != int64(3) || result.Rows[1][0] != int64(4) {
		t.Fatalf("failed overwrite changed destination: %#v", result.Rows)
	}
}

func TestSparkConnectorStaticOverwriteRejectsProfileDrift(t *testing.T) {
	statement := "MERGE `p.d.target`\n" +
		"USING (SELECT * FROM `p.d.source`)\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED THEN INSERT ROW\n" +
		"WHEN MATCHED THEN DELETE"
	_, model, err := translateSQLWithModel(ports.QueryRequest{SQL: statement})
	if err == nil || model != sparkStaticOverwriteModel || !strings.Contains(err.Error(), sparkStaticOverwriteModel) {
		t.Fatalf("profile drift result: model=%q err=%v", model, err)
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
