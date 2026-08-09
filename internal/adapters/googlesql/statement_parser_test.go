package googlesql

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

func TestStatementParserMapsPublicGoogleSQLCorpus(t *testing.T) {
	parser := newSyntaxMapper(t)
	tests := []struct {
		name string
		sql  string
		kind queryast.StatementKind
	}{
		{
			name: "select literals function and clauses",
			sql: "SELECT COUNT(*) AS total, TIMESTAMP '2026-08-08 01:02:03+00' AS observed_at, " +
				"STRUCT(3 AS score, 'nested' AS name) AS payload, [1, 2, 3] AS tags " +
				"FROM `test-project.analytics.events` WHERE ordinal >= 1 ORDER BY ordinal DESC LIMIT 2 OFFSET 1",
			kind: queryast.StatementSelect,
		},
		{
			name: "union all",
			sql:  "SELECT 1 AS ordinal UNION ALL SELECT 2 AS ordinal",
			kind: queryast.StatementSelect,
		},
		{
			name: "public query source between predicate",
			sql:  "SELECT id, label, score, active FROM `test-project.analytics.events` WHERE id BETWEEN 2 AND 4",
			kind: queryast.StatementSelect,
		},
		{
			name: "insert values",
			sql: "INSERT INTO `test-project.analytics.events` (ordinal, observed_at, payload, tags) VALUES " +
				"(1, TIMESTAMP '2026-08-08 01:02:03+00', STRUCT(3 AS score, 'nested' AS name), ['alpha', 'beta'])",
			kind: queryast.StatementInsert,
		},
		{
			name: "insert select",
			sql: "INSERT INTO `test-project.analytics.events` (ordinal) " +
				"SELECT 1 AS ordinal UNION ALL SELECT 2 AS ordinal",
			kind: queryast.StatementInsert,
		},
		{
			name: "update",
			sql:  "UPDATE `test-project.analytics.events` AS target SET payload = 'changed', ordinal = ordinal + 1 WHERE ordinal = 1",
			kind: queryast.StatementUpdate,
		},
		{
			name: "delete",
			sql:  "DELETE FROM `test-project.analytics.events` AS target WHERE ordinal = 1",
			kind: queryast.StatementDelete,
		},
		{
			name: "constant-false merge",
			sql: "MERGE `test-project.analytics.destination` " +
				"USING (SELECT * FROM `test-project.analytics.temporary`) ON FALSE " +
				"WHEN NOT MATCHED THEN INSERT ROW WHEN NOT MATCHED BY SOURCE THEN DELETE",
			kind: queryast.StatementMerge,
		},
		{
			name: "general merge assignments",
			sql: "MERGE `test-project.analytics.destination` AS target " +
				"USING `test-project.analytics.source` AS source ON target.id = source.id " +
				"WHEN MATCHED THEN UPDATE SET payload = source.payload " +
				"WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)",
			kind: queryast.StatementMerge,
		},
		{
			name: "partition merge script",
			sql: "DECLARE partitions_to_delete DEFAULT " +
				"(SELECT ARRAY_AGG(DISTINCT(date_trunc(`partition_date`, DAY)) IGNORE NULLS) " +
				"FROM `test-project.analytics.temporary`); " +
				"MERGE `test-project.analytics.destination` AS `target` " +
				"USING `test-project.analytics.temporary` AS `source` ON FALSE " +
				"WHEN NOT MATCHED BY SOURCE AND (TRUE) AND date_trunc(`target`.`partition_date`, DAY) " +
				"IN UNNEST(partitions_to_delete) THEN DELETE " +
				"WHEN NOT MATCHED BY TARGET THEN INSERT(`id`,`partition_date`,`payload`) " +
				"VALUES(`source`.`id`,`source`.`partition_date`,`source`.`payload`)",
			kind: queryast.StatementScript,
		},
		{
			name: "declare and set script",
			sql:  "DECLARE value INT64 DEFAULT 1; SET value = value + 1",
			kind: queryast.StatementScript,
		},
		{
			name: "typed struct and array literals",
			sql:  "SELECT STRUCT<score INT64, name STRING>(3, 'nested'), ARRAY<INT64>[1, 2]",
			kind: queryast.StatementSelect,
		},
		{
			name: "create table",
			sql:  "CREATE TABLE `test-project.analytics.events` (id INT64 NOT NULL, amount NUMERIC(20), payload STRUCT<name STRING>, tags ARRAY<STRING>)",
			kind: queryast.StatementCreateTable,
		},
		{name: "drop table", sql: "DROP TABLE `test-project.analytics.events`", kind: queryast.StatementDropTable},
		{name: "create view", sql: "CREATE VIEW `test-project.analytics.event_ids` AS SELECT id FROM analytics.events", kind: queryast.StatementCreateView},
		{name: "replace view", sql: "CREATE OR REPLACE VIEW `test-project.analytics.event_ids` AS SELECT id FROM analytics.events", kind: queryast.StatementCreateView},
		{name: "drop view", sql: "DROP VIEW `test-project.analytics.event_ids`", kind: queryast.StatementDropView},
		{name: "alter table", sql: "ALTER TABLE `test-project.analytics.events` ADD COLUMN amount NUMERIC(38, 9)", kind: queryast.StatementAlterTable},
		{name: "truncate table", sql: "TRUNCATE TABLE `test-project.analytics.events`", kind: queryast.StatementTruncateTable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement, err := parser.Parse(context.Background(), ports.QueryRequest{ProjectID: "test-project", SQL: tt.sql})
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if statement.Kind() != tt.kind {
				t.Fatalf("kind = %q, want %q", statement.Kind(), tt.kind)
			}
			if statement.SemanticFingerprint() == "" || statement.Source().Digest() == "" {
				t.Fatal("mapped statement omitted stable identity")
			}
		})
	}
}

func TestStatementParserMapsQueryAndDMLStructure(t *testing.T) {
	parser := newSyntaxMapper(t)
	statement, err := parser.Parse(context.Background(), ports.QueryRequest{SQL: "SELECT COUNT(*) AS total FROM `p.d.t` WHERE id >= 1 ORDER BY id DESC LIMIT 2"})
	if err != nil {
		t.Fatal(err)
	}
	selectStatement := statement.(*queryast.SelectStatement)
	query := selectStatement.Query()
	selectBody := query.Body().(*queryast.SelectQuery)
	if len(selectBody.Items()) != 1 || selectBody.Items()[0].Expression().Kind() != queryast.ExpressionFunction {
		t.Fatalf("select items = %#v", selectBody.Items())
	}
	table := selectBody.From().(*queryast.TableRelation)
	if got := strings.Join(table.Path().Segments(), "."); got != "p.d.t" {
		t.Fatalf("table path = %q", got)
	}
	if query.Limit() == nil || *query.Limit() != 2 || len(query.OrderBy()) != 1 {
		t.Fatalf("query clauses = limit:%v order:%#v", query.Limit(), query.OrderBy())
	}

	statement, err = parser.Parse(context.Background(), ports.QueryRequest{SQL: "SELECT 1 UNION ALL SELECT 2"})
	if err != nil {
		t.Fatal(err)
	}
	set := statement.(*queryast.SelectStatement).Query().Body().(*queryast.SetOperationQuery)
	if set.Operator() != queryast.SetUnion || !set.All() {
		t.Fatalf("set operation = %q all=%t", set.Operator(), set.All())
	}

	statement, err = parser.Parse(context.Background(), ports.QueryRequest{SQL: "INSERT INTO `p.d.t` VALUES (TIMESTAMP '2026-08-08 01:02:03+00', STRUCT(1 AS id), [1, 2])"})
	if err != nil {
		t.Fatal(err)
	}
	insert := statement.(*queryast.InsertStatement)
	row := insert.Rows()[0]
	if len(row) != 3 || row[0].Kind() != queryast.ExpressionTemporal || row[1].Kind() != queryast.ExpressionStruct || row[2].Kind() != queryast.ExpressionArray {
		t.Fatalf("insert row = %#v", row)
	}

	statement, err = parser.Parse(context.Background(), ports.QueryRequest{SQL: "UPDATE `p.d.t` SET amount = NUMERIC '1.2500', wide = BIGNUMERIC '1.2e3' WHERE id = 1"})
	if err != nil {
		t.Fatal(err)
	}
	assignments := statement.(*queryast.UpdateStatement).Assignments()
	if len(assignments) != 2 {
		t.Fatalf("assignments = %#v", assignments)
	}
	numeric := assignments[0].Value().(*queryast.DecimalLiteral)
	bigNumeric := assignments[1].Value().(*queryast.DecimalLiteral)
	if numeric.Type() != queryast.TypeNumeric || numeric.CanonicalValue() != "1.25" ||
		bigNumeric.Type() != queryast.TypeBigNumeric || bigNumeric.CanonicalValue() != "1200" {
		t.Fatalf("decimal literals = (%q %q), (%q %q)", numeric.Type(), numeric.CanonicalValue(), bigNumeric.Type(), bigNumeric.CanonicalValue())
	}

	statement, err = parser.Parse(context.Background(), ports.QueryRequest{SQL: "SELECT id FROM `p.d.t` WHERE id NOT BETWEEN 2 AND 4"})
	if err != nil {
		t.Fatal(err)
	}
	between := statement.(*queryast.SelectStatement).Query().Body().(*queryast.SelectQuery).Where().(*queryast.BetweenExpression)
	if !between.Not() || between.Value().Kind() != queryast.ExpressionIdentifier ||
		between.Low().Kind() != queryast.ExpressionInteger || between.High().Kind() != queryast.ExpressionInteger {
		t.Fatalf("NOT BETWEEN expression = %#v", between)
	}
}

func TestStatementParserRejectsBackendSyntaxAndRetainsSQL(t *testing.T) {
	parser := newSyntaxMapper(t)
	for _, sql := range []string{
		"SELECT TIMESTAMPTZ '2042-01-02 03:04:05+00' AS customer_secret",
		"SELECT {'name': 'customer_secret'} AS payload",
	} {
		statement, err := parser.Parse(context.Background(), ports.QueryRequest{SQL: sql})
		if !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("Parse() = (%#v, %v), want invalid", statement, err)
		}
		if !strings.Contains(err.Error(), "customer_secret") || !strings.Contains(err.Error(), sql) {
			t.Fatalf("error omitted submitted SQL: %v", err)
		}
	}
}

func TestStatementParserKeepsExternalASTOwnerAliveDuringMapping(t *testing.T) {
	parser := newSyntaxMapper(t)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				runtime.GC()
			}
		}
	}()
	for index := 0; index < 20; index++ {
		statement, err := parser.Parse(context.Background(), ports.QueryRequest{SQL: "SELECT COUNT(*) FROM `p.d.t` WHERE id >= 1"})
		if err != nil || statement.Kind() != queryast.StatementSelect {
			close(stop)
			<-done
			t.Fatalf("Parse() = (%#v, %v)", statement, err)
		}
	}
	close(stop)
	<-done
}

type syntaxMapper struct{}

func newSyntaxMapper(t testing.TB) syntaxMapper {
	t.Helper()
	if err := initialize(); err != nil {
		t.Fatal(err)
	}
	return syntaxMapper{}
}

func (syntaxMapper) Parse(ctx context.Context, request ports.QueryRequest) (queryast.Statement, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	document, err := parseExternal(request.SQL)
	if err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(document.owner)
	mapper := statementMapper{sourceDigest: document.source.Digest()}
	statements := make([]queryast.Statement, 0, len(document.statements))
	for _, external := range document.statements {
		statement, err := mapper.mapStatement(external)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	if len(statements) == 1 {
		return statements[0], nil
	}
	return queryast.NewScriptStatement(document.source, statements)
}
