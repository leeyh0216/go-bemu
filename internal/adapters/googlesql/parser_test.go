package googlesql_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	parseradapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestParserCreatesBoundedRecursiveSchema(t *testing.T) {
	parser := newParser(t)
	command, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{
		ProjectID: "test-project",
		SQL: `
            /* GoogleSQL owns comments and quoted identifiers. */
			CREATE TABLE ` + "`analytics`.`events`" + ` (
			  id INT64 NOT NULL,
			  amount NUMERIC,
			  exact NUMERIC(38, 9),
			  wide BIGNUMERIC,
			  payload STRUCT<name STRING, scores ARRAY<INT64>>,
			  labels ARRAY<STRING>
			); -- one optional trailing semicolon
		`,
	})
	if err != nil || !matched {
		t.Fatalf("ParseDDL() matched = %t, error = %v", matched, err)
	}
	if command.Kind() != domain.DDLCreateTable {
		t.Fatalf("kind = %q", command.Kind())
	}
	if got, want := command.Table(), (domain.TableReference{
		ProjectID: "test-project", DatasetID: "analytics", TableID: "events",
	}); got != want {
		t.Fatalf("table = %#v, want %#v", got, want)
	}
	schema := command.Schema()
	if len(schema) != 6 {
		t.Fatalf("schema = %#v", schema)
	}
	assertField(t, schema[0], "id", "INT64", "REQUIRED", nil, nil)
	assertField(t, schema[1], "amount", "NUMERIC", "NULLABLE", nil, nil)
	assertField(t, schema[2], "exact", "NUMERIC", "NULLABLE", int64Pointer(38), int64Pointer(9))
	assertField(t, schema[3], "wide", "BIGNUMERIC", "NULLABLE", nil, nil)
	assertEffectiveDecimal(t, schema[1], 38, 9)
	assertEffectiveDecimal(t, schema[3], 38, 18)
	assertField(t, schema[4], "payload", "STRUCT", "NULLABLE", nil, nil)
	assertField(t, schema[4].Fields[1], "scores", "INT64", "REPEATED", nil, nil)
	assertField(t, schema[5], "labels", "STRING", "REPEATED", nil, nil)

	// The command owns its recursive schema.
	schema[4].Fields[0].Name = "changed"
	if command.Schema()[4].Fields[0].Name != "name" {
		t.Fatal("DDL command exposed mutable schema state")
	}
}

func TestParserProducesEverySupportedDDLCommand(t *testing.T) {
	parser := newParser(t)
	tests := []struct {
		name    string
		sql     string
		kind    domain.DDLKind
		nameArg string
		newName string
		field   domain.Field
	}{
		{name: "drop table", sql: "DROP TABLE analytics.events", kind: domain.DDLDropTable},
		{name: "truncate table", sql: "TRUNCATE TABLE analytics.events", kind: domain.DDLTruncateTable},
		{name: "add column", sql: "ALTER TABLE analytics.events ADD COLUMN score NUMERIC(38,9)", kind: domain.DDLAddColumn,
			field: domain.Field{Name: "score", Type: "NUMERIC", Mode: "NULLABLE", Precision: int64Pointer(38), Scale: int64Pointer(9), Fields: []domain.Field{}}},
		{name: "drop column", sql: "ALTER TABLE analytics.events DROP COLUMN score", kind: domain.DDLDropColumn, nameArg: "score"},
		{name: "rename column", sql: "ALTER TABLE analytics.events RENAME COLUMN score TO amount", kind: domain.DDLRenameColumn, nameArg: "score", newName: "amount"},
		{name: "set data type", sql: "ALTER TABLE analytics.events ALTER COLUMN score SET DATA TYPE BIGNUMERIC(38,18)", kind: domain.DDLAlterColumnType,
			nameArg: "score", field: domain.Field{Name: "score", Type: "BIGNUMERIC", Mode: "NULLABLE", Precision: int64Pointer(38), Scale: int64Pointer(18), Fields: []domain.Field{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{
				ProjectID: "test-project", SQL: tt.sql,
			})
			if err != nil || !matched {
				t.Fatalf("ParseDDL() matched = %t, error = %v", matched, err)
			}
			if command.Kind() != tt.kind || command.Name() != tt.nameArg || command.NewName() != tt.newName ||
				!reflect.DeepEqual(command.Field(), tt.field) {
				t.Fatalf("command = kind=%q name=%q new=%q field=%#v", command.Kind(), command.Name(), command.NewName(), command.Field())
			}
			if command.Table() != (domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}) {
				t.Fatalf("table = %#v", command.Table())
			}
		})
	}
}

func TestParserResolvesDefaultAndQualifiedTableReferences(t *testing.T) {
	parser := newParser(t)
	tests := []struct {
		sql     string
		request ports.QueryRequest
		want    domain.TableReference
	}{
		{
			sql:     "DROP TABLE events",
			request: ports.QueryRequest{ProjectID: "request-project", DefaultProjectID: "default-project", DefaultDataset: "analytics"},
			want:    domain.TableReference{ProjectID: "default-project", DatasetID: "analytics", TableID: "events"},
		},
		{
			sql: "DROP TABLE `analytics`.`events`", request: ports.QueryRequest{ProjectID: "request-project"},
			want: domain.TableReference{ProjectID: "request-project", DatasetID: "analytics", TableID: "events"},
		},
		{
			sql: "DROP TABLE `source-project.analytics.events`", request: ports.QueryRequest{ProjectID: "request-project"},
			want: domain.TableReference{ProjectID: "source-project", DatasetID: "analytics", TableID: "events"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			tt.request.SQL = tt.sql
			command, matched, err := parser.ParseDDL(context.Background(), tt.request)
			if err != nil || !matched || command.Table() != tt.want {
				t.Fatalf("ParseDDL() = (%#v, %t, %v), want table %#v", command, matched, err, tt.want)
			}
		})
	}
}

func TestParserRejectsScriptsAndUnsupportedDDLWithoutCommand(t *testing.T) {
	parser := newParser(t)
	tests := []struct {
		name string
		sql  string
		kind error
	}{
		{name: "script", sql: "CREATE TABLE analytics.events (id INT64); DROP TABLE analytics.events", kind: domain.ErrUnsupported},
		{name: "create if not exists", sql: "CREATE TABLE IF NOT EXISTS analytics.events (id INT64)", kind: domain.ErrUnsupported},
		{name: "ctas", sql: "CREATE TABLE analytics.events AS SELECT 1 AS id", kind: domain.ErrUnsupported},
		{name: "partition", sql: "CREATE TABLE analytics.events (id INT64) PARTITION BY id", kind: domain.ErrUnsupported},
		{name: "multiple alter actions", sql: "ALTER TABLE analytics.events ADD COLUMN a INT64, DROP COLUMN b", kind: domain.ErrUnsupported},
		{name: "truncate where", sql: "TRUNCATE TABLE analytics.events WHERE id > 0", kind: domain.ErrUnsupported},
		{name: "geography", sql: "CREATE TABLE analytics.events (place GEOGRAPHY)", kind: domain.ErrUnsupported},
		{name: "precision exceeds Spark", sql: "CREATE TABLE analytics.events (amount BIGNUMERIC(39,18))", kind: domain.ErrUnsupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, _, err := parser.ParseDDL(context.Background(), ports.QueryRequest{ProjectID: "test-project", SQL: tt.sql})
			if !errors.Is(err, tt.kind) {
				t.Fatalf("ParseDDL() error = %v, want %v", err, tt.kind)
			}
			if command.Validate() == nil {
				t.Fatalf("rejection produced a valid command: %#v", command)
			}
		})
	}
}

func TestParserDelegatesOfficialNonDDLAndRejectsEngineOnlySyntax(t *testing.T) {
	parser := newParser(t)
	command, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{SQL: "SELECT 1"})
	if err != nil || matched || command.Validate() == nil {
		t.Fatalf("official non-DDL = (%#v, %t, %v)", command, matched, err)
	}
	for _, sql := range []string{
		"/* execution-engine syntax */ INSERT INTO target VALUES (TIMESTAMPTZ '2026-08-08 01:02:03+00')",
		"-- execution-engine syntax\nUPDATE target SET payload = {'name': 'value'}",
	} {
		command, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{SQL: sql})
		if !errors.Is(err, domain.ErrInvalid) || matched || command.Validate() == nil {
			t.Fatalf("engine-only syntax = (%#v, %t, %v)", command, matched, err)
		}
	}

	const secretSQL = "CREATE TABLE analytics.customer_secret (id ARRAY<"
	_, _, err = parser.ParseDDL(context.Background(), ports.QueryRequest{ProjectID: "test-project", SQL: secretSQL})
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("syntax error = %v", err)
	}
	if !strings.Contains(err.Error(), "customer_secret") || !strings.Contains(err.Error(), secretSQL) {
		t.Fatalf("syntax error omitted SQL: %v", err)
	}
}

func newParser(t *testing.T) *parseradapter.Parser {
	t.Helper()
	parser, err := parseradapter.NewParser()
	if err != nil {
		t.Fatalf("NewParser() error = %v", err)
	}
	return parser
}

func assertField(t *testing.T, got domain.Field, name, typ, mode string, precision, scale *int64) {
	t.Helper()
	if got.Name != name || got.Type != typ || got.Mode != mode ||
		!reflect.DeepEqual(got.Precision, precision) || !reflect.DeepEqual(got.Scale, scale) {
		t.Fatalf("field = %#v", got)
	}
}

func assertEffectiveDecimal(t *testing.T, field domain.Field, precision, scale int64) {
	t.Helper()
	got, err := field.EffectiveDecimalParameters()
	if err != nil || got.Precision != precision || got.Scale != scale {
		t.Fatalf("EffectiveDecimalParameters(%s) = (%#v, %v)", field.Name, got, err)
	}
}

func int64Pointer(value int64) *int64 { return &value }
