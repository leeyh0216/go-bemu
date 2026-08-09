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

func TestParserCreatesNestedSparkBoundedSchema(t *testing.T) {
	parser := newParser(t)
	command, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{
		ProjectID: "test-project",
		SQL: `
            /* comments are parsed by GoogleSQL, not stripped by BQEMU */
			CREATE TABLE ` + "`analytics`.`events`" + ` (
              id INT64 NOT NULL,
              amount NUMERIC,
			  integer_decimal NUMERIC(29),
              exact NUMERIC(38, 9),
              wide BIGNUMERIC(38, 18),
			  wide_default BIGNUMERIC,
              payload STRUCT<name STRING, scores ARRAY<INT64>, flags STRUCT<active BOOL>>,
              labels ARRAY<STRING>
            ); -- optional trailing semicolon
        `,
	})
	if err != nil {
		t.Fatalf("ParseDDL() error = %v", err)
	}
	if !matched {
		t.Fatal("ParseDDL() matched = false")
	}
	if command.Kind != domain.DDLCreateTable {
		t.Fatalf("kind = %q", command.Kind)
	}
	if got, want := command.Table, (domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}); got != want {
		t.Fatalf("table = %#v, want %#v", got, want)
	}
	if len(command.Schema) != 8 {
		t.Fatalf("schema = %#v", command.Schema)
	}
	assertField(t, command.Schema[0], "id", "INT64", "REQUIRED", nil, nil)
	assertField(t, command.Schema[1], "amount", "NUMERIC", "NULLABLE", nil, nil)
	assertField(t, command.Schema[2], "integer_decimal", "NUMERIC", "NULLABLE", int64Pointer(29), nil)
	assertField(t, command.Schema[3], "exact", "NUMERIC", "NULLABLE", int64Pointer(38), int64Pointer(9))
	assertField(t, command.Schema[4], "wide", "BIGNUMERIC", "NULLABLE", int64Pointer(38), int64Pointer(18))
	assertField(t, command.Schema[5], "wide_default", "BIGNUMERIC", "NULLABLE", nil, nil)
	assertEffectiveDecimal(t, command.Schema[1], 38, 9)
	assertEffectiveDecimal(t, command.Schema[2], 29, 0)
	assertEffectiveDecimal(t, command.Schema[5], 38, 18)

	payload := command.Schema[6]
	assertField(t, payload, "payload", "STRUCT", "NULLABLE", nil, nil)
	if len(payload.Fields) != 3 {
		t.Fatalf("payload fields = %#v", payload.Fields)
	}
	assertField(t, payload.Fields[0], "name", "STRING", "NULLABLE", nil, nil)
	assertField(t, payload.Fields[1], "scores", "INT64", "REPEATED", nil, nil)
	assertField(t, payload.Fields[2], "flags", "STRUCT", "NULLABLE", nil, nil)
	assertField(t, payload.Fields[2].Fields[0], "active", "BOOLEAN", "NULLABLE", nil, nil)
	assertField(t, command.Schema[7], "labels", "STRING", "REPEATED", nil, nil)
}

func TestParserLeavesNonDDLDuckDBLiteralsUntouched(t *testing.T) {
	parser := newParser(t)
	_, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{
		SQL: "INSERT INTO events VALUES ({'score': 3}, ['alpha'], TIMESTAMPTZ '2026-08-08 01:02:03+00')",
	})
	if err != nil {
		t.Fatalf("ParseDDL() error = %v", err)
	}
	if matched {
		t.Fatal("ParseDDL() matched non-DDL")
	}
}

func TestParserResolvesOneTwoAndThreePartTableReferences(t *testing.T) {
	parser := newParser(t)
	tests := []struct {
		name    string
		sql     string
		request ports.QueryRequest
		want    domain.TableReference
	}{
		{
			name: "one part",
			sql:  "DROP TABLE `events`",
			request: ports.QueryRequest{
				ProjectID: "request-project", DefaultProjectID: "default-project", DefaultDataset: "analytics",
			},
			want: domain.TableReference{ProjectID: "default-project", DatasetID: "analytics", TableID: "events"},
		},
		{
			name:    "two quoted parts",
			sql:     "DROP TABLE `analytics`.`events`",
			request: ports.QueryRequest{ProjectID: "request-project"},
			want:    domain.TableReference{ProjectID: "request-project", DatasetID: "analytics", TableID: "events"},
		},
		{
			name:    "one quoted three part path",
			sql:     "DROP TABLE `source-project.analytics.events`",
			request: ports.QueryRequest{ProjectID: "request-project"},
			want:    domain.TableReference{ProjectID: "source-project", DatasetID: "analytics", TableID: "events"},
		},
		{
			name:    "three separately quoted parts",
			sql:     "DROP TABLE `source-project`.`analytics`.`events`",
			request: ports.QueryRequest{ProjectID: "request-project"},
			want:    domain.TableReference{ProjectID: "source-project", DatasetID: "analytics", TableID: "events"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.request.SQL = tt.sql
			command, matched, err := parser.ParseDDL(context.Background(), tt.request)
			if err != nil {
				t.Fatalf("ParseDDL() error = %v", err)
			}
			if !matched {
				t.Fatal("ParseDDL() matched = false")
			}
			if command.Table != tt.want {
				t.Fatalf("table = %#v, want %#v", command.Table, tt.want)
			}
		})
	}
}

func TestParserCanonicalizesSupportedScalarTypes(t *testing.T) {
	parser := newParser(t)
	command, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{
		ProjectID: "test-project",
		SQL: `CREATE TABLE analytics.scalars (
			bool_value BOOL,
			integer_value INTEGER,
			float_value FLOAT,
			decimal_value DECIMAL(10,2),
			bigdecimal_value BIGDECIMAL(38,18),
			string_value STRING,
			bytes_value BYTES,
			date_value DATE,
			datetime_value DATETIME,
			time_value TIME,
			timestamp_value TIMESTAMP,
			json_value JSON
		)`,
	})
	if err != nil || !matched {
		t.Fatalf("ParseDDL() matched = %t, error = %v", matched, err)
	}
	wantTypes := []string{
		"BOOLEAN", "INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC", "STRING",
		"BYTES", "DATE", "DATETIME", "TIME", "TIMESTAMP", "JSON",
	}
	if len(command.Schema) != len(wantTypes) {
		t.Fatalf("schema = %#v", command.Schema)
	}
	for index, wantType := range wantTypes {
		if command.Schema[index].Type != wantType || command.Schema[index].Mode != "NULLABLE" {
			t.Fatalf("schema[%d] = %#v, want type %s", index, command.Schema[index], wantType)
		}
	}
}

func TestParserProducesSupportedAlterCommands(t *testing.T) {
	parser := newParser(t)
	tests := []struct {
		name string
		sql  string
		want domain.DDLCommand
	}{
		{
			name: "add nested column",
			sql:  "ALTER TABLE analytics.events ADD COLUMN payload STRUCT<name STRING, values ARRAY<NUMERIC(38,9)>>",
			want: domain.DDLCommand{
				Kind: domain.DDLAddColumn,
				Field: domain.Field{Name: "payload", Type: "STRUCT", Mode: "NULLABLE", Fields: []domain.Field{
					{Name: "name", Type: "STRING", Mode: "NULLABLE"},
					{Name: "values", Type: "NUMERIC", Mode: "REPEATED", Precision: int64Pointer(38), Scale: int64Pointer(9)},
				}},
			},
		},
		{
			name: "drop column",
			sql:  "ALTER TABLE analytics.events DROP COLUMN old_value",
			want: domain.DDLCommand{Kind: domain.DDLDropColumn, Name: "old_value"},
		},
		{
			name: "rename column",
			sql:  "ALTER TABLE analytics.events RENAME COLUMN old_value TO new_value",
			want: domain.DDLCommand{Kind: domain.DDLRenameColumn, Name: "old_value", NewName: "new_value"},
		},
		{
			name: "alter column type",
			sql:  "ALTER TABLE analytics.events ALTER COLUMN amount SET DATA TYPE BIGNUMERIC(38,18)",
			want: domain.DDLCommand{
				Kind: domain.DDLAlterColumnType, Name: "amount",
				Field: domain.Field{Name: "amount", Type: "BIGNUMERIC", Mode: "NULLABLE", Precision: int64Pointer(38), Scale: int64Pointer(18)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{ProjectID: "test-project", SQL: tt.sql})
			if err != nil {
				t.Fatalf("ParseDDL() error = %v", err)
			}
			if !matched {
				t.Fatal("ParseDDL() matched = false")
			}
			want := tt.want
			want.Table = domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
			if !reflect.DeepEqual(command, want) {
				t.Fatalf("command = %#v, want %#v", command, want)
			}
		})
	}
}

func TestParserDoesNotClaimNonDDL(t *testing.T) {
	parser := newParser(t)
	command, matched, err := parser.ParseDDL(context.Background(), ports.QueryRequest{SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("ParseDDL() error = %v", err)
	}
	if matched {
		t.Fatalf("ParseDDL() = (%#v, true), want unmatched", command)
	}
}

func TestParserRejectsUnsupportedDDLBeforeProducingACommand(t *testing.T) {
	parser := newParser(t)
	tests := []struct {
		name string
		sql  string
		kind error
	}{
		{name: "create if not exists", sql: "CREATE TABLE IF NOT EXISTS analytics.events (id INT64)", kind: domain.ErrUnsupported},
		{name: "or replace", sql: "CREATE OR REPLACE TABLE analytics.events (id INT64)", kind: domain.ErrUnsupported},
		{name: "temporary", sql: "CREATE TEMP TABLE analytics.events (id INT64)", kind: domain.ErrUnsupported},
		{name: "ctas", sql: "CREATE TABLE analytics.events AS SELECT 1 AS id", kind: domain.ErrUnsupported},
		{name: "options", sql: "CREATE TABLE analytics.events (id INT64) OPTIONS(description='x')", kind: domain.ErrUnsupported},
		{name: "partition", sql: "CREATE TABLE analytics.events (id INT64) PARTITION BY id", kind: domain.ErrUnsupported},
		{name: "primary key", sql: "CREATE TABLE analytics.events (id INT64 PRIMARY KEY NOT ENFORCED)", kind: domain.ErrUnsupported},
		{name: "default", sql: "CREATE TABLE analytics.events (id INT64 DEFAULT 1)", kind: domain.ErrUnsupported},
		{name: "generated", sql: "CREATE TABLE analytics.events (id INT64 AS (1) STORED)", kind: domain.ErrUnsupported},
		{name: "collate", sql: "CREATE TABLE analytics.events (name STRING COLLATE 'und:ci')", kind: domain.ErrUnsupported},
		{name: "multiple alter actions", sql: "ALTER TABLE analytics.events ADD COLUMN a INT64, DROP COLUMN b", kind: domain.ErrUnsupported},
		{name: "alter options", sql: "ALTER TABLE analytics.events SET OPTIONS(description='x')", kind: domain.ErrUnsupported},
		{name: "drop if exists", sql: "DROP TABLE IF EXISTS analytics.events", kind: domain.ErrUnsupported},
		{name: "create view", sql: "CREATE VIEW analytics.events AS SELECT 1", kind: domain.ErrUnsupported},
		{name: "geography", sql: "CREATE TABLE analytics.events (place GEOGRAPHY)", kind: domain.ErrUnsupported},
		{name: "precision exceeds Spark", sql: "CREATE TABLE analytics.events (amount BIGNUMERIC(39,18))", kind: domain.ErrInvalid},
		{name: "multiple statements", sql: "CREATE TABLE analytics.events (id INT64); DROP TABLE analytics.events", kind: domain.ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, _, err := parser.ParseDDL(context.Background(), ports.QueryRequest{ProjectID: "test-project", SQL: tt.sql})
			if !errors.Is(err, tt.kind) {
				t.Fatalf("ParseDDL() command = %#v, error = %v, want %v", command, err, tt.kind)
			}
			if !reflect.DeepEqual(command, domain.DDLCommand{}) {
				t.Fatalf("ParseDDL() produced command before rejection: %#v", command)
			}
		})
	}
}

func TestParserRedactsSyntaxErrors(t *testing.T) {
	parser := newParser(t)
	const secretSQL = "CREATE TABLE analytics.customer_secret (id ARRAY<"
	_, _, err := parser.ParseDDL(context.Background(), ports.QueryRequest{ProjectID: "test-project", SQL: secretSQL})
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("ParseDDL() error = %v", err)
	}
	if strings.Contains(err.Error(), "customer_secret") || strings.Contains(err.Error(), secretSQL) {
		t.Fatalf("syntax error leaked SQL: %v", err)
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
	if got.Name != name || got.Type != typ || got.Mode != mode || !reflect.DeepEqual(got.Precision, precision) || !reflect.DeepEqual(got.Scale, scale) {
		t.Fatalf("field = %#v, want name=%q type=%q mode=%q precision=%v scale=%v", got, name, typ, mode, precision, scale)
	}
}

func assertEffectiveDecimal(t *testing.T, field domain.Field, precision, scale int64) {
	t.Helper()
	got, err := field.EffectiveDecimalParameters()
	if err != nil || got.Precision != precision || got.Scale != scale {
		t.Fatalf("EffectiveDecimalParameters(%s) = (%#v, %v), want (%d, %d, nil)", field.Name, got, err, precision, scale)
	}
}

func int64Pointer(value int64) *int64 { return &value }
