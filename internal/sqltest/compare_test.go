package sqltest

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestCompareCanonicalizesTypedNestedRows(t *testing.T) {
	precision, scale := int64(20), int64(4)
	rounding := string(domain.RoundingModeHalfEven)
	fields := []Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "amount", Type: "NUMERIC", Mode: "NULLABLE", Precision: &precision, Scale: &scale, RoundingMode: &rounding},
		{Name: "raw", Type: "BYTES", Mode: "NULLABLE"},
		{Name: "seen_at", Type: "TIMESTAMP", Mode: "NULLABLE"},
		{Name: "items", Type: "RECORD", Mode: "REPEATED", Fields: []Field{{Name: "name", Type: "STRING", Mode: "NULLABLE"}}},
	}
	test := Case{
		ID: "canonical-values", RowOrder: RowOrderUnordered,
		Expected: Expected{Kind: ExpectedRows, Schema: fields, Rows: [][]any{
			{json.Number("2"), "1.2000", "Yg==", "2026-08-08T00:00:00Z", []any{map[string]any{"name": "second"}}},
			{json.Number("1"), "2.5000", "YQ==", "2026-08-08T00:00:01Z", []any{map[string]any{"name": "first"}}},
		}},
	}
	actual := &domain.QueryResult{
		Columns: fixtureFieldsToDomain(fields),
		Rows: [][]any{
			{int64(1), "2.5", []byte("a"), time.Date(2026, 8, 8, 0, 0, 1, 0, time.UTC), []map[string]any{{"name": "first"}}},
			{int64(2), "1.2", []byte("b"), int64(1786147200000000), []map[string]any{{"name": "second"}}},
		},
	}
	if err := Compare(test, Outcome{Result: actual}); err != nil {
		t.Fatal(err)
	}
}

func TestCompareReportsFirstSchemaRowAndOrderingDifference(t *testing.T) {
	field := Field{Name: "id", Type: "INT64", Mode: "REQUIRED"}
	test := Case{
		ID: "ordered", RowOrder: RowOrderOrdered,
		Expected: Expected{Kind: ExpectedRows, Schema: []Field{field}, Rows: [][]any{{json.Number("1")}, {json.Number("2")}}},
	}
	wrongSchema := &domain.QueryResult{Columns: []domain.Field{{Name: "other", Type: "INT64", Mode: "REQUIRED"}}}
	if err := Compare(test, Outcome{Result: wrongSchema}); err == nil || !strings.Contains(err.Error(), `schema[0].name = "other"`) {
		t.Fatalf("schema error = %v", err)
	}
	wrongOrder := &domain.QueryResult{
		Columns: fixtureFieldsToDomain(test.Expected.Schema), Rows: [][]any{{int64(2)}, {int64(1)}},
	}
	if err := Compare(test, Outcome{Result: wrongOrder}); err == nil || !strings.Contains(err.Error(), "row[0]") {
		t.Fatalf("row error = %v", err)
	}
}

func TestCompareHandlesAffectedRowsAndStableErrors(t *testing.T) {
	affected := int64(2)
	test := Case{ID: "affected", Expected: Expected{Kind: ExpectedAffected, AffectedRows: &affected}}
	if err := Compare(test, Outcome{Result: &domain.QueryResult{AffectedRows: 2}}); err != nil {
		t.Fatal(err)
	}
	test = Case{ID: "invalid", Expected: Expected{Kind: ExpectedError, Error: &ExpectedFailure{Phase: ErrorPhaseAnalyze, Code: "invalid-query"}}}
	if err := Compare(test, Outcome{Failure: &Failure{Phase: ErrorPhaseAnalyze, Code: "invalid-query"}}); err != nil {
		t.Fatal(err)
	}
}

func TestCompareValidatesCatalogAndPhysicalTablePostconditions(t *testing.T) {
	affected := int64(1)
	field := Field{Name: "id", Type: "INT64", Mode: "REQUIRED"}
	expectedTable := ExpectedTable{
		ProjectID: "p", DatasetID: "d", TableID: "t", Exists: true,
		RowOrder: RowOrderOrdered, Schema: []Field{field}, Rows: [][]any{{json.Number("1")}},
	}
	test := Case{ID: "postcondition", Expected: Expected{
		Kind: ExpectedAffected, AffectedRows: &affected, Tables: []ExpectedTable{expectedTable},
	}}
	outcome := Outcome{
		Result: &domain.QueryResult{AffectedRows: 1},
		Tables: map[string]TableOutcome{
			tableOutcomeKey("p", "d", "t"): {
				Exists: true, Schema: fixtureFieldsToDomain(expectedTable.Schema), Rows: [][]any{{int64(1)}},
			},
		},
	}
	if err := Compare(test, outcome); err != nil {
		t.Fatal(err)
	}
	outcome.Tables[tableOutcomeKey("p", "d", "t")] = TableOutcome{
		Exists: true, Schema: fixtureFieldsToDomain(expectedTable.Schema), Rows: [][]any{{int64(2)}},
	}
	if err := Compare(test, outcome); err == nil || !strings.Contains(err.Error(), "table p.d.t row[0]") {
		t.Fatalf("table row error = %v", err)
	}
}
