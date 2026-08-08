package rest

import (
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestQueryResultRESTPreservesRecursiveDecimalSchemaAndRows(t *testing.T) {
	precision10, scale2 := int64(10), int64(2)
	job := &domain.Job{
		State: domain.JobDone,
		Result: &domain.QueryResult{
			Columns: []domain.Field{
				{Name: "amount", Type: "BIGNUMERIC", Precision: &precision10, Scale: &scale2, RoundingMode: domain.RoundingModeHalfEven},
				{Name: "details", Type: "STRUCT", Fields: []domain.Field{{Name: "nested", Type: "NUMERIC", RoundingMode: domain.RoundingModeHalfAwayFromZero}}},
				{Name: "amounts", Type: "BIGNUMERIC", Mode: "REPEATED"},
			},
			Rows: [][]any{{
				"12345678.90",
				map[string]any{"nested": "1.000000000"},
				[]any{"2.000000000000000000", "3.000000000000000000"},
			}},
		},
	}
	response, err := queryResponseFromDomain(job, 0, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if response.Schema == nil || len(response.Schema.Fields) != 3 {
		t.Fatalf("query schema = %#v", response.Schema)
	}
	decimal := response.Schema.Fields[0]
	if decimal.Type != "BIGNUMERIC" || decimal.Precision == nil || *decimal.Precision != 10 || decimal.Scale == nil || *decimal.Scale != 2 || decimal.RoundingMode != domain.RoundingModeHalfEven {
		t.Fatalf("query decimal schema = %#v", decimal)
	}
	if response.Schema.Fields[1].Fields[0].Type != "NUMERIC" || response.Schema.Fields[1].Fields[0].RoundingMode != domain.RoundingModeHalfAwayFromZero || response.Schema.Fields[2].Mode != "REPEATED" {
		t.Fatalf("recursive query schema = %#v", response.Schema.Fields)
	}
	cells := response.Rows[0].Fields
	if cells[0].Value != "12345678.90" {
		t.Fatalf("decimal row = %#v", cells[0])
	}
	nested := cells[1].Value.(tableRow).Fields
	if nested[0].Value != "1.000000000" {
		t.Fatalf("nested decimal row = %#v", nested)
	}
	repeated := cells[2].Value.([]tableCell)
	if len(repeated) != 2 || repeated[1].Value != "3.000000000000000000" {
		t.Fatalf("repeated decimal row = %#v", repeated)
	}
}

func TestLoadSchemaWireConversionPreservesRecursiveRoundingMode(t *testing.T) {
	wire := []tableFieldSchema{{
		Name: "items", Type: "STRUCT", Mode: "REPEATED", Fields: []tableFieldSchema{{
			Name: "amount", Type: "NUMERIC", RoundingMode: domain.RoundingModeHalfEven,
		}},
	}}
	canonical := loadFieldsFromWire(wire)
	if canonical[0].Fields[0].RoundingMode != domain.RoundingModeHalfEven {
		t.Fatalf("load wire conversion lost rounding mode: %#v", canonical)
	}
	roundTrip := loadFieldsToWire(canonical)
	if roundTrip[0].Fields[0].RoundingMode != domain.RoundingModeHalfEven {
		t.Fatalf("load response conversion lost rounding mode: %#v", roundTrip)
	}
}
