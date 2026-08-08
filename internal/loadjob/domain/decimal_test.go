package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewJobRejectsUnsupportedSchemaBeforePublication(t *testing.T) {
	precision := int64(39)
	for _, testCase := range []struct {
		name  string
		field Field
	}{
		{name: "decimal precision 39", field: Field{Name: "amount", Type: "BIGNUMERIC", Precision: &precision}},
		{name: "geography", field: Field{Name: "location", Type: "GEOGRAPHY"}},
		{name: "nested geography", field: Field{
			Name: "items", Type: "RECORD", Mode: "REPEATED",
			Fields: []Field{{Name: "location", Type: "GEOGRAPHY"}},
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := NewJob(JobReference{ProjectID: "p", Location: "US", JobID: "j"}, LoadConfiguration{
				SourceURIs:   []string{"gs://bucket/input.parquet"},
				Destination:  TableReference{ProjectID: "p", DatasetID: "d", TableID: "t"},
				SourceFormat: FormatParquet,
				Schema:       []Field{testCase.field},
			}, time.Unix(0, 0))
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("NewJob error = %v, want ErrUnsupported", err)
			}
		})
	}
}

func TestDecimalLoadCapabilityIDsAreStable(t *testing.T) {
	if CapabilityParquetNestedRepeatedV1 != "load.parquet.nested-repeated.unsupported-v1" {
		t.Fatalf("nested/repeated capability = %q", CapabilityParquetNestedRepeatedV1)
	}
	if CapabilityDecimalRoundingV1 != "load.decimal-rounding.unsupported-v1" || !strings.HasSuffix(CapabilityDecimalRoundingV1, "-v1") {
		t.Fatalf("decimal rounding capability = %q", CapabilityDecimalRoundingV1)
	}
}
