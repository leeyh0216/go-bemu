package application

import (
	"errors"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestValidateDDLTypeConversionMatchesBigQueryWideningRules(t *testing.T) {
	precision7, precision8, scale2, scale1 := int64(7), int64(8), int64(2), int64(1)
	tests := []struct {
		name   string
		before domain.Field
		after  domain.Field
		valid  bool
	}{
		{name: "int64 to numeric", before: domain.Field{Type: "INT64"}, after: domain.Field{Type: "NUMERIC"}, valid: true},
		{name: "integer alias to float alias", before: domain.Field{Type: "INTEGER"}, after: domain.Field{Type: "FLOAT"}, valid: true},
		{name: "numeric to bignumeric", before: domain.Field{Type: "NUMERIC"}, after: domain.Field{Type: "BIGNUMERIC"}, valid: true},
		{name: "numeric to float64", before: domain.Field{Type: "NUMERIC"}, after: domain.Field{Type: "FLOAT64"}, valid: true},
		{name: "numeric precision and scale widen", before: domain.Field{Type: "NUMERIC", Precision: &precision7, Scale: &scale2}, after: domain.Field{Type: "NUMERIC", Precision: &precision8, Scale: &scale2}, valid: true},
		{name: "numeric precision narrows", before: domain.Field{Type: "NUMERIC", Precision: &precision8, Scale: &scale2}, after: domain.Field{Type: "NUMERIC", Precision: &precision7, Scale: &scale2}},
		{name: "numeric scale narrows", before: domain.Field{Type: "NUMERIC", Precision: &precision7, Scale: &scale2}, after: domain.Field{Type: "NUMERIC", Precision: &precision7, Scale: &scale1}},
		{name: "bignumeric narrows", before: domain.Field{Type: "BIGNUMERIC"}, after: domain.Field{Type: "NUMERIC"}},
		{name: "string to int64 is not a ddl widening", before: domain.Field{Type: "STRING"}, after: domain.Field{Type: "INT64"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateDDLTypeConversion(test.before, test.after)
			if test.valid {
				if err != nil {
					t.Fatalf("validateDDLTypeConversion() error = %v", err)
				}
				return
			}
			if !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), domain.CapabilitySchemaTypeConversionV1) {
				t.Fatalf("validateDDLTypeConversion() error = %v", err)
			}
		})
	}
}
