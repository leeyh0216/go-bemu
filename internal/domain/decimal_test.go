package domain

import (
	"errors"
	"strings"
	"testing"
)

func decimalPointer(value int64) *int64 { return &value }

func TestEffectiveDecimalParametersUsesSparkCompatiblePolicy(t *testing.T) {
	tests := []struct {
		name  string
		field Field
		want  DecimalParameters
	}{
		{name: "numeric default", field: Field{Name: "amount", Type: "NUMERIC"}, want: DecimalParameters{Precision: 38, Scale: 9}},
		{name: "bignumeric default", field: Field{Name: "amount", Type: "BIGNUMERIC"}, want: DecimalParameters{Precision: 38, Scale: 18}},
		{name: "precision without scale", field: Field{Name: "amount", Type: "NUMERIC", Precision: decimalPointer(20)}, want: DecimalParameters{Precision: 20, Scale: 0}},
		{name: "numeric explicit", field: Field{Name: "amount", Type: "NUMERIC", Precision: decimalPointer(38), Scale: decimalPointer(9)}, want: DecimalParameters{Precision: 38, Scale: 9}},
		{name: "bignumeric Spark boundary", field: Field{Name: "amount", Type: "BIGNUMERIC", Precision: decimalPointer(38), Scale: decimalPointer(38)}, want: DecimalParameters{Precision: 38, Scale: 38}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.field.EffectiveDecimalParameters()
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("parameters = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestFieldValidationRejectsUnsupportedDecimalParameters(t *testing.T) {
	tests := []struct {
		name  string
		field Field
		part  string
	}{
		{name: "negative precision", field: Field{Name: "amount", Type: "NUMERIC", Precision: decimalPointer(-1)}, part: "must be positive"},
		{name: "negative scale", field: Field{Name: "amount", Type: "BIGNUMERIC", Precision: decimalPointer(38), Scale: decimalPointer(-1)}, part: "between 0 and 38"},
		{name: "scale exceeds precision", field: Field{Name: "amount", Type: "BIGNUMERIC", Precision: decimalPointer(10), Scale: decimalPointer(11)}, part: "must not exceed precision"},
		{name: "numeric scale exceeds nine", field: Field{Name: "amount", Type: "NUMERIC", Precision: decimalPointer(38), Scale: decimalPointer(10)}, part: "must not exceed 9"},
		{name: "numeric integer digits exceed twenty nine", field: Field{Name: "amount", Type: "NUMERIC", Precision: decimalPointer(30), Scale: decimalPointer(0)}, part: "integer digits must not exceed 29"},
		{name: "scale without precision", field: Field{Name: "amount", Type: "BIGNUMERIC", Scale: decimalPointer(2)}, part: "without precision"},
		{name: "parameters on non-decimal", field: Field{Name: "value", Type: "STRING", Precision: decimalPointer(10)}, part: "valid only"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.field.Validate()
			if err == nil || !strings.Contains(err.Error(), test.part) {
				t.Fatalf("error = %v, want containing %q", err, test.part)
			}
		})
	}
}

func TestFieldValidationClassifiesDecimalPrecisionAndGeographyAsUnsupported(t *testing.T) {
	tooWide := (Field{Name: "amount", Type: "BIGNUMERIC", Precision: decimalPointer(39)}).Validate()
	if !errors.Is(tooWide, ErrUnsupported) || !strings.Contains(tooWide.Error(), CapabilitySparkDecimal38V1) {
		t.Fatalf("precision error = %v, want stable unsupported capability", tooWide)
	}
	geography := (Field{Name: "location", Type: "GEOGRAPHY"}).Validate()
	if !errors.Is(geography, ErrUnsupported) || !strings.Contains(geography.Error(), GapGeographyUnsupportedV1) {
		t.Fatalf("GEOGRAPHY error = %v, want stable unsupported capability", geography)
	}
}

func TestValidateDecimalValueRejectsRoundingAndOverflow(t *testing.T) {
	field := Field{Name: "amount", Type: "BIGNUMERIC", Precision: decimalPointer(5), Scale: decimalPointer(2)}
	for _, value := range []string{"123.45", "-0.01", "0"} {
		if err := field.ValidateDecimalValue(value); err != nil {
			t.Fatalf("valid decimal %q: %v", value, err)
		}
	}
	for _, value := range []string{"1234.56", "1.001", "not-decimal"} {
		if err := field.ValidateDecimalValue(value); err == nil {
			t.Fatalf("invalid decimal %q was accepted", value)
		}
	}
}
