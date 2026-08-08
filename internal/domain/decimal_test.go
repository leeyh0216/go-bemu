package domain

import (
	"errors"
	"strings"
	"testing"
)

func decimalPointer(value int64) *int64 { return &value }

func TestEffectiveDecimalParametersUsesEngineCompatiblePolicy(t *testing.T) {
	tests := []struct {
		name  string
		field Field
		want  DecimalParameters
	}{
		{name: "numeric default", field: Field{Name: "amount", Type: "NUMERIC"}, want: DecimalParameters{Precision: 38, Scale: 9}},
		{name: "bignumeric default", field: Field{Name: "amount", Type: "BIGNUMERIC"}, want: DecimalParameters{Precision: 38, Scale: 18}},
		{name: "precision without scale", field: Field{Name: "amount", Type: "NUMERIC", Precision: decimalPointer(20)}, want: DecimalParameters{Precision: 20, Scale: 0}},
		{name: "numeric explicit", field: Field{Name: "amount", Type: "NUMERIC", Precision: decimalPointer(38), Scale: decimalPointer(9)}, want: DecimalParameters{Precision: 38, Scale: 9}},
		{name: "bignumeric engine boundary", field: Field{Name: "amount", Type: "BIGNUMERIC", Precision: decimalPointer(38), Scale: decimalPointer(38)}, want: DecimalParameters{Precision: 38, Scale: 38}},
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
	if !errors.Is(tooWide, ErrUnsupported) || !strings.Contains(tooWide.Error(), CapabilityDecimalPrecision38V1) {
		t.Fatalf("precision error = %v, want stable unsupported capability", tooWide)
	}
	geography := (Field{Name: "location", Type: "GEOGRAPHY"}).Validate()
	if !errors.Is(geography, ErrUnsupported) || !strings.Contains(geography.Error(), GapGeographyUnsupportedV1) {
		t.Fatalf("GEOGRAPHY error = %v, want stable unsupported capability", geography)
	}
}

func TestFieldValidationRejectsNestedFieldsOnScalars(t *testing.T) {
	field := Field{
		Name: "amount", Type: "NUMERIC",
		Fields: []Field{{Name: "unexpected", Type: "STRING"}},
	}
	err := field.Validate()
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "must not define nested fields") {
		t.Fatalf("scalar nested-field error = %v", err)
	}
}

func TestNormalizeDecimalValueUsesBigQueryRoundingModesExactly(t *testing.T) {
	precision, scale := int64(5), int64(2)
	tests := []struct {
		name  string
		mode  RoundingMode
		input string
		want  string
	}{
		{name: "omitted positive tie", input: "1.025", want: "1.03"},
		{name: "omitted negative tie", input: "-1.025", want: "-1.03"},
		{name: "unspecified uses default", mode: RoundingModeUnspecified, input: "2.345", want: "2.35"},
		{name: "explicit away", mode: RoundingModeHalfAwayFromZero, input: "-2.345", want: "-2.35"},
		{name: "even positive even tie", mode: RoundingModeHalfEven, input: "1.025", want: "1.02"},
		{name: "even negative even tie", mode: RoundingModeHalfEven, input: "-1.025", want: "-1.02"},
		{name: "even positive odd tie", mode: RoundingModeHalfEven, input: "1.035", want: "1.04"},
		{name: "even negative odd tie", mode: RoundingModeHalfEven, input: "-1.035", want: "-1.04"},
		{name: "base ten exponent", mode: RoundingModeHalfEven, input: "+1234e-3", want: "1.23"},
		{name: "fixed scale zero", input: "0", want: "0.00"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field := Field{Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale, RoundingMode: test.mode}
			got, err := field.NormalizeDecimalValue(test.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("normalized value = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNormalizeDecimalValueRejectsGrammarAndPostRoundOverflow(t *testing.T) {
	precision, scale := int64(3), int64(2)
	field := Field{Name: "amount", Type: "NUMERIC", Precision: &precision, Scale: &scale}
	for _, value := range []string{"1/2", "0x10", "0b10", "0o10", "0x1p2", "1_000", "NaN", "Inf", " 1.0"} {
		if _, err := field.NormalizeDecimalValue(value); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), DecimalValueInvalidV1) || !strings.Contains(err.Error(), value) {
			t.Fatalf("grammar error for %q = %v", value, err)
		}
	}
	for _, value := range []string{"9.995", "-9.995"} {
		if _, err := field.NormalizeDecimalValue(value); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), DecimalValueOverflowV1) || !strings.Contains(err.Error(), value) {
			t.Fatalf("post-round overflow for %q = %v", value, err)
		}
	}
}

func TestFieldValidationPreservesTypedRoundingModeContract(t *testing.T) {
	for _, mode := range []RoundingMode{"", RoundingModeUnspecified, RoundingModeHalfAwayFromZero, RoundingModeHalfEven} {
		if err := (Field{Name: "amount", Type: "NUMERIC", RoundingMode: mode}).Validate(); err != nil {
			t.Fatalf("rounding mode %q: %v", mode, err)
		}
	}
	if err := (Field{Name: "amount", Type: "NUMERIC", RoundingMode: "ROUND_DOWN"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid decimal rounding mode = %v", err)
	}
	if err := (Field{Name: "label", Type: "STRING", RoundingMode: RoundingModeHalfEven}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-decimal rounding mode = %v", err)
	}
	original := []Field{{Name: "payload", Type: "STRUCT", Fields: []Field{{Name: "amount", Type: "NUMERIC", RoundingMode: RoundingModeHalfEven}}}}
	clone := CloneFields(original)
	if clone[0].Fields[0].RoundingMode != RoundingModeHalfEven {
		t.Fatalf("clone lost rounding mode: %#v", clone)
	}
}

func TestTableDefaultRoundingCapabilityIDIsStable(t *testing.T) {
	if GapTableDefaultRoundingV1 != "schema.table-default-rounding-mode.unsupported-v1" {
		t.Fatalf("table default rounding capability = %q", GapTableDefaultRoundingV1)
	}
}
