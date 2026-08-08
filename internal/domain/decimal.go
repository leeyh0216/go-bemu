package domain

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

const (
	SparkDecimalMaxPrecision int64 = 38
	SparkDecimalMaxScale     int64 = 38

	CapabilitySparkDecimal38V1 = "schema.decimal.spark-precision-38-v1"
	CapabilityEngineSchemaV1   = "schema.engine-capabilities-v1"
	GapQueryDecimalLineageV1   = "query.results.decimal-lineage-v1"
	GapGeographyUnsupportedV1  = "schema.geography.unsupported-v1"
	DecimalValueInvalidV1      = "decimal.value.invalid-v1"
	DecimalValueOverflowV1     = "decimal.value.overflow-v1"
	GapTableDefaultRoundingV1  = "schema.table-default-rounding-mode.unsupported-v1"
)

// RoundingMode is the canonical BigQuery roundingMode enum. Its zero value is
// intentionally reserved for an omitted field so persistence can distinguish
// omission from an explicitly supplied enum value.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/RoundingMode
type RoundingMode string

const (
	RoundingModeUnspecified      RoundingMode = "ROUNDING_MODE_UNSPECIFIED"
	RoundingModeHalfAwayFromZero RoundingMode = "ROUND_HALF_AWAY_FROM_ZERO"
	RoundingModeHalfEven         RoundingMode = "ROUND_HALF_EVEN"
)

var decimalLiteralPattern = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

// DecimalParameters is the effective decimal shape supplied to an engine or
// wire codec. Field.Precision and Field.Scale remain unchanged so canonical
// metadata distinguishes omitted parameters from explicit values.
type DecimalParameters struct {
	Precision int64
	Scale     int64
}

// EffectiveRoundingMode applies BigQuery's default without changing the
// canonical omission state stored in Field.RoundingMode.
func (f Field) EffectiveRoundingMode() (RoundingMode, error) {
	if fieldType := strings.ToUpper(f.Type); fieldType != "NUMERIC" && fieldType != "BIGNUMERIC" {
		return "", fmt.Errorf("%w: field %q is not a decimal type", ErrInvalid, f.Name)
	}
	switch f.RoundingMode {
	case "", RoundingModeUnspecified, RoundingModeHalfAwayFromZero:
		return RoundingModeHalfAwayFromZero, nil
	case RoundingModeHalfEven:
		return RoundingModeHalfEven, nil
	default:
		return "", fmt.Errorf("%w: decimal field %q has unsupported rounding mode %q", ErrInvalid, f.Name, f.RoundingMode)
	}
}

// NormalizeDecimalValue parses only BigQuery's base-10 decimal literal
// grammar, rounds exactly to the field scale, verifies post-round precision,
// and returns a deterministic fixed-scale decimal string.
func (f Field) NormalizeDecimalValue(input string) (string, error) {
	parameters, err := f.EffectiveDecimalParameters()
	if err != nil {
		return "", err
	}
	roundingMode, err := f.EffectiveRoundingMode()
	if err != nil {
		return "", err
	}
	if !decimalLiteralPattern.MatchString(input) {
		return "", decimalValueError(f, DecimalValueInvalidV1, len(input), "does not use the supported base-10 grammar")
	}
	rational, ok := new(big.Rat).SetString(input)
	if !ok {
		return "", decimalValueError(f, DecimalValueInvalidV1, len(input), "cannot be parsed as a base-10 decimal")
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(parameters.Scale), nil)
	scaled := new(big.Rat).Mul(rational, new(big.Rat).SetInt(factor))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(scaled.Num(), scaled.Denom(), remainder)
	if remainder.Sign() != 0 {
		twiceRemainder := new(big.Int).Lsh(new(big.Int).Abs(remainder), 1)
		comparison := twiceRemainder.Cmp(scaled.Denom())
		increment := comparison > 0
		if comparison == 0 {
			increment = roundingMode == RoundingModeHalfAwayFromZero || quotient.Bit(0) == 1
		}
		if increment {
			if scaled.Sign() < 0 {
				quotient.Sub(quotient, big.NewInt(1))
			} else {
				quotient.Add(quotient, big.NewInt(1))
			}
		}
	}
	digits := len(new(big.Int).Abs(quotient).String())
	if quotient.Sign() == 0 {
		digits = 1
	}
	if int64(digits) > parameters.Precision {
		return "", decimalValueError(f, DecimalValueOverflowV1, len(input), fmt.Sprintf("exceeds precision %d after rounding", parameters.Precision))
	}
	return formatScaledDecimal(quotient, parameters.Scale), nil
}

func decimalValueError(field Field, code string, valueBytes int, reason string) error {
	return fmt.Errorf("%w: code=%s decimal field %q value_bytes=%d %s", ErrInvalid, code, field.Name, valueBytes, reason)
}

func formatScaledDecimal(value *big.Int, scale int64) string {
	negative := value.Sign() < 0
	digits := new(big.Int).Abs(value).String()
	if scale > 0 {
		minimum := int(scale) + 1
		if len(digits) < minimum {
			digits = strings.Repeat("0", minimum-len(digits)) + digits
		}
		point := len(digits) - int(scale)
		digits = digits[:point] + "." + digits[point:]
	}
	if negative {
		return "-" + digits
	}
	return digits
}

// EffectiveDecimalParameters applies BigQuery's parameter omission rules and
// then narrows BIGNUMERIC to Spark DecimalType's precision-38 boundary.
func (f Field) EffectiveDecimalParameters() (DecimalParameters, error) {
	fieldType := strings.ToUpper(f.Type)
	if fieldType != "NUMERIC" && fieldType != "BIGNUMERIC" {
		return DecimalParameters{}, fmt.Errorf("%w: field %q is not a decimal type", ErrInvalid, f.Name)
	}
	if f.Precision == nil && f.Scale == nil {
		if fieldType == "NUMERIC" {
			return DecimalParameters{Precision: 38, Scale: 9}, nil
		}
		return DecimalParameters{Precision: 38, Scale: 18}, nil
	}
	if f.Precision == nil {
		return DecimalParameters{}, fmt.Errorf("%w: decimal field %q cannot specify scale without precision", ErrInvalid, f.Name)
	}

	precision := *f.Precision
	scale := int64(0)
	if f.Scale != nil {
		scale = *f.Scale
	}
	if precision < 1 {
		return DecimalParameters{}, fmt.Errorf("%w: decimal field %q precision must be positive", ErrInvalid, f.Name)
	}
	if precision > SparkDecimalMaxPrecision {
		return DecimalParameters{}, fmt.Errorf(
			"%w: capability=%s decimal field %q precision %d exceeds Spark maximum %d",
			ErrUnsupported, CapabilitySparkDecimal38V1, f.Name, precision, SparkDecimalMaxPrecision,
		)
	}
	if scale < 0 || scale > SparkDecimalMaxScale {
		return DecimalParameters{}, fmt.Errorf("%w: decimal field %q scale must be between 0 and %d", ErrInvalid, f.Name, SparkDecimalMaxScale)
	}
	if scale > precision {
		return DecimalParameters{}, fmt.Errorf("%w: decimal field %q scale must not exceed precision", ErrInvalid, f.Name)
	}
	if fieldType == "NUMERIC" && scale > 9 {
		return DecimalParameters{}, fmt.Errorf("%w: NUMERIC field %q scale must not exceed 9", ErrInvalid, f.Name)
	}
	if fieldType == "NUMERIC" && precision-scale > 29 {
		return DecimalParameters{}, fmt.Errorf("%w: NUMERIC field %q integer digits must not exceed 29", ErrInvalid, f.Name)
	}
	return DecimalParameters{Precision: precision, Scale: scale}, nil
}

func CloneFields(fields []Field) []Field {
	result := make([]Field, len(fields))
	for index, field := range fields {
		result[index] = field
		result[index].Precision = CloneOptionalInt64(field.Precision)
		result[index].Scale = CloneOptionalInt64(field.Scale)
		result[index].Fields = CloneFields(field.Fields)
	}
	return result
}

func CloneOptionalInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
