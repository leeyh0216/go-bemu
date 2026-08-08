package domain

import (
	"fmt"
	"math/big"
	"strings"
)

const (
	SparkDecimalMaxPrecision int64 = 38
	SparkDecimalMaxScale     int64 = 38

	CapabilitySparkDecimal38V1 = "schema.decimal.spark-precision-38-v1"
	GapGeographyUnsupportedV1  = "schema.geography.unsupported-v1"
)

// DecimalParameters is the effective decimal shape supplied to an engine or
// wire codec. Field.Precision and Field.Scale remain unchanged so canonical
// metadata distinguishes omitted parameters from explicit values.
type DecimalParameters struct {
	Precision int64
	Scale     int64
}

// ValidateDecimalValue verifies that a decimal literal fits the field without
// rounding or narrowing. Adapters call it before any row mutation.
func (f Field) ValidateDecimalValue(input string) error {
	parameters, err := f.EffectiveDecimalParameters()
	if err != nil {
		return err
	}
	rational, ok := new(big.Rat).SetString(input)
	if !ok {
		return fmt.Errorf("%w: decimal field %q has invalid value %q", ErrInvalid, f.Name, input)
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(parameters.Scale), nil)
	rational.Mul(rational, new(big.Rat).SetInt(factor))
	if rational.Denom().Cmp(big.NewInt(1)) != 0 {
		return fmt.Errorf("%w: decimal field %q value %q exceeds scale %d", ErrInvalid, f.Name, input, parameters.Scale)
	}
	digits := len(new(big.Int).Abs(rational.Num()).String())
	if rational.Num().Sign() == 0 {
		digits = 1
	}
	if int64(digits) > parameters.Precision {
		return fmt.Errorf("%w: decimal field %q value %q exceeds precision %d", ErrInvalid, f.Name, input, parameters.Precision)
	}
	return nil
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
