package domain

import (
	"fmt"
	"strings"
)

const (
	SparkDecimalMaxPrecision int64 = 38
	SparkDecimalMaxScale     int64 = 38
)

// DecimalParameters is the effective Spark-compatible decimal shape used by
// a physical engine. Field.Precision and Field.Scale remain unchanged so the
// canonical catalog can distinguish omitted parameters from explicit ones.
type DecimalParameters struct {
	Precision int64
	Scale     int64
}

// EffectiveDecimalParameters resolves the agreed emulator defaults. BigQuery's
// full BIGNUMERIC range is intentionally narrowed to Spark DecimalType's maximum
// precision so both NUMERIC families can be stored as native engine decimals.
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
	if precision < 1 || precision > SparkDecimalMaxPrecision {
		return DecimalParameters{}, fmt.Errorf("%w: decimal field %q precision must be between 1 and %d", ErrInvalid, f.Name, SparkDecimalMaxPrecision)
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
