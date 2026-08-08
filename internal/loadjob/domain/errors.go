package domain

import "errors"

var (
	ErrNotFound     = errors.New("load job resource not found")
	ErrConflict     = errors.New("load job resource already exists")
	ErrInvalid      = errors.New("invalid load job resource")
	ErrPrecondition = errors.New("load job precondition failed")
	ErrUnsupported  = errors.New("load job feature is not implemented")
)

const CapabilityParquetNestedRepeatedV1 = "load.parquet.nested-repeated.unsupported-v1"
