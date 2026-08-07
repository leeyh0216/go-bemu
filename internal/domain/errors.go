package domain

import "errors"

var (
	ErrNotFound     = errors.New("resource not found")
	ErrConflict     = errors.New("resource already exists")
	ErrInvalid      = errors.New("invalid resource")
	ErrInvalidQuery = errors.New("invalid query")
	ErrPrecondition = errors.New("precondition failed")
	ErrUnsupported  = errors.New("operation not implemented")
	ErrBackend      = errors.New("backend execution failed")
)
