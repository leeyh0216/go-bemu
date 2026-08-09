package domain

// Stable codes mirror the official gRPC status categories while keeping
// generated protobuf types out of the domain. In particular, UNIMPLEMENTED is
// reserved for request features that this emulator deliberately does not yet
// support; malformed supported options remain INVALID_ARGUMENT.
// Source: https://grpc.io/docs/guides/status-codes/

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrorCanceled           ErrorCode = "CANCELED"
	ErrorDeadlineExceeded   ErrorCode = "DEADLINE_EXCEEDED"
	ErrorInvalidArgument    ErrorCode = "INVALID_ARGUMENT"
	ErrorNotFound           ErrorCode = "NOT_FOUND"
	ErrorFailedPrecondition ErrorCode = "FAILED_PRECONDITION"
	ErrorResourceExhausted  ErrorCode = "RESOURCE_EXHAUSTED"
	ErrorUnimplemented      ErrorCode = "UNIMPLEMENTED"
	ErrorUnavailable        ErrorCode = "UNAVAILABLE"
	ErrorInternal           ErrorCode = "INTERNAL"
)

// Error carries a stable category across the application/transport boundary.
// Adapters must not expose Cause directly to clients because backend errors can
// contain SQL values, file names, or credentials.
type Error struct {
	Code      ErrorCode
	Operation string
	Cause     error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Operation == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Operation, e.Code)
}

func (e *Error) Unwrap() error { return e.Cause }

func NewError(code ErrorCode, operation string, cause error) error {
	return &Error{Code: code, Operation: operation, Cause: cause}
}

func CodeOf(err error) ErrorCode {
	var typed *Error
	if errors.As(err, &typed) {
		return typed.Code
	}
	return ErrorInternal
}
