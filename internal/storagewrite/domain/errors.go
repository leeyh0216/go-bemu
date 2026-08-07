package domain

// Stable codes mirror the official gRPC and StorageError categories while
// keeping generated protobuf types out of the domain:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#storageerror

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrorInvalidArgument    ErrorCode = "INVALID_ARGUMENT"
	ErrorNotFound           ErrorCode = "NOT_FOUND"
	ErrorFailedPrecondition ErrorCode = "FAILED_PRECONDITION"
	ErrorResourceExhausted  ErrorCode = "RESOURCE_EXHAUSTED"
	ErrorAlreadyExists      ErrorCode = "ALREADY_EXISTS"
	ErrorOutOfRange         ErrorCode = "OUT_OF_RANGE"
	ErrorUnimplemented      ErrorCode = "UNIMPLEMENTED"
	ErrorInternal           ErrorCode = "INTERNAL"
)

// Error carries a stable category across application and transport boundaries.
// Cause is retained for logs/tests but adapters must not expose it verbatim: a
// database error can contain row values, SQL text, or local file paths.
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

type StreamErrorCode string

const (
	StreamAlreadyCommitted StreamErrorCode = "STREAM_ALREADY_COMMITTED"
	StreamNotFound         StreamErrorCode = "STREAM_NOT_FOUND"
	InvalidStreamType      StreamErrorCode = "INVALID_STREAM_TYPE"
	InvalidStreamState     StreamErrorCode = "INVALID_STREAM_STATE"
	StreamFinalized        StreamErrorCode = "STREAM_FINALIZED"
	OffsetAlreadyExists    StreamErrorCode = "OFFSET_ALREADY_EXISTS"
	OffsetOutOfRange       StreamErrorCode = "OFFSET_OUT_OF_RANGE"
)

// StreamError is returned inside BatchCommitWriteStreamsResponse. A non-empty
// list means that zero streams were committed; it is not a partially successful
// result.
type StreamError struct {
	Code    StreamErrorCode
	Stream  string
	Message string
}
