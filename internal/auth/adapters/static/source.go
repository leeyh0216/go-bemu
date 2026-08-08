package static

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
	authports "github.com/leeyh0216/go-bemu/internal/auth/ports"
)

// FileSource reads credential material with a caller-selected hard byte limit.
// It intentionally excludes the configured path and OS error text from Error;
// those values can contain deployment details that do not belong in logs.
// Context is checked before os.Open and after io.ReadAll. A FIFO/device can
// still block inside those OS calls, so production configuration should use a
// regular file until a cancellable I/O source adapter is introduced.
//   - https://pkg.go.dev/os#Open
//   - https://pkg.go.dev/io#LimitReader
type FileSource struct {
	path string
}

func NewFileSource(path string) (*FileSource, error) {
	if path == "" {
		return nil, newSourceError(sourceEmptyPath, nil)
	}
	return &FileSource{path: path}, nil
}

func (s *FileSource) Read(ctx context.Context, maxBytes int64) (payload []byte, resultErr error) {
	if maxBytes < 1 || maxBytes == math.MaxInt64 {
		return nil, newSourceError(sourceInvalidBound, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, newSourceError(sourceContextBeforeOpen, err)
	}

	file, err := os.Open(filepath.Clean(s.path))
	if err != nil {
		return nil, newSourceError(sourceOpenFailed, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && resultErr == nil {
			clear(payload)
			payload = nil
			resultErr = newSourceError(sourceCloseFailed, closeErr)
		}
	}()

	payload, err = io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		clear(payload)
		return nil, newSourceError(sourceReadFailed, err)
	}
	if int64(len(payload)) > maxBytes {
		clear(payload)
		return nil, newSourceError(sourcePayloadTooLarge, nil)
	}
	if err := ctx.Err(); err != nil {
		clear(payload)
		return nil, newSourceError(sourceContextAfterRead, err)
	}
	return payload, nil
}

type sourceFailure string

const (
	sourceUnknown           sourceFailure = "unknown"
	sourceEmptyPath         sourceFailure = "empty-path"
	sourceInvalidBound      sourceFailure = "invalid-byte-bound"
	sourceContextBeforeOpen sourceFailure = "context-ended-before-open"
	sourceOpenFailed        sourceFailure = "open-failed"
	sourceCloseFailed       sourceFailure = "close-failed"
	sourceReadFailed        sourceFailure = "read-failed"
	sourcePayloadTooLarge   sourceFailure = "payload-above-maximum"
	sourceContextAfterRead  sourceFailure = "context-ended-after-read"
)

type sourceError struct {
	failure sourceFailure
	cause   error
}

func newSourceError(failure sourceFailure, cause error) *sourceError {
	return &sourceError{failure: normalizeSourceFailure(failure), cause: cause}
}

func (e *sourceError) Error() string {
	return "static token source failure: operation=read-static-token-set shape=" + string(normalizeSourceFailure(e.failure))
}

func (e *sourceError) Unwrap() error { return e.cause }

func sourceDiagnostic(err error) authdomain.DiagnosticCode {
	var sourceErr *sourceError
	if !errors.As(err, &sourceErr) {
		return authdomain.DiagnosticTokenSourceUnknown
	}
	switch normalizeSourceFailure(sourceErr.failure) {
	case sourceInvalidBound:
		return authdomain.DiagnosticTokenSourceInvalidBound
	case sourceContextBeforeOpen:
		return authdomain.DiagnosticTokenSourceContextBefore
	case sourceOpenFailed:
		return authdomain.DiagnosticTokenSourceOpenFailed
	case sourceCloseFailed:
		return authdomain.DiagnosticTokenSourceCloseFailed
	case sourceReadFailed:
		return authdomain.DiagnosticTokenSourceReadFailed
	case sourcePayloadTooLarge:
		return authdomain.DiagnosticTokenSourcePayloadTooLarge
	case sourceContextAfterRead:
		return authdomain.DiagnosticTokenSourceContextAfter
	default:
		return authdomain.DiagnosticTokenSourceUnknown
	}
}

func normalizeSourceFailure(failure sourceFailure) sourceFailure {
	switch failure {
	case sourceEmptyPath, sourceInvalidBound, sourceContextBeforeOpen, sourceOpenFailed,
		sourceCloseFailed, sourceReadFailed, sourcePayloadTooLarge, sourceContextAfterRead:
		return failure
	default:
		return sourceUnknown
	}
}

var _ authports.TokenSetSource = (*FileSource)(nil)
