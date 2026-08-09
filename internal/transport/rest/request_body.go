package rest

// REST request bodies are decoded once at the public HTTP edge. Google API
// clients may gzip requests and use HTTP/1.1 chunked transfer framing, so
// ContentLength is not an admission bound. The compressed and decoded streams
// have independent byte budgets to prevent both oversized wire requests and
// gzip amplification.
//
// Sources:
//   - net/http Request body semantics: https://pkg.go.dev/net/http#Request
//   - bounded server request bodies: https://pkg.go.dev/net/http#MaxBytesReader
//   - gzip reader behavior: https://pkg.go.dev/compress/gzip#NewReader
//   - BigQuery tables.insert: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

type requestBodyLimits struct {
	maxCompressedBytes   int64
	maxUncompressedBytes int64
}

type requestBodyLimitsContextKey struct{}
type requestBodyOutcomeContextKey struct{}

type requestBodyOutcome struct {
	mu  sync.Mutex
	err error
}

func (o *requestBodyOutcome) fail(err error) {
	if err == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err == nil {
		o.err = err
	}
}

func (o *requestBodyOutcome) failure() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}

func normalizedRequestBodyLimits(compressed, uncompressed int64) requestBodyLimits {
	if compressed <= 0 {
		compressed = maximumJSONBodyBytes
	}
	if uncompressed <= 0 {
		uncompressed = maximumJSONBodyBytes
	}
	return requestBodyLimits{maxCompressedBytes: compressed, maxUncompressedBytes: uncompressed}
}

func requestBodyLimitsFromContext(ctx context.Context) (requestBodyLimits, bool) {
	limits, ok := ctx.Value(requestBodyLimitsContextKey{}).(requestBodyLimits)
	return limits, ok
}

func recordRequestBodyFailure(ctx context.Context, err error) {
	if outcome, ok := ctx.Value(requestBodyOutcomeContextKey{}).(*requestBodyOutcome); ok {
		outcome.fail(err)
	}
}

// WithRequestBodyLimits applies independent wire and decoded byte ceilings to
// every REST request. Runtime configuration validation requires positive
// values; defaults are retained here for embedded/test server callers.
func WithRequestBodyLimits(maxCompressedBytes, maxUncompressedBytes int64) Option {
	return func(server *Server) {
		server.requestBodyLimits = normalizedRequestBodyLimits(maxCompressedBytes, maxUncompressedBytes)
	}
}

func requestBodyMiddleware(limits requestBodyLimits, mediaUploadLimits *requestBodyLimits, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestLimits, captureBody := limits, true
		if mediaUploadLimits != nil && isMediaUploadRequest(r) {
			requestLimits, captureBody = *mediaUploadLimits, false
		}
		ctx := context.WithValue(r.Context(), requestBodyLimitsContextKey{}, requestLimits)
		outcome := &requestBodyOutcome{}
		ctx = context.WithValue(ctx, requestBodyOutcomeContextKey{}, outcome)
		r = r.WithContext(ctx)
		encoding, parseErr := parseRequestContentEncoding(r.Header.Values("Content-Encoding"))
		if parseErr != nil {
			_ = r.Body.Close()
			logRequestBodyBoundary(ctx, encoding, nil, nil, parseErr)
			writeError(w, parseErr)
			return
		}

		compressed := newDigestingReader(r.Body)
		compressed.capture = captureBody
		compressedBody := http.MaxBytesReader(w, &readerCloser{Reader: compressed, Closer: r.Body}, requestLimits.maxCompressedBytes)
		var decoded io.ReadCloser
		switch encoding {
		case "gzip":
			gzipReader, err := gzip.NewReader(compressedBody)
			if err != nil {
				_ = compressedBody.Close()
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					err = requestBodyTooLarge(err)
				} else {
					err = &httpProtocolError{status: http.StatusBadRequest, reason: "invalid", message: "invalid gzip request body", err: err}
				}
				logRequestBodyBoundary(ctx, encoding, compressed, nil, err)
				writeError(w, err)
				return
			}
			decoded = &multiReadCloser{Reader: gzipReader, closers: []io.Closer{gzipReader, compressedBody}}
			r.ContentLength = -1
			r.Header.Del("Content-Encoding")
		default:
			decoded = &multiReadCloser{Reader: compressedBody, closers: []io.Closer{compressedBody}}
		}

		uncompressed := newDigestingReader(decoded)
		uncompressed.capture = captureBody
		limitedBody := http.MaxBytesReader(w, &readerCloser{Reader: uncompressed, Closer: decoded}, requestLimits.maxUncompressedBytes)
		r.Body = &outcomeReadCloser{ReadCloser: limitedBody, outcome: outcome, encoding: encoding}
		defer func() {
			_ = r.Body.Close()
			logRequestBodyBoundary(ctx, encoding, compressed, uncompressed, outcome.failure())
		}()
		next.ServeHTTP(w, r)
	})
}

func isMediaUploadRequest(r *http.Request) bool {
	path := r.URL.Path
	return strings.HasPrefix(path, "/upload/bigquery/v2/projects/") || strings.HasPrefix(path, "/resumable/upload/bigquery/v2/projects/")
}

func parseRequestContentEncoding(values []string) (string, error) {
	if len(values) == 0 {
		return "identity", nil
	}
	tokens := make([]string, 0, len(values))
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			token = strings.ToLower(strings.TrimSpace(token))
			if token == "" {
				return "malformed", &httpProtocolError{status: http.StatusBadRequest, reason: "invalid", message: "malformed Content-Encoding"}
			}
			tokens = append(tokens, token)
		}
	}
	if len(tokens) != 1 {
		return "multiple", &httpProtocolError{status: http.StatusBadRequest, reason: "invalid", message: "multiple Content-Encoding values are not supported"}
	}
	switch tokens[0] {
	case "identity", "gzip":
		return tokens[0], nil
	default:
		return "unsupported", &httpProtocolError{status: http.StatusUnsupportedMediaType, reason: "invalid", message: "Content-Encoding is not supported"}
	}
}

type digestingReader struct {
	reader  io.Reader
	hash    hash.Hash
	payload bytes.Buffer
	bytes   int64
	capture bool
}

func newDigestingReader(reader io.Reader) *digestingReader {
	return &digestingReader{reader: reader, hash: sha256.New()}
}

func (r *digestingReader) Read(payload []byte) (int, error) {
	n, err := r.reader.Read(payload)
	if n > 0 {
		r.bytes += int64(n)
		_, _ = r.hash.Write(payload[:n])
		if r.capture {
			_, _ = r.payload.Write(payload[:n])
		}
	}
	return n, err
}

func (r *digestingReader) digest() string {
	return "sha256:" + hex.EncodeToString(r.hash.Sum(nil))
}

type readerCloser struct {
	io.Reader
	io.Closer
}

type outcomeReadCloser struct {
	io.ReadCloser
	outcome  *requestBodyOutcome
	encoding string
}

func (r *outcomeReadCloser) Read(payload []byte) (int, error) {
	n, err := r.ReadCloser.Read(payload)
	if err != nil && !errors.Is(err, io.EOF) {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			r.outcome.fail(requestBodyTooLarge(err))
		case r.encoding == "gzip":
			r.outcome.fail(&httpProtocolError{status: http.StatusBadRequest, reason: "invalid", message: "invalid gzip request body", err: err})
		default:
			r.outcome.fail(&httpProtocolError{status: http.StatusBadRequest, reason: "invalid", message: "invalid request body", err: err})
		}
	}
	return n, err
}

type multiReadCloser struct {
	io.Reader
	closers []io.Closer
	once    sync.Once
	err     error
}

func (r *multiReadCloser) Close() error {
	r.once.Do(func() {
		for _, closer := range r.closers {
			if err := closer.Close(); r.err == nil && err != nil {
				r.err = err
			}
		}
	})
	return r.err
}

func logRequestBodyBoundary(ctx context.Context, encoding string, compressed, uncompressed *digestingReader, err error) {
	outcome := "accepted"
	if err != nil {
		outcome = "rejected"
	}
	attrs := append(observability.ContextAttrs(ctx),
		"event", "boundary.enter", "boundary", "http.body",
		"operation", "rest.request_body.decode", "encoding", encoding, "outcome", outcome,
	)
	if compressed != nil {
		attrs = append(attrs, "compressed_bytes_read", compressed.bytes, "compressed_digest", compressed.digest())
		if compressed.capture {
			attrs = append(attrs, "compressed_body", compressed.payload.String())
		}
	}
	if uncompressed != nil {
		attrs = append(attrs, "uncompressed_bytes_read", uncompressed.bytes, "uncompressed_digest", uncompressed.digest())
		if uncompressed.capture {
			attrs = append(attrs, "uncompressed_body", uncompressed.payload.String())
		}
	}
	if err != nil {
		var protocolError *httpProtocolError
		if errors.As(err, &protocolError) {
			attrs = append(attrs, "http_status", protocolError.status, "reason", protocolError.reason)
		} else if errors.Is(err, domain.ErrInvalid) {
			attrs = append(attrs, "http_status", http.StatusBadRequest, "reason", "invalid")
		}
		attrs = append(attrs, observability.ErrorAttrs(err)...)
		slog.WarnContext(ctx, "request body boundary", attrs...)
		return
	}
	slog.InfoContext(ctx, "request body boundary", attrs...)
}
