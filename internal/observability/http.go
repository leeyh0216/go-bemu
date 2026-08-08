package observability

// Trace identifiers follow the W3C traceparent shape when supplied, while
// malformed identifiers are replaced locally rather than echoed into logs.
// Official source: https://www.w3.org/TR/trace-context/

import (
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(payload []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	written, err := r.ResponseWriter.Write(payload)
	r.bytes += written
	return written, err
}

func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := SafeID(r.Header.Get("X-Request-Id"))
		if requestID == "" {
			requestID = NewID()
		}
		traceID := traceIDFromHTTP(r)
		ctx := WithRequestMetadata(r.Context(), requestID, traceID)
		r = r.WithContext(ctx)
		attrs := append(ContextAttrs(ctx),
			"event", "boundary.enter", "boundary", "http", "method", r.Method,
			"path", r.URL.Path, "query", MetadataEntries(r.URL.Query()), "headers", MetadataEntries(r.Header),
			"remote_addr", r.RemoteAddr, "content_length", r.ContentLength,
		)
		slog.InfoContext(ctx, "request", attrs...)
		recorder := &responseRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		if recorder.status == 0 {
			recorder.status = http.StatusOK
		}
		exitAttrs := append(ContextAttrs(ctx),
			"event", "boundary.exit", "boundary", "http", "method", r.Method,
			"path", r.URL.Path, "status", recorder.status, "response_bytes", recorder.bytes,
			"duration_ms", time.Since(started).Milliseconds(),
		)
		slog.InfoContext(ctx, "response", exitAttrs...)
	})
}

func traceIDFromHTTP(r *http.Request) string {
	if traceparent := r.Header.Get("traceparent"); traceparent != "" {
		parts := strings.Split(traceparent, "-")
		if len(parts) >= 2 {
			if traceID := SafeID(parts[1]); traceID != "" {
				return traceID
			}
		}
	}
	if cloudTrace := r.Header.Get("X-Cloud-Trace-Context"); cloudTrace != "" {
		if traceID := SafeID(strings.Split(cloudTrace, "/")[0]); traceID != "" {
			return traceID
		}
	}
	return NewID()
}
