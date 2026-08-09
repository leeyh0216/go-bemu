package observability

// Trace identifiers follow the W3C traceparent shape when supplied, while
// malformed identifiers are replaced locally rather than echoed into logs.
// Official source: https://www.w3.org/TR/trace-context/

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	capture     bodyCapture
	operationID string
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
	r.capture.Write(payload[:written])
	return written, err
}

// SetHTTPOperation lets the route adapter attach a generated operation ID
// without coupling observability to the REST contract package.
func SetHTTPOperation(w http.ResponseWriter, operationID string) {
	if recorder, ok := w.(*responseRecorder); ok {
		recorder.operationID = operationID
	}
}

type bodyCapture struct {
	limit   int64
	bytes   int64
	payload []byte
}

func (c *bodyCapture) Write(payload []byte) {
	c.bytes += int64(len(payload))
	remaining := c.limit - int64(len(c.payload))
	if remaining <= 0 {
		return
	}
	if int64(len(payload)) > remaining {
		payload = payload[:remaining]
	}
	c.payload = append(c.payload, payload...)
}

type captureReadCloser struct {
	io.ReadCloser
	capture *bodyCapture
}

func (r captureReadCloser) Read(payload []byte) (int, error) {
	count, err := r.ReadCloser.Read(payload)
	if count > 0 {
		r.capture.Write(payload[:count])
	}
	return count, err
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
		timeline := ProcessTimeline()
		requestCapture := bodyCapture{limit: timeline.PayloadLimit()}
		if r.Body != nil {
			r.Body = captureReadCloser{ReadCloser: r.Body, capture: &requestCapture}
		}
		attrs := append(ContextAttrs(ctx),
			"event", "boundary.enter", "boundary", "http", "method", r.Method,
			"path", r.URL.Path, "query", MetadataEntries(r.URL.Query()), "headers", MetadataEntries(r.Header),
			"remote_addr", r.RemoteAddr, "content_length", r.ContentLength,
		)
		slog.InfoContext(ctx, "request", attrs...)
		recorder := &responseRecorder{ResponseWriter: w, capture: bodyCapture{limit: timeline.PayloadLimit()}}
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
		operationID := recorder.operationID
		if operationID == "" {
			operationID = "http." + strings.ToLower(r.Method)
		}
		// The polling endpoint returns the retained payloads themselves. Recording
		// that response would recursively retain each prior snapshot and turn a
		// passive inspector into unbounded amplification.
		if strings.HasPrefix(operationID, "bqemu.diagnostics.timeline.") {
			return
		}
		timeline.Record(DiagnosticEvent{RequestID: requestID, TraceID: traceID, Protocol: "http", OperationID: operationID, Phase: "request", Method: r.Method, Path: r.URL.Path, Peer: r.RemoteAddr, Headers: r.Header, OriginalBytes: requestCapture.bytes, Truncated: requestCapture.bytes > int64(len(requestCapture.payload))}, requestCapture.payload)
		timeline.Record(DiagnosticEvent{RequestID: requestID, TraceID: traceID, Protocol: "http", OperationID: operationID, Phase: "response", Method: r.Method, Path: r.URL.Path, Headers: recorder.Header(), Status: http.StatusText(recorder.status), DurationNanos: time.Since(started).Nanoseconds(), OriginalBytes: recorder.capture.bytes, Truncated: recorder.capture.bytes > int64(len(recorder.capture.payload))}, recorder.capture.payload)
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
