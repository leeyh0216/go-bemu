package rest

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/observability"
)

func TestHTTPBoundaryUsesSharedEventContractAndTimeline(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	timeline := observability.ConfigureTimeline(observability.TimelineConfig{MaxEvents: 10, MaxBytes: 4 << 10, MaxPayloadBytes: 4})
	handler := HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SetHTTPOperation(w, "bqemu.test")
		_, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte("response"))
	}))
	request := httptest.NewRequest(http.MethodPost, "/path?access_token=query-value", strings.NewReader("request"))
	request.Header.Set("Authorization", "Bearer header-value")
	request.Header.Set("X-Request-Id", "request-one")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	for _, expected := range []string{"boundary.enter", "boundary.exit", "request-one", "access_token=query-value", "authorization=Bearer header-value"} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("log omitted %q: %s", expected, output.String())
		}
	}
	snapshot := timeline.Snapshot(0, 0)
	if len(snapshot.Events) != 2 || snapshot.Events[0].OperationID != "bqemu.test" || !snapshot.Events[0].Truncated {
		t.Fatalf("timeline = %#v", snapshot.Events)
	}
}
