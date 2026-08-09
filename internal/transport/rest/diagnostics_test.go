package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/contracttest"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

func TestDiagnosticTimelineAPI(t *testing.T) {
	contracttest.Operation(t, "bqemu.diagnostics.timeline.get")
	contracttest.Operation(t, "bqemu.diagnostics.timeline.clear")
	timeline := observability.ConfigureTimeline(observability.TimelineConfig{MaxEvents: 10, MaxBytes: 4 << 10, MaxPayloadBytes: 64})
	timeline.Record(observability.DiagnosticEvent{Protocol: "grpc", OperationID: "rpc", RequestID: "request-1", Status: "OK"}, []byte("payload"))
	server := NewCatalogServer(nil, nil, "", withDiagnosticsAPI()).Handler()
	request := httptest.NewRequest(http.MethodGet, "/bqemu/v1/diagnostics/timeline?protocol=grpc&requestId=request-1", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body)
	}
	var snapshot observability.TimelineSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if got, want := len(snapshot.Events), 1; got != want {
		t.Fatalf("events = %d, want %d", got, want)
	}
	if got, want := snapshot.Events[0].OperationID, "rpc"; got != want {
		t.Fatalf("operation = %q, want %q", got, want)
	}

	clear := httptest.NewRequest(http.MethodPost, "/bqemu/v1/diagnostics/timeline:clear", nil)
	cleared := httptest.NewRecorder()
	server.ServeHTTP(cleared, clear)
	if cleared.Code != http.StatusNoContent {
		t.Fatalf("clear status = %d", cleared.Code)
	}
	if got := len(timeline.Snapshot(0, 0).Events); got != 0 {
		t.Fatalf("clear retained %d events; diagnostics polling must not self-amplify", got)
	}
}
