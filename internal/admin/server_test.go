package admin

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/contractspec"
	"github.com/leeyh0216/go-bemu/internal/contracttest"
)

type fakeClock struct{ values []time.Time }

func (f *fakeClock) Now() time.Time {
	value := f.values[0]
	if len(f.values) > 1 {
		f.values = f.values[1:]
	}
	return value
}

type fakeRuntime struct {
	snapshot RuntimeSnapshot
	stack    StackCapture
}

func (f fakeRuntime) Snapshot(_, _ time.Time) RuntimeSnapshot { return f.snapshot }
func (f fakeRuntime) GoroutineStack(int) StackCapture         { return f.stack }

func TestDiagnosticsExposeRuntimeAndBoundedStackMetadata(t *testing.T) {
	contracttest.Operation(t, "bqemu.admin.runtime.get")
	contracttest.Operation(t, "bqemu.admin.goroutines.get")
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	var logs bytes.Buffer
	server, err := New(Options{
		MaxStackBytes: 64 << 10,
		Clock:         &fakeClock{values: []time.Time{now, now, now.Add(time.Millisecond), now, now.Add(2 * time.Millisecond)}},
		Runtime: fakeRuntime{
			snapshot: RuntimeSnapshot{APIVersion: APIVersion, CapturedAt: now, Goroutines: 7},
			stack:    StackCapture{Payload: []byte("goroutine 1 [running]:\nexample"), Truncated: true},
		},
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	runtimeResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(runtimeResponse, httptest.NewRequest(http.MethodGet, "/bqemu/v1/admin/diagnostics/runtime", nil))
	if runtimeResponse.Code != http.StatusOK {
		t.Fatalf("runtime status = %d", runtimeResponse.Code)
	}
	var snapshot RuntimeSnapshot
	if err := json.Unmarshal(runtimeResponse.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Goroutines != 7 {
		t.Fatalf("goroutines = %d", snapshot.Goroutines)
	}

	stackResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(stackResponse, httptest.NewRequest(http.MethodGet, "/bqemu/v1/admin/diagnostics/goroutines", nil))
	if stackResponse.Code != http.StatusOK || stackResponse.Body.String() != "goroutine 1 [running]:\nexample" {
		t.Fatalf("stack response = %d %q", stackResponse.Code, stackResponse.Body.String())
	}
	if stackResponse.Header().Get("X-BQEMU-Truncated") != "true" || !strings.HasPrefix(stackResponse.Header().Get("X-BQEMU-Payload-Digest"), "sha256:") {
		t.Fatalf("stack headers = %v", stackResponse.Header())
	}
	if !strings.Contains(logs.String(), "goroutine 1") {
		t.Fatalf("stack omitted from logs: %s", logs.String())
	}
}

func TestAdminTokenProtectsEveryEndpointAndLogsRejectedCredential(t *testing.T) {
	contracttest.Operation(t, "bqemu.admin.health")
	directory := t.TempDir()
	path := filepath.Join(directory, "token")
	token := "admin-secret-token-value"
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	server, err := New(Options{TokenFile: path, MaxStackBytes: 64 << 10, Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}

	for _, endpoint := range []string{"/healthz", "/bqemu/v1/admin/diagnostics/runtime", "/bqemu/v1/admin/diagnostics/goroutines"} {
		unauthorized := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, endpoint, nil)
		request.Header.Set("Authorization", "Bearer wrong-secret-token")
		server.Handler().ServeHTTP(unauthorized, request)
		if unauthorized.Code != http.StatusUnauthorized {
			t.Fatalf("%s status = %d", endpoint, unauthorized.Code)
		}

		authorized := httptest.NewRecorder()
		request = httptest.NewRequest(http.MethodGet, endpoint, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		server.Handler().ServeHTTP(authorized, request)
		if authorized.Code != http.StatusOK {
			t.Fatalf("%s authorized status = %d", endpoint, authorized.Code)
		}
	}
	if strings.Contains(logs.String(), token) || !strings.Contains(logs.String(), "wrong-secret-token") {
		t.Fatalf("admin authentication diagnostics = %s", logs.String())
	}
}

func TestTokenFileAndStackLimitsFailFast(t *testing.T) {
	if _, err := New(Options{MaxStackBytes: 1}); err == nil {
		t.Fatal("expected stack limit error")
	}
	path := filepath.Join(t.TempDir(), "short-token")
	if err := os.WriteFile(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{MaxStackBytes: 64 << 10, TokenFile: path}); err == nil {
		t.Fatal("expected token error")
	}
}

func TestAdminRouteBindingsMatchOperationManifest(t *testing.T) {
	server := &Server{}
	actual := make(map[string]bool)
	for _, binding := range server.routeBindings() {
		if actual[binding.operationID] {
			t.Fatalf("duplicate admin operation binding %q", binding.operationID)
		}
		actual[binding.operationID] = true
	}
	for _, route := range contractspec.RESTRoutes("admin") {
		if !actual[route.OperationID] {
			t.Errorf("manifest admin operation %q has no handler binding", route.OperationID)
		}
		delete(actual, route.OperationID)
	}
	for operationID := range actual {
		t.Errorf("admin handler binding %q is absent from the operation manifest", operationID)
	}
}
