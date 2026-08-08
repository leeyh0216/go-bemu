package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/contracttest"
)

func TestRuntimeCatalogMetadataSurvivesRestart(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	contracttest.Operation(t, "bqemu.projects.create")
	contracttest.Operation(t, "bqemu.projects.get")
	contracttest.Operation(t, "bigquery.datasets.insert")
	contracttest.Operation(t, "bigquery.datasets.get")
	contracttest.Operation(t, "bigquery.tables.insert")
	contracttest.Operation(t, "bigquery.tables.get")

	directory := t.TempDir()
	httpAddress := unusedLoopbackAddress(t)
	grpcAddress := unusedLoopbackAddress(t)
	baseURL := "http://" + httpAddress
	configPath := filepath.Join(directory, "bqemu.yaml")
	configBody := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
runtime:
  shutdownTimeout: "5s"
  serverDrainTimeout: "2s"
  storageCloseTimeout: "2s"
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, httpAddress, baseURL, grpcAddress,
		filepath.Join(directory, "engine.duckdb"), filepath.Join(directory, "tmp"),
		filepath.Join(directory, "state.sqlite"))
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(configBody)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	stop := startRuntimeForRestartTest(t, configPath, baseURL)
	runtimeRequest(t, baseURL, http.MethodPost, "/bqemu/v1/projects", `{
  "projectId":"persisted-project","friendlyName":"Persisted project","description":"restart fixture"
}`, http.StatusOK)
	runtimeRequest(t, baseURL, http.MethodPost, "/bigquery/v2/projects/persisted-project/datasets", `{
  "datasetReference":{"datasetId":"analytics"},"location":"EU","labels":{"owner":"restart-test"}
}`, http.StatusOK)
	runtimeRequest(t, baseURL, http.MethodPost, "/bigquery/v2/projects/persisted-project/datasets/analytics/tables", `{
  "tableReference":{"tableId":"events"},"description":"durable table",
  "schema":{"fields":[
    {"name":"event_id","type":"INT64","mode":"REQUIRED"},
    {"name":"payload","type":"STRUCT","fields":[
      {"name":"amount","type":"BIGNUMERIC","precision":"38","scale":"18","roundingMode":"ROUND_HALF_EVEN"}
    ]}
  ]}
}`, http.StatusOK)
	stop()

	stop = startRuntimeForRestartTest(t, configPath, baseURL)
	t.Cleanup(stop)
	project := runtimeRequest(t, baseURL, http.MethodGet,
		"/bqemu/v1/projects/persisted-project", "", http.StatusOK)
	if project["id"] != "persisted-project" || project["description"] != "restart fixture" {
		t.Fatalf("restarted project = %#v", project)
	}
	dataset := runtimeRequest(t, baseURL, http.MethodGet,
		"/bigquery/v2/projects/persisted-project/datasets/analytics", "", http.StatusOK)
	labels, _ := dataset["labels"].(map[string]any)
	if dataset["location"] != "EU" || labels["owner"] != "restart-test" {
		t.Fatalf("restarted dataset = %#v", dataset)
	}
	table := runtimeRequest(t, baseURL, http.MethodGet,
		"/bigquery/v2/projects/persisted-project/datasets/analytics/tables/events", "", http.StatusOK)
	fields := table["schema"].(map[string]any)["fields"].([]any)
	payload := fields[1].(map[string]any)
	amount := payload["fields"].([]any)[0].(map[string]any)
	if table["description"] != "durable table" || amount["precision"] != "38" ||
		amount["scale"] != "18" || amount["roundingMode"] != "ROUND_HALF_EVEN" {
		t.Fatalf("restarted table = %#v", table)
	}
}

func startRuntimeForRestartTest(t *testing.T, configPath, baseURL string) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"--config", configPath}, io.Discard)
	}()

	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/readyz", nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case runErr := <-done:
			cancel()
			t.Fatalf("runtime stopped before readiness: %v", runErr)
		case <-deadline.C:
			cancel()
			t.Fatal("runtime did not become ready")
		case <-time.After(25 * time.Millisecond):
		}
	}

	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("runtime shutdown: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("runtime did not stop")
		}
	}
}

func runtimeRequest(t *testing.T, baseURL, method, path, body string, expectedStatus int) map[string]any {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, baseURL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expectedStatus {
		t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, response.StatusCode, expectedStatus, payload)
	}
	if len(payload) == 0 {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode %s %s response: %v; body=%s", method, path, err, payload)
	}
	return decoded
}

func unusedLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
