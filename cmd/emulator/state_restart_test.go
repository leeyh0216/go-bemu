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

func TestRuntimeJobMetadataSurvivesRestart(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	contracttest.Operation(t, "bigquery.jobs.insert")
	contracttest.Operation(t, "bigquery.jobs.get")
	contracttest.Operation(t, "bigquery.jobs.getQueryResults")

	directory := t.TempDir()
	httpAddress := unusedLoopbackAddress(t)
	grpcAddress := unusedLoopbackAddress(t)
	baseURL := "http://" + httpAddress
	configPath := filepath.Join(directory, "bqemu.yaml")
	configBody := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: test-project
  location: US
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
load:
  gcsEndpoint: "http://127.0.0.1:1"
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
	runtimeRequest(t, baseURL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", `{
  "jobReference":{"projectId":"test-project","jobId":"persisted-query","location":"US"},
  "configuration":{"labels":{"purpose":"restart"},"query":{"query":"SELECT 42 AS answer","useLegacySql":false,"priority":"BATCH"}}
}`, http.StatusOK)
	queryBeforeRestart := waitForRuntimeJob(t, baseURL, "persisted-query")
	if queryBeforeRestart["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("query job did not complete: %#v", queryBeforeRestart)
	}
	runtimeRequest(t, baseURL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", `{
  "jobReference":{"projectId":"test-project","jobId":"persisted-load","location":"US"},
  "configuration":{"load":{
    "sourceUris":["gs://restart-fixtures/input.csv"],
    "destinationTable":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},
    "sourceFormat":"CSV","writeDisposition":"WRITE_TRUNCATE","createDisposition":"CREATE_IF_NEEDED"
  }}
}`, http.StatusOK)
	loadBeforeRestart := waitForRuntimeJob(t, baseURL, "persisted-load")
	if loadBeforeRestart["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("load job did not complete: %#v", loadBeforeRestart)
	}
	stop()

	stop = startRuntimeForRestartTest(t, configPath, baseURL)
	t.Cleanup(stop)
	restartedQuery := runtimeRequest(t, baseURL, http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs/persisted-query?location=US", "", http.StatusOK)
	queryConfiguration := restartedQuery["configuration"].(map[string]any)["query"].(map[string]any)
	queryLabels := restartedQuery["configuration"].(map[string]any)["labels"].(map[string]any)
	if restartedQuery["status"].(map[string]any)["state"] != "DONE" ||
		queryConfiguration["query"] != "SELECT 42 AS answer" || queryConfiguration["priority"] != "BATCH" ||
		queryLabels["purpose"] != "restart" {
		t.Fatalf("restarted query job = %#v", restartedQuery)
	}
	missingRows := runtimeRequest(t, baseURL, http.MethodGet,
		"/bigquery/v2/projects/test-project/queries/persisted-query?location=US", "", http.StatusInternalServerError)
	queryError := missingRows["error"].(map[string]any)
	queryErrors := queryError["errors"].([]any)
	if queryErrors[0].(map[string]any)["reason"] != "backendError" ||
		!strings.Contains(queryError["message"].(string), "capability=query.results.restart-payload-unavailable-v1") {
		t.Fatalf("restarted query result error = %#v", missingRows)
	}

	restartedLoad := runtimeRequest(t, baseURL, http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs/persisted-load?location=US", "", http.StatusOK)
	loadConfiguration := restartedLoad["configuration"].(map[string]any)["load"].(map[string]any)
	destination := loadConfiguration["destinationTable"].(map[string]any)
	loadStatus := restartedLoad["status"].(map[string]any)
	loadStatistics := restartedLoad["statistics"].(map[string]any)["load"].(map[string]any)
	if loadStatus["state"] != "DONE" || loadStatus["errorResult"].(map[string]any)["reason"] != "notImplemented" ||
		destination["tableId"] != "events" || loadConfiguration["writeDisposition"] != "WRITE_TRUNCATE" ||
		loadStatistics["inputFiles"] != "0" || loadStatistics["outputRows"] != "0" {
		t.Fatalf("restarted load job = %#v", restartedLoad)
	}
}

func waitForRuntimeJob(t *testing.T, baseURL, jobID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		job := runtimeRequest(t, baseURL, http.MethodGet,
			"/bigquery/v2/projects/test-project/jobs/"+jobID+"?location=US", "", http.StatusOK)
		if job["status"].(map[string]any)["state"] == "DONE" {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s did not reach DONE: %#v", jobID, job)
		}
		time.Sleep(25 * time.Millisecond)
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
