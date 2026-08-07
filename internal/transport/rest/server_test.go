package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
)

type testClock struct{ value time.Time }

func (c testClock) Now() time.Time { return c.value }

type testIDs struct{ next atomic.Int64 }

func (ids *testIDs) NewID() string { return fmt.Sprintf("fixed-%d", ids.next.Add(1)) }

func TestBigQueryRESTMetadataAndSynchronousQuery(t *testing.T) {
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	queries := application.NewQueryService(memory.NewJobRepository(), warehouse, clock, &testIDs{})
	server := httptest.NewServer(NewServer(catalog, queries, warehouse, "").Handler())
	t.Cleanup(server.Close)

	request := func(method, path, body string, expectedStatus int) map[string]any {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), method, server.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		payload, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != expectedStatus {
			t.Fatalf("%s %s: status %d, body %s", method, path, response.StatusCode, payload)
		}
		if len(payload) == 0 {
			return nil
		}
		var decoded map[string]any
		if err := json.Unmarshal(payload, &decoded); err != nil {
			t.Fatalf("decode %s: %v", payload, err)
		}
		return decoded
	}

	discovery := request(http.MethodGet, "/$discovery/rest?version=v2", "", http.StatusOK)
	if discovery["id"] != "bigquery:v2" || discovery["baseUrl"] != server.URL+"/bigquery/v2/" {
		t.Fatalf("unexpected discovery document: %#v", discovery)
	}
	resources := discovery["resources"].(map[string]any)
	jobMethods := resources["jobs"].(map[string]any)["methods"].(map[string]any)
	jobListParameters := jobMethods["list"].(map[string]any)["parameters"].(map[string]any)
	for _, parameter := range []string{"projection", "minCreationTime", "maxCreationTime", "parentJobId"} {
		if jobListParameters[parameter] == nil {
			t.Fatalf("jobs.list discovery is missing bq CLI parameter %q", parameter)
		}
	}

	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", `{"datasetReference":{"datasetId":"analytics"},"location":"US"}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", `{"datasetReference":{"datasetId":"analytics"}}`, http.StatusConflict)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", `{
		"tableReference":{"tableId":"events"},
		"schema":{"fields":[{"name":"id","type":"INT64","mode":"REQUIRED"},{"name":"name","type":"STRING"}]}
	}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.analytics.events`"+` VALUES (1, 'first'), (2, 'second')",
		"useLegacySql":false
	}`, http.StatusOK)
	response := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"SELECT id, name FROM `+"`test-project.analytics.events`"+` ORDER BY id",
		"useLegacySql":false
	}`, http.StatusOK)
	if response["jobComplete"] != true || response["totalRows"] != "2" {
		t.Fatalf("unexpected query response: %#v", response)
	}
	rows, ok := response["rows"].([]any)
	if !ok || len(rows) != 2 {
		t.Fatalf("unexpected REST rows: %#v", response["rows"])
	}

	table := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusOK)
	if table["id"] != "test-project:analytics.events" {
		t.Fatalf("unexpected table resource: %#v", table)
	}

	job := request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", `{
		"jobReference":{"projectId":"test-project","jobId":"bq-cli-job","location":"US"},
		"configuration":{"query":{"query":"SELECT COUNT(*) AS row_count FROM `+"`test-project.analytics.events`"+`","useLegacySql":false}}
	}`, http.StatusOK)
	if jobReference, ok := job["jobReference"].(map[string]any); !ok || jobReference["jobId"] != "bq-cli-job" {
		t.Fatalf("unexpected jobs.insert response: %#v", job)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		job = request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs/bq-cli-job?location=US", "", http.StatusOK)
		status := job["status"].(map[string]any)
		if status["state"] == "DONE" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not complete: %#v", job)
		}
		time.Sleep(10 * time.Millisecond)
	}
	jobResults := request(http.MethodGet, "/bigquery/v2/projects/test-project/queries/bq-cli-job?location=US", "", http.StatusOK)
	if jobResults["jobComplete"] != true || jobResults["totalRows"] != "1" {
		t.Fatalf("unexpected jobs.getQueryResults response: %#v", jobResults)
	}
	listed := request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs?maxResults=10", "", http.StatusOK)
	if _, ok := listed["jobs"].([]any); !ok {
		t.Fatalf("unexpected jobs.list response: %#v", listed)
	}
}

func TestOptionalConsoleSPAHandler(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(directory+"/index.html", []byte("<main>console</main>"), 0o600); err != nil {
		t.Fatal(err)
	}
	handler := newSPAHandler(directory)
	request := httptest.NewRequest(http.MethodGet, "/projects/test-project", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte("console")) {
		t.Fatalf("unexpected SPA fallback: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
