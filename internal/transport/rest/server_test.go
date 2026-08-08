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
	"github.com/leeyh0216/go-bemu/internal/capabilityspec"
	"github.com/leeyh0216/go-bemu/internal/contractspec"
	"github.com/leeyh0216/go-bemu/internal/contracttest"
)

type testClock struct{ value time.Time }

func (c testClock) Now() time.Time { return c.value }

type testIDs struct{ next atomic.Int64 }

func (ids *testIDs) NewID() string { return fmt.Sprintf("fixed-%d", ids.next.Add(1)) }

func TestBigQueryRESTMetadataAndSynchronousQuery(t *testing.T) {
	contracttest.Operation(t, "bqemu.capabilities.get")
	contracttest.Operation(t, "bqemu.console.get")
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	queries := application.NewQueryService(memory.NewJobRepository(), warehouse, clock, &testIDs{})
	server := httptest.NewServer(NewServer(
		catalog, queries, warehouse, "", WithCapabilityProfiles(capabilityspec.Profiles()),
	).Handler())
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
	if capabilities := request(http.MethodGet, "/bqemu/v1/capabilities", "", http.StatusOK); capabilities["kind"] != "bqemu#capabilityRegistry" {
		t.Fatalf("unexpected capabilities: %#v", capabilities)
	} else if profiles, ok := capabilities["profiles"].([]any); !ok || len(profiles) == 0 {
		t.Fatalf("capability profile snapshot is absent: %#v", capabilities)
	}
	if console := request(http.MethodGet, "/bqemu/v1/console", "", http.StatusOK); console["kind"] != "bqemu#consoleAPI" {
		t.Fatalf("unexpected console metadata: %#v", console)
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
	contracttest.Operation(t, "bqemu.console.assets")
	contracttest.Operation(t, "bqemu.console.redirect")
	directory := t.TempDir()
	if err := os.WriteFile(directory+"/index.html", []byte("<main>console</main>"), 0o600); err != nil {
		t.Fatal(err)
	}
	warehouse := &catalogTestWarehouse{}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, catalogTestClock{})
	handler := NewServer(catalog, nil, warehouse, "", WithConsoleDirectory(directory)).Handler()
	request := httptest.NewRequest(http.MethodGet, "/console/projects/test-project", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte("console")) {
		t.Fatalf("unexpected SPA fallback: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	redirect := httptest.NewRecorder()
	handler.ServeHTTP(redirect, httptest.NewRequest(http.MethodGet, "/", nil))
	if redirect.Code != http.StatusTemporaryRedirect || redirect.Header().Get("Location") != "/console/" {
		t.Fatalf("unexpected console redirect: status=%d location=%q", redirect.Code, redirect.Header().Get("Location"))
	}
}

func TestRESTRouteBindingsMatchOperationManifest(t *testing.T) {
	server := NewServer(nil, nil, nil, "", WithTableDataAPI(nil), WithConsoleDirectory(t.TempDir()))
	actual := make(map[string]bool)
	for _, operationID := range server.operationIDs() {
		if actual[operationID] {
			t.Fatalf("duplicate REST operation binding %q", operationID)
		}
		actual[operationID] = true
	}
	expected := contractspec.RESTRoutes("public-core", "public-query", "public-tabledata", "public-console")
	for _, route := range expected {
		if !actual[route.OperationID] {
			t.Errorf("manifest REST operation %q has no handler binding", route.OperationID)
		}
		delete(actual, route.OperationID)
	}
	for operationID := range actual {
		t.Errorf("handler binding %q is absent from the operation manifest", operationID)
	}
}

func TestDiscoveryMethodsMatchOperationManifest(t *testing.T) {
	document := discoveryDocument("http://127.0.0.1:9050", extendQueryDiscovery, extendTableDataDiscovery)
	actual := make(map[string]string)
	resources := document["resources"].(map[string]any)
	for resourceName, resourceValue := range resources {
		methods := resourceValue.(map[string]any)["methods"].(map[string]any)
		for methodName, methodValue := range methods {
			method := methodValue.(map[string]any)
			operationID := method["id"].(string)
			if actual[operationID] != "" {
				t.Fatalf("discovery operation %q is duplicated at %s.%s", operationID, resourceName, methodName)
			}
			actual[operationID] = method["httpMethod"].(string) + " /bigquery/v2/" + method["path"].(string)
			if operationID == "bigquery.jobs.insert" && (method["supportsMediaUpload"] != nil || method["mediaUpload"] != nil) {
				t.Fatal("jobs.insert advertises media upload paths that are not registered")
			}
		}
	}
	for _, operation := range contractspec.RESTRoutes() {
		if !operation.Discovery {
			continue
		}
		expected := operation.Method + " " + operation.Path
		if actual[operation.OperationID] != expected {
			t.Errorf("discovery operation %s = %q, want %q", operation.OperationID, actual[operation.OperationID], expected)
		}
		delete(actual, operation.OperationID)
	}
	for operationID, shape := range actual {
		t.Errorf("discovery advertises undeclared operation %s as %s", operationID, shape)
	}
}
