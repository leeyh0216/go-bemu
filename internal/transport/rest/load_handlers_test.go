package rest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/objectstore"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	loadApplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
)

func TestCombinedJobsAPIExecutesParquetLoadAndPreservesQueryJobs(t *testing.T) {
	ctx := context.Background()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	ids := &testIDs{}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "name", Type: "STRING"}},
	}); err != nil {
		t.Fatal(err)
	}
	parquet := createRESTParquet(t, "SELECT 1::BIGINT AS id, 'first'::VARCHAR AS name UNION ALL SELECT 2, 'second'")
	parquetPayload, err := os.ReadFile(parquet)
	if err != nil {
		t.Fatal(err)
	}
	fakeGCS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/storage/v1/b/load-bucket/o":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{
				"name": "spark/part-00000.parquet", "size": fmt.Sprint(len(parquetPayload)), "generation": "1",
			}}})
		case strings.HasSuffix(r.URL.Path, "/o/spark/part-00000.parquet") && r.URL.Query().Get("alt") == "media":
			_, _ = w.Write(parquetPayload)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fakeGCS.Close)
	gcs, err := objectstore.NewGCSJSON(objectstore.GCSJSONConfig{Endpoint: fakeGCS.URL, Client: fakeGCS.Client()})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := objectstore.NewGCSOnlyRouter(gcs)
	if err != nil {
		t.Fatal(err)
	}
	loadConfig := loadApplication.DefaultConfig()
	loadConfig.TempDirectory = t.TempDir()
	loadConfig.OperationTimeout = 5 * time.Second
	loads, err := loadApplication.NewService(
		loadApplication.NewMemoryJobRepository(), objects, NewLoadTableCatalog(catalog), warehouse,
		clock, ids, loadConfig,
	)
	if err != nil {
		t.Fatal(err)
	}
	queries := application.NewQueryService(memory.NewJobRepository(), warehouse, clock, ids)
	server := httptest.NewServer(NewServerWithLoadJobs(catalog, queries, loads, warehouse, "").Handler())
	t.Cleanup(server.Close)

	body := fmt.Sprintf(`{
		"jobReference":{"projectId":"test-project","jobId":"load-one","location":"US"},
		"configuration":{"load":{
			"sourceUris":[%q],
			"destinationTable":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},
			"sourceFormat":"PARQUET","writeDisposition":"WRITE_APPEND"
		}}
	}`, "gs://load-bucket/spark/*.parquet")
	job := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", body, http.StatusOK)
	job = waitForRESTLoad(t, server.URL, "load-one")
	status := job["status"].(map[string]any)
	if status["errorResult"] != nil {
		t.Fatalf("load job failed: %#v", job)
	}
	statistics := job["statistics"].(map[string]any)["load"].(map[string]any)
	if statistics["inputFiles"] != "1" || statistics["outputRows"] != "2" {
		t.Fatalf("unexpected load statistics: %#v", statistics)
	}

	// Same tuple and configuration is a read-only retry, not a second append.
	retry := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", body, http.StatusOK)
	if retry["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("idempotent retry did not return existing job: %#v", retry)
	}
	query := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/queries",
		`{"query":"SELECT count(*) AS rows FROM `+"`test-project.analytics.events`"+`","useLegacySql":false}`, http.StatusOK)
	if query["totalRows"] != "1" || query["rows"].([]any)[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"] != "2" {
		t.Fatalf("unexpected query result after load: %#v", query)
	}
	listed := restLoadRequest(t, server.URL, http.MethodGet, "/bigquery/v2/projects/test-project/jobs?location=US", "", http.StatusOK)
	if len(listed["jobs"].([]any)) < 1 {
		t.Fatalf("load job missing from list: %#v", listed)
	}
}

func TestCombinedJobsAPIReturnsStrictLoadGapsAsTerminalJobs(t *testing.T) {
	ctx := context.Background()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Now().UTC()}
	ids := &testIDs{}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	_, _ = catalog.CreateProject(ctx, domain.Project{ID: "test-project"})
	_, _ = catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"})
	_, _ = catalog.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "events", Schema: []domain.Field{{Name: "id", Type: "INT64"}}})
	config := loadApplication.DefaultConfig()
	config.TempDirectory = t.TempDir()
	loads, err := loadApplication.NewService(loadApplication.NewMemoryJobRepository(), objectstore.FileSystem{}, NewLoadTableCatalog(catalog), warehouse, clock, ids, config)
	if err != nil {
		t.Fatal(err)
	}
	queries := application.NewQueryService(memory.NewJobRepository(), warehouse, clock, ids)
	server := httptest.NewServer(NewServerWithLoadJobs(catalog, queries, loads, warehouse, "").Handler())
	t.Cleanup(server.Close)

	body := `{"jobReference":{"jobId":"avro-gap","location":"US"},"configuration":{"load":{"sourceUris":["file:///does-not-exist.avro"],"destinationTable":{"datasetId":"analytics","tableId":"events"},"sourceFormat":"AVRO"}}}`
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", body, http.StatusOK)
	job := waitForRESTLoad(t, server.URL, "avro-gap")
	errorResult := job["status"].(map[string]any)["errorResult"].(map[string]any)
	if errorResult["reason"] != "notImplemented" {
		t.Fatalf("unexpected gap job: %#v", job)
	}

	both := `{"configuration":{"query":{"query":"SELECT 1"},"load":{"sourceUris":["file:///x"],"destinationTable":{"datasetId":"analytics","tableId":"events"},"sourceFormat":"PARQUET"}}}`
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", both, http.StatusBadRequest)
}

func restLoadRequest(t *testing.T, baseURL, method, path, body string, expectedStatus int) map[string]any {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, baseURL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expectedStatus {
		t.Fatalf("%s %s: status=%d payload=%s", method, path, response.StatusCode, payload)
	}
	if len(payload) == 0 {
		return nil
	}
	var result map[string]any
	if err := json.Unmarshal(payload, &result); err != nil {
		t.Fatalf("decode response %s: %v", payload, err)
	}
	return result
}

func waitForRESTLoad(t *testing.T, baseURL, jobID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job := restLoadRequest(t, baseURL, http.MethodGet, "/bigquery/v2/projects/test-project/jobs/"+jobID+"?location=US", "", http.StatusOK)
		if job["status"].(map[string]any)["state"] == "DONE" {
			return job
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("REST load job did not reach DONE")
	return nil
}

func createRESTParquet(t *testing.T, query string) string {
	t.Helper()
	database, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	path := filepath.Join(t.TempDir(), "source.parquet")
	quotedPath := "'" + strings.ReplaceAll(path, "'", "''") + "'"
	if _, err := database.Exec("COPY (" + query + ") TO " + quotedPath + " (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	return path
}
