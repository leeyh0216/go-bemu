package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
)

func TestGoogleSQLDDLMutatesCatalogAndStorageThroughPublicREST(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := &anonymousRESTClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(
		memory.NewCatalogRepository(), warehouse, clock,
		application.WithDDLStorage(warehouse), application.WithTableDataReader(warehouse),
	)
	parser, err := googlesqladapter.NewParser()
	if err != nil {
		t.Fatal(err)
	}
	queries := newRESTTestQueryService(
		memory.NewJobRepository(), warehouse, clock, &testIDs{},
		application.WithQueryAnalyzer(warehouse),
		application.WithQueryDestinationCatalog(catalog),
		application.WithQueryDDLParser(parser),
		application.WithQueryDDLExecutor(catalog),
	)
	server := httptest.NewServer(NewServer(catalog, queries, warehouse, "", WithTableDataAPI(catalog)).Handler())
	t.Cleanup(server.Close)
	request := func(path, body string, status int) map[string]any {
		return staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodPost, path, body, status)
	}
	request("/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	request("/bigquery/v2/projects/test-project/datasets", `{"datasetReference":{"datasetId":"analytics"},"location":"US"}`, http.StatusOK)
	query := func(sql string, status int) map[string]any {
		return request("/bigquery/v2/projects/test-project/queries", `{"query":`+ddlQuoteJSON(sql)+`,"useLegacySql":false}`, status)
	}

	query("CREATE TABLE `test-project.analytics.events` (id INT64 NOT NULL, note STRING, convertible STRING)", http.StatusOK)
	table := staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusOK)
	fields := table["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 3 || fields[0].(map[string]any)["name"] != "id" {
		t.Fatalf("CREATE schema = %#v", fields)
	}
	query("ALTER TABLE `test-project.analytics.events` ADD COLUMN score NUMERIC(10,2)", http.StatusOK)

	request("/bigquery/v2/projects/test-project/jobs", `{
      "jobReference":{"projectId":"test-project","jobId":"ddl-async-rename"},
      "configuration":{"query":{"query":"ALTER TABLE `+"`test-project.analytics.events`"+` RENAME COLUMN note TO message","useLegacySql":false}}
    }`, http.StatusOK)
	async := waitForDDLJob(t, ctx, server.URL, "ddl-async-rename")
	if async["status"].(map[string]any)["errorResult"] != nil {
		t.Fatalf("asynchronous DDL failed: %#v", async)
	}
	statement := async["statistics"].(map[string]any)["query"].(map[string]any)["statementType"]
	if statement != "ALTER_TABLE" {
		t.Fatalf("ALTER statementType = %#v", statement)
	}

	query("INSERT INTO `test-project.analytics.events` VALUES (1, 'not-an-integer', '7', 2.50)", http.StatusOK)
	if result := query("ALTER TABLE `test-project.analytics.events` ALTER COLUMN message SET DATA TYPE INT64", http.StatusOK); result["errors"] == nil {
		t.Fatal("incompatible SET DATA TYPE unexpectedly succeeded")
	}
	if result := query("ALTER TABLE `test-project.analytics.events` ALTER COLUMN convertible SET DATA TYPE INT64", http.StatusOK); result["errors"] != nil {
		t.Fatalf("compatible SET DATA TYPE failed: %#v", result)
	}
	if result := query("ALTER TABLE `test-project.analytics.events` DROP COLUMN score", http.StatusOK); result["errors"] != nil {
		t.Fatalf("DROP COLUMN failed: %#v", result)
	}
	table = staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusOK)
	fields = table["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 3 || fields[1].(map[string]any)["name"] != "message" || fields[2].(map[string]any)["type"] != "INT64" {
		t.Fatalf("ALTER schema = %#v", fields)
	}
	data := staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?maxResults=10", "", http.StatusOK)
	if data["totalRows"] != "1" {
		t.Fatalf("ALTER data = %#v", data)
	}

	query("TRUNCATE TABLE `test-project.analytics.events`", http.StatusOK)
	data = staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?maxResults=10", "", http.StatusOK)
	if data["totalRows"] != "0" {
		t.Fatalf("TRUNCATE data = %#v", data)
	}

	query("CREATE TABLE `test-project.analytics.hidden` (id INT64); DROP TABLE `test-project.analytics.hidden`", http.StatusNotImplemented)
	staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/hidden", "", http.StatusNotFound)
	query("DROP TABLE `test-project.analytics.events`", http.StatusOK)
	staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusNotFound)
}

func waitForDDLJob(t *testing.T, ctx context.Context, baseURL, jobID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job := staticOverwriteRESTRequest(t, ctx, baseURL, http.MethodGet,
			"/bigquery/v2/projects/test-project/jobs/"+jobID, "", http.StatusOK)
		if job["status"].(map[string]any)["state"] == "DONE" {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("DDL job did not complete")
	return nil
}

func ddlQuoteJSON(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
