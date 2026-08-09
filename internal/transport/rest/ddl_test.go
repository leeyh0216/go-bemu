package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	stateadapter "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/application"
)

func TestSingleStatementDDLMutatesCanonicalCatalogThroughPublicREST(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	stateStore, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(filepath.Join(t.TempDir(), "state.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stateStore.Close() })
	clock := &anonymousRESTClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(stateStore, warehouse, clock, application.WithTableDataReader(warehouse))
	ddlParser, err := googlesqladapter.NewParser()
	if err != nil {
		t.Fatal(err)
	}
	queries := application.NewQueryService(memory.NewJobRepository(), warehouse, clock, &testIDs{},
		application.WithQueryAnalyzer(warehouse), application.WithQueryDestinationCatalog(catalog),
		application.WithQueryDDLParser(ddlParser), application.WithQueryDDLExecutor(catalog))
	server := httptest.NewServer(NewServer(catalog, queries, warehouse, "", WithTableDataAPI(catalog)).Handler())
	t.Cleanup(server.Close)
	request := func(path, body string, status int) map[string]any {
		return staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodPost, path, body, status)
	}
	request("/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	request("/bigquery/v2/projects/test-project/datasets", `{"datasetReference":{"datasetId":"analytics"},"location":"US"}`, http.StatusOK)
	query := func(sql string, status int) map[string]any {
		return request("/bigquery/v2/projects/test-project/queries", `{"query":`+quoteJSON(sql)+`,"useLegacySql":false}`, status)
	}

	query("CREATE TABLE `test-project.analytics.events` (id INT64 NOT NULL, note STRING)", http.StatusOK)
	table := staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusOK)
	fields := table["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 2 || fields[0].(map[string]any)["name"] != "id" {
		t.Fatalf("CREATE canonical schema = %#v", fields)
	}

	query("ALTER TABLE `test-project.analytics.events` ADD COLUMN score NUMERIC(10,2)", http.StatusOK)
	query("ALTER TABLE `test-project.analytics.events` RENAME COLUMN note TO message", http.StatusOK)
	query("ALTER TABLE `test-project.analytics.events` ADD COLUMN convertible STRING", http.StatusOK)
	request("/bigquery/v2/projects/test-project/jobs", `{
      "jobReference":{"projectId":"test-project","jobId":"ddl-async"},
      "configuration":{"query":{"query":"ALTER TABLE `+"`test-project.analytics.events`"+` RENAME COLUMN score TO amount","useLegacySql":false}}
    }`, http.StatusOK)
	var async map[string]any
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		async = staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet, "/bigquery/v2/projects/test-project/jobs/ddl-async", "", http.StatusOK)
		if async["status"].(map[string]any)["state"] == "DONE" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if async == nil || async["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("asynchronous DDL job did not complete: %#v", async)
	}
	query("INSERT INTO `test-project.analytics.events` VALUES (1, 'not-an-integer', 2.50, '7')", http.StatusOK)
	if result := query("ALTER TABLE `test-project.analytics.events` ALTER COLUMN message SET DATA TYPE INT64", http.StatusOK); result["errors"] == nil {
		t.Fatal("incompatible SET DATA TYPE unexpectedly succeeded")
	}
	if result := query("ALTER TABLE `test-project.analytics.events` ALTER COLUMN convertible SET DATA TYPE INT64", http.StatusOK); result["errors"] != nil {
		t.Fatalf("compatible SET DATA TYPE failed: %#v", result)
	}
	if result := query("ALTER TABLE `test-project.analytics.events` DROP COLUMN amount", http.StatusOK); result["errors"] != nil {
		t.Fatalf("DROP COLUMN failed: %#v", result)
	}
	table = staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusOK)
	fields = table["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 3 || fields[0].(map[string]any)["type"] != "INT64" || fields[1].(map[string]any)["name"] != "message" || fields[1].(map[string]any)["type"] != "STRING" || fields[2].(map[string]any)["name"] != "convertible" || fields[2].(map[string]any)["type"] != "INT64" {
		t.Fatalf("ALTER canonical schema = %#v", fields)
	}
	data := staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?maxResults=10", "", http.StatusOK)
	if data["totalRows"] != "1" {
		t.Fatalf("ALTER physical schema did not accept updated row: %#v", data)
	}
	row := data["rows"].([]any)[0].(map[string]any)["f"].([]any)
	if row[1].(map[string]any)["v"] != "not-an-integer" {
		t.Fatalf("SET DATA TYPE changed physical row: %#v", row)
	}
	if row[2].(map[string]any)["v"] != "7" {
		t.Fatalf("compatible SET DATA TYPE did not convert row: %#v", row)
	}

	query("SELECT 1; CREATE TABLE `test-project.analytics.hidden` (id INT64)", http.StatusNotImplemented)
	staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/hidden", "", http.StatusNotFound)
	query("DROP TABLE `test-project.analytics.events`", http.StatusOK)
	staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusNotFound)
}

func quoteJSON(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
