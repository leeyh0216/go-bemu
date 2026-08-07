package rest

// Query destination and polling contract sources:
//   - JobConfigurationQuery: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
//   - getQueryResults: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
//   - connector 0.44.2 copyData/materialization:
//     https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L315-L331

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
)

func TestQueryDestinationAndPagingCrossPublicRESTEdge(t *testing.T) {
	ctx, cancel := staticOverwriteRESTTestContext(t)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	queries := application.NewQueryService(
		memory.NewJobRepository(), warehouse, clock, &testIDs{},
		application.WithQueryMaterializer(warehouse), application.WithQueryDestinationCatalog(catalog),
	)
	server := httptest.NewServer(NewServer(catalog, queries, warehouse, "").Handler())
	t.Cleanup(server.Close)
	request := func(method, path, body string, wantStatus int) map[string]any {
		t.Helper()
		return staticOverwriteRESTRequest(t, ctx, server.URL, method, path, body, wantStatus)
	}

	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", `{"datasetReference":{"datasetId":"analytics"},"location":"US"}`, http.StatusOK)
	for _, tableID := range []string{"source", "destination"} {
		request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", fmt.Sprintf(`{
			"tableReference":{"tableId":%q},
			"schema":{"fields":[{"name":"id","type":"INT64","mode":"REQUIRED"},{"name":"payload","type":"STRING"}]}
		}`, tableID), http.StatusOK)
	}
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.analytics.source`"+` VALUES (2, 'two'), (3, 'three')","useLegacySql":false
	}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.analytics.destination`"+` VALUES (1, 'one')","useLegacySql":false
	}`, http.StatusOK)

	appendBody := queryDestinationJobBody(t, "spark-copy-0442", "US",
		"SELECT * FROM `test-project.analytics.source` WHERE id = 2", "destination", "WRITE_APPEND")
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", appendBody, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", appendBody, http.StatusConflict)
	driftedBody := queryDestinationJobBody(t, "spark-copy-0442", "US",
		"SELECT * FROM `test-project.analytics.source` WHERE id = 3", "destination", "WRITE_APPEND")
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", driftedBody, http.StatusConflict)
	request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs/spark-copy-0442?location=EU", "", http.StatusNotFound)
	completed := waitForRESTQueryJob(t, ctx, request, "spark-copy-0442", "US")
	queryConfiguration := completed["configuration"].(map[string]any)["query"].(map[string]any)
	jobConfiguration := completed["configuration"].(map[string]any)
	destination := queryConfiguration["destinationTable"].(map[string]any)
	if destination["projectId"] != "test-project" || destination["datasetId"] != "analytics" ||
		destination["tableId"] != "destination" || queryConfiguration["writeDisposition"] != "WRITE_APPEND" ||
		queryConfiguration["priority"] != "INTERACTIVE" {
		t.Fatalf("jobs.get did not round-trip destination configuration: %#v", queryConfiguration)
	}
	if labels, present := jobConfiguration["labels"].(map[string]any); !present || len(labels) != 0 {
		t.Fatalf("jobs.get did not preserve connector empty labels: %#v", jobConfiguration)
	}
	assertRESTQueryIDs(t, request, "destination", []string{"1", "2"})

	emptyBody := queryDestinationJobBody(t, "write-empty", "US",
		"SELECT * FROM `test-project.analytics.source` WHERE id = 3", "destination", "WRITE_EMPTY")
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", emptyBody, http.StatusOK)
	emptyJob := waitForRESTQueryJob(t, ctx, request, "write-empty", "US")
	if emptyJob["status"].(map[string]any)["errorResult"] == nil {
		t.Fatalf("WRITE_EMPTY job unexpectedly succeeded: %#v", emptyJob)
	}
	assertRESTQueryIDs(t, request, "destination", []string{"1", "2"})

	truncateBody := queryDestinationJobBody(t, "write-truncate", "US",
		"SELECT * FROM `test-project.analytics.source` WHERE id = 3", "destination", "WRITE_TRUNCATE")
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", truncateBody, http.StatusOK)
	waitForRESTQueryJob(t, ctx, request, "write-truncate", "US")
	assertRESTQueryIDs(t, request, "destination", []string{"3"})

	createBody := queryDestinationJobBody(t, "view-materialization", "US",
		"SELECT id, payload FROM `test-project.analytics.source` ORDER BY id", "materialized", "WRITE_EMPTY")
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", createBody, http.StatusOK)
	waitForRESTQueryJob(t, ctx, request, "view-materialization", "US")
	materialized := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/materialized", "", http.StatusOK)
	if materialized["id"] != "test-project:analytics.materialized" {
		t.Fatalf("materialized table metadata = %#v", materialized)
	}

	page := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"SELECT id, payload FROM `+"`test-project.analytics.materialized`"+` ORDER BY id",
		"useLegacySql":false,"maxResults":1,"location":"US"
	}`, http.StatusOK)
	if rows, ok := page["rows"].([]any); !ok || len(rows) != 1 || page["pageToken"] == nil {
		t.Fatalf("first result page = %#v", page)
	}
	pageJob := queryJobReference(page)
	pageToken := page["pageToken"].(string)
	secondPath := fmt.Sprintf("/bigquery/v2/projects/test-project/queries/%s?location=US&maxResults=1&pageToken=%s",
		pageJob["jobId"], url.QueryEscape(pageToken))
	second := request(http.MethodGet, secondPath, "", http.StatusOK)
	if rows, ok := second["rows"].([]any); !ok || len(rows) != 1 {
		t.Fatalf("second result page = %#v", second)
	}
	request(http.MethodGet, fmt.Sprintf("/bigquery/v2/projects/test-project/queries/spark-copy-0442?location=US&pageToken=%s", url.QueryEscape(pageToken)), "", http.StatusBadRequest)

	jobsPage := request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs?location=US&maxResults=1", "", http.StatusOK)
	jobsToken, ok := jobsPage["nextPageToken"].(string)
	if !ok || jobsToken == "" {
		t.Fatalf("jobs.list first page has no continuation: %#v", jobsPage)
	}
	request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs?location=US&maxResults=1&pageToken="+url.QueryEscape(jobsToken), "", http.StatusOK)
	request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs?location=EU&pageToken="+url.QueryEscape(jobsToken), "", http.StatusBadRequest)

	for name, body := range map[string]string{
		"dry run":     `{"query":"SELECT 1","dryRun":true}`,
		"priority":    `{"query":"SELECT 1","priority":"BATCH"}`,
		"parameters":  `{"query":"SELECT @id","queryParameters":[]}`,
		"labels":      `{"query":"SELECT 1","labels":{"component":"test"}}`,
		"job timeout": `{"query":"SELECT 1","jobTimeoutMs":2000}`,
	} {
		t.Run("reject synchronous "+name, func(t *testing.T) {
			request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", body, http.StatusBadRequest)
		})
	}
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"SELECT 1","requestId":"123e4567-e89b-12d3-a456-426614174000",
		"timeoutMs":1000,"formatOptions":{"useInt64Timestamp":true}
	}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries",
		`{"query":"SELECT 1","requestId":"1234567890123456789012345678901234567"}`, http.StatusBadRequest)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries",
		`{"query":"SELECT 1","timeoutMs":-1}`, http.StatusBadRequest)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", `{
		"jobReference":{"projectId":"test-project","jobId":"unsupported-options","location":"US"},
		"configuration":{"dryRun":true,"jobTimeoutMs":"1000","labels":{"component":"test"},
			"query":{"query":"SELECT @id","priority":"BATCH","queryParameters":[]}}
	}`, http.StatusBadRequest)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", `{
		"jobReference":{"projectId":"test-project","jobId":"bad-priority","location":"US"},
		"configuration":{"query":{"query":"SELECT 1","priority":"URGENT"}}
	}`, http.StatusBadRequest)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", `{
		"jobReference":{"projectId":"test-project","jobId":"bad-label","location":"US"},
		"configuration":{"labels":{"Uppercase":"secret-value"},"query":{"query":"SELECT 1","priority":"INTERACTIVE"}}
	}`, http.StatusBadRequest)
}

func queryDestinationJobBody(t *testing.T, jobID, location, sql, tableID, disposition string) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"jobReference": map[string]any{"projectId": "test-project", "jobId": jobID, "location": location},
		"configuration": map[string]any{"labels": map[string]string{}, "query": map[string]any{
			"query": sql, "useLegacySql": false,
			"priority":         "INTERACTIVE",
			"destinationTable": map[string]any{"projectId": "test-project", "datasetId": "analytics", "tableId": tableID},
			"writeDisposition": disposition, "createDisposition": "CREATE_IF_NEEDED",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func queryJobReference(resource map[string]any) map[string]any {
	reference, _ := resource["jobReference"].(map[string]any)
	return reference
}

func waitForRESTQueryJob(t *testing.T, ctx interface{ Done() <-chan struct{} }, request func(string, string, string, int) map[string]any, jobID, location string) map[string]any {
	t.Helper()
	for {
		job := request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs/"+jobID+"?location="+url.QueryEscape(location), "", http.StatusOK)
		if job["status"].(map[string]any)["state"] == "DONE" {
			return job
		}
		select {
		case <-ctx.Done():
			t.Fatalf("query job %s did not finish", jobID)
		case <-time.After(time.Millisecond):
		}
	}
}

func assertRESTQueryIDs(t *testing.T, request func(string, string, string, int) map[string]any, tableID string, want []string) {
	t.Helper()
	result := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", fmt.Sprintf(`{
		"query":"SELECT id FROM `+"`test-project.analytics.%s`"+` ORDER BY id","useLegacySql":false
	}`, tableID), http.StatusOK)
	rows, ok := result["rows"].([]any)
	if !ok || len(rows) != len(want) {
		t.Fatalf("table %s rows = %#v, want IDs %v", tableID, result["rows"], want)
	}
	for index, expected := range want {
		cells := rows[index].(map[string]any)["f"].([]any)
		if cells[0].(map[string]any)["v"] != expected {
			t.Fatalf("table %s row %d = %#v, want id=%s", tableID, index, rows[index], expected)
		}
	}
}
