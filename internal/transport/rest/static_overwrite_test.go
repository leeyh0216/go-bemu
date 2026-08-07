package rest

// End-to-end contract for the REST half of Spark direct overwrite.
//
// The connector commits its temporary table through Storage Write first, then
// submits the constant-false MERGE as a query job, polls that job, and deletes
// the temporary table. This test starts at the public HTTP transport and uses a
// real DuckDB warehouse so routing, job state, the versioned SQL adapter, and
// cleanup cannot drift independently.
//
// Sources:
//   - connector 0.44.2: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java
//   - jobs.insert: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
//   - jobs.get: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
)

func TestSparkStaticOverwriteCrossesRESTJobLifecycle(t *testing.T) {
	ctx, cancel := staticOverwriteRESTTestContext(t)
	defer cancel()
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

	request := func(method, path, body string, wantStatus int) map[string]any {
		t.Helper()
		return staticOverwriteRESTRequest(t, ctx, server.URL, method, path, body, wantStatus)
	}
	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", `{"datasetReference":{"datasetId":"analytics"},"location":"US"}`, http.StatusOK)
	for _, tableID := range []string{"destination", "temporary"} {
		body := fmt.Sprintf(`{
			"tableReference":{"tableId":%q},
			"schema":{"fields":[{"name":"id","type":"INT64","mode":"REQUIRED"},{"name":"payload","type":"STRING"}]}
		}`, tableID)
		request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", body, http.StatusOK)
	}
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.analytics.destination`"+` VALUES (1, 'old'), (2, 'remove')",
		"useLegacySql":false
	}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.analytics.temporary`"+` VALUES (3, 'new'), (4, 'replacement')",
		"useLegacySql":false
	}`, http.StatusOK)

	merge := "MERGE `test-project.analytics.destination`\n" +
		"USING (SELECT * FROM `test-project.analytics.temporary`)\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED THEN INSERT ROW\n" +
		"WHEN NOT MATCHED BY SOURCE THEN DELETE"
	jobBody, err := json.Marshal(map[string]any{
		"jobReference":  map[string]any{"projectId": "test-project", "jobId": "spark-overwrite-0442", "location": "US"},
		"configuration": map[string]any{"query": map[string]any{"query": merge, "useLegacySql": false}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", string(jobBody), http.StatusOK)

	for {
		job := request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs/spark-overwrite-0442?location=US", "", http.StatusOK)
		status := job["status"].(map[string]any)
		if status["state"] == "DONE" {
			if status["errorResult"] != nil {
				t.Fatalf("static overwrite job failed: %#v", status)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("static overwrite job deadline: %v", ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}

	result := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"SELECT id, payload FROM `+"`test-project.analytics.destination`"+` ORDER BY id",
		"useLegacySql":false
	}`, http.StatusOK)
	rows, ok := result["rows"].([]any)
	if !ok || len(rows) != 2 {
		t.Fatalf("overwrite rows = %#v", result["rows"])
	}
	request(http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/analytics/tables/temporary", "", http.StatusNoContent)
	request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/temporary", "", http.StatusNotFound)
}

func staticOverwriteRESTRequest(t *testing.T, ctx context.Context, baseURL, method, path, body string, wantStatus int) map[string]any {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewBufferString(body))
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
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s: status=%d body=%s", method, path, response.StatusCode, payload)
	}
	if len(payload) == 0 {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode response %s: %v", payload, err)
	}
	return decoded
}

func staticOverwriteRESTTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 15 * time.Second
	if configured := os.Getenv("BQEMU_REST_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("BQEMU_REST_TEST_TIMEOUT must be a positive Go duration: %q", configured)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}
