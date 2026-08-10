package rest

// Public REST contract for a constant-false GoogleSQL MERGE submitted through
// the standard query job lifecycle.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
)

func TestConstantFalseMergeCrossesRESTJobLifecycle(t *testing.T) {
	ctx, cancel := staticOverwriteRESTTestContext(t)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	gateway, err := googlesqladapter.NewGateway(catalog)
	if err != nil {
		t.Fatal(err)
	}
	queries, err := application.NewQueryService(
		memory.NewJobRepository(), clock, &testIDs{},
		application.WithGoogleSQLGateway(gateway),
		application.WithStatementExecutor(warehouse),
		application.WithStatementMaterializer(warehouse),
		application.WithQueryDDLExecutor(catalog),
		application.WithQueryDestinationCatalog(catalog),
	)
	if err != nil {
		t.Fatal(err)
	}
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

	// An expression outside the supported GoogleSQL subset must fail before the
	// MERGE can reach DuckDB. The public job boundary must therefore leave both
	// the destination contents and the source table untouched.
	unsupportedMerge := "MERGE `test-project.analytics.destination` AS target " +
		"USING `test-project.analytics.temporary` AS source " +
		"ON READ_CSV('/private/unsupported.csv') " +
		"WHEN MATCHED THEN DELETE"
	unsupportedBody, err := json.Marshal(map[string]any{
		"query": unsupportedMerge, "useLegacySql": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	// BigQuery synchronous query responses carry analysis failures in a 200
	// getQueryResults envelope rather than turning them into an HTTP error.
	unsupported := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", string(unsupportedBody), http.StatusOK)
	errors, ok := unsupported["errors"].([]any)
	if !ok || len(errors) != 1 {
		t.Fatalf("unsupported MERGE response = %#v", unsupported)
	}
	message := errors[0].(map[string]any)["message"].(string)
	if !strings.Contains(message, "READ_CSV") || !strings.Contains(message, "query.googlesql.analysis-invalid-v1") {
		t.Fatalf("unsupported MERGE error = %q", message)
	}
	// The rejection must be independent of the MERGE action in which the
	// unsupported expression occurs. In particular, an UPDATE action must not
	// reach DuckDB merely because its match predicate itself is supported.
	unsupportedUpdate := "MERGE `test-project.analytics.destination` AS target " +
		"USING `test-project.analytics.temporary` AS source " +
		"ON target.id = source.id " +
		"WHEN MATCHED THEN UPDATE SET payload = READ_CSV('/private/unsupported.csv')"
	unsupportedUpdateBody, err := json.Marshal(map[string]any{
		"query": unsupportedUpdate, "useLegacySql": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	unsupported = request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", string(unsupportedUpdateBody), http.StatusOK)
	errors, ok = unsupported["errors"].([]any)
	if !ok || len(errors) != 1 {
		t.Fatalf("unsupported MERGE update response = %#v", unsupported)
	}
	message = errors[0].(map[string]any)["message"].(string)
	if !strings.Contains(message, "READ_CSV") || !strings.Contains(message, "query.googlesql.analysis-invalid-v1") {
		t.Fatalf("unsupported MERGE update error = %q", message)
	}
	unchanged := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"SELECT id, payload FROM `+"`test-project.analytics.destination`"+` ORDER BY id",
		"useLegacySql":false
	}`, http.StatusOK)
	if rows, ok := unchanged["rows"].([]any); !ok || len(rows) != 2 {
		t.Fatalf("unsupported MERGE mutated destination: %#v", unchanged["rows"])
	}

	merge := "MERGE `test-project.analytics.destination`\n" +
		"USING (SELECT * FROM `test-project.analytics.temporary`)\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED THEN INSERT ROW\n" +
		"WHEN NOT MATCHED BY SOURCE THEN DELETE"
	jobBody, err := json.Marshal(map[string]any{
		"jobReference":  map[string]any{"projectId": "test-project", "jobId": "merge-overwrite", "location": "US"},
		"configuration": map[string]any{"query": map[string]any{"query": merge, "useLegacySql": false}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", string(jobBody), http.StatusOK)

	for {
		job := request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs/merge-overwrite?location=US", "", http.StatusOK)
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
