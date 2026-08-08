package rest

// Public REST job contract for an atomic GoogleSQL script whose final MERGE
// replaces only partitions present in a temporary source table.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
)

const (
	restMergeTargetAlias = "target_rows"
	restMergeSourceAlias = "source_rows"
)

func TestPartitionMergeScriptCrossesRESTJobLifecycle(t *testing.T) {
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
		memory.NewJobRepository(), nil, clock, &testIDs{},
		application.WithGoogleSQLGateway(gateway),
		application.WithStatementExecutor(warehouse),
		application.WithStatementMaterializer(warehouse),
		application.WithQueryDDLExecutor(catalog),
		application.WithQueryDestinationCatalog(catalog),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := staticOverwriteRESTTestContext(t)
		defer cleanupCancel()
		_ = queries.Close(cleanupCtx)
	})
	server := httptest.NewServer(NewServer(catalog, queries, warehouse, "").Handler())
	t.Cleanup(server.Close)

	request := func(method, path, body string, wantStatus int) map[string]any {
		t.Helper()
		return staticOverwriteRESTRequest(t, ctx, server.URL, method, path, body, wantStatus)
	}
	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets",
		`{"datasetReference":{"datasetId":"analytics"},"location":"US"}`, http.StatusOK)
	for _, tableID := range []string{"destination", "temporary"} {
		timePartitioning := ""
		if tableID == "destination" {
			timePartitioning = `,"timePartitioning":{"type":"DAY","field":"partition_date"}`
		}
		body := fmt.Sprintf(`{
			"tableReference":{"tableId":%q},
			"schema":{"fields":[
				{"name":"id","type":"INT64"},
				{"name":"partition_date","type":"DATE"},
				{"name":"payload","type":"STRING"}
			]}%s
		}`, tableID, timePartitioning)
		request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", body, http.StatusOK)
	}
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.analytics.destination`"+` VALUES `+
		`(1, DATE '2026-01-01', 'old-one'), (2, DATE '2026-01-02', 'keep'), (3, NULL, 'old-null')",
		"useLegacySql":false
	}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.analytics.temporary`"+` VALUES `+
		`(4, DATE '2026-01-01', 'new-one'), (5, DATE '2026-01-01', 'new-one-again'), (6, NULL, 'new-null')",
		"useLegacySql":false
	}`, http.StatusOK)
	script := restPartitionMergeScript()
	submitQueryJob(t, request, "partition-merge", script, http.StatusOK)
	status := waitForQueryJob(t, ctx.Done(), request, "partition-merge")
	if status["errorResult"] != nil {
		t.Fatalf("dynamic overwrite job failed: %#v", status)
	}
	completedJob := request(http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs/partition-merge?location=US", "", http.StatusOK)
	statistics := completedJob["statistics"].(map[string]any)
	queryStatistics := statistics["query"].(map[string]any)
	if queryStatistics["statementType"] != "SCRIPT" {
		t.Fatalf("dynamic overwrite statementType = %#v, want SCRIPT", queryStatistics["statementType"])
	}
	if statistics["numDmlAffectedRows"] != "4" || queryStatistics["numDmlAffectedRows"] != "4" {
		t.Fatalf("dynamic overwrite DML statistics = %#v", statistics)
	}

	result := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"SELECT id FROM `+"`test-project.analytics.destination`"+` ORDER BY id",
		"useLegacySql":false
	}`, http.StatusOK)
	assertRESTQueryScalarValues(t, result, []string{"2", "3", "4", "5", "6"})

	scriptResult := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"DECLARE ordinary_value INT64 DEFAULT 1; SET ordinary_value = ordinary_value + 1; SELECT ordinary_value AS value",
		"useLegacySql":false
	}`, http.StatusOK)
	assertRESTQueryScalarValues(t, scriptResult, []string{"2"})

	missingSourceScript := strings.ReplaceAll(script, "analytics.temporary", "analytics.missing_source")
	missingResponse := submitQueryJob(t, request, "partition-merge-missing-source", missingSourceScript, http.StatusNotFound)
	missingError := missingResponse["error"].(map[string]any)
	missingErrors := missingError["errors"].([]any)
	if missingErrors[0].(map[string]any)["reason"] != "notFound" {
		t.Fatalf("missing source reason = %#v, want notFound", missingError)
	}
	request(http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs/partition-merge-missing-source?location=US", "", http.StatusNotFound)

	if err := queries.Close(ctx); err != nil {
		t.Fatalf("close query service: %v", err)
	}
}

func submitQueryJob(
	t *testing.T,
	request func(string, string, string, int) map[string]any,
	jobID, query string,
	wantStatus int,
) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jobReference": map[string]any{
			"projectId": "test-project", "jobId": jobID, "location": "US",
		},
		"configuration": map[string]any{
			"query": map[string]any{"query": query, "useLegacySql": false},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", string(body), wantStatus)
}

func waitForQueryJob(
	t *testing.T,
	done <-chan struct{},
	request func(string, string, string, int) map[string]any,
	jobID string,
) map[string]any {
	t.Helper()
	for {
		job := request(http.MethodGet,
			"/bigquery/v2/projects/test-project/jobs/"+jobID+"?location=US", "", http.StatusOK)
		status := job["status"].(map[string]any)
		if status["state"] == "DONE" {
			return status
		}
		select {
		case <-done:
			t.Fatal("dynamic overwrite job exceeded configurable REST test timeout")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func assertRESTQueryScalarValues(t *testing.T, response map[string]any, want []string) {
	t.Helper()
	rows, ok := response["rows"].([]any)
	if !ok || len(rows) != len(want) {
		t.Fatalf("query rows = %#v, want %d rows", response["rows"], len(want))
	}
	for index, row := range rows {
		fields := row.(map[string]any)["f"].([]any)
		value := fields[0].(map[string]any)["v"]
		if value != want[index] {
			t.Fatalf("row %d value = %#v, want %q", index, value, want[index])
		}
	}
}

func restPartitionMergeScript() string {
	return "DECLARE partitions_to_delete DEFAULT " +
		"(SELECT ARRAY_AGG(DISTINCT(date_trunc(`partition_date`, DAY)) IGNORE NULLS) FROM `test-project.analytics.temporary`); \n" +
		"MERGE `test-project.analytics.destination` AS `" + restMergeTargetAlias + "`\n" +
		"USING `test-project.analytics.temporary` AS `" + restMergeSourceAlias + "`\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED BY SOURCE AND (TRUE) AND date_trunc(`" + restMergeTargetAlias + "`.`partition_date`, DAY) " +
		"IN UNNEST(partitions_to_delete) THEN DELETE\n" +
		"WHEN NOT MATCHED BY TARGET THEN\n" +
		"INSERT(`id`,`partition_date`,`payload`) VALUES(`" + restMergeSourceAlias + "`.`id`,`" +
		restMergeSourceAlias + "`.`partition_date`,`" + restMergeSourceAlias + "`.`payload`)"
}
