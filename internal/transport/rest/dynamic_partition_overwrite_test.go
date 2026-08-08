package rest

// Public REST job contract for Spark 0.44.2 dynamic time-partition overwrite.
// The exact script producer is pinned here so connector template drift fails at
// jobs.insert before a job record or DuckDB transaction is created.
//
// Sources:
//   - connector script producer:
//     https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870
//   - jobs.insert and jobs.get:
//     https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
//     https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	v0442 "github.com/leeyh0216/go-bemu/internal/adapters/sparkbigquery/v0442"
	"github.com/leeyh0216/go-bemu/internal/application"
)

const (
	restDynamicTargetAlias = "__target_0123456789abcdef0123456789abcdef"
	restDynamicSourceAlias = "__source_fedcba9876543210fedcba9876543210"
)

func TestSparkDynamicTimePartitionOverwriteCrossesRESTJobLifecycle(t *testing.T) {
	ctx, cancel := staticOverwriteRESTTestContext(t)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	analyzer, err := v0442.NewAnalyzer(warehouse)
	if err != nil {
		t.Fatal(err)
	}
	queries, err := application.NewQueryService(
		memory.NewJobRepository(), warehouse, analyzer, warehouse, catalog, clock, &testIDs{},
		application.WithQueryAnalyzer(analyzer),
		application.WithQueryDestinationCatalog(catalog),
		application.WithQueryMaterializer(warehouse),
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
	for _, tableID := range []string{"destination", "temporary", "invalid_temporary"} {
		timePartitioning := ""
		if tableID == "destination" {
			timePartitioning = `,"timePartitioning":{"type":"DAY","field":"partition_date"}`
		}
		idType := "INT64"
		if tableID == "invalid_temporary" {
			idType = "STRING"
		}
		body := fmt.Sprintf(`{
			"tableReference":{"tableId":%q},
			"schema":{"fields":[
				{"name":"id","type":%q},
				{"name":"partition_date","type":"DATE"},
				{"name":"payload","type":"STRING"}
			]}%s
		}`, tableID, idType, timePartitioning)
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
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.analytics.invalid_temporary`"+` VALUES `+
		`('not-an-int', DATE '2026-01-02', 'must-rollback')",
		"useLegacySql":false
	}`, http.StatusOK)

	script := restDynamicTimeOverwriteFixture()
	submitQueryJob(t, request, "spark-dynamic-overwrite-0442", script, http.StatusOK)
	status := waitForQueryJob(t, ctx.Done(), request, "spark-dynamic-overwrite-0442")
	if status["errorResult"] != nil {
		t.Fatalf("dynamic overwrite job failed: %#v", status)
	}
	completedJob := request(http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs/spark-dynamic-overwrite-0442?location=US", "", http.StatusOK)
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

	// A connector-shaped UUID alias drift is rejected during admission. The
	// follow-up jobs.get proves no PENDING job side effect was published.
	drifted := strings.Replace(script, restDynamicTargetAlias, "__target_not-a-connector-uuid", 1)
	assertQueryJobRejectedBeforeCreation(t, request, "spark-dynamic-overwrite-drift", drifted)
	assertQueryJobRejectedBeforeCreation(t, request, "spark-dynamic-overwrite-range", restDynamicRangeOverwriteFixture())
	assertQueryJobRejectedBeforeCreation(t, request, "spark-general-script", "DECLARE ordinary_value DEFAULT 1; SELECT ordinary_value")

	// Canonical source/destination types are checked before DuckDB can apply an
	// implicit STRING-to-INT64 cast. The DONE error remains an invalidQuery and no
	// destination transaction starts.
	rollbackScript := strings.ReplaceAll(script, "analytics.temporary", "analytics.invalid_temporary")
	submitQueryJob(t, request, "spark-dynamic-overwrite-rollback", rollbackScript, http.StatusOK)
	rollbackStatus := waitForQueryJob(t, ctx.Done(), request, "spark-dynamic-overwrite-rollback")
	if rollbackStatus["errorResult"] == nil {
		t.Fatalf("rollback fixture unexpectedly succeeded: %#v", rollbackStatus)
	}
	if rollbackStatus["errorResult"].(map[string]any)["reason"] != "invalidQuery" {
		t.Fatalf("schema drift reason = %#v, want invalidQuery", rollbackStatus["errorResult"])
	}
	result = request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"SELECT id FROM `+"`test-project.analytics.destination`"+` ORDER BY id",
		"useLegacySql":false
	}`, http.StatusOK)
	assertRESTQueryScalarValues(t, result, []string{"2", "3", "4", "5", "6"})

	missingSourceScript := strings.ReplaceAll(script, "analytics.temporary", "analytics.missing_source")
	missingResponse := submitQueryJob(t, request, "spark-dynamic-overwrite-missing-source", missingSourceScript, http.StatusNotFound)
	missingError := missingResponse["error"].(map[string]any)
	missingErrors := missingError["errors"].([]any)
	if missingErrors[0].(map[string]any)["reason"] != "notFound" {
		t.Fatalf("missing source reason = %#v, want notFound", missingError)
	}
	request(http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs/spark-dynamic-overwrite-missing-source?location=US", "", http.StatusNotFound)

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

func assertQueryJobRejectedBeforeCreation(
	t *testing.T,
	request func(string, string, string, int) map[string]any,
	jobID, query string,
) {
	t.Helper()
	submitQueryJob(t, request, jobID, query, http.StatusNotImplemented)
	request(http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs/"+jobID+"?location=US", "", http.StatusNotFound)
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

func restDynamicTimeOverwriteFixture() string {
	return "DECLARE partitions_to_delete DEFAULT " +
		"(SELECT ARRAY_AGG(DISTINCT(date_trunc(`partition_date`, DAY)) IGNORE NULLS) FROM `test-project.analytics.temporary`); \n" +
		"MERGE `test-project.analytics.destination` AS `" + restDynamicTargetAlias + "`\n" +
		"USING `test-project.analytics.temporary` AS `" + restDynamicSourceAlias + "`\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED BY SOURCE AND (TRUE) AND date_trunc(`" + restDynamicTargetAlias + "`.`partition_date`, DAY) " +
		"IN UNNEST(partitions_to_delete) THEN DELETE\n" +
		"WHEN NOT MATCHED BY TARGET THEN\n" +
		"INSERT(`id`,`partition_date`,`payload`) VALUES(`" + restDynamicSourceAlias + "`.`id`,`" +
		restDynamicSourceAlias + "`.`partition_date`,`" + restDynamicSourceAlias + "`.`payload`)"
}

func restDynamicRangeOverwriteFixture() string {
	return "DECLARE partitions_to_delete DEFAULT " +
		"(SELECT ARRAY_AGG(DISTINCT(IFNULL(IF(partition_id >= 100, 0, RANGE_BUCKET(partition_id, GENERATE_ARRAY(0, 100, 10))), -1)) IGNORE NULLS) " +
		"FROM `test-project.analytics.temporary`); \n" +
		"MERGE `test-project.analytics.destination` AS `" + restDynamicTargetAlias + "`\n" +
		"USING `test-project.analytics.temporary` AS `" + restDynamicSourceAlias + "`\n" +
		"ON FALSE\n" +
		"WHEN NOT MATCHED BY SOURCE AND (`" + restDynamicTargetAlias + "`.`partition_id` IS NULL OR `" + restDynamicTargetAlias + "`.`partition_id` >= -9223372036854775808) " +
		"AND IFNULL(IF(`" + restDynamicTargetAlias + "`.`partition_id` >= 100, 0, RANGE_BUCKET(`" + restDynamicTargetAlias + "`.`partition_id`, GENERATE_ARRAY(0, 100, 10))), -1) " +
		"IN UNNEST(partitions_to_delete) THEN DELETE\n" +
		"WHEN NOT MATCHED BY TARGET THEN\n" +
		"INSERT(`id`,`partition_id`,`payload`) VALUES(`" + restDynamicSourceAlias + "`.`id`,`" +
		restDynamicSourceAlias + "`.`partition_id`,`" + restDynamicSourceAlias + "`.`payload`)"
}
