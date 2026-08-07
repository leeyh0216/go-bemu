package rest

// Anonymous query-result compatibility contract:
//   - JobConfigurationQuery.destinationTable is populated for anonymous
//     results: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
//   - anonymous datasets are hidden and tables live for approximately 24h:
//     https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored
//   - omitted locations are inferred from referenced/default/destination
//     datasets: https://cloud.google.com/bigquery/docs/locations#specify_locations
//   - connector 0.44.2 reads the completed job's generated destination here:
//     https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L1150-L1240
//   - connector 0.44.2 default materialization expiration is 24 hours:
//     https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/MaterializationConfiguration.java

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

type anonymousRESTClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (clock *anonymousRESTClock) Now() time.Time {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.now
}

func (clock *anonymousRESTClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func TestAnonymousDestinationAndLocationInferenceCrossPublicRESTEdge(t *testing.T) {
	ctx, cancel := staticOverwriteRESTTestContext(t)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	clock := &anonymousRESTClock{now: now}
	catalog := application.NewCatalogService(
		memory.NewCatalogRepository(), warehouse, clock,
		application.WithTableDataReader(warehouse),
	)
	queries := application.NewQueryService(
		memory.NewJobRepository(), warehouse, clock, &testIDs{},
		application.WithQueryAnalyzer(warehouse), application.WithQueryMaterializer(warehouse),
		application.WithQueryDestinationCatalog(catalog), application.WithAnonymousQueryTTL(24*time.Hour),
	)
	server := httptest.NewServer(NewServer(catalog, queries, warehouse, "", WithTableDataAPI(catalog)).Handler())
	t.Cleanup(server.Close)
	request := func(method, path, body string, wantStatus int) map[string]any {
		t.Helper()
		return staticOverwriteRESTRequest(t, ctx, server.URL, method, path, body, wantStatus)
	}

	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	for datasetID, location := range map[string]string{"eu_source": "EU", "us_source": "US"} {
		request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", fmt.Sprintf(
			`{"datasetReference":{"datasetId":%q},"location":%q}`, datasetID, location), http.StatusOK)
		request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/"+datasetID+"/tables", `{
			"tableReference":{"tableId":"events"},
			"schema":{"fields":[{"name":"id","type":"INT64","mode":"REQUIRED"}]}
		}`, http.StatusOK)
	}
	ddl := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"DROP TABLE `+"`test-project.eu_source.events`"+`","useLegacySql":false
	}`, http.StatusNotImplemented)
	errorResource := ddl["error"].(map[string]any)
	if !strings.Contains(errorResource["message"].(string), domain.GapQueryDDLCatalogSyncV1) {
		t.Fatalf("DDL error lacks stable capability: %#v", ddl)
	}
	request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/eu_source/tables/events", "", http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.eu_source.events`"+` VALUES (1), (2)","useLegacySql":false
	}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"INSERT INTO `+"`test-project.us_source.events`"+` VALUES (3)","useLegacySql":false
	}`, http.StatusOK)

	jobsBeforeScripts := request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs?maxResults=100", "", http.StatusOK)
	jobCountBeforeScripts := len(jobsBeforeScripts["jobs"].([]any))
	scripts := []string{
		"SELECT 1; DROP TABLE `test-project.eu_source.events`",
		"SELECT 1; ALTER TABLE `test-project.eu_source.events` ADD COLUMN note VARCHAR",
		"INSERT INTO `test-project.eu_source.events` VALUES (99); CREATE TABLE `test-project.eu_source.created_by_script` (id BIGINT)",
	}
	for index, script := range scripts {
		syncFailure := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", fmt.Sprintf(
			`{"query":%q,"useLegacySql":false}`, script), http.StatusNotImplemented)
		assertQueryScriptGap(t, syncFailure)

		asyncFailure := request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", fmt.Sprintf(`{
			"jobReference":{"projectId":"test-project","jobId":"rejected-script-%d"},
			"configuration":{"query":{"query":%q,"useLegacySql":false}}
		}`, index, script), http.StatusNotImplemented)
		assertQueryScriptGap(t, asyncFailure)
	}
	jobsAfterScripts := request(http.MethodGet, "/bigquery/v2/projects/test-project/jobs?maxResults=100", "", http.StatusOK)
	if got := len(jobsAfterScripts["jobs"].([]any)); got != jobCountBeforeScripts {
		t.Fatalf("rejected scripts created jobs: before=%d after=%d", jobCountBeforeScripts, got)
	}
	events := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/eu_source/tables/events", "", http.StatusOK)
	fields := events["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 1 || fields[0].(map[string]any)["name"] != "id" {
		t.Fatalf("rejected ALTER changed canonical schema: %#v", fields)
	}
	data := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/eu_source/tables/events/data?maxResults=10", "", http.StatusOK)
	if data["totalRows"] != "2" {
		t.Fatalf("rejected DROP/INSERT changed physical rows: %#v", data)
	}
	request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/eu_source/tables/created_by_script", "", http.StatusNotFound)

	inserted := request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", `{
		"jobReference":{"projectId":"test-project","jobId":"connector-anonymous"},
		"configuration":{"labels":{},"query":{
			"query":"SELECT id FROM `+"`test-project.eu_source.events`"+` ORDER BY id",
			"useLegacySql":false,"priority":"INTERACTIVE"
		}}
	}`, http.StatusOK)
	insertReference := queryJobReference(inserted)
	if insertReference["location"] != "EU" {
		t.Fatalf("jobs.insert inferred reference = %#v, want EU", insertReference)
	}
	insertQuery := inserted["configuration"].(map[string]any)["query"].(map[string]any)
	insertDestination := insertQuery["destinationTable"].(map[string]any)
	assertAnonymousDestinationReference(t, insertDestination)

	completed := waitForRESTQueryJob(t, ctx, request, "connector-anonymous", "EU")
	completedQuery := completed["configuration"].(map[string]any)["query"].(map[string]any)
	completedDestination := completedQuery["destinationTable"].(map[string]any)
	if fmt.Sprint(completedDestination) != fmt.Sprint(insertDestination) {
		t.Fatalf("generated destination changed while polling: insert=%#v get=%#v", insertDestination, completedDestination)
	}
	if completedQuery["writeDisposition"] != "WRITE_EMPTY" || completedQuery["createDisposition"] != "CREATE_IF_NEEDED" {
		t.Fatalf("anonymous destination dispositions = %#v", completedQuery)
	}

	datasetID := insertDestination["datasetId"].(string)
	tableID := insertDestination["tableId"].(string)
	table := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/"+datasetID+"/tables/"+tableID, "", http.StatusOK)
	wantExpiration := strconv.FormatInt(now.Add(24*time.Hour).UnixMilli(), 10)
	if table["expirationTime"] != wantExpiration || table["location"] != "EU" {
		t.Fatalf("anonymous table lifecycle metadata = %#v, want expiration=%s location=EU", table, wantExpiration)
	}

	visible := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets", "", http.StatusOK)
	if datasets := visible["datasets"].([]any); len(datasets) != 2 {
		t.Fatalf("default datasets.list exposed hidden dataset: %#v", visible)
	}
	all := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets?all=true", "", http.StatusOK)
	if datasets := all["datasets"].([]any); len(datasets) != 3 {
		t.Fatalf("datasets.list?all=true did not expose hidden dataset: %#v", all)
	}
	request(http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/"+datasetID, "", http.StatusConflict)

	defaultDatasetResult := request(http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{
		"query":"SELECT COUNT(*) AS row_count FROM `+"`events`"+`","useLegacySql":false,
		"defaultDataset":{"projectId":"test-project","datasetId":"eu_source"}
	}`, http.StatusOK)
	if reference := queryJobReference(defaultDatasetResult); reference["location"] != "EU" {
		t.Fatalf("defaultDataset query location = %#v, want EU", reference)
	}

	request(http.MethodPost, "/bigquery/v2/projects/test-project/jobs", `{
		"jobReference":{"projectId":"test-project","jobId":"mixed-locations"},
		"configuration":{"query":{"useLegacySql":false,"query":
			"SELECT id FROM `+"`test-project.eu_source.events`"+` UNION ALL SELECT id FROM `+"`test-project.us_source.events`"+`"}}
	}`, http.StatusBadRequest)

	clock.Set(now.Add(24 * time.Hour))
	request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/"+datasetID+"/tables/"+tableID, "", http.StatusNotFound)
	request(http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/"+datasetID, "", http.StatusNoContent)
	request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/"+datasetID, "", http.StatusNotFound)
}

func assertQueryScriptGap(t *testing.T, response map[string]any) {
	t.Helper()
	errorResource := response["error"].(map[string]any)
	if !strings.Contains(errorResource["message"].(string), domain.GapQueryScriptsUnsupportedV1) {
		t.Fatalf("script error lacks stable capability: %#v", response)
	}
}

func assertAnonymousDestinationReference(t *testing.T, destination map[string]any) {
	t.Helper()
	projectID, _ := destination["projectId"].(string)
	datasetID, _ := destination["datasetId"].(string)
	tableID, _ := destination["tableId"].(string)
	if projectID != "test-project" || len(datasetID) <= len("_bqemu_anonymous_") ||
		datasetID[:len("_bqemu_anonymous_")] != "_bqemu_anonymous_" ||
		len(tableID) <= len("_bqemu_query_") || tableID[:len("_bqemu_query_")] != "_bqemu_query_" {
		t.Fatalf("anonymous destination reference = %#v", destination)
	}
}
