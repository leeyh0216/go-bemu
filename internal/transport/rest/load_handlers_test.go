package rest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	"github.com/leeyh0216/go-bemu/internal/contracttest"
	"github.com/leeyh0216/go-bemu/internal/domain"
	loadApplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

func TestCombinedJobsAPIExecutesParquetLoadAndPreservesQueryJobs(t *testing.T) {
	contracttest.Operation(t, "bigquery.jobs.insert")
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
	evolvedParquet := createRESTParquet(t, "SELECT NULL::BIGINT AS id, 'evolved'::VARCHAR AS name, 9.5::DOUBLE AS score")
	evolvedPayload, err := os.ReadFile(evolvedParquet)
	if err != nil {
		t.Fatal(err)
	}
	fakeGCS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/storage/v1/b/load-bucket/o":
			if strings.HasPrefix(r.URL.Query().Get("prefix"), "evolved/") {
				_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{
					"name": "evolved/part-00000.parquet", "size": fmt.Sprint(len(evolvedPayload)), "generation": "2",
				}}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{
				"name": "input/part-00000.parquet", "size": fmt.Sprint(len(parquetPayload)), "generation": "1",
			}}})
		case strings.HasSuffix(r.URL.Path, "/o/evolved/part-00000.parquet") && r.URL.Query().Get("alt") == "media":
			_, _ = w.Write(evolvedPayload)
		case strings.HasSuffix(r.URL.Path, "/o/input/part-00000.parquet") && r.URL.Query().Get("alt") == "media":
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
	loadConfig := loadApplication.DefaultConfig()
	loadConfig.TempDirectory = t.TempDir()
	loadConfig.OperationTimeout = 5 * time.Second
	loads, err := loadApplication.NewService(
		loadApplication.NewMemoryJobRepository(), gcs, NewLoadTableCatalog(catalog), warehouse,
		clock, ids, loadConfig,
	)
	if err != nil {
		t.Fatal(err)
	}
	queries := newRESTTestQueryService(
		memory.NewJobRepository(), warehouse, clock, ids,
		application.WithGoogleSQLGateway(newRESTGoogleSQLGateway(catalog)),
		application.WithQueryDestinationCatalog(catalog),
		application.WithStatementMaterializer(warehouse),
	)
	server := httptest.NewServer(NewServerWithLoadJobs(catalog, queries, loads, warehouse, "").Handler())
	t.Cleanup(server.Close)

	body := fmt.Sprintf(`{
		"jobReference":{"projectId":"test-project","jobId":"load-one","location":"US"},
		"configuration":{"load":{
			"sourceUris":[%q],
			"destinationTable":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},
			"sourceFormat":"PARQUET","writeDisposition":"WRITE_APPEND",
			"parquetOptions":{},"decimalTargetTypes":null,"nullMarkers":[],
			"projectionFields":[],"timestampTargetPrecision":[]
		}}
	}`, "gs://load-bucket/input/*.parquet")
	job := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", body, http.StatusOK)
	job = waitForRESTLoad(t, server.URL, "load-one")
	status := job["status"].(map[string]any)
	if status["errorResult"] != nil {
		t.Fatalf("load job failed: %#v", job)
	}
	statistics := job["statistics"].(map[string]any)["load"].(map[string]any)
	if statistics["inputFiles"] != "1" || statistics["inputFileBytes"] != fmt.Sprint(len(parquetPayload)) ||
		statistics["outputBytes"] != fmt.Sprint(len(parquetPayload)) || statistics["outputRows"] != "2" {
		t.Fatalf("unexpected load statistics: %#v", statistics)
	}

	// Same tuple and configuration is a read-only retry, not a second append.
	retry := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", body, http.StatusOK)
	if retry["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("idempotent retry did not return existing job: %#v", retry)
	}
	query := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/queries",
		`{"query":"SELECT count(*) AS row_count FROM `+"`test-project.analytics.events`"+`","useLegacySql":false}`, http.StatusOK)
	if query["totalRows"] != "1" || query["rows"].([]any)[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"] != "2" {
		t.Fatalf("unexpected query result after load: %#v", query)
	}

	updateBody := fmt.Sprintf(`{
		"jobReference":{"projectId":"test-project","jobId":"load-schema-update","location":"US"},
		"configuration":{"load":{
			"sourceUris":[%q],
			"destinationTable":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},
			"sourceFormat":"PARQUET","writeDisposition":"WRITE_APPEND",
			"schemaUpdateOptions":["ALLOW_FIELD_ADDITION","ALLOW_FIELD_RELAXATION"],
			"schema":{"fields":[
				{"name":"id","type":"INT64","mode":"NULLABLE"},
				{"name":"name","type":"STRING","mode":"NULLABLE"},
				{"name":"score","type":"FLOAT64","mode":"NULLABLE"}
			]}
		}}
	}`, "gs://load-bucket/evolved/*.parquet")
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", updateBody, http.StatusOK)
	updatedJob := waitForRESTLoad(t, server.URL, "load-schema-update")
	if updatedJob["status"].(map[string]any)["errorResult"] != nil {
		t.Fatalf("schema-updating load failed: %#v", updatedJob)
	}
	updateOptions := updatedJob["configuration"].(map[string]any)["load"].(map[string]any)["schemaUpdateOptions"].([]any)
	if len(updateOptions) != 2 {
		t.Fatalf("schema update options were not preserved: %#v", updatedJob)
	}
	updatedTable := restLoadRequest(t, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusOK)
	fields := updatedTable["schema"].(map[string]any)["fields"].([]any)
	if len(fields) != 3 || fields[0].(map[string]any)["mode"] != "NULLABLE" || fields[2].(map[string]any)["name"] != "score" {
		t.Fatalf("updated table schema = %#v", updatedTable)
	}
	query = restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/queries",
		`{"query":"SELECT count(*) AS row_count FROM `+"`test-project.analytics.events`"+` WHERE id IS NULL AND score = 9.5","useLegacySql":false}`, http.StatusOK)
	if query["rows"].([]any)[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"] != "1" {
		t.Fatalf("unexpected query result after schema update: %#v", query)
	}

	createBody := fmt.Sprintf(`{
		"jobReference":{"projectId":"test-project","jobId":"load-create","location":"US"},
		"configuration":{"load":{
			"sourceUris":[%q],
			"destinationTable":{"projectId":"test-project","datasetId":"analytics","tableId":"created_events"},
			"sourceFormat":"PARQUET","writeDisposition":"WRITE_EMPTY","createDisposition":"CREATE_IF_NEEDED",
			"parquetOptions":{"enableListInference":true},
			"timePartitioning":{"type":"DAY","expirationMs":"86400000"},
			"clustering":{"fields":["id"]}
		}}
	}`, "gs://load-bucket/input/*.parquet")
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", createBody, http.StatusOK)
	createdJob := waitForRESTLoad(t, server.URL, "load-create")
	if createdJob["status"].(map[string]any)["errorResult"] != nil {
		t.Fatalf("destination-creating load failed: %#v", createdJob)
	}
	createdOptions := createdJob["configuration"].(map[string]any)["load"].(map[string]any)["parquetOptions"].(map[string]any)
	if createdOptions["enableListInference"] != true {
		t.Fatalf("load job lost Parquet options: %#v", createdJob)
	}
	createdLoad := createdJob["configuration"].(map[string]any)["load"].(map[string]any)
	if createdLoad["timePartitioning"].(map[string]any)["expirationMs"] != "86400000" ||
		createdLoad["clustering"].(map[string]any)["fields"].([]any)[0] != "id" {
		t.Fatalf("load job lost destination layout: %#v", createdJob)
	}
	createdTable := restLoadRequest(t, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/created_events", "", http.StatusOK)
	if createdTable["location"] != "US" ||
		createdTable["timePartitioning"].(map[string]any)["expirationMs"] != "86400000" ||
		createdTable["clustering"].(map[string]any)["fields"].([]any)[0] != "id" {
		t.Fatalf("created table metadata = %#v", createdTable)
	}
	query = restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/queries",
		`{"query":"SELECT count(*) AS row_count FROM `+"`test-project.analytics.created_events`"+` WHERE _PARTITIONDATE IS NOT NULL","useLegacySql":false}`, http.StatusOK)
	if query["rows"].([]any)[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"] != "2" {
		t.Fatalf("unexpected query result after destination creation: %#v", query)
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
	gcs, err := objectstore.NewGCSJSON(objectstore.GCSJSONConfig{Endpoint: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	loadJobs := loadApplication.NewMemoryJobRepository()
	loads, err := loadApplication.NewService(loadJobs, gcs, NewLoadTableCatalog(catalog), warehouse, clock, ids, config)
	if err != nil {
		t.Fatal(err)
	}
	queries := newRESTTestQueryService(
		memory.NewJobRepository(), warehouse, clock, ids,
		application.WithGoogleSQLGateway(newRESTGoogleSQLGateway(catalog)),
		application.WithQueryDestinationCatalog(catalog),
		application.WithStatementMaterializer(warehouse),
	)
	server := httptest.NewServer(NewServerWithLoadJobs(catalog, queries, loads, warehouse, "").Handler())
	t.Cleanup(server.Close)

	invalidSource := `{"jobReference":{"jobId":"invalid-source","location":"US"},"configuration":{"load":{"sourceUris":["file:///must-not-be-read.parquet"],"destinationTable":{"datasetId":"analytics","tableId":"events"},"sourceFormat":"PARQUET"}}}`
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", invalidSource, http.StatusBadRequest)
	if _, err := loadJobs.Get(ctx, loadDomain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: "invalid-source",
	}); !errors.Is(err, loadDomain.ErrNotFound) {
		t.Fatalf("invalid source persisted a load job: %v", err)
	}

	body := `{"jobReference":{"jobId":"avro-gap","location":"US"},"configuration":{"load":{"sourceUris":["gs://test-bucket/does-not-exist.avro"],"destinationTable":{"datasetId":"analytics","tableId":"events"},"sourceFormat":"AVRO"}}}`
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", body, http.StatusOK)
	job := waitForRESTLoad(t, server.URL, "avro-gap")
	errorResult := job["status"].(map[string]any)["errorResult"].(map[string]any)
	if errorResult["reason"] != "notImplemented" {
		t.Fatalf("unexpected gap job: %#v", job)
	}
	if _, present := job["statistics"].(map[string]any)["load"].(map[string]any)["outputBytes"]; present {
		t.Fatalf("failed load exposed successful outputBytes: %#v", job)
	}

	activeOption := `{"jobReference":{"jobId":"parquet-option-gap","location":"US"},"configuration":{"load":{"sourceUris":["gs://test-bucket/does-not-exist.parquet"],"destinationTable":{"datasetId":"analytics","tableId":"events"},"sourceFormat":"PARQUET","parquetOptions":{"enumAsString":true}}}}`
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", activeOption, http.StatusOK)
	job = waitForRESTLoad(t, server.URL, "parquet-option-gap")
	errorResult = job["status"].(map[string]any)["errorResult"].(map[string]any)
	if errorResult["reason"] != "notImplemented" {
		t.Fatalf("active Parquet option did not remain an explicit gap: %#v", job)
	}

	malformedOption := `{"configuration":{"load":{"sourceUris":["gs://test-bucket/x"],"destinationTable":{"datasetId":"analytics","tableId":"events"},"sourceFormat":"PARQUET","parquetOptions":[]}}}`
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", malformedOption, http.StatusBadRequest)

	both := `{"configuration":{"query":{"query":"SELECT 1"},"load":{"sourceUris":["gs://test-bucket/x"],"destinationTable":{"datasetId":"analytics","tableId":"events"},"sourceFormat":"PARQUET"}}}`
	restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", both, http.StatusBadRequest)

	for _, schema := range []string{
		`{"fields":[{"name":"location","type":"GEOGRAPHY"}]}`,
		`{"fields":[{"name":"amount","type":"BIGNUMERIC","precision":"39","scale":"1"}]}`,
	} {
		unsupportedSchema := `{"jobReference":{"jobId":"unsupported-schema","location":"US"},"configuration":{"load":{"sourceUris":["gs://test-bucket/must-not-be-read.parquet"],"destinationTable":{"datasetId":"analytics","tableId":"events"},"sourceFormat":"PARQUET","schema":` + schema + `}}}`
		restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/jobs", unsupportedSchema, http.StatusNotImplemented)
	}
}

func TestLoadCompatibilityOptionsAcceptOnlyPinnedNeutralShapes(t *testing.T) {
	accepted := map[string]string{
		"absent":                 `{}`,
		"empty options":          `{"parquetOptions":{}}`,
		"explicit false":         `{"parquetOptions":{"enableListInference":false,"enumAsString":false}}`,
		"list inference":         `{"parquetOptions":{"enableListInference":true}}`,
		"explicit empty options": `{"decimalTargetTypes":null,"nullMarkers":[],"projectionFields":[],"timestampTargetPrecision":[]}`,
	}
	for name, payload := range accepted {
		t.Run("accept "+name, func(t *testing.T) {
			var wire loadConfigurationResource
			if err := json.Unmarshal([]byte(payload), &wire); err != nil {
				t.Fatal(err)
			}
			unsupported, err := unsupportedLoadOptions([]byte(payload), wire)
			if err != nil {
				t.Fatal(err)
			}
			if len(unsupported) != 0 {
				t.Fatalf("unsupported options = %v", unsupported)
			}
		})
	}

	rejected := map[string]struct {
		payload string
		field   string
	}{
		"null parquet object":  {payload: `{"parquetOptions":null}`, field: "parquetOptions:"},
		"enum as string":       {payload: `{"parquetOptions":{"enumAsString":true}}`, field: "parquetOptions.enumAsString:"},
		"null parquet flag":    {payload: `{"parquetOptions":{"enumAsString":null}}`, field: "parquetOptions.enumAsString:"},
		"map target":           {payload: `{"parquetOptions":{"mapTargetType":"ARRAY_OF_STRUCT"}}`, field: "parquetOptions.mapTargetType:"},
		"future parquet field": {payload: `{"parquetOptions":{"futureOption":false}}`, field: "parquetOptions.futureOption:"},
		"empty decimal list":   {payload: `{"decimalTargetTypes":[]}`, field: "decimalTargetTypes:"},
		"decimal target":       {payload: `{"decimalTargetTypes":["NUMERIC"]}`, field: "decimalTargetTypes:"},
		"null null markers":    {payload: `{"nullMarkers":null}`, field: "nullMarkers:"},
		"null marker":          {payload: `{"nullMarkers":["NULL"]}`, field: "nullMarkers:"},
		"projection":           {payload: `{"projectionFields":["value"]}`, field: "projectionFields:"},
		"timestamp precision":  {payload: `{"timestampTargetPrecision":[6]}`, field: "timestampTargetPrecision:"},
		"unknown load option":  {payload: `{"futureOption":false}`, field: "futureOption:"},
	}
	for name, test := range rejected {
		t.Run("reject "+name, func(t *testing.T) {
			var wire loadConfigurationResource
			if err := json.Unmarshal([]byte(test.payload), &wire); err != nil {
				t.Fatal(err)
			}
			unsupported, err := unsupportedLoadOptions([]byte(test.payload), wire)
			if err != nil {
				t.Fatal(err)
			}
			if len(unsupported) != 1 || !strings.HasPrefix(unsupported[0], test.field) {
				t.Fatalf("unsupported options = %v, want one %q fingerprint", unsupported, test.field)
			}
		})
	}
}

func TestLoadCompatibilityOptionsRejectMalformedWireTypes(t *testing.T) {
	for name, payload := range map[string]string{
		"parquet array":      `{"parquetOptions":[]}`,
		"decimal scalar":     `{"decimalTargetTypes":"NUMERIC"}`,
		"null marker scalar": `{"nullMarkers":"NULL"}`,
		"projection scalar":  `{"projectionFields":"value"}`,
		"timestamp strings":  `{"timestampTargetPrecision":["6"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			var wire loadConfigurationResource
			if err := json.Unmarshal([]byte(payload), &wire); err == nil {
				t.Fatal("malformed compatibility option was accepted")
			}
		})
	}
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
