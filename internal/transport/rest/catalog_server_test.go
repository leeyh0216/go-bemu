package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/contracttest"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type catalogTestClock struct{ now time.Time }

func (c catalogTestClock) Now() time.Time { return c.now }

type catalogTestWarehouse struct {
	datasets  []string
	tables    []string
	additions []domain.SchemaAddition
}

var _ ports.Warehouse = (*catalogTestWarehouse)(nil)

func (*catalogTestWarehouse) Ping(context.Context) error { return nil }
func (w *catalogTestWarehouse) CreateDataset(_ context.Context, projectID, datasetID string) error {
	w.datasets = append(w.datasets, projectID+"/"+datasetID)
	return nil
}
func (*catalogTestWarehouse) DropDataset(context.Context, string, string) error { return nil }
func (w *catalogTestWarehouse) CreateTable(_ context.Context, table domain.Table) error {
	w.tables = append(w.tables, table.ProjectID+"/"+table.DatasetID+"/"+table.ID)
	return nil
}
func (w *catalogTestWarehouse) ApplySchemaAdditions(_ context.Context, _ domain.Table, additions []domain.SchemaAddition) error {
	w.additions = append(w.additions, additions...)
	return nil
}
func (*catalogTestWarehouse) DropTable(context.Context, string, string, string) error { return nil }
func (*catalogTestWarehouse) Query(context.Context, ports.QueryRequest) (domain.QueryResult, error) {
	return domain.QueryResult{}, nil
}

func TestCatalogRESTMetadataPatchETagAndSchemaEvolution(t *testing.T) {
	contracttest.Operation(t, "bigquery.datasets.update")
	contracttest.Operation(t, "bigquery.tables.update")
	warehouse := &catalogTestWarehouse{}
	clock := catalogTestClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	server := httptest.NewServer(NewCatalogServer(catalog, warehouse, "").Handler())
	t.Cleanup(server.Close)
	request := catalogRequestHelper(t, server.URL)
	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", `{
		"datasetReference":{"datasetId":"analytics"},"location":"EU","description":"before"
	}`, http.StatusOK)
	dataset := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics", "", http.StatusOK)
	datasetETag := dataset["etag"].(string)
	dataset = catalogRequestWithETag(t, server.URL, http.MethodPatch, "/bigquery/v2/projects/test-project/datasets/analytics", `{
		"description":"after","labels":{"tier":"gold"},"defaultTableExpirationMs":"86400000"
	}`, datasetETag, http.StatusOK)
	if dataset["description"] != "after" || dataset["defaultTableExpirationMs"] != "86400000" || dataset["etag"] == datasetETag {
		t.Fatalf("unexpected dataset patch: %#v", dataset)
	}
	dataset = catalogRequestWithETag(t, server.URL, http.MethodPut, "/bigquery/v2/projects/test-project/datasets/analytics", `{
		"datasetReference":{"projectId":"test-project","datasetId":"analytics"},
		"location":"EU","description":"replaced","labels":{"tier":"silver"}
	}`, dataset["etag"].(string), http.StatusOK)
	if dataset["description"] != "replaced" {
		t.Fatalf("unexpected dataset replacement: %#v", dataset)
	}
	catalogRequestWithETag(t, server.URL, http.MethodPatch, "/bigquery/v2/projects/test-project/datasets/analytics", `{"description":"stale"}`, datasetETag, http.StatusPreconditionFailed)

	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", `{
		"tableReference":{"tableId":"events"},"schema":{"fields":[
			{"name":"id","type":"INT64"},
			{"name":"payload","type":"RECORD","fields":[{"name":"name","type":"STRING"}]}
		]}
	}`, http.StatusOK)
	table := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusOK)
	tableETag := table["etag"].(string)
	table = catalogRequestWithETag(t, server.URL, http.MethodPatch, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", `{
		"description":"patched","labels":{"owner":"test"},"expirationTime":"1800000000000",
		"schema":{"fields":[
			{"name":"id","type":"INT64","mode":"NULLABLE"},
			{"name":"payload","type":"RECORD","mode":"NULLABLE","fields":[
				{"name":"name","type":"STRING","mode":"NULLABLE"},
				{"name":"score","type":"FLOAT64","mode":"NULLABLE"}
			]},
			{"name":"tags","type":"STRING","mode":"REPEATED"}
		]}
	}`, tableETag, http.StatusOK)
	if table["description"] != "patched" || table["expirationTime"] != "1800000000000" || len(warehouse.additions) != 2 {
		t.Fatalf("unexpected table patch: table=%#v additions=%#v", table, warehouse.additions)
	}
	table = catalogRequestWithETag(t, server.URL, http.MethodPut, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", `{
		"tableReference":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},
		"type":"TABLE","location":"EU","description":"replaced",
		"schema":{"fields":[
			{"name":"id","type":"INT64","mode":"NULLABLE"},
			{"name":"payload","type":"RECORD","mode":"NULLABLE","fields":[
				{"name":"name","type":"STRING","mode":"NULLABLE"},
				{"name":"score","type":"FLOAT64","mode":"NULLABLE"}
			]},
			{"name":"tags","type":"STRING","mode":"REPEATED"}
		]}
	}`, table["etag"].(string), http.StatusOK)
	if table["description"] != "replaced" {
		t.Fatalf("unexpected table replacement: %#v", table)
	}
	latestETag := table["etag"].(string)
	catalogRequestWithETag(t, server.URL, http.MethodPatch, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", `{
		"schema":{"fields":[{"name":"id","type":"STRING"}]}
	}`, latestETag, http.StatusBadRequest)
}

func TestCatalogRESTPreservesDecimalParametersAndRejectsUnsupportedTypesBeforeStorage(t *testing.T) {
	warehouse := &catalogTestWarehouse{}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, catalogTestClock{now: time.Now()})
	server := httptest.NewServer(NewCatalogServer(catalog, warehouse, "").Handler())
	t.Cleanup(server.Close)
	request := catalogRequestHelper(t, server.URL)

	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project"}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", `{"datasetReference":{"datasetId":"analytics"}}`, http.StatusOK)
	table := request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", `{
		"tableReference":{"tableId":"decimals"},
		"schema":{"fields":[
			{"name":"numeric_default","type":"NUMERIC"},
			{"name":"big_explicit","type":"BIGNUMERIC","precision":"38","scale":"18"},
			{"name":"items","type":"STRUCT","mode":"REPEATED","fields":[
				{"name":"amount","type":"NUMERIC","precision":"20","scale":"2"}
			]}
		]}
	}`, http.StatusOK)

	fields := table["schema"].(map[string]any)["fields"].([]any)
	defaultDecimal := fields[0].(map[string]any)
	if _, present := defaultDecimal["precision"]; present {
		t.Fatalf("omitted precision was synthesized in REST metadata: %#v", defaultDecimal)
	}
	explicitDecimal := fields[1].(map[string]any)
	if explicitDecimal["precision"] != "38" || explicitDecimal["scale"] != "18" {
		t.Fatalf("explicit decimal parameters were not preserved: %#v", explicitDecimal)
	}
	nested := fields[2].(map[string]any)
	if nested["mode"] != "REPEATED" || nested["type"] != "STRUCT" {
		t.Fatalf("nested repeated identity was not preserved: %#v", nested)
	}

	createdBefore := len(warehouse.tables)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", `{
		"tableReference":{"tableId":"too_wide"},
		"schema":{"fields":[{"name":"amount","type":"BIGNUMERIC","precision":"39","scale":"1"}]}
	}`, http.StatusNotImplemented)
	if len(warehouse.tables) != createdBefore {
		t.Fatal("unsupported decimal schema reached the physical warehouse")
	}

	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", `{
		"tableReference":{"tableId":"places"},
		"schema":{"fields":[{"name":"location","type":"GEOGRAPHY"}]}
	}`, http.StatusNotImplemented)
	if len(warehouse.tables) != createdBefore {
		t.Fatal("unsupported GEOGRAPHY schema reached the physical warehouse")
	}
}

func TestCatalogRESTCreateGetListDeleteAndDiscovery(t *testing.T) {
	contracttest.Operation(t, "bqemu.discovery.get")
	contracttest.Operation(t, "bqemu.discovery.googleapis.get")
	contracttest.Operation(t, "bqemu.health.live")
	contracttest.Operation(t, "bqemu.health.ready")
	contracttest.Operation(t, "bqemu.projects.create")
	contracttest.Operation(t, "bqemu.projects.list")
	contracttest.Operation(t, "bqemu.projects.get")
	contracttest.Operation(t, "bqemu.projects.delete")
	contracttest.Operation(t, "bigquery.projects.list")
	warehouse := &catalogTestWarehouse{}
	clock := catalogTestClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	server := httptest.NewServer(NewCatalogServer(catalog, warehouse, "").Handler())
	t.Cleanup(server.Close)
	request := catalogRequestHelper(t, server.URL)

	if liveness := request(http.MethodGet, "/healthz", "", http.StatusOK); liveness["status"] != "ok" {
		t.Fatalf("unexpected liveness: %#v", liveness)
	}
	if readiness := request(http.MethodGet, "/readyz", "", http.StatusOK); readiness["status"] != "ready" {
		t.Fatalf("unexpected readiness: %#v", readiness)
	}
	discovery := request(http.MethodGet, "/$discovery/rest?version=v2", "", http.StatusOK)
	aliasDiscovery := request(http.MethodGet, "/discovery/v1/apis/bigquery/v2/rest", "", http.StatusOK)
	if aliasDiscovery["id"] != discovery["id"] {
		t.Fatalf("discovery alias returned another document: %#v", aliasDiscovery)
	}
	resources := discovery["resources"].(map[string]any)
	if discovery["id"] != "bigquery:v2" || resources["datasets"] == nil || resources["tables"] == nil || resources["jobs"] != nil {
		t.Fatalf("catalog discovery advertised the wrong surface: %#v", discovery)
	}
	datasetMethods := resources["datasets"].(map[string]any)["methods"].(map[string]any)
	for method, parameters := range map[string][]string{
		"insert": {"accessPolicyVersion"},
		"get":    {"accessPolicyVersion", "datasetView"},
		"patch":  {"accessPolicyVersion", "updateMode"},
		"update": {"accessPolicyVersion", "updateMode"},
		"list":   {"filter"},
	} {
		actual := datasetMethods[method].(map[string]any)["parameters"].(map[string]any)
		for _, parameter := range parameters {
			if actual[parameter] == nil {
				t.Fatalf("datasets.%s discovery is missing bq CLI parameter %q", method, parameter)
			}
		}
	}
	projects := resources["projects"].(map[string]any)
	projectMethods := projects["methods"].(map[string]any)
	projectListMethod := projectMethods["list"].(map[string]any)
	if order, ok := projectListMethod["parameterOrder"].([]any); !ok || order == nil || len(order) != 0 {
		t.Fatalf("projects.list parameterOrder must be an empty array, got %#v", projectListMethod["parameterOrder"])
	}
	tableMethods := resources["tables"].(map[string]any)["methods"].(map[string]any)
	tablePatchParameters := tableMethods["patch"].(map[string]any)["parameters"].(map[string]any)
	if tablePatchParameters["autodetect_schema"] == nil {
		t.Fatal("tables.patch discovery is missing the bq CLI autodetect_schema parameter")
	}
	if tableMethods["get"].(map[string]any)["parameters"].(map[string]any)["autodetect_schema"] != nil {
		t.Fatal("tables.get must not advertise the mutation-only autodetect_schema parameter")
	}

	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project","futureProjectField":"ignored"}`, http.StatusOK)
	project := request(http.MethodGet, "/bqemu/v1/projects/test-project", "", http.StatusOK)
	if project["id"] != "test-project" {
		t.Fatalf("unexpected emulator project: %#v", project)
	}
	emulatorProjects := request(http.MethodGet, "/bqemu/v1/projects", "", http.StatusOK)
	if emulatorProjects["totalItems"] != float64(1) {
		t.Fatalf("unexpected emulator project list: %#v", emulatorProjects)
	}
	projectList := request(http.MethodGet, "/bigquery/v2/projects?maxResults=1", "", http.StatusOK)
	if projectList["totalItems"] != float64(1) {
		t.Fatalf("unexpected project list: %#v", projectList)
	}
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", `{
		"datasetReference":{"datasetId":"analytics"},"location":"EU","labels":{"tier":"test"},
		"futureDatasetField":{"accepted":true}
	}`, http.StatusOK)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", `{
		"datasetReference":{"datasetId":"archive"},"location":"EU"
	}`, http.StatusOK)
	dataset := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics", "", http.StatusOK)
	if dataset["location"] != "EU" || dataset["id"] != "test-project:analytics" {
		t.Fatalf("dataset metadata was not preserved: %#v", dataset)
	}
	firstPage := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets?maxResults=1", "", http.StatusOK)
	token, ok := firstPage["nextPageToken"].(string)
	if !ok || token == "" || len(firstPage["datasets"].([]any)) != 1 {
		t.Fatalf("unexpected first page: %#v", firstPage)
	}
	secondPage := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets?pageToken="+url.QueryEscape(token)+"&maxResults=1", "", http.StatusOK)
	if len(secondPage["datasets"].([]any)) != 1 || secondPage["nextPageToken"] != nil {
		t.Fatalf("unexpected second page: %#v", secondPage)
	}

	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", `{
		"tableReference":{"tableId":"events"},
		"schema":{"fields":[{"name":"event_id","type":"INT64","mode":"REQUIRED"}]},
		"timePartitioning":{"type":"DAY","expirationMs":"86400000"},
		"futureTableField":"ignored"
	}`, http.StatusOK)
	table := request(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events", "", http.StatusOK)
	if table["id"] != "test-project:analytics.events" {
		t.Fatalf("unexpected table: %#v", table)
	}
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", `{
		"tableReference":{"projectId":"other-project","tableId":"bad"},
		"schema":{"fields":[{"name":"id","type":"INT64"}]}
	}`, http.StatusBadRequest)
	request(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables", `{
		"tableReference":{"tableId":"bad_partition"},
		"schema":{"fields":[{"name":"id","type":"INT64"}]},
		"timePartitioning":{"type":"DAY","expirationMs":"not-an-integer"}
	}`, http.StatusBadRequest)

	request(http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/analytics", "", http.StatusConflict)
	request(http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/analytics?deleteContents=true", "", http.StatusNoContent)
	request(http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/archive", "", http.StatusNoContent)
	request(http.MethodDelete, "/bqemu/v1/projects/test-project", "", http.StatusNoContent)
	if len(warehouse.datasets) != 2 || len(warehouse.tables) != 1 {
		t.Fatalf("unexpected outbound port calls: datasets=%v tables=%v", warehouse.datasets, warehouse.tables)
	}
}

func TestCatalogRESTRejectsMalformedPaginationAndJSON(t *testing.T) {
	warehouse := &catalogTestWarehouse{}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, catalogTestClock{now: time.Now()})
	server := httptest.NewServer(NewCatalogServer(catalog, warehouse, "").Handler())
	t.Cleanup(server.Close)
	request := catalogRequestHelper(t, server.URL)
	request(http.MethodGet, "/bigquery/v2/projects?pageToken=not-base64!", "", http.StatusBadRequest)
	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project"} trailing`, http.StatusBadRequest)
}

func catalogRequestHelper(t *testing.T, baseURL string) func(string, string, string, int) map[string]any {
	t.Helper()
	return func(method, path, body string, expectedStatus int) map[string]any {
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
			t.Fatalf("%s %s: got %d, want %d; body=%s", method, path, response.StatusCode, expectedStatus, payload)
		}
		if len(payload) == 0 {
			return nil
		}
		var decoded map[string]any
		if err := json.Unmarshal(payload, &decoded); err != nil {
			t.Fatal(fmt.Errorf("decode response %s: %w", payload, err))
		}
		return decoded
	}
}

func catalogRequestWithETag(t *testing.T, baseURL, method, path, body, etag string, expectedStatus int) map[string]any {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, baseURL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("If-Match", etag)
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
		t.Fatalf("%s %s: got %d, want %d; body=%s", method, path, response.StatusCode, expectedStatus, payload)
	}
	if len(payload) == 0 {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}
