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
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type catalogTestClock struct{ now time.Time }

func (c catalogTestClock) Now() time.Time { return c.now }

type catalogTestWarehouse struct {
	datasets []string
	tables   []string
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
func (*catalogTestWarehouse) DropTable(context.Context, string, string, string) error { return nil }
func (*catalogTestWarehouse) Query(context.Context, ports.QueryRequest) (domain.QueryResult, error) {
	return domain.QueryResult{}, nil
}

func TestCatalogRESTCreateGetListDeleteAndDiscovery(t *testing.T) {
	warehouse := &catalogTestWarehouse{}
	clock := catalogTestClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	server := httptest.NewServer(NewCatalogServer(catalog, warehouse, "").Handler())
	t.Cleanup(server.Close)
	request := catalogRequestHelper(t, server.URL)

	if readiness := request(http.MethodGet, "/readyz", "", http.StatusOK); readiness["status"] != "ready" {
		t.Fatalf("unexpected readiness: %#v", readiness)
	}
	discovery := request(http.MethodGet, "/$discovery/rest?version=v2", "", http.StatusOK)
	resources := discovery["resources"].(map[string]any)
	if discovery["id"] != "bigquery:v2" || resources["datasets"] == nil || resources["tables"] == nil || resources["jobs"] != nil {
		t.Fatalf("catalog discovery advertised the wrong surface: %#v", discovery)
	}

	request(http.MethodPost, "/bqemu/v1/projects", `{"projectId":"test-project","futureProjectField":"ignored"}`, http.StatusOK)
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
