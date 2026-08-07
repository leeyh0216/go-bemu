package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestTableDataListPagesNestedRowsAcrossPublicRESTEdge(t *testing.T) {
	ctx, cancel := requestBodyTestContext(t)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := catalogTestClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(
		memory.NewCatalogRepository(), warehouse, clock,
		application.WithTableDataReader(warehouse),
		application.WithTableDataOperationTimeout(5*time.Second),
		application.WithMaxTableDataPageRows(1),
	)
	createTableDataRESTFixture(t, ctx, catalog, warehouse)
	server := httptest.NewServer(NewCatalogServer(catalog, warehouse, "", WithTableDataAPI(catalog)).Handler())
	t.Cleanup(server.Close)
	request := func(path string, wantStatus int) map[string]any {
		t.Helper()
		return staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet, path, "", wantStatus)
	}

	discovery := request("/$discovery/rest?version=v2", http.StatusOK)
	resources := discovery["resources"].(map[string]any)
	if resources["tabledata"] == nil {
		t.Fatalf("tabledata discovery resource is missing: %#v", resources)
	}

	path := "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?maxResults=100"
	first := request(path, http.StatusOK)
	if first["kind"] != "bigquery#tableDataList" || first["totalRows"] != "2" || first["etag"] == "" {
		t.Fatalf("first table data page metadata = %#v", first)
	}
	rows := first["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("first table data rows = %#v", rows)
	}
	cells := rows[0].(map[string]any)["f"].([]any)
	if cells[0].(map[string]any)["v"] != "1" {
		t.Fatalf("INT64 cell = %#v", cells[0])
	}
	record := cells[2].(map[string]any)["v"].(map[string]any)["f"].([]any)
	if record[0].(map[string]any)["v"] != "3" || record[1].(map[string]any)["v"] != "nested-one" {
		t.Fatalf("STRUCT cell = %#v", record)
	}
	repeated := cells[3].(map[string]any)["v"].([]any)
	if len(repeated) != 2 || repeated[0].(map[string]any)["v"] != "alpha" || repeated[1].(map[string]any)["v"] != "beta" {
		t.Fatalf("REPEATED cell = %#v", repeated)
	}
	wantTimestamp := time.Date(2026, 8, 8, 1, 2, 3, 123456000, time.UTC)
	if cells[4].(map[string]any)["v"] != "1786150923.123456" {
		t.Fatalf("default TIMESTAMP cell = %#v", cells[4])
	}
	int64Timestamp := request(path+"&formatOptions.useInt64Timestamp=true", http.StatusOK)
	int64Cells := int64Timestamp["rows"].([]any)[0].(map[string]any)["f"].([]any)
	if int64Cells[4].(map[string]any)["v"] != strconv.FormatInt(wantTimestamp.UnixMicro(), 10) {
		t.Fatalf("int64 TIMESTAMP cell = %#v", int64Cells[4])
	}
	token := first["pageToken"].(string)
	second := request("/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?pageToken="+url.QueryEscape(token), http.StatusOK)
	if rows := second["rows"].([]any); len(rows) != 1 || second["pageToken"] != nil {
		t.Fatalf("second table data page = %#v", second)
	}
	request("/bigquery/v2/projects/test-project/datasets/analytics/tables/other/data?pageToken="+url.QueryEscape(token), http.StatusBadRequest)
	beyond := request("/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?startIndex=99", http.StatusOK)
	if beyond["totalRows"] != "2" || beyond["rows"] != nil || beyond["pageToken"] != nil {
		t.Fatalf("beyond-end table data page = %#v", beyond)
	}
	request("/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?selectedFields=id", http.StatusBadRequest)
}

func createTableDataRESTFixture(t *testing.T, ctx context.Context, catalog *application.CatalogService, warehouse *duckdb.Warehouse) {
	t.Helper()
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "label", Type: "STRING"},
			{Name: "payload", Type: "RECORD", Fields: []domain.Field{
				{Name: "score", Type: "INT64"}, {Name: "name", Type: "STRING"},
			}},
			{Name: "tags", Type: "STRING", Mode: "REPEATED"},
			{Name: "event_time", Type: "TIMESTAMP"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := warehouse.Query(ctx, ports.QueryRequest{SQL: `
		INSERT INTO ` + "`test-project.analytics.events`" + ` VALUES
		(1, 'first', {'score': 3, 'name': 'nested-one'}, ['alpha', 'beta'], TIMESTAMPTZ '2026-08-08 01:02:03.123456+00'),
		(2, NULL, {'score': NULL, 'name': 'nested-two'}, [], TIMESTAMPTZ '1969-12-31 23:59:59.000001+00')
	`})
	if err != nil {
		t.Fatal(err)
	}
}
