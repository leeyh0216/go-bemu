package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	stateadapter "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestTableDataInsertAllStoresTypedRowsAndReturnsRowErrors(t *testing.T) {
	ctx := context.Background()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	statePath := filepath.Join(t.TempDir(), "state.sqlite")
	store, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(statePath))
	if err != nil {
		t.Fatal(err)
	}
	service := application.NewCatalogService(store, warehouse, restFixedClock{now: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)}, application.WithTableDataReader(warehouse), application.WithTableDataWriter(warehouse), application.WithTableDataInsertIDLedger(store))
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "events", Schema: []domain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "note", Type: "STRING"}, {Name: "occurred", Type: "TIMESTAMP"},
	}}); err != nil {
		t.Fatal(err)
	}
	server := NewServer(service, nil, warehouse, "", WithTableDataAPI(service))
	body := []byte(`{"rows":[{"insertId":"one","json":{"id":"7","note":"saved","occurred":"2026-08-09T01:02:03Z"}}]}`)
	request := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/insertAll", bytes.NewReader(body))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("insert status=%d body=%s", response.Code, response.Body.String())
	}
	list := httptest.NewRecorder()
	server.Handler().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?formatOptions.useInt64Timestamp=true", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	var page struct {
		TotalRows string `json:"totalRows"`
		Rows      []struct {
			Fields []struct {
				Value any `json:"v"`
			} `json:"f"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.TotalRows != "1" || len(page.Rows) != 1 || page.Rows[0].Fields[0].Value != "7" || page.Rows[0].Fields[1].Value != "saved" {
		t.Fatalf("page=%s", list.Body.String())
	}
	bad := httptest.NewRecorder()
	server.Handler().ServeHTTP(bad, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/insertAll", bytes.NewBufferString(`{"rows":[{"json":{"id":"not-an-int"}}]}`)))
	if bad.Code != http.StatusOK || !bytes.Contains(bad.Body.Bytes(), []byte(`"index":0`)) || !bytes.Contains(bad.Body.Bytes(), []byte(`"location":"id"`)) {
		t.Fatalf("bad response status=%d body=%s", bad.Code, bad.Body.String())
	}
	again := httptest.NewRecorder()
	server.Handler().ServeHTTP(again, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data", nil))
	if !bytes.Contains(again.Body.Bytes(), []byte(`"totalRows":"1"`)) {
		t.Fatalf("invalid row mutated table: %s", again.Body.String())
	}
	overlarge := httptest.NewRecorder()
	overlargeBody := `{"rows":[{"json":{"id":9,"note":"` + strings.Repeat("x", maximumJSONBodyBytes) + `"}}]}`
	server.Handler().ServeHTTP(overlarge, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/insertAll", bytes.NewBufferString(overlargeBody)))
	if overlarge.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized insert status=%d body=%s", overlarge.Code, overlarge.Body.String())
	}
	afterOversized := httptest.NewRecorder()
	server.Handler().ServeHTTP(afterOversized, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data", nil))
	if !bytes.Contains(afterOversized.Body.Bytes(), []byte(`"totalRows":"1"`)) {
		t.Fatalf("oversized request mutated table: %s", afterOversized.Body.String())
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(statePath))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	restarted := application.NewCatalogService(reopened, warehouse, restFixedClock{now: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)}, application.WithTableDataReader(warehouse), application.WithTableDataWriter(warehouse), application.WithTableDataInsertIDLedger(reopened))
	retryServer := NewServer(restarted, nil, warehouse, "", WithTableDataAPI(restarted))
	retry := httptest.NewRecorder()
	retryServer.Handler().ServeHTTP(retry, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/insertAll", bytes.NewReader(body)))
	if retry.Code != http.StatusOK {
		t.Fatalf("retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	afterRestart := httptest.NewRecorder()
	retryServer.Handler().ServeHTTP(afterRestart, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data", nil))
	if !bytes.Contains(afterRestart.Body.Bytes(), []byte(`"totalRows":"1"`)) {
		t.Fatalf("duplicate insertId after state restart: %s", afterRestart.Body.String())
	}
}

func TestTableDataInsertAllSupportsTemporalDecimalBytesAndNestedRows(t *testing.T) {
	ctx := context.Background()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	service := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, restFixedClock{now: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)}, application.WithTableDataReader(warehouse), application.WithTableDataWriter(warehouse))
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "typed", Schema: []domain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "nullable", Type: "STRING"}, {Name: "amount", Type: "NUMERIC"}, {Name: "payload", Type: "BYTES"},
		{Name: "day", Type: "DATE"}, {Name: "at", Type: "DATETIME"}, {Name: "clock", Type: "TIME"}, {Name: "instant", Type: "TIMESTAMP"},
		{Name: "profile", Type: "RECORD", Fields: []domain.Field{{Name: "name", Type: "STRING", Mode: "REQUIRED"}, {Name: "rank", Type: "INT64"}}},
		{Name: "tags", Type: "STRING", Mode: "REPEATED"},
	}}); err != nil {
		t.Fatal(err)
	}
	server := NewServer(service, nil, warehouse, "", WithTableDataAPI(service))
	body := `{"rows":[{"json":{"id":8,"nullable":null,"amount":"12.340","payload":"AQI=","day":"2026-08-09","at":"2026-08-09 01:02:03.123456","clock":"01:02:03.123456","instant":"2026-08-09T01:02:03.123456Z","profile":{"name":"nested","rank":3},"tags":["one","two"]}}]}`
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/analytics/tables/typed/insertAll", bytes.NewBufferString(body)))
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte("insertErrors")) {
		t.Fatalf("insert status=%d body=%s", response.Code, response.Body.String())
	}
	list := httptest.NewRecorder()
	server.Handler().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/analytics/tables/typed/data?formatOptions.useInt64Timestamp=true", nil))
	if list.Code != http.StatusOK || !bytes.Contains(list.Body.Bytes(), []byte(`"totalRows":"1"`)) || !bytes.Contains(list.Body.Bytes(), []byte("nested")) || !bytes.Contains(list.Body.Bytes(), []byte("one")) {
		t.Fatalf("typed list status=%d body=%s", list.Code, list.Body.String())
	}
}

type restFixedClock struct{ now time.Time }

func (c restFixedClock) Now() time.Time { return c.now }
