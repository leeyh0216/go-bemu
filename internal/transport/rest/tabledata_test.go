package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	tabledatabudget "github.com/leeyh0216/go-bemu/internal/tabledata"
)

type fixedTableDataPage struct {
	page        ports.TableDataPage
	err         error
	honorOffset bool
}

func (source fixedTableDataPage) ListTableData(_ context.Context, _, _, _ string, offset int64, maximum ports.TableDataMaxResults) (ports.TableDataPage, error) {
	page := source.page
	if source.honorOffset {
		if offset >= int64(len(page.Rows)) {
			page.Rows = nil
		} else {
			page.Rows = page.Rows[offset:]
		}
	}
	if maximum.Present && len(page.Rows) > maximum.Value {
		page.Rows = page.Rows[:maximum.Value]
	}
	return page, source.err
}

func TestEncodeTableDataListResponseTrimsByExactWireBytesAndBoundsSingleRow(t *testing.T) {
	page := ports.TableDataPage{
		Schema: []domain.Field{{Name: "payload", Type: "STRING"}},
		Rows:   [][]any{{strings.Repeat("a", 2_000)}, {strings.Repeat("b", 2_000)}}, TotalRows: 2,
		MaxResponseBytes: 10_000, MaxRowBytes: 10_000,
	}
	oneRowPage := page
	oneRowPage.Rows = page.Rows[:1]
	oneRow, err := encodeTableDataListResponse(oneRowPage, tableDataFormatOptions{}, "sha256:test-scope", 0)
	if err != nil {
		t.Fatal(err)
	}

	oneRowBody := tableDataEncodedBody(t, oneRow)
	page.MaxResponseBytes = int64(len(oneRowBody))
	trimmed, err := encodeTableDataListResponse(page, tableDataFormatOptions{}, "sha256:test-scope", 0)
	if err != nil {
		t.Fatal(err)
	}
	trimmedBody := tableDataEncodedBody(t, trimmed)
	if trimmed.rowCount != 1 || int64(len(trimmedBody)) > page.MaxResponseBytes {
		t.Fatalf("trimmed response rows=%d bytes=%d limit=%d", trimmed.rowCount, len(trimmedBody), page.MaxResponseBytes)
	}
	var decoded map[string]any
	if err := json.Unmarshal(trimmedBody, &decoded); err != nil {
		t.Fatalf("decode bounded payload: %v", err)
	}
	if decoded["pageToken"] == nil || len(decoded["rows"].([]any)) != 1 {
		t.Fatalf("bounded payload shape = %#v", decoded)
	}

	oneRowPage.MaxResponseBytes = int64(len(oneRowBody) - 1)
	oneRowPage.MaxRowBytes = int64(len(oneRowBody))
	exception, err := encodeTableDataListResponse(oneRowPage, tableDataFormatOptions{}, "sha256:test-scope", 0)
	if err != nil {
		t.Fatalf("single-row exception: %v", err)
	}
	exceptionBody := tableDataEncodedBody(t, exception)
	if exception.rowCount != 1 || int64(len(exceptionBody)) <= oneRowPage.MaxResponseBytes || int64(len(exceptionBody)) > oneRowPage.MaxRowBytes {
		t.Fatalf("single-row exception rows=%d bytes=%d normal=%d hard=%d", exception.rowCount, len(exceptionBody), oneRowPage.MaxResponseBytes, oneRowPage.MaxRowBytes)
	}

	oneRowPage.MaxResponseBytes = 10_000
	oneRowPage.MaxRowBytes = 8
	if _, err := encodeTableDataListResponse(oneRowPage, tableDataFormatOptions{}, "sha256:test-scope", 0); !errors.Is(err, tabledatabudget.ErrRowTooLarge) {
		t.Fatalf("hard row limit error = %v", err)
	}

	expanded := ports.TableDataPage{
		Schema: []domain.Field{{Name: "escaped", Type: "STRING"}, {Name: "binary", Type: "BYTES"}},
		Rows:   [][]any{{strings.Repeat(`"\\`, 100), bytes.Repeat([]byte{0xff}, 100)}}, TotalRows: 1,
		MaxResponseBytes: 10_000, MaxRowBytes: 10_000,
	}
	expandedResponse, err := encodeTableDataListResponse(expanded, tableDataFormatOptions{}, "sha256:expanded", 0)
	if err != nil {
		t.Fatal(err)
	}
	expandedBody := tableDataEncodedBody(t, expandedResponse)
	if len(expandedBody) <= 300 {
		t.Fatalf("escaped/base64 response did not expand on wire: %d bytes", len(expandedBody))
	}
	for _, delta := range []int64{-1, 0, 1} {
		threshold := expanded
		threshold.MaxResponseBytes = 1
		threshold.MaxRowBytes = int64(len(expandedBody)) + delta
		_, err := encodeTableDataListResponse(threshold, tableDataFormatOptions{}, "sha256:expanded", 0)
		if delta < 0 && !errors.Is(err, tabledatabudget.ErrResponseTooLarge) {
			t.Fatalf("hard envelope threshold delta=%d error=%v", delta, err)
		}
		if delta >= 0 && err != nil {
			t.Fatalf("hard envelope threshold delta=%d error=%v", delta, err)
		}
	}
}

func tableDataEncodedBody(t *testing.T, response encodedTableDataList) []byte {
	t.Helper()
	var body bytes.Buffer
	written, err := response.WriteTo(&body)
	if err != nil {
		t.Fatal(err)
	}
	if written != response.size || written != int64(body.Len()) {
		t.Fatalf("encoded table data bytes = written %d, declared %d, buffered %d", written, response.size, body.Len())
	}
	return body.Bytes()
}

func TestTableDataWireBudgetsAcrossPublicRESTEdge(t *testing.T) {
	ctx, cancel := requestBodyTestContext(t)
	defer cancel()
	reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	path := "/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data"
	scope := tableDataPageScope(reference)
	base := ports.TableDataPage{
		Schema: []domain.Field{{Name: "payload", Type: "STRING"}},
		Rows:   [][]any{{strings.Repeat("a", 2_000)}, {strings.Repeat("b", 2_000)}}, TotalRows: 2,
		MaxResponseBytes: 10_000, MaxRowBytes: 10_000,
	}
	oneRowPage := base
	oneRowPage.Rows = base.Rows[:1]
	oneRow, err := encodeTableDataListResponse(oneRowPage, tableDataFormatOptions{}, scope, 0)
	if err != nil {
		t.Fatal(err)
	}
	oneRowBody := tableDataEncodedBody(t, oneRow)

	request := func(t *testing.T, page ports.TableDataPage, requestPath string) (*http.Response, []byte) {
		t.Helper()
		server := httptest.NewServer(NewCatalogServer(
			nil, specialFloatQueryEngine{}, "", WithTableDataAPI(fixedTableDataPage{page: page, honorOffset: true}),
		).Handler())
		t.Cleanup(server.Close)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+requestPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return response, body
	}

	t.Run("exact page trim and continuation", func(t *testing.T) {
		page := base
		page.MaxResponseBytes = int64(len(oneRowBody))
		response, body := request(t, page, path)
		if response.StatusCode != http.StatusOK || response.ContentLength != int64(len(body)) || int64(len(body)) > page.MaxResponseBytes {
			t.Fatalf("status=%d content_length=%d body_bytes=%d limit=%d", response.StatusCode, response.ContentLength, len(body), page.MaxResponseBytes)
		}
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		if rows := decoded["rows"].([]any); len(rows) != 1 {
			t.Fatalf("trimmed public rows = %#v", rows)
		}
		cursor, err := decodeQueryPageToken(decoded["pageToken"].(string), "tabledata-list", scope)
		if err != nil || cursor != "1" {
			t.Fatalf("continuation cursor=%q error=%v", cursor, err)
		}
		response, body = request(t, page, path+"?pageToken="+url.QueryEscape(decoded["pageToken"].(string)))
		if response.StatusCode != http.StatusOK {
			t.Fatalf("continued status=%d body=%s", response.StatusCode, body)
		}
		decoded = nil
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		continuedRows := decoded["rows"].([]any)
		continuedValue := continuedRows[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"]
		if continuedValue != strings.Repeat("b", 2_000) {
			t.Fatalf("continued first value shape = type %T", continuedValue)
		}
	})

	t.Run("single row exception", func(t *testing.T) {
		page := oneRowPage
		page.MaxResponseBytes = int64(len(oneRowBody) - 1)
		page.MaxRowBytes = int64(len(oneRowBody))
		response, body := request(t, page, path)
		if response.StatusCode != http.StatusOK || int64(len(body)) <= page.MaxResponseBytes || int64(len(body)) > page.MaxRowBytes {
			t.Fatalf("status=%d body_bytes=%d normal=%d hard=%d", response.StatusCode, len(body), page.MaxResponseBytes, page.MaxRowBytes)
		}
	})

	t.Run("zero row metadata", func(t *testing.T) {
		page := ports.TableDataPage{Schema: base.Schema, TotalRows: 0, MaxResponseBytes: 1_024, MaxRowBytes: 1_024}
		response, body := request(t, page, path)
		if response.StatusCode != http.StatusOK || response.ContentLength != int64(len(body)) {
			t.Fatalf("status=%d content_length=%d body_bytes=%d", response.StatusCode, response.ContentLength, len(body))
		}
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded["kind"] != "bigquery#tableDataList" || decoded["totalRows"] != "0" || decoded["etag"] == nil || decoded["rows"] != nil || decoded["pageToken"] != nil {
			t.Fatalf("zero-row metadata = %#v", decoded)
		}
	})

	t.Run("hard row limit", func(t *testing.T) {
		page := oneRowPage
		page.MaxResponseBytes = 100
		page.MaxRowBytes = 100
		response, body := request(t, page, path)
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("status=%d body=%s", response.StatusCode, body)
		}
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		reasons := decoded["error"].(map[string]any)["errors"].([]any)
		if reasons[0].(map[string]any)["reason"] != "responseTooLarge" || strings.Contains(string(body), strings.Repeat("a", 20)) {
			t.Fatalf("bounded error response = %s", body)
		}
	})

	t.Run("operation deadline", func(t *testing.T) {
		server := httptest.NewServer(NewCatalogServer(
			nil, specialFloatQueryEngine{}, "", WithTableDataAPI(fixedTableDataPage{err: context.DeadlineExceeded}),
		).Handler())
		t.Cleanup(server.Close)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("deadline status=%d body=%s", response.StatusCode, body)
		}
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		reasons := decoded["error"].(map[string]any)["errors"].([]any)
		if reasons[0].(map[string]any)["reason"] != "backendError" {
			t.Fatalf("deadline response = %s", body)
		}
	})
}

func TestTableDataListDoesNotUseDuckDBColumnNamesAsWireBytes(t *testing.T) {
	ctx, cancel := requestBodyTestContext(t)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	catalog := application.NewCatalogService(
		memory.NewCatalogRepository(), warehouse,
		catalogTestClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)},
		application.WithTableDataReader(warehouse),
		application.WithTableDataOperationTimeout(5*time.Second),
		application.WithMaxTableDataPageRows(10),
		application.WithMaxTableDataResponseBytes(1_024),
		application.WithMaxTableDataRowBytes(1_024),
	)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "long_fields", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	fieldNames := make([]string, 0, 5)
	schema := make([]domain.Field, 0, 5)
	for _, suffix := range []string{"a", "b", "c", "d", "e"} {
		// BigQuery standard column names are limited to 300 characters. Five
		// valid 250-character names make DuckDB's name-bearing JSON exceed the
		// local 1024-byte limit while the schema-ordered f/v row stays small.
		// https://cloud.google.com/bigquery/docs/schemas#column_names
		name := "field_" + suffix + "_" + strings.Repeat(suffix, 242)
		if len(name) != 250 {
			t.Fatalf("long field name length = %d, want 250", len(name))
		}
		fieldNames = append(fieldNames, name)
		schema = append(schema, domain.Field{Name: name, Type: "STRING"})
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "long_fields", ID: "tiny_values",
		Schema: schema,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{
		SQL: "INSERT INTO `test-project.long_fields.tiny_values` VALUES ('a', 'b', 'c', 'd', 'e')",
	}); err != nil {
		t.Fatal(err)
	}
	backendProbe, err := warehouse.Query(ctx, ports.QueryRequest{
		SQL: "SELECT octet_length(CAST(to_json(data) AS BLOB)) > 1024 AS exceeds " +
			"FROM (SELECT * FROM `test-project.long_fields.tiny_values`) AS data",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(backendProbe.Rows) != 1 || len(backendProbe.Rows[0]) != 1 || backendProbe.Rows[0][0] != true {
		t.Fatalf("DuckDB JSON regression precondition = %#v, want backend bytes over 1024", backendProbe.Rows)
	}

	server := httptest.NewServer(NewCatalogServer(catalog, warehouse, "", WithTableDataAPI(catalog)).Handler())
	t.Cleanup(server.Close)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+
		"/bigquery/v2/projects/test-project/datasets/long_fields/tables/tiny_values/data?maxResults=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	const wantWireBytes = 169
	if response.StatusCode != http.StatusOK || response.ContentLength != int64(len(body)) || len(body) != wantWireBytes {
		t.Fatalf("public table data status=%d content_length=%d wire_bytes=%d want_wire_bytes=%d body=%s",
			response.StatusCode, response.ContentLength, len(body), wantWireBytes, body)
	}
	for _, name := range fieldNames {
		if bytes.Contains(body, []byte(name)) {
			t.Fatalf("tabledata.list row unexpectedly encoded schema field names")
		}
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	rows, ok := decoded["rows"].([]any)
	if !ok || len(rows) != 1 || decoded["totalRows"] != "1" {
		t.Fatalf("public table data shape = %#v", decoded)
	}
	cells := rows[0].(map[string]any)["f"].([]any)
	if len(cells) != 5 || cells[0].(map[string]any)["v"] != "a" || cells[4].(map[string]any)["v"] != "e" {
		t.Fatalf("public f/v row = %#v", cells)
	}
}

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
	zero := request("/bigquery/v2/projects/test-project/datasets/analytics/tables/events/data?maxResults=0", http.StatusOK)
	if zero["totalRows"] != "2" || zero["rows"] != nil || zero["pageToken"] != nil {
		t.Fatalf("explicit-zero table data page = %#v", zero)
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
