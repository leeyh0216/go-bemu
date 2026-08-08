package rest

// BigQuery REST FLOAT64 representation:
// https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

type specialFloatStatementExecutor struct{}

func (specialFloatStatementExecutor) Ping(context.Context) error { return nil }

func (specialFloatStatementExecutor) ExecuteStatement(context.Context, semantic.Statement) (domain.QueryResult, error) {
	return domain.QueryResult{
		Columns: []domain.Column{
			{Name: "finite", Type: "FLOAT64"},
			{Name: "positive_infinity", Type: "FLOAT64"},
			{Name: "negative_infinity", Type: "FLOAT64"},
			{Name: "not_a_number", Type: "FLOAT64"},
		},
		Rows: [][]any{{1.25, math.Inf(1), math.Inf(-1), math.NaN()}},
	}, nil
}

type specialFloatTableData struct{}

func (specialFloatTableData) ListTableData(context.Context, string, string, string, int64, ports.TableDataMaxResults) (ports.TableDataPage, error) {
	return ports.TableDataPage{
		Schema: []domain.Field{
			{Name: "finite", Type: "FLOAT64"},
			{Name: "positive_infinity", Type: "FLOAT64"},
			{Name: "negative_infinity", Type: "FLOAT64"},
			{Name: "not_a_number", Type: "FLOAT64"},
		},
		Rows:      [][]any{{1.25, math.Inf(1), math.Inf(-1), math.NaN()}},
		TotalRows: 1,
	}, nil
}

func TestQueryResponseEncodesNonFiniteFloatTokensAcrossPublicRESTEdge(t *testing.T) {
	ctx, cancel := staticOverwriteRESTTestContext(t)
	defer cancel()
	executor := specialFloatStatementExecutor{}
	queries := newRESTTestQueryService(
		memory.NewJobRepository(), executor,
		testClock{value: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}, &testIDs{},
	)
	server := httptest.NewServer(NewServer(nil, queries, executor, "").Handler())
	t.Cleanup(server.Close)

	response := staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodPost,
		"/bigquery/v2/projects/test-project/queries", `{"query":"SELECT special_float_values","useLegacySql":false}`,
		http.StatusOK)
	assertNonFiniteFloatRow(t, response)
}

func TestTableDataListEncodesNonFiniteFloatTokensAcrossPublicRESTEdge(t *testing.T) {
	ctx, cancel := staticOverwriteRESTTestContext(t)
	defer cancel()
	server := httptest.NewServer(NewCatalogServer(
		nil, specialFloatStatementExecutor{}, "", WithTableDataAPI(specialFloatTableData{}),
	).Handler())
	t.Cleanup(server.Close)

	response := staticOverwriteRESTRequest(t, ctx, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/datasets/analytics/tables/float_values/data", "", http.StatusOK)
	if response["kind"] != "bigquery#tableDataList" || response["totalRows"] != "1" {
		t.Fatalf("tabledata.list metadata = %#v", response)
	}
	assertNonFiniteFloatRow(t, response)
}

func assertNonFiniteFloatRow(t *testing.T, response map[string]any) {
	t.Helper()
	rows, ok := response["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("FLOAT64 rows = %#v, want one row", response["rows"])
	}
	cells, ok := rows[0].(map[string]any)["f"].([]any)
	if !ok || len(cells) != 4 {
		t.Fatalf("FLOAT64 cells = %#v, want four cells", rows[0])
	}
	if value, ok := cells[0].(map[string]any)["v"].(float64); !ok || value != 1.25 {
		t.Fatalf("finite FLOAT64 cell = %#v, want JSON number 1.25", cells[0])
	}
	want := []string{"Infinity", "-Infinity", "NaN"}
	for index, expected := range want {
		value, _ := cells[index+1].(map[string]any)["v"].(string)
		if value != expected {
			t.Fatalf("FLOAT64 cell %d = %q, want %q", index+1, value, expected)
		}
	}
}
