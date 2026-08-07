package duckdb

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestListTableDataPagesRealDuckDBRowsAndReportsExactTotal(t *testing.T) {
	ctx, cancel := duckDBTableDataTestContext(t)
	defer cancel()
	warehouse, reference := newDuckDBTableDataFixture(t, ctx)

	schema := []domain.Field{{Name: "id", Type: "INT64"}, {Name: "payload", Type: "STRING"}, {Name: "active", Type: "BOOL"}}
	page, err := warehouse.ListTableData(ctx, ports.TableDataReadRequest{Reference: reference, Schema: schema, Offset: 1, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalRows != 3 || len(page.Rows) != 2 {
		t.Fatalf("page = %#v, want two of three rows", page)
	}
	want := [][]any{{int64(2), "second", false}, {int64(3), nil, true}}
	for rowIndex := range want {
		for columnIndex := range want[rowIndex] {
			if page.Rows[rowIndex][columnIndex] != want[rowIndex][columnIndex] {
				t.Fatalf("value [%d][%d] = %#v, want %#v", rowIndex, columnIndex, page.Rows[rowIndex][columnIndex], want[rowIndex][columnIndex])
			}
		}
	}

	beyond, err := warehouse.ListTableData(ctx, ports.TableDataReadRequest{Reference: reference, Schema: schema, Offset: 99, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if beyond.TotalRows != 3 || len(beyond.Rows) != 0 {
		t.Fatalf("beyond-total page = %#v, want empty page with exact total", beyond)
	}
}

func TestListTableDataStructuredLogsOmitRawRowsAndNames(t *testing.T) {
	ctx, cancel := duckDBTableDataTestContext(t)
	defer cancel()
	warehouse, reference := newDuckDBTableDataFixture(t, ctx)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	if _, err := warehouse.ListTableData(ctx, ports.TableDataReadRequest{
		Reference: reference,
		Schema:    []domain.Field{{Name: "id", Type: "INT64"}, {Name: "payload", Type: "STRING"}, {Name: "active", Type: "BOOL"}},
		Limit:     3,
	}); err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	for _, raw := range []string{"private-project", "sensitive_dataset", "private_table", "row-secret-marker"} {
		if strings.Contains(output, raw) {
			t.Fatalf("table data logs leaked %q: %s", raw, output)
		}
	}
	for _, field := range []string{"table_reference_digest", "result_digest", "row_count", "total_rows", tableDataModelVersion} {
		if !strings.Contains(output, field) {
			t.Fatalf("table data logs lack %q: %s", field, output)
		}
	}
}

func newDuckDBTableDataFixture(t *testing.T, ctx context.Context) (*Warehouse, domain.TableReference) {
	t.Helper()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	reference := domain.TableReference{ProjectID: "private-project", DatasetID: "sensitive_dataset", TableID: "private_table"}
	if err := warehouse.CreateDataset(ctx, reference.ProjectID, reference.DatasetID); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.CreateTable(ctx, domain.Table{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID, ID: reference.TableID,
		Schema: []domain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "payload", Type: "STRING"},
			{Name: "active", Type: "BOOL", Mode: "REQUIRED"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{SQL: "INSERT INTO `private-project.sensitive_dataset.private_table` VALUES (1, 'row-secret-marker', true), (2, 'second', false), (3, NULL, true)"}); err != nil {
		t.Fatal(err)
	}
	return warehouse, reference
}

func duckDBTableDataTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 10 * time.Second
	if configured := os.Getenv("BQEMU_TABLEDATA_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("BQEMU_TABLEDATA_TEST_TIMEOUT must be a positive Go duration: %q", configured)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}
