package duckdb

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	tabledatabudget "github.com/leeyh0216/go-bemu/internal/tabledata"
)

func TestListTableDataPagesRealDuckDBRowsAndReportsExactTotal(t *testing.T) {
	ctx, cancel := duckDBTableDataTestContext(t)
	defer cancel()
	warehouse, reference := newDuckDBTableDataFixture(t, ctx)

	schema := []domain.Field{{Name: "id", Type: "INT64"}, {Name: "payload", Type: "STRING"}, {Name: "active", Type: "BOOL"}}
	page, err := warehouse.ListTableData(ctx, ports.TableDataReadRequest{
		Reference: reference, Schema: schema, Offset: 1, Limit: 2,
		MaxResponseBytes: 10_000_000, MaxRowBytes: 100_000_000,
	})
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

	beyond, err := warehouse.ListTableData(ctx, ports.TableDataReadRequest{
		Reference: reference, Schema: schema, Offset: 99, Limit: 2,
		MaxResponseBytes: 10_000_000, MaxRowBytes: 100_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if beyond.TotalRows != 3 || len(beyond.Rows) != 0 {
		t.Fatalf("beyond-total page = %#v, want empty page with exact total", beyond)
	}
}

func TestListTableDataTrimsCanonicalPageAndRejectsOversizedRow(t *testing.T) {
	ctx, cancel := duckDBTableDataTestContext(t)
	defer cancel()
	warehouse, reference := newDuckDBTableDataFixture(t, ctx)
	schema := []domain.Field{{Name: "id", Type: "INT64"}, {Name: "payload", Type: "STRING"}, {Name: "active", Type: "BOOL"}}

	probe, err := warehouse.ListTableData(ctx, ports.TableDataReadRequest{
		Reference: reference, Schema: schema, Limit: 1,
		MaxResponseBytes: 10_000, MaxRowBytes: 10_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	budget := tabledatabudget.NewAccumulator(0)
	if included, err := budget.Add(probe.Rows[0], 10_000); err != nil || !included {
		t.Fatalf("measure probe row = included %v, error %v", included, err)
	}
	oneRowBytes := budget.Metrics().Bytes
	trimmed, err := warehouse.ListTableData(ctx, ports.TableDataReadRequest{
		Reference: reference, Schema: schema, Limit: 3,
		MaxResponseBytes: oneRowBytes, MaxRowBytes: 10_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(trimmed.Rows) != 1 || trimmed.TotalRows != 3 {
		t.Fatalf("trimmed page = %#v, want one of three rows", trimmed)
	}

	_, err = warehouse.ListTableData(ctx, ports.TableDataReadRequest{
		Reference: reference, Schema: schema, Limit: 1,
		MaxResponseBytes: 10_000, MaxRowBytes: 8,
	})
	if !errors.Is(err, tabledatabudget.ErrRowTooLarge) {
		t.Fatalf("oversized row error = %v", err)
	}
	if !strings.Contains(err.Error(), "limit_bytes=8") || strings.Contains(err.Error(), "backend_bytes=") {
		t.Fatalf("oversized row was not rejected by the canonical byte gate: %v", err)
	}
}

func TestListTableDataStructuredLogsRetainRawRowsAndNames(t *testing.T) {
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
		if !strings.Contains(output, raw) {
			t.Fatalf("table data logs omitted %q: %s", raw, output)
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
	physical, err := renderPhysicalTable(reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx,
		"INSERT INTO "+physical+" VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
		int64(1), "row-secret-marker", true,
		int64(2), "second", false,
		int64(3), nil, true,
	); err != nil {
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
