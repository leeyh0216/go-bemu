package application

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type fakeTableDataReader struct {
	reference domain.TableReference
	offset    int64
	limit     int
	page      ports.TableDataPage
	err       error
	calls     int
	deadline  time.Time
	calledAt  time.Time
}

func (reader *fakeTableDataReader) ListTableData(ctx context.Context, request ports.TableDataReadRequest) (ports.TableDataPage, error) {
	reader.calls++
	reader.calledAt = time.Now()
	reader.reference = request.Reference
	reader.offset = request.Offset
	reader.limit = request.Limit
	reader.deadline, _ = ctx.Deadline()
	return reader.page, reader.err
}

func TestListTableDataBoundsReaderWithRequestDerivedOperationTimeout(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{page: ports.TableDataPage{TotalRows: 0}}
	repository := memory.NewCatalogRepository()
	operationTimeout := 250 * time.Millisecond
	service := NewCatalogService(repository, &fakeWarehouse{}, fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)},
		WithTableDataReader(reader), WithTableDataOperationTimeout(operationTimeout))
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	table := createTableDataCatalogFixture(t, ctx, service, nil)
	if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, 0, 1); err != nil {
		t.Fatal(err)
	}
	remaining := reader.deadline.Sub(reader.calledAt)
	if reader.deadline.IsZero() || remaining <= 0 || remaining > operationTimeout {
		t.Fatalf("reader deadline remaining = %v, want within (0, %v]", remaining, operationTimeout)
	}
}

func TestListTableDataResolvesLiveSchemaAndDelegatesBoundedPage(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{page: ports.TableDataPage{Rows: [][]any{{int64(2), "second"}}, TotalRows: 3}}
	service, repository := newTableDataCatalogService(t, ctx, reader, 2)
	table := createTableDataCatalogFixture(t, ctx, service, nil)

	page, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	wantReference := domain.TableReference{ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID}
	if reader.calls != 1 || reader.reference != wantReference || reader.offset != 1 || reader.limit != 1 {
		t.Fatalf("reader call = count %d, reference %#v, offset %d, limit %d", reader.calls, reader.reference, reader.offset, reader.limit)
	}
	if page.TotalRows != 3 || len(page.Rows) != 1 || len(page.Schema) != len(table.Schema) || page.Schema[1].Name != "payload" {
		t.Fatalf("table data page = %#v", page)
	}
	if _, err := repository.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID); err != nil {
		t.Fatalf("live table metadata changed: %v", err)
	}
}

func TestListTableDataDefaultsAndClampsLimitBeforeAdapter(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{}
	service, _ := newTableDataCatalogService(t, ctx, reader, 2)
	table := createTableDataCatalogFixture(t, ctx, service, nil)

	for _, test := range []struct {
		name   string
		offset int64
		limit  int
		want   int
	}{
		{name: "zero defaults", offset: 0, limit: 0, want: 2},
		{name: "over configured maximum clamps", offset: 0, limit: 3, want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, test.offset, test.limit); err != nil {
				t.Fatal(err)
			}
			if reader.limit != test.want {
				t.Fatalf("reader limit = %d, want %d", reader.limit, test.want)
			}
		})
	}
	if reader.calls != 2 {
		t.Fatalf("reader calls = %d, want two", reader.calls)
	}
	for _, test := range []struct {
		name   string
		offset int64
		limit  int
	}{
		{name: "negative offset", offset: -1, limit: 1},
		{name: "negative limit", offset: 0, limit: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, test.offset, test.limit); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("error = %v, want invalid", err)
			}
		})
	}
	if reader.calls != 2 {
		t.Fatalf("reader calls after invalid inputs = %d, want two", reader.calls)
	}
}

func TestListTableDataAppliesNotFoundAndExpirationBeforePhysicalRead(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{}
	service, repository := newTableDataCatalogService(t, ctx, reader, 10)
	expires := time.Date(2026, 8, 7, 23, 59, 59, 0, time.UTC)
	table := createTableDataCatalogFixture(t, ctx, service, &expires)

	if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, 0, 1); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired table error = %v, want not found", err)
	}
	if _, err := repository.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired metadata error = %v, want removed", err)
	}
	if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, "missing", 0, 1); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing table error = %v, want not found", err)
	}
	if reader.calls != 0 {
		t.Fatalf("reader calls = %d, want zero", reader.calls)
	}
}

func newTableDataCatalogService(t *testing.T, ctx context.Context, reader ports.TableDataReader, maxRows int) (*CatalogService, *memory.CatalogRepository) {
	t.Helper()
	repository := memory.NewCatalogRepository()
	service := NewCatalogService(repository, &fakeWarehouse{}, fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)},
		WithTableDataReader(reader), WithMaxTableDataPageRows(maxRows))
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	return service, repository
}

func createTableDataCatalogFixture(t *testing.T, ctx context.Context, service *CatalogService, expiration *time.Time) domain.Table {
	t.Helper()
	table, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema:         []domain.Field{{Name: "id", Type: "INT64"}, {Name: "payload", Type: "STRING"}},
		ExpirationTime: expiration,
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func tableDataApplicationTestContext(t *testing.T) (context.Context, context.CancelFunc) {
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
