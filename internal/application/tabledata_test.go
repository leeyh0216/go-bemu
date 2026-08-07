package application

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type fakeTableDataReader struct {
	reference        domain.TableReference
	offset           int64
	limit            int
	page             ports.TableDataPage
	err              error
	calls            int
	deadline         time.Time
	calledAt         time.Time
	maxResponseBytes int64
	maxRowBytes      int64
}

func (reader *fakeTableDataReader) ListTableData(ctx context.Context, request ports.TableDataReadRequest) (ports.TableDataPage, error) {
	reader.calls++
	reader.calledAt = time.Now()
	reader.reference = request.Reference
	reader.offset = request.Offset
	reader.limit = request.Limit
	reader.maxResponseBytes = request.MaxResponseBytes
	reader.maxRowBytes = request.MaxRowBytes
	reader.deadline, _ = ctx.Deadline()
	return reader.page, reader.err
}

func TestListTableDataOperationTimeoutIncludesCatalogAdmission(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{}
	service, _ := newTableDataCatalogService(t, ctx, reader, 10)
	table := createTableDataCatalogFixture(t, ctx, service, nil)
	operationTimeout := 30 * time.Millisecond
	service.tableDataOperationTimeout = operationTimeout
	service.resourceMutationMu.Lock()
	defer service.resourceMutationMu.Unlock()

	started := time.Now()
	_, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, 0, ports.TableDataMaxResults{Value: 1, Present: true})
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("admission error = %v, want deadline exceeded", err)
	}
	if elapsed < operationTimeout/2 || elapsed > time.Second {
		t.Fatalf("admission elapsed = %v, want bounded by configured timeout %v", elapsed, operationTimeout)
	}
	if reader.calls != 0 {
		t.Fatalf("reader calls = %d, want zero before admission", reader.calls)
	}
}

func TestListTableDataCallerCancellationWinsDuringCatalogAdmission(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{}
	service, _ := newTableDataCatalogService(t, ctx, reader, 10)
	table := createTableDataCatalogFixture(t, ctx, service, nil)
	service.tableDataOperationTimeout = 5 * time.Second
	service.resourceMutationMu.Lock()
	defer service.resourceMutationMu.Unlock()

	canceled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	started := time.Now()
	_, err := service.ListTableData(canceled, table.ProjectID, table.DatasetID, table.ID, 0, ports.TableDataMaxResults{Value: 1, Present: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("admission error = %v, want caller cancellation", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled admission took %v", elapsed)
	}
	if reader.calls != 0 {
		t.Fatalf("reader calls = %d, want zero before admission", reader.calls)
	}
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
	if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, 0, ports.TableDataMaxResults{Value: 1, Present: true}); err != nil {
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

	page, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, 1, ports.TableDataMaxResults{Value: 1, Present: true})
	if err != nil {
		t.Fatal(err)
	}
	wantReference := domain.TableReference{ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID}
	if reader.calls != 1 || reader.reference != wantReference || reader.offset != 1 || reader.limit != 1 {
		t.Fatalf("reader call = count %d, reference %#v, offset %d, limit %d", reader.calls, reader.reference, reader.offset, reader.limit)
	}
	if reader.maxResponseBytes != 10_000_000 || reader.maxRowBytes != 100_000_000 ||
		page.MaxResponseBytes != reader.maxResponseBytes || page.MaxRowBytes != reader.maxRowBytes {
		t.Fatalf("byte policy request=(%d,%d) page=(%d,%d)", reader.maxResponseBytes, reader.maxRowBytes, page.MaxResponseBytes, page.MaxRowBytes)
	}
	if page.TotalRows != 3 || len(page.Rows) != 1 || len(page.Schema) != len(table.Schema) || page.Schema[1].Name != "payload" {
		t.Fatalf("table data page = %#v", page)
	}
	if _, err := repository.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID); err != nil {
		t.Fatalf("live table metadata changed: %v", err)
	}
}

func TestListTableDataRejectsAdapterThatViolatesRowByteContract(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{page: ports.TableDataPage{
		Rows: [][]any{{int64(1), strings.Repeat("x", 256)}}, TotalRows: 1,
	}}
	service, _ := newTableDataCatalogService(t, ctx, reader, 10)
	service.maxTableDataResponseBytes = 128
	service.maxTableDataRowBytes = 128
	table := createTableDataCatalogFixture(t, ctx, service, nil)

	if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, 0, ports.TableDataMaxResults{Value: 1, Present: true}); !errors.Is(err, ErrTableDataAdapterContract) {
		t.Fatalf("adapter byte contract error = %v, want adapter contract violation", err)
	}
}

func TestListTableDataRejectsAdapterPagingContractViolations(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{}
	service, _ := newTableDataCatalogService(t, ctx, reader, 10)
	table := createTableDataCatalogFixture(t, ctx, service, nil)

	for _, test := range []struct {
		name       string
		offset     int64
		maximum    ports.TableDataMaxResults
		page       ports.TableDataPage
		wantReason string
	}{
		{
			name: "explicit zero leaks a row", maximum: ports.TableDataMaxResults{Present: true},
			page: ports.TableDataPage{Rows: [][]any{{int64(1), "value"}}, TotalRows: 1}, wantReason: "effective limit 0",
		},
		{
			name: "page exceeds effective limit", maximum: ports.TableDataMaxResults{Value: 1, Present: true},
			page: ports.TableDataPage{Rows: [][]any{{int64(1), "one"}, {int64(2), "two"}}, TotalRows: 2}, wantReason: "effective limit 1",
		},
		{
			name: "negative total", maximum: ports.TableDataMaxResults{Value: 1, Present: true},
			page: ports.TableDataPage{TotalRows: -1}, wantReason: "negative total row count",
		},
		{
			name: "row starts beyond total", offset: 2, maximum: ports.TableDataMaxResults{Value: 1, Present: true},
			page: ports.TableDataPage{Rows: [][]any{{int64(3), "outside"}}, TotalRows: 2}, wantReason: "exceeds the reported total",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader.page = test.page
			_, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, test.offset, test.maximum)
			if !errors.Is(err, ErrTableDataAdapterContract) || !strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("adapter contract error = %v, want reason %q", err, test.wantReason)
			}
		})
	}
}

func TestListTableDataDefaultsAndClampsLimitBeforeAdapter(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{}
	service, _ := newTableDataCatalogService(t, ctx, reader, 2)
	table := createTableDataCatalogFixture(t, ctx, service, nil)

	for _, test := range []struct {
		name    string
		offset  int64
		maximum ports.TableDataMaxResults
		want    int
	}{
		{name: "omitted defaults", offset: 0, maximum: ports.TableDataMaxResults{}, want: 2},
		{name: "explicit zero remains zero", offset: 0, maximum: ports.TableDataMaxResults{Value: 0, Present: true}, want: 0},
		{name: "over configured maximum clamps", offset: 0, maximum: ports.TableDataMaxResults{Value: 3, Present: true}, want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, test.offset, test.maximum); err != nil {
				t.Fatal(err)
			}
			if reader.limit != test.want {
				t.Fatalf("reader limit = %d, want %d", reader.limit, test.want)
			}
		})
	}
	if reader.calls != 3 {
		t.Fatalf("reader calls = %d, want three", reader.calls)
	}
	for _, test := range []struct {
		name    string
		offset  int64
		maximum ports.TableDataMaxResults
	}{
		{name: "negative offset", offset: -1, maximum: ports.TableDataMaxResults{Value: 1, Present: true}},
		{name: "negative limit", offset: 0, maximum: ports.TableDataMaxResults{Value: -1, Present: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, test.offset, test.maximum); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("error = %v, want invalid", err)
			}
		})
	}
	if reader.calls != 3 {
		t.Fatalf("reader calls after invalid inputs = %d, want three", reader.calls)
	}
}

func TestListTableDataAppliesNotFoundAndExpirationBeforePhysicalRead(t *testing.T) {
	ctx, cancel := tableDataApplicationTestContext(t)
	defer cancel()
	reader := &fakeTableDataReader{}
	service, repository := newTableDataCatalogService(t, ctx, reader, 10)
	expires := time.Date(2026, 8, 7, 23, 59, 59, 0, time.UTC)
	table := createTableDataCatalogFixture(t, ctx, service, &expires)

	if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, table.ID, 0, ports.TableDataMaxResults{Value: 1, Present: true}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired table error = %v, want not found", err)
	}
	if _, err := repository.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired metadata error = %v, want removed", err)
	}
	if _, err := service.ListTableData(ctx, table.ProjectID, table.DatasetID, "missing", 0, ports.TableDataMaxResults{Value: 1, Present: true}); !errors.Is(err, domain.ErrNotFound) {
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
