package application

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type mutableCatalogClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (clock *mutableCatalogClock) Now() time.Time {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.now
}

func (clock *mutableCatalogClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

type expirationWarehouse struct {
	fakeWarehouse
	mu            sync.Mutex
	droppedTables []string
}

type failingDatasetCatalog struct{ *memory.CatalogRepository }

func (catalog failingDatasetCatalog) CreateDataset(context.Context, domain.Dataset) error {
	return errors.New("injected anonymous dataset metadata failure")
}

type failingDatasetCompensationWarehouse struct{ fakeWarehouse }

func (*failingDatasetCompensationWarehouse) DropDataset(context.Context, string, string) error {
	return errors.New("injected anonymous dataset storage cleanup failure")
}

type failDeleteTableOnceCatalog struct {
	*memory.CatalogRepository
	mu     sync.Mutex
	failed bool
}

type blockingTableUpdateCatalog struct {
	*memory.CatalogRepository
	entered chan struct{}
	release chan struct{}
}

func (catalog *blockingTableUpdateCatalog) UpdateTable(ctx context.Context, table domain.Table) error {
	close(catalog.entered)
	select {
	case <-catalog.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return catalog.CatalogRepository.UpdateTable(ctx, table)
}

func (catalog *failDeleteTableOnceCatalog) DeleteTable(ctx context.Context, projectID, datasetID, tableID string) error {
	catalog.mu.Lock()
	if !catalog.failed {
		catalog.failed = true
		catalog.mu.Unlock()
		return errors.New("injected table metadata deletion failure")
	}
	catalog.mu.Unlock()
	return catalog.CatalogRepository.DeleteTable(ctx, projectID, datasetID, tableID)
}

func (warehouse *expirationWarehouse) DropTable(_ context.Context, projectID, datasetID, tableID string) error {
	warehouse.mu.Lock()
	defer warehouse.mu.Unlock()
	warehouse.droppedTables = append(warehouse.droppedTables, projectID+"/"+datasetID+"/"+tableID)
	return nil
}

func TestEnsureAnonymousDatasetIsConcurrentAndRejectsReservedCollision(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	repository := memory.NewCatalogRepository()
	warehouse := &fakeWarehouse{}
	service := NewCatalogService(repository, warehouse, fixedClock{now: time.Unix(1, 0)})
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}

	const callers = 16
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			dataset, err := service.EnsureAnonymousDataset(ctx, "test-project", "_bqemu_anonymous_eu", "EU")
			if err == nil && (!dataset.Hidden || dataset.Location != "EU") {
				err = errors.New("anonymous dataset shape mismatch")
			}
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(warehouse.datasets) != 1 {
		t.Fatalf("physical dataset creates = %v, want one", warehouse.datasets)
	}

	if _, err := service.CreateDataset(ctx, domain.Dataset{
		ProjectID: "test-project", ID: "_bqemu_anonymous_collision", Location: "EU",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureAnonymousDataset(ctx, "test-project", "_bqemu_anonymous_collision", "EU"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("reserved-name collision error = %v, want conflict", err)
	}
}

func TestAnonymousDatasetMetadataFailureCompensatesPhysicalSchema(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	base := memory.NewCatalogRepository()
	if err := base.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	warehouse := &fakeWarehouse{}
	service := NewCatalogService(failingDatasetCatalog{CatalogRepository: base}, warehouse, fixedClock{now: time.Unix(1, 0)})
	if _, err := service.EnsureAnonymousDataset(ctx, "test-project", "_bqemu_anonymous_eu", "EU"); err == nil {
		t.Fatal("injected anonymous dataset metadata failure unexpectedly succeeded")
	}
	if len(warehouse.datasets) != 1 || len(warehouse.dropped) != 1 || warehouse.dropped[0] != "test-project/_bqemu_anonymous_eu" {
		t.Fatalf("anonymous dataset compensation: creates=%v drops=%v", warehouse.datasets, warehouse.dropped)
	}
	if _, err := base.GetDataset(ctx, "test-project", "_bqemu_anonymous_eu"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("failed publication left metadata: %v", err)
	}
}

func TestAnonymousDatasetCompensationFailureIsReturned(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	base := memory.NewCatalogRepository()
	if err := base.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	service := NewCatalogService(
		failingDatasetCatalog{CatalogRepository: base},
		&failingDatasetCompensationWarehouse{}, fixedClock{now: time.Unix(1, 0)},
	)
	_, err := service.EnsureAnonymousDataset(ctx, "test-project", "_bqemu_anonymous_eu", "EU")
	if err == nil || !strings.Contains(err.Error(), "metadata failure") || !strings.Contains(err.Error(), "storage cleanup failure") {
		t.Fatalf("joined anonymous dataset publication/cleanup error = %v", err)
	}
}

func TestExpiredTableCleanupDropsPhysicalBeforeMetadata(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	clock := &mutableCatalogClock{now: now}
	repository := memory.NewCatalogRepository()
	warehouse := &expirationWarehouse{}
	service := NewCatalogService(repository, warehouse, clock)
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureAnonymousDataset(ctx, "test-project", "_bqemu_anonymous_eu", "EU"); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	for _, tableID := range []string{"_bqemu_query_get", "_bqemu_query_list"} {
		if _, err := service.CreateTable(ctx, domain.Table{
			ProjectID: "test-project", DatasetID: "_bqemu_anonymous_eu", ID: tableID,
			Schema: []domain.Field{{Name: "value", Type: "INT64"}}, ExpirationTime: &expires,
		}); err != nil {
			t.Fatal(err)
		}
	}
	clock.Set(expires)
	if _, err := service.GetTable(ctx, "test-project", "_bqemu_anonymous_eu", "_bqemu_query_get"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired tables.get error = %v, want not found", err)
	}
	if tables, err := service.ListTables(ctx, "test-project", "_bqemu_anonymous_eu"); err != nil || len(tables) != 0 {
		t.Fatalf("tables.list after cleanup = %#v, err=%v", tables, err)
	}
	warehouse.mu.Lock()
	dropped := append([]string(nil), warehouse.droppedTables...)
	warehouse.mu.Unlock()
	if len(dropped) != 2 {
		t.Fatalf("physical expiration drops = %v, want get and list cleanup", dropped)
	}
	for _, tableID := range []string{"_bqemu_query_get", "_bqemu_query_list"} {
		if _, err := repository.GetTable(ctx, "test-project", "_bqemu_anonymous_eu", tableID); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("expired metadata %s error = %v, want not found", tableID, err)
		}
	}
}

func TestExpiredTableCleanupRetriesAfterMetadataDeletionFailure(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	clock := &mutableCatalogClock{now: now}
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	repository := &failDeleteTableOnceCatalog{CatalogRepository: memory.NewCatalogRepository()}
	service := NewCatalogService(repository, warehouse, clock)
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureAnonymousDataset(ctx, "test-project", "_bqemu_anonymous_eu", "EU"); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "_bqemu_anonymous_eu", ID: "_bqemu_query_retry",
		Schema: []domain.Field{{Name: "value", Type: "INT64"}}, ExpirationTime: &expires,
	}); err != nil {
		t.Fatal(err)
	}
	clock.Set(expires)
	if _, err := service.GetTable(ctx, "test-project", "_bqemu_anonymous_eu", "_bqemu_query_retry"); err == nil || errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("first cleanup error = %v, want injected metadata failure", err)
	}
	if _, err := repository.GetTable(ctx, "test-project", "_bqemu_anonymous_eu", "_bqemu_query_retry"); err != nil {
		t.Fatalf("failed cleanup must retain retry metadata: %v", err)
	}
	if _, err := service.GetTable(ctx, "test-project", "_bqemu_anonymous_eu", "_bqemu_query_retry"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("retry cleanup error = %v, want not found", err)
	}
	if _, err := warehouse.ListTableData(ctx, ports.TableDataReadRequest{
		Reference: domain.TableReference{
			ProjectID: "test-project", DatasetID: "_bqemu_anonymous_eu", TableID: "_bqemu_query_retry",
		},
		Schema: []domain.Field{{Name: "value", Type: "INT64"}}, Limit: 1,
	}); err == nil {
		t.Fatal("retry cleanup left physical table queryable")
	}
}

func TestTableExpirationExtensionSerializesBeforeLazyCleanup(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	clock := &mutableCatalogClock{now: now}
	repository := &blockingTableUpdateCatalog{
		CatalogRepository: memory.NewCatalogRepository(),
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	warehouse := &expirationWarehouse{}
	service := NewCatalogService(repository, warehouse, clock)
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "EU"}); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "value", Type: "INT64"}}, ExpirationTime: &expires,
	}); err != nil {
		t.Fatal(err)
	}

	extended := expires.Add(time.Hour)
	updated := make(chan error, 1)
	go func() {
		_, err := service.UpdateTable(ctx, "test-project", "analytics", "events", TablePatch{
			ExpirationTime: PatchValue[*time.Time]{Set: true, Value: &extended},
		})
		updated <- err
	}()
	select {
	case <-repository.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	clock.Set(expires)
	read := make(chan struct {
		table domain.Table
		err   error
	}, 1)
	go func() {
		table, err := service.GetTable(ctx, "test-project", "analytics", "events")
		read <- struct {
			table domain.Table
			err   error
		}{table: table, err: err}
	}()
	close(repository.release)
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case result := <-read:
		if result.err != nil || result.table.ExpirationTime == nil || !result.table.ExpirationTime.Equal(extended) {
			t.Fatalf("table after concurrent expiration extension = %#v, err=%v", result.table, result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	warehouse.mu.Lock()
	dropped := len(warehouse.droppedTables)
	warehouse.mu.Unlock()
	if dropped != 0 {
		t.Fatalf("lazy cleanup dropped a concurrently extended table %d times", dropped)
	}
}

func TestExpiredTableNameCanBeRecreatedWithoutPriorRead(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	clock := &mutableCatalogClock{now: now}
	repository := memory.NewCatalogRepository()
	warehouse := &expirationWarehouse{}
	service := NewCatalogService(repository, warehouse, clock)
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "EU"}); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "old_value", Type: "INT64"}}, ExpirationTime: &expires,
	}); err != nil {
		t.Fatal(err)
	}
	clock.Set(expires)
	recreated, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "new_value", Type: "STRING"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(recreated.Schema) != 1 || recreated.Schema[0].Name != "new_value" {
		t.Fatalf("recreated table = %#v", recreated)
	}
	warehouse.mu.Lock()
	dropped := append([]string(nil), warehouse.droppedTables...)
	warehouse.mu.Unlock()
	if len(dropped) != 1 {
		t.Fatalf("expired predecessor drops = %v, want one", dropped)
	}
}

func TestConcurrentQueryPublicationAndDatasetDeleteCannotBothSucceed(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	for iteration := 0; iteration < 32; iteration++ {
		repository := memory.NewCatalogRepository()
		service := NewCatalogService(repository, &fakeWarehouse{}, fixedClock{now: time.Unix(1, 0)})
		if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
			t.Fatal(err)
		}
		if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan struct {
			operation string
			err       error
		}, 2)
		go func() {
			<-start
			results <- struct {
				operation string
				err       error
			}{operation: "publish", err: service.PublishMaterializedTable(ctx, domain.Table{
				ProjectID: "test-project", DatasetID: "analytics", ID: "query_result",
				Schema: []domain.Field{{Name: "id", Type: "INT64"}},
			})}
		}()
		go func() {
			<-start
			results <- struct {
				operation string
				err       error
			}{operation: "delete", err: service.DeleteDataset(ctx, "test-project", "analytics", false)}
		}()
		close(start)
		first, second := <-results, <-results
		successes := 0
		for _, result := range []struct {
			operation string
			err       error
		}{first, second} {
			if result.err == nil {
				successes++
				continue
			}
			if !errors.Is(result.err, domain.ErrConflict) && !errors.Is(result.err, domain.ErrNotFound) {
				t.Fatalf("iteration %d %s error = %v", iteration, result.operation, result.err)
			}
		}
		if successes != 1 {
			t.Fatalf("iteration %d publish/delete successes=%d results=%#v %#v", iteration, successes, first, second)
		}
	}
}
