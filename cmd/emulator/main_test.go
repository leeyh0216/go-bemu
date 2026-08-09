package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	stateadapter "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestRunPrintEffectiveConfigDoesNotStartListeners(t *testing.T) {
	var output bytes.Buffer
	err := run(t.Context(), []string{
		"--set", "defaults.projectId=test-project",
		"--set", "server.http.address=invalid-listener-address",
		"--print-effective-config",
	}, &output)
	if err == nil || !strings.Contains(err.Error(), "server.http.address") {
		// Effective output still validates before it is printed; an invalid
		// address must fail without attempting any network side effect.
		if err == nil {
			t.Fatal("expected invalid effective configuration")
		}
		t.Fatalf("unexpected error: %v", err)
	}

	output.Reset()
	if err := run(t.Context(), []string{
		"--set", "defaults.projectId=test-project",
		"--set", "database.dsn=:memory:",
		"--print-effective-config",
	}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "projectId: test-project") || !strings.Contains(output.String(), "dsn: ':memory:'") {
		t.Fatalf("unexpected effective configuration:\n%s", output.String())
	}
}

func TestConfigureLoggerAppliesFormatAndLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := configureLogger(config.LoggingConfig{Level: "warn", Format: "json"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("filtered")
	logger.Warn("retained", "model_version", config.APIVersion)
	encoded := output.String()
	if strings.Contains(encoded, "filtered") || !strings.Contains(encoded, `"msg":"retained"`) || !strings.Contains(encoded, config.APIVersion) {
		t.Fatalf("unexpected log output: %s", encoded)
	}
	if _, err := configureLogger(config.LoggingConfig{Level: "trace", Format: "json"}, &output); err == nil {
		t.Fatal("expected unsupported level error")
	}
	if _, err := configureLogger(config.LoggingConfig{Level: "info", Format: "xml"}, &output); err == nil {
		t.Fatal("expected unsupported format error")
	}
}

func TestPrepareDirectoryCreatesConfiguredPath(t *testing.T) {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	directory := filepath.Join(t.TempDir(), "nested", "bqemu")
	if err := prepareDirectory(context.Background(), directory); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("configured path is not a directory: %s", directory)
	}
}

func TestEnsureDefaultProjectIsIdempotentAcrossStateRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bqemu-state.sqlite")
	warehouse := &tableDataCompositionWarehouse{}
	for iteration := 0; iteration < 2; iteration++ {
		store, err := stateadapter.Open(t.Context(), stateadapter.DefaultConfig(path))
		if err != nil {
			t.Fatal(err)
		}
		service := composeCatalogService(config.Defaults(), store, warehouse, shutdownClock{})
		if err := ensureDefaultProject(t.Context(), service, "local-project"); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		project, err := service.GetProject(t.Context(), "local-project")
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		if project.FriendlyName != "BQEMU default project" {
			_ = store.Close()
			t.Fatalf("default project = %#v", project)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBootstrapCatalogCreatesMultipleResourcesIdempotently(t *testing.T) {
	store, err := stateadapter.Open(t.Context(), stateadapter.DefaultConfig(filepath.Join(t.TempDir(), "state.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := composeCatalogService(config.Defaults(), store, &tableDataCompositionWarehouse{}, shutdownClock{})
	bootstrap := config.BootstrapConfig{Projects: []config.BootstrapProject{{ID: "one", Datasets: []config.BootstrapDataset{{ID: "us_data", Location: "US"}}}, {ID: "two", Datasets: []config.BootstrapDataset{{ID: "eu_data", Location: "EU", Description: "bootstrap"}}}}}
	for range 2 {
		if err := bootstrapCatalog(t.Context(), service, bootstrap); err != nil {
			t.Fatal(err)
		}
	}
	for _, reference := range []struct{ project, dataset, location string }{{"one", "us_data", "US"}, {"two", "eu_data", "EU"}} {
		dataset, err := service.GetDataset(t.Context(), reference.project, reference.dataset)
		if err != nil || dataset.Location != reference.location {
			t.Fatalf("dataset=%#v err=%v", dataset, err)
		}
	}
}

func TestBootstrapCatalogIsIdempotentAcrossStateRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	bootstrap := config.BootstrapConfig{Projects: []config.BootstrapProject{{ID: "one", Datasets: []config.BootstrapDataset{{ID: "data", Location: "US"}}}}}
	for range 2 {
		store, err := stateadapter.Open(t.Context(), stateadapter.DefaultConfig(path))
		if err != nil {
			t.Fatal(err)
		}
		service := composeCatalogService(config.Defaults(), store, &tableDataCompositionWarehouse{}, shutdownClock{})
		if err := bootstrapCatalog(t.Context(), service, bootstrap); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		if _, err := service.GetDataset(t.Context(), "one", "data"); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMaterializationTargetBootstrapSurvivesStateRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	bootstrap := config.BootstrapConfig{Projects: []config.BootstrapProject{{ID: "results-project", Datasets: []config.BootstrapDataset{{ID: "managed_results", Location: "US"}}}}}
	target := config.MaterializationConfig{ProjectID: "results-project", DatasetID: "managed_results", Expiration: config.Duration(time.Hour)}
	for range 2 {
		store, err := stateadapter.Open(t.Context(), stateadapter.DefaultConfig(path))
		if err != nil {
			t.Fatal(err)
		}
		service := composeCatalogService(config.Defaults(), store, &tableDataCompositionWarehouse{}, shutdownClock{})
		if err := bootstrapCatalog(t.Context(), service, bootstrap); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		if err := verifyMaterializationTarget(t.Context(), service, target); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMaterializationTargetRequiresExistingDataset(t *testing.T) {
	store, err := stateadapter.Open(t.Context(), stateadapter.DefaultConfig(filepath.Join(t.TempDir(), "state.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := composeCatalogService(config.Defaults(), store, &tableDataCompositionWarehouse{}, shutdownClock{})
	err = verifyMaterializationTarget(t.Context(), service, config.MaterializationConfig{ProjectID: "results-project", DatasetID: "missing", Expiration: config.Duration(time.Hour)})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("error=%v, want not found", err)
	}
}

type mutableEmulatorClock struct{ now time.Time }

func (clock *mutableEmulatorClock) Now() time.Time { return clock.now }

func TestExpiredManagedMaterializationCleansAfterSQLiteAndDuckDBRestart(t *testing.T) {
	ctx := t.Context()
	statePath := filepath.Join(t.TempDir(), "state.sqlite")
	warehousePath := filepath.Join(t.TempDir(), "warehouse.duckdb")
	now := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	expires := now.Add(time.Hour)
	clock := &mutableEmulatorClock{now: now}

	state, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(statePath))
	if err != nil {
		t.Fatal(err)
	}
	warehouse, err := duckdb.New(warehousePath)
	if err != nil {
		_ = state.Close()
		t.Fatal(err)
	}
	service := composeCatalogService(config.Defaults(), state, warehouse, clock)
	if _, err := service.CreateProject(ctx, domain.Project{ID: "results-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "results-project", ID: "managed_results", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "results-project", DatasetID: "managed_results", ID: "managed_query_result",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}}, ExpirationTime: &expires,
	}); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.Close(); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	clock.now = expires
	state, err = stateadapter.Open(ctx, stateadapter.DefaultConfig(statePath))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	warehouse, err = duckdb.New(warehousePath)
	if err != nil {
		t.Fatal(err)
	}
	defer warehouse.Close()
	service = composeCatalogService(config.Defaults(), state, warehouse, clock)
	if err := service.RecoverCatalogState(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetTable(ctx, "results-project", "managed_results", "managed_query_result"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired managed result error = %v, want not found", err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "results-project", DatasetID: "managed_results", ID: "managed_query_result",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}); err != nil {
		t.Fatalf("recreate after durable expiration cleanup: %v", err)
	}
}

func TestBootstrapCatalogRejectsExistingLocationConflict(t *testing.T) {
	store, err := stateadapter.Open(t.Context(), stateadapter.DefaultConfig(filepath.Join(t.TempDir(), "state.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := composeCatalogService(config.Defaults(), store, &tableDataCompositionWarehouse{}, shutdownClock{})
	if err := bootstrapCatalog(t.Context(), service, config.BootstrapConfig{Projects: []config.BootstrapProject{{ID: "one", Datasets: []config.BootstrapDataset{{ID: "data", Location: "US"}}}}}); err != nil {
		t.Fatal(err)
	}
	err = bootstrapCatalog(t.Context(), service, config.BootstrapConfig{Projects: []config.BootstrapProject{{ID: "one", Datasets: []config.BootstrapDataset{{ID: "data", Location: "EU"}}}}})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("error=%v", err)
	}
}

func TestBootstrapCatalogRejectsExistingMetadataConflict(t *testing.T) {
	store, err := stateadapter.Open(t.Context(), stateadapter.DefaultConfig(filepath.Join(t.TempDir(), "state.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := composeCatalogService(config.Defaults(), store, &tableDataCompositionWarehouse{}, shutdownClock{})
	base := config.BootstrapConfig{Projects: []config.BootstrapProject{{ID: "one", Datasets: []config.BootstrapDataset{{ID: "data", Location: "US", Description: "one", Labels: map[string]string{"team": "one"}}}}}}
	if err := bootstrapCatalog(t.Context(), service, base); err != nil {
		t.Fatal(err)
	}
	conflict := base
	conflict.Projects[0].Datasets[0].Labels = map[string]string{"team": "two"}
	if err := bootstrapCatalog(t.Context(), service, conflict); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("error=%v", err)
	}
}

type healthCheckerFunc func(context.Context) error

func (check healthCheckerFunc) Ping(ctx context.Context) error { return check(ctx) }

func TestCompositeReadinessRequiresStateAndWarehouse(t *testing.T) {
	stateErr := errors.New("state unavailable")
	warehouseCalled := false
	checks := compositeHealthChecker{
		healthCheckerFunc(func(context.Context) error { return stateErr }),
		healthCheckerFunc(func(context.Context) error { warehouseCalled = true; return nil }),
	}
	if err := checks.Ping(t.Context()); !errors.Is(err, stateErr) {
		t.Fatalf("readiness error = %v, want state failure", err)
	}
	if warehouseCalled {
		t.Fatal("warehouse readiness ran after state failure")
	}
	checks[0] = healthCheckerFunc(func(context.Context) error { return nil })
	if err := checks.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !warehouseCalled {
		t.Fatal("warehouse readiness was not checked")
	}
}

type tableDataCompositionWarehouse struct {
	request ports.TableDataReadRequest
}

func (*tableDataCompositionWarehouse) CreateDataset(context.Context, string, string) error {
	return nil
}
func (*tableDataCompositionWarehouse) DropDataset(context.Context, string, string) error {
	return nil
}
func (*tableDataCompositionWarehouse) CreateTable(context.Context, domain.Table) error {
	return nil
}
func (*tableDataCompositionWarehouse) ApplySchemaAdditions(context.Context, domain.Table, []domain.SchemaAddition) error {
	return nil
}
func (*tableDataCompositionWarehouse) DropTable(context.Context, string, string, string) error {
	return nil
}
func (warehouse *tableDataCompositionWarehouse) ListTableData(_ context.Context, request ports.TableDataReadRequest) (ports.TableDataPage, error) {
	warehouse.request = request
	return ports.TableDataPage{Rows: [][]any{{int64(1)}}, TotalRows: 1}, nil
}
func (*tableDataCompositionWarehouse) InsertTableData(context.Context, ports.TableDataWriteRequest) error {
	return nil
}

func TestComposeCatalogServiceAppliesFileTableDataByteLimits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bqemu.yaml")
	contents := `
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
tableData:
  operationTimeout: "2s"
  maxPageRows: 7
  maxResponseBytes: 1234
  maxRowBytes: 5678
`
	if err := os.WriteFile(path, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}
	warehouse := &tableDataCompositionWarehouse{}
	service := composeCatalogService(loaded.Config, memory.NewCatalogRepository(), warehouse, shutdownClock{})
	ctx := t.Context()
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}); err != nil {
		t.Fatal(err)
	}
	page, err := service.ListTableData(ctx, "test-project", "analytics", "events", 0, ports.TableDataMaxResults{Value: 99, Present: true})
	if err != nil {
		t.Fatal(err)
	}
	if warehouse.request.Limit != 7 || warehouse.request.MaxResponseBytes != 1234 || warehouse.request.MaxRowBytes != 5678 {
		t.Fatalf("runtime table data request policy = %#v", warehouse.request)
	}
	if page.MaxResponseBytes != 1234 || page.MaxRowBytes != 5678 {
		t.Fatalf("runtime table data response policy = %#v", page)
	}
}

type shutdownQueryEngine struct {
	started  chan struct{}
	canceled chan struct{}
	release  <-chan struct{}
	active   atomic.Bool
}

func (engine *shutdownQueryEngine) Query(ctx context.Context, _ ports.QueryRequest) (domain.QueryResult, error) {
	engine.active.Store(true)
	close(engine.started)
	<-ctx.Done()
	close(engine.canceled)
	if engine.release != nil {
		<-engine.release
	}
	engine.active.Store(false)
	return domain.QueryResult{}, ctx.Err()
}

type shutdownClock struct{}

func (shutdownClock) Now() time.Time { return time.Unix(1, 0) }

type shutdownIDs struct{}

func (shutdownIDs) NewID() string { return "shutdown-query" }

type shutdownStorageCloser struct {
	engine *shutdownQueryEngine
	order  *atomic.Int64
	want   int64
	called atomic.Bool
}

func (closer *shutdownStorageCloser) Close(context.Context) error {
	if closer.engine.active.Load() {
		return errors.New("storage close raced active query")
	}
	if got := closer.order.Add(1); got != closer.want {
		return fmt.Errorf("storage close order = %d, want %d", got, closer.want)
	}
	closer.called.Store(true)
	return nil
}

func TestShutdownCancelsOpenQueryBeforeStorageClose(t *testing.T) {
	ctx, cancel := shutdownTestContext(t)
	defer cancel()
	engine := &shutdownQueryEngine{started: make(chan struct{}), canceled: make(chan struct{})}
	queries := application.NewQueryService(
		memory.NewJobRepository(), engine, shutdownClock{}, shutdownIDs{},
		application.WithQueryOperationTimeout(time.Minute),
	)
	if _, err := queries.Submit(ctx, application.QueryInput{
		ProjectID: "test-project", JobID: "open-query", SQL: "SELECT 1",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-engine.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var order atomic.Int64
	read := &shutdownStorageCloser{engine: engine, order: &order, want: 1}
	write := &shutdownStorageCloser{engine: engine, order: &order, want: 2}
	queryErr, readErr, writeErr := shutdownQueryAndStorage(ctx, queries, read, write)
	if err := errors.Join(queryErr, readErr, writeErr); err != nil {
		t.Fatal(err)
	}
	select {
	case <-engine.canceled:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !read.called.Load() || !write.called.Load() {
		t.Fatalf("storage closers called: read=%v write=%v", read.called.Load(), write.called.Load())
	}
	if _, err := queries.Submit(ctx, application.QueryInput{
		ProjectID: "test-project", JobID: "after-close", SQL: "SELECT 2",
	}); !errors.Is(err, application.ErrQueryServiceClosed) {
		t.Fatalf("query admitted after shutdown: %v", err)
	}
}

func TestShutdownSkipsStorageWhenOpenQueryExceedsCloseBudget(t *testing.T) {
	ctx, cancel := shutdownTestContext(t)
	defer cancel()
	release := make(chan struct{})
	engine := &shutdownQueryEngine{
		started: make(chan struct{}), canceled: make(chan struct{}), release: release,
	}
	queries := application.NewQueryService(
		memory.NewJobRepository(), engine, shutdownClock{}, shutdownIDs{},
		application.WithQueryOperationTimeout(time.Minute),
	)
	if _, err := queries.Submit(ctx, application.QueryInput{
		ProjectID: "test-project", JobID: "blocked-shutdown-query", SQL: "SELECT 1",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-engine.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var order atomic.Int64
	read := &shutdownStorageCloser{engine: engine, order: &order, want: 1}
	write := &shutdownStorageCloser{engine: engine, order: &order, want: 2}
	expiredCtx, cancelClose := context.WithCancel(ctx)
	cancelClose()
	queryErr, readErr, writeErr := shutdownQueryAndStorage(expiredCtx, queries, read, write)
	if !errors.Is(queryErr, context.Canceled) || readErr != nil || writeErr != nil {
		t.Fatalf("bounded shutdown errors: query=%v read=%v write=%v", queryErr, readErr, writeErr)
	}
	select {
	case <-engine.canceled:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if read.called.Load() || write.called.Load() || order.Load() != 0 {
		t.Fatalf("storage closed while query remained active: read=%v write=%v order=%d", read.called.Load(), write.called.Load(), order.Load())
	}
	close(release)
	if err := queries.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func shutdownTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 10 * time.Second
	if configured := os.Getenv("BQEMU_SHUTDOWN_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("BQEMU_SHUTDOWN_TEST_TIMEOUT must be a positive Go duration: %q", configured)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}
