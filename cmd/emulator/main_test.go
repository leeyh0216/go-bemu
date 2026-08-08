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

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
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

type tableDataCompositionWarehouse struct {
	request ports.TableDataReadRequest
}

type tableDataSchemaAdapter struct{}

func (tableDataSchemaAdapter) ValidateSchemaIntent(context.Context, engine.SchemaIntent) error {
	return nil
}

func (*tableDataCompositionWarehouse) PlanSchema(ctx context.Context, intent engine.SchemaIntent) (engine.SchemaPlan, error) {
	identity, _ := engine.NewIdentity("table-data-test", "1")
	capabilities, err := engine.NewCapabilities(engine.CapabilitiesDescriptor{
		Identity:  identity,
		Decimal:   engine.DecimalCapabilities{Supported: true, MaxPrecision: domain.SparkDecimalMaxPrecision, MaxScale: domain.SparkDecimalMaxScale},
		Composite: engine.CompositeCapabilities{MaxStructDepth: 15, MaxListDepth: 15},
		DDL: map[engine.DDLOperation]engine.DDLCapability{
			engine.DDLCreateTable: {Guarantee: engine.DDLGuaranteeAtomicPhysicalStatement},
			engine.DDLAddColumn:   {Guarantee: engine.DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 15},
		},
	})
	if err != nil {
		return engine.SchemaPlan{}, err
	}
	planner, err := engine.NewSchemaPlanner(capabilities, tableDataSchemaAdapter{})
	if err != nil {
		return engine.SchemaPlan{}, err
	}
	return planner.Plan(ctx, intent)
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
func (*tableDataCompositionWarehouse) CreatePlannedTable(context.Context, engine.SchemaPlan, domain.Table) error {
	return nil
}
func (*tableDataCompositionWarehouse) ApplySchemaAdditions(context.Context, domain.Table, []domain.SchemaAddition) error {
	return nil
}
func (*tableDataCompositionWarehouse) ApplyPlannedSchemaAdditions(context.Context, engine.SchemaPlan, domain.Table, []domain.SchemaAddition) error {
	return nil
}
func (*tableDataCompositionWarehouse) DropTable(context.Context, string, string, string) error {
	return nil
}
func (warehouse *tableDataCompositionWarehouse) ListTableData(_ context.Context, request ports.TableDataReadRequest) (ports.TableDataPage, error) {
	warehouse.request = request
	return ports.TableDataPage{Rows: [][]any{{int64(1)}}, TotalRows: 1}, nil
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
	service := composeCatalogService(loaded.Config, memory.NewCatalogRepository(), warehouse, warehouse, shutdownClock{})
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
	queries := newMainTestQueryService(
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
	queries := newMainTestQueryService(
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
