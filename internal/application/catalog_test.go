package application

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fakeWarehouse struct {
	datasets      []string
	tables        []string
	dropped       []string
	additions     []domain.SchemaAddition
	capabilities  *engine.Capabilities
	schemaPlanner *engine.SchemaPlanner
	plannerErr    error
	plannerCalls  int
}

var _ ports.Warehouse = (*fakeWarehouse)(nil)

func (*fakeWarehouse) Ping(context.Context) error { return nil }

type fakeWarehouseSchemaAdapter struct{ warehouse *fakeWarehouse }

func (adapter fakeWarehouseSchemaAdapter) ValidateSchemaIntent(context.Context, engine.SchemaIntent) error {
	adapter.warehouse.plannerCalls++
	return adapter.warehouse.plannerErr
}

func (w *fakeWarehouse) PlanSchema(ctx context.Context, intent engine.SchemaIntent) (engine.SchemaPlan, error) {
	if w.schemaPlanner == nil {
		capabilities := fakeWarehouseCapabilities()
		if w.capabilities != nil {
			capabilities = *w.capabilities
		}
		planner, err := engine.NewSchemaPlanner(capabilities, fakeWarehouseSchemaAdapter{warehouse: w})
		if err != nil {
			return engine.SchemaPlan{}, err
		}
		w.schemaPlanner = planner
	}
	return w.schemaPlanner.Plan(ctx, intent)
}

func fakeWarehouseCapabilities() engine.Capabilities {
	identity, err := engine.NewIdentity("fake", "1")
	if err != nil {
		panic(err)
	}
	capabilities, err := engine.NewCapabilities(engine.CapabilitiesDescriptor{
		Identity:  identity,
		Decimal:   engine.DecimalCapabilities{Supported: true, MaxPrecision: domain.SupportedDecimalMaxPrecision, MaxScale: domain.SupportedDecimalMaxScale},
		Composite: engine.CompositeCapabilities{MaxStructDepth: 15, MaxListDepth: 15},
		DDL: map[engine.DDLOperation]engine.DDLCapability{
			engine.DDLCreateTable: {Guarantee: engine.DDLGuaranteeAtomicPhysicalStatement},
			engine.DDLAddColumn:   {Guarantee: engine.DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 15},
		},
	})
	if err != nil {
		panic(err)
	}
	return capabilities
}

func (w *fakeWarehouse) ValidateSchema([]domain.Field) error {
	w.plannerCalls++
	return w.plannerErr
}
func (w *fakeWarehouse) CreateDataset(_ context.Context, projectID, datasetID string) error {
	w.datasets = append(w.datasets, projectID+"/"+datasetID)
	return nil
}
func (w *fakeWarehouse) DropDataset(_ context.Context, projectID, datasetID string) error {
	w.dropped = append(w.dropped, projectID+"/"+datasetID)
	return nil
}
func (w *fakeWarehouse) CreateTable(_ context.Context, table domain.Table) error {
	w.tables = append(w.tables, table.ProjectID+"/"+table.DatasetID+"/"+table.ID)
	return nil
}
func (w *fakeWarehouse) CreatePlannedTable(_ context.Context, _ engine.SchemaPlan, table domain.Table) error {
	w.tables = append(w.tables, table.ProjectID+"/"+table.DatasetID+"/"+table.ID)
	return nil
}
func (w *fakeWarehouse) ApplySchemaAdditions(_ context.Context, _ domain.Table, additions []domain.SchemaAddition) error {
	w.additions = append(w.additions, additions...)
	return nil
}
func (w *fakeWarehouse) ApplyPlannedSchemaAdditions(_ context.Context, _ engine.SchemaPlan, _ domain.Table, additions []domain.SchemaAddition) error {
	w.additions = append(w.additions, additions...)
	return nil
}

func TestDeleteDatasetRequiresDeleteContentsForNonEmptyDataset(t *testing.T) {
	ctx := context.Background()
	repository := memory.NewCatalogRepository()
	warehouse := &fakeWarehouse{}
	service := NewCatalogService(repository, warehouse, fixedClock{now: time.Now()})
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
	if err := service.DeleteDataset(ctx, "test-project", "analytics", false); err == nil {
		t.Fatal("non-empty dataset deletion without deleteContents must fail")
	}
	if len(warehouse.dropped) != 0 {
		t.Fatalf("warehouse was modified on rejected delete: %v", warehouse.dropped)
	}
	if err := service.DeleteDataset(ctx, "test-project", "analytics", true); err != nil {
		t.Fatal(err)
	}
	if len(warehouse.dropped) != 1 {
		t.Fatalf("expected one physical dataset drop, got %v", warehouse.dropped)
	}
}
func (*fakeWarehouse) DropTable(context.Context, string, string, string) error { return nil }
func (*fakeWarehouse) Query(context.Context, ports.QueryRequest) (domain.QueryResult, error) {
	return domain.QueryResult{}, nil
}

func TestCatalogUseCaseDependsOnWarehousePort(t *testing.T) {
	ctx := context.Background()
	repository := memory.NewCatalogRepository()
	warehouse := &fakeWarehouse{}
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	service := NewCatalogService(repository, warehouse, fixedClock{now: now}, WithDefaultLocation("EU"))

	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	dataset, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"})
	if err != nil {
		t.Fatal(err)
	}
	if dataset.CreatedAt != now || dataset.Location != "EU" || len(warehouse.datasets) != 1 {
		t.Fatalf("unexpected dataset or adapter calls: %#v %#v", dataset, warehouse.datasets)
	}
	_, err = service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(warehouse.tables) != 1 || warehouse.tables[0] != "test-project/analytics/events" {
		t.Fatalf("unexpected table adapter calls: %#v", warehouse.tables)
	}
}

func TestCatalogRejectsReplaceableEngineSchemaBoundsBeforeMutation(t *testing.T) {
	ctx := context.Background()
	repository := memory.NewCatalogRepository()
	descriptor := fakeWarehouseCapabilities().Descriptor()
	descriptor.Decimal.MaxPrecision, descriptor.Decimal.MaxScale = 10, 4
	capabilities, err := engine.NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	warehouse := &fakeWarehouse{capabilities: &capabilities}
	service := NewCatalogService(repository, warehouse, fixedClock{now: time.Now()})
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	precision, scale := int64(11), int64(2)
	_, err = service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "too_wide",
		Schema: []domain.Field{{Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale}},
	})
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.CapabilityEngineSchemaV1) {
		t.Fatalf("engine capability error = %v", err)
	}
	if len(warehouse.tables) != 0 || warehouse.plannerCalls != 0 {
		t.Fatalf("capability rejection crossed planner/storage: tables=%v planner_calls=%d", warehouse.tables, warehouse.plannerCalls)
	}
	if _, getErr := repository.GetTable(ctx, "test-project", "analytics", "too_wide"); !errors.Is(getErr, domain.ErrNotFound) {
		t.Fatalf("rejected table metadata error = %v", getErr)
	}

	warehouse.plannerErr = errors.New("replaceable engine cannot plan this schema")
	_, err = service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "planner_rejected",
		Schema: []domain.Field{{Name: "value", Type: "STRING"}},
	})
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), "cannot plan") {
		t.Fatalf("planner error = %v", err)
	}
	if len(warehouse.tables) != 0 || warehouse.plannerCalls != 1 {
		t.Fatalf("planner rejection crossed storage: tables=%v planner_calls=%d", warehouse.tables, warehouse.plannerCalls)
	}
}

func TestCatalogValidatesReplaceableEngineBoundsBeforeSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	repository := memory.NewCatalogRepository()
	descriptor := fakeWarehouseCapabilities().Descriptor()
	descriptor.Decimal.MaxPrecision, descriptor.Decimal.MaxScale = 10, 4
	capabilities, err := engine.NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	warehouse := &fakeWarehouse{capabilities: &capabilities}
	service := NewCatalogService(repository, warehouse, fixedClock{now: time.Now()})
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	original, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	warehouse.plannerCalls = 0
	precision, scale := int64(11), int64(2)
	_, err = service.UpdateTable(ctx, "test-project", "analytics", "events", TablePatch{
		Schema: PatchValue[[]domain.Field]{Set: true, Value: []domain.Field{
			{Name: "id", Type: "INT64"},
			{Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale},
		}},
	})
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.CapabilityEngineSchemaV1) {
		t.Fatalf("schema evolution capability error = %v", err)
	}
	if len(warehouse.additions) != 0 || warehouse.plannerCalls != 0 {
		t.Fatalf("capability rejection crossed schema mutation: additions=%v planner_calls=%d", warehouse.additions, warehouse.plannerCalls)
	}
	stored, getErr := repository.GetTable(ctx, "test-project", "analytics", "events")
	if getErr != nil {
		t.Fatal(getErr)
	}
	if len(stored.Schema) != len(original.Schema) {
		t.Fatalf("rejected schema was persisted: %#v", stored.Schema)
	}
}
