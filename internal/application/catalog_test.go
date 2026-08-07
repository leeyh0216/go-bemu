package application

import (
	"context"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fakeWarehouse struct {
	datasets  []string
	tables    []string
	dropped   []string
	additions []domain.SchemaAddition
}

var _ ports.Warehouse = (*fakeWarehouse)(nil)

func (*fakeWarehouse) Ping(context.Context) error { return nil }
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
func (w *fakeWarehouse) ApplySchemaAdditions(_ context.Context, _ domain.Table, additions []domain.SchemaAddition) error {
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
