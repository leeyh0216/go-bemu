package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestPublishLoadedTableSchemaRequiresExpectedAdditiveRelaxingTransition(t *testing.T) {
	ctx := context.Background()
	repository := memory.NewCatalogRepository()
	service := NewCatalogService(repository, &fakeWarehouse{}, fixedClock{now: time.Unix(2, 0)})
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	before := []domain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}}
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events", Schema: before,
	}); err != nil {
		t.Fatal(err)
	}
	reference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	after := []domain.Field{
		{Name: "id", Type: "INT64", Mode: "NULLABLE"},
		{Name: "score", Type: "FLOAT64"},
	}
	stale := []domain.Field{{Name: "other", Type: "STRING"}}
	if err := service.PublishLoadedTableSchema(ctx, reference, stale, after); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("stale publication error = %v", err)
	}
	if err := service.PublishLoadedTableSchema(ctx, reference, before, after); err != nil {
		t.Fatal(err)
	}
	updated, err := service.GetTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
	if err != nil {
		t.Fatal(err)
	}
	if len(updated.Schema) != 2 || updated.Schema[0].Mode != "NULLABLE" || updated.Schema[1].Name != "score" {
		t.Fatalf("updated table = %#v", updated)
	}
}
