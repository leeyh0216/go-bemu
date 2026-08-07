package application

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestMetadataPatchAndAdditiveSchemaUseExplicitPorts(t *testing.T) {
	if CapabilityRESTMetadataPatchV1 != "CAP-REST-METADATA-PATCH-V1" {
		t.Fatalf("metadata patch capability ID drifted: %s", CapabilityRESTMetadataPatchV1)
	}
	ctx := context.Background()
	warehouse := &fakeWarehouse{}
	repository := memory.NewCatalogRepository()
	now := time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC)
	service := NewCatalogService(repository, warehouse, fixedClock{now: now})
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "EU"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{
			{Name: "id", Type: "INT64"},
			{Name: "payload", Type: "RECORD", Fields: []domain.Field{{Name: "name", Type: "STRING"}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	defaultExpiration := int64(86_400_000)
	dataset, err := service.UpdateDataset(ctx, "test-project", "analytics", DatasetPatch{
		Description:              PatchValue[string]{Set: true, Value: "updated"},
		Labels:                   PatchValue[map[string]string]{Set: true, Value: map[string]string{"tier": "gold"}},
		DefaultTableExpirationMs: PatchValue[*int64]{Set: true, Value: &defaultExpiration},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dataset.Description != "updated" || dataset.Labels["tier"] != "gold" || *dataset.DefaultTableExpirationMs != defaultExpiration {
		t.Fatalf("unexpected dataset update: %#v", dataset)
	}

	expires := now.Add(48 * time.Hour)
	proposed := []domain.Field{
		{Name: "id", Type: "INT64"},
		{Name: "payload", Type: "RECORD", Fields: []domain.Field{
			{Name: "name", Type: "STRING"}, {Name: "score", Type: "FLOAT64"},
		}},
		{Name: "tags", Type: "STRING", Mode: "REPEATED"},
	}
	table, err := service.UpdateTable(ctx, "test-project", "analytics", "events", TablePatch{
		Description:    PatchValue[string]{Set: true, Value: "updated table"},
		ExpirationTime: PatchValue[*time.Time]{Set: true, Value: &expires},
		Schema:         PatchValue[[]domain.Field]{Set: true, Value: proposed},
	})
	if err != nil {
		t.Fatal(err)
	}
	if table.Description != "updated table" || table.ExpirationTime == nil || len(table.Schema) != 3 || len(warehouse.additions) != 2 {
		t.Fatalf("unexpected table update: table=%#v additions=%#v", table, warehouse.additions)
	}
	if strings.Join(warehouse.additions[0].Path, ".") != "payload.score" || strings.Join(warehouse.additions[1].Path, ".") != "tags" {
		t.Fatalf("unexpected schema paths: %#v", warehouse.additions)
	}
}

func TestIllegalSchemaPatchDoesNotReachWarehouseOrCatalog(t *testing.T) {
	ctx := context.Background()
	warehouse := &fakeWarehouse{}
	repository := memory.NewCatalogRepository()
	service := NewCatalogService(repository, warehouse, fixedClock{now: time.Now()})
	_, _ = service.CreateProject(ctx, domain.Project{ID: "test-project"})
	_, _ = service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"})
	_, _ = service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	})
	_, err := service.UpdateTable(ctx, "test-project", "analytics", "events", TablePatch{
		Schema: PatchValue[[]domain.Field]{Set: true, Value: []domain.Field{{Name: "id", Type: "STRING"}}},
	})
	if err == nil || !strings.Contains(err.Error(), domain.CapabilitySchemaAdditiveV1) {
		t.Fatalf("expected capability error, got %v", err)
	}
	if len(warehouse.additions) != 0 {
		t.Fatalf("illegal change reached warehouse: %#v", warehouse.additions)
	}
	table, _ := repository.GetTable(ctx, "test-project", "analytics", "events")
	if table.Schema[0].Type != "INT64" {
		t.Fatalf("illegal change reached catalog: %#v", table.Schema)
	}
}
