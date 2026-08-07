package memory

import (
	"context"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestCatalogRepositoryReturnsDetachedMetadata(t *testing.T) {
	ctx := context.Background()
	repository := NewCatalogRepository()
	if err := repository.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if err := repository.CreateDataset(ctx, domain.Dataset{
		ProjectID: "test-project", ID: "analytics", Labels: map[string]string{"owner": "data"},
	}); err != nil {
		t.Fatal(err)
	}
	table := domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema:           []domain.Field{{Name: "payload", Type: "RECORD", Fields: []domain.Field{{Name: "id", Type: "INT64"}}}},
		ClusteringFields: []string{"payload"}, TimePartitioning: &domain.TimePartitioning{Type: "DAY", Field: "event_date"},
	}
	if err := repository.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}

	dataset, _ := repository.GetDataset(ctx, "test-project", "analytics")
	dataset.Labels["owner"] = "mutated"
	loadedTable, _ := repository.GetTable(ctx, "test-project", "analytics", "events")
	loadedTable.Schema[0].Fields[0].Name = "mutated"
	loadedTable.ClusteringFields[0] = "mutated"
	loadedTable.TimePartitioning.Type = "MONTH"

	dataset, _ = repository.GetDataset(ctx, "test-project", "analytics")
	loadedTable, _ = repository.GetTable(ctx, "test-project", "analytics", "events")
	if dataset.Labels["owner"] != "data" || loadedTable.Schema[0].Fields[0].Name != "id" ||
		loadedTable.ClusteringFields[0] != "payload" || loadedTable.TimePartitioning.Type != "DAY" {
		t.Fatalf("stored metadata was mutated through a returned value: %#v %#v", dataset, loadedTable)
	}
}
