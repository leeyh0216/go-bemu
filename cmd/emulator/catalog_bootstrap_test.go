package main

import (
	"errors"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestBootstrapCatalogCreatesConfiguredResourcesAndImplicitDefault(t *testing.T) {
	cfg := config.Defaults()
	cfg.Defaults.ProjectID = "default-project"
	cfg.Query.Materialization.ProjectID = "analytics-project"
	cfg.Query.Materialization.DatasetID = "events"
	cfg.Bootstrap.Projects = []config.BootstrapProjectConfig{
		{
			ID: "analytics-project", FriendlyName: "Analytics",
			Datasets: []config.BootstrapDatasetConfig{
				{ID: "events", Labels: map[string]string{"tier": "gold"}},
				{ID: "archive", Location: "EU"},
			},
		},
	}
	catalog := composeCatalogService(
		cfg, memory.NewCatalogRepository(), &tableDataCompositionWarehouse{}, nil,
		&tableDataCompositionWarehouse{}, shutdownClock{},
	)

	for attempt := 0; attempt < 2; attempt++ {
		if err := bootstrapCatalog(t.Context(), cfg, catalog); err != nil {
			t.Fatalf("bootstrap attempt %d: %v", attempt, err)
		}
	}
	if _, err := catalog.GetProject(t.Context(), "default-project"); err != nil {
		t.Fatal(err)
	}
	project, err := catalog.GetProject(t.Context(), "analytics-project")
	if err != nil || project.FriendlyName != "Analytics" {
		t.Fatalf("project = %#v, %v", project, err)
	}
	events, err := catalog.GetDataset(t.Context(), "analytics-project", "events")
	if err != nil || events.Location != "US" || events.Labels["tier"] != "gold" {
		t.Fatalf("events dataset = %#v, %v", events, err)
	}
	archive, err := catalog.GetDataset(t.Context(), "analytics-project", "archive")
	if err != nil || archive.Location != "EU" {
		t.Fatalf("archive dataset = %#v, %v", archive, err)
	}
	if err := application.ValidateQueryMaterializationTarget(
		t.Context(), catalog,
		cfg.Query.Materialization.ProjectID, cfg.Query.Materialization.DatasetID,
	); err != nil {
		t.Fatalf("bootstrapped materialization target = %v", err)
	}
}

func TestBootstrapCatalogRejectsExistingMetadataDrift(t *testing.T) {
	cfg := config.Defaults()
	cfg.Bootstrap.Projects = []config.BootstrapProjectConfig{{
		ID: "existing-project", FriendlyName: "Configured",
	}}
	catalog := composeCatalogService(
		cfg, memory.NewCatalogRepository(), &tableDataCompositionWarehouse{}, nil,
		&tableDataCompositionWarehouse{}, shutdownClock{},
	)
	if _, err := catalog.CreateProject(t.Context(), domain.Project{
		ID: "existing-project", FriendlyName: "Different",
	}); err != nil {
		t.Fatal(err)
	}
	err := bootstrapCatalog(t.Context(), cfg, catalog)
	if !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("bootstrap drift error = %v", err)
	}
	if _, err := catalog.GetProject(t.Context(), cfg.Defaults.ProjectID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("preflight created default project before drift failure: %v", err)
	}
}
