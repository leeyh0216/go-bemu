package application

import (
	"context"
	"errors"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestCatalogBootstrapperCreatesMultipleProjectsAndDatasetsIdempotently(t *testing.T) {
	catalog := newBootstrapCatalogFake()
	expiration := int64(3_600_000)
	labels := map[string]string{"environment": "local"}
	plan, err := NewCatalogBootstrapPlan([]CatalogBootstrapProjectDescriptor{
		{
			Project: domain.Project{ID: "primary-project", FriendlyName: "Primary"},
			Datasets: []domain.Dataset{{
				ProjectID: "primary-project", ID: "analytics", Location: "US",
				Labels: labels, DefaultTableExpirationMs: &expiration,
			}},
		},
		{Project: domain.Project{ID: "secondary-project"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	labels["environment"] = "changed"
	expiration = 1
	bootstrapper, err := NewCatalogBootstrapper(catalog)
	if err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := bootstrapper.Apply(context.Background(), plan); err != nil {
			t.Fatalf("Apply() attempt %d error = %v", attempt, err)
		}
	}
	if catalog.projectCreates != 2 || catalog.datasetCreates != 1 {
		t.Fatalf("creates projects=%d datasets=%d", catalog.projectCreates, catalog.datasetCreates)
	}
	stored := catalog.datasets["primary-project/analytics"]
	if stored.Labels["environment"] != "local" || stored.DefaultTableExpirationMs == nil || *stored.DefaultTableExpirationMs != 3_600_000 {
		t.Fatalf("stored dataset mutated through descriptor alias: %#v", stored)
	}
}

func TestCatalogBootstrapperPreflightsAllDriftBeforeCreating(t *testing.T) {
	catalog := newBootstrapCatalogFake()
	catalog.projects["existing-project"] = domain.Project{ID: "existing-project", FriendlyName: "different"}
	plan, err := NewCatalogBootstrapPlan([]CatalogBootstrapProjectDescriptor{
		{Project: domain.Project{ID: "missing-project"}},
		{Project: domain.Project{ID: "existing-project", FriendlyName: "configured"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	bootstrapper, err := NewCatalogBootstrapper(catalog)
	if err != nil {
		t.Fatal(err)
	}
	err = bootstrapper.Apply(context.Background(), plan)
	if !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("Apply() error = %v, want precondition", err)
	}
	if catalog.projectCreates != 0 || catalog.datasetCreates != 0 {
		t.Fatalf("preflight created resources projects=%d datasets=%d", catalog.projectCreates, catalog.datasetCreates)
	}
}

type bootstrapCatalogFake struct {
	projects       map[string]domain.Project
	datasets       map[string]domain.Dataset
	projectCreates int
	datasetCreates int
}

func newBootstrapCatalogFake() *bootstrapCatalogFake {
	return &bootstrapCatalogFake{
		projects: make(map[string]domain.Project), datasets: make(map[string]domain.Dataset),
	}
}

func (catalog *bootstrapCatalogFake) GetProject(_ context.Context, id string) (domain.Project, error) {
	project, found := catalog.projects[id]
	if !found {
		return domain.Project{}, domain.ErrNotFound
	}
	return project, nil
}

func (catalog *bootstrapCatalogFake) CreateProject(_ context.Context, project domain.Project) (domain.Project, error) {
	catalog.projectCreates++
	catalog.projects[project.ID] = project
	return project, nil
}

func (catalog *bootstrapCatalogFake) GetDataset(_ context.Context, projectID, datasetID string) (domain.Dataset, error) {
	dataset, found := catalog.datasets[projectID+"/"+datasetID]
	if !found {
		return domain.Dataset{}, domain.ErrNotFound
	}
	return cloneBootstrapDataset(dataset), nil
}

func (catalog *bootstrapCatalogFake) CreateDataset(_ context.Context, dataset domain.Dataset) (domain.Dataset, error) {
	catalog.datasetCreates++
	catalog.datasets[dataset.ProjectID+"/"+dataset.ID] = cloneBootstrapDataset(dataset)
	return dataset, nil
}
