package application

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

// CatalogBootstrapCatalog is the narrow use-case boundary required to make
// configured catalog resources available before listeners start.
type CatalogBootstrapCatalog interface {
	GetProject(context.Context, string) (domain.Project, error)
	CreateProject(context.Context, domain.Project) (domain.Project, error)
	GetDataset(context.Context, string, string) (domain.Dataset, error)
	CreateDataset(context.Context, domain.Dataset) (domain.Dataset, error)
}

type CatalogBootstrapProjectDescriptor struct {
	Project  domain.Project
	Datasets []domain.Dataset
}

// CatalogBootstrapPlan owns an immutable copy of startup resources.
type CatalogBootstrapPlan struct {
	projects []CatalogBootstrapProjectDescriptor
}

func NewCatalogBootstrapPlan(descriptors []CatalogBootstrapProjectDescriptor) (CatalogBootstrapPlan, error) {
	seenProjects := make(map[string]struct{}, len(descriptors))
	projects := make([]CatalogBootstrapProjectDescriptor, len(descriptors))
	for projectIndex, descriptor := range descriptors {
		project := descriptor.Project
		if err := project.Validate(); err != nil {
			return CatalogBootstrapPlan{}, fmt.Errorf("%w: bootstrap project[%d] is invalid", err, projectIndex)
		}
		if _, duplicate := seenProjects[project.ID]; duplicate {
			return CatalogBootstrapPlan{}, fmt.Errorf("%w: bootstrap project ID is duplicated", domain.ErrInvalid)
		}
		seenProjects[project.ID] = struct{}{}

		seenDatasets := make(map[string]struct{}, len(descriptor.Datasets))
		datasets := make([]domain.Dataset, len(descriptor.Datasets))
		for datasetIndex, configured := range descriptor.Datasets {
			dataset := cloneBootstrapDataset(configured)
			if dataset.ProjectID != project.ID {
				return CatalogBootstrapPlan{}, fmt.Errorf("%w: bootstrap dataset[%d] project does not match its parent", domain.ErrInvalid, datasetIndex)
			}
			if err := dataset.Validate(); err != nil {
				return CatalogBootstrapPlan{}, fmt.Errorf("%w: bootstrap dataset[%d] is invalid", err, datasetIndex)
			}
			if _, duplicate := seenDatasets[dataset.ID]; duplicate {
				return CatalogBootstrapPlan{}, fmt.Errorf("%w: bootstrap dataset ID is duplicated", domain.ErrInvalid)
			}
			seenDatasets[dataset.ID] = struct{}{}
			datasets[datasetIndex] = dataset
		}
		projects[projectIndex] = CatalogBootstrapProjectDescriptor{Project: project, Datasets: datasets}
	}
	return CatalogBootstrapPlan{projects: projects}, nil
}

type CatalogBootstrapper struct {
	catalog CatalogBootstrapCatalog
}

func NewCatalogBootstrapper(catalog CatalogBootstrapCatalog) (*CatalogBootstrapper, error) {
	if catalog == nil || isNilBootstrapCatalog(catalog) {
		return nil, fmt.Errorf("%w: bootstrap catalog is required", domain.ErrPrecondition)
	}
	return &CatalogBootstrapper{catalog: catalog}, nil
}

type bootstrapProjectState struct {
	descriptor CatalogBootstrapProjectDescriptor
	exists     bool
	datasets   map[string]bool
}

// Apply first checks every existing resource for drift, then creates missing
// resources in declaration order. A retry after an interrupted startup is
// therefore idempotent and never overwrites user-managed metadata.
func (bootstrapper *CatalogBootstrapper) Apply(ctx context.Context, plan CatalogBootstrapPlan) error {
	states := make([]bootstrapProjectState, len(plan.projects))
	for index, descriptor := range plan.projects {
		state := bootstrapProjectState{
			descriptor: cloneBootstrapDescriptor(descriptor),
			datasets:   make(map[string]bool, len(descriptor.Datasets)),
		}
		existingProject, err := bootstrapper.catalog.GetProject(ctx, descriptor.Project.ID)
		switch {
		case err == nil:
			state.exists = true
			if !sameBootstrapProject(existingProject, descriptor.Project) {
				return fmt.Errorf("%w: configured project metadata differs from canonical catalog", domain.ErrPrecondition)
			}
		case errors.Is(err, domain.ErrNotFound):
			states[index] = state
			continue
		default:
			return err
		}

		for _, dataset := range descriptor.Datasets {
			existingDataset, err := bootstrapper.catalog.GetDataset(ctx, dataset.ProjectID, dataset.ID)
			switch {
			case err == nil:
				if !sameBootstrapDataset(existingDataset, dataset) {
					return fmt.Errorf("%w: configured dataset metadata differs from canonical catalog", domain.ErrPrecondition)
				}
				state.datasets[dataset.ID] = true
			case errors.Is(err, domain.ErrNotFound):
				state.datasets[dataset.ID] = false
			default:
				return err
			}
		}
		states[index] = state
	}

	for _, state := range states {
		if !state.exists {
			if _, err := bootstrapper.catalog.CreateProject(ctx, state.descriptor.Project); err != nil {
				return err
			}
		}
		for _, dataset := range state.descriptor.Datasets {
			if state.datasets[dataset.ID] {
				continue
			}
			if _, err := bootstrapper.catalog.CreateDataset(ctx, dataset); err != nil {
				return err
			}
		}
	}
	return nil
}

func isNilBootstrapCatalog(catalog CatalogBootstrapCatalog) bool {
	value := reflect.ValueOf(catalog)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func sameBootstrapProject(existing, desired domain.Project) bool {
	return existing.ID == desired.ID && existing.FriendlyName == desired.FriendlyName &&
		existing.Description == desired.Description
}

func sameBootstrapDataset(existing, desired domain.Dataset) bool {
	return existing.ProjectID == desired.ProjectID && existing.ID == desired.ID &&
		existing.FriendlyName == desired.FriendlyName && existing.Description == desired.Description &&
		existing.Location == desired.Location && maps.Equal(existing.Labels, desired.Labels) &&
		sameOptionalInt64(existing.DefaultTableExpirationMs, desired.DefaultTableExpirationMs) &&
		sameOptionalInt64(existing.DefaultPartitionExpirationMs, desired.DefaultPartitionExpirationMs) &&
		!existing.Hidden
}

func sameOptionalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func cloneBootstrapDescriptor(descriptor CatalogBootstrapProjectDescriptor) CatalogBootstrapProjectDescriptor {
	datasets := make([]domain.Dataset, len(descriptor.Datasets))
	for index, dataset := range descriptor.Datasets {
		datasets[index] = cloneBootstrapDataset(dataset)
	}
	return CatalogBootstrapProjectDescriptor{Project: descriptor.Project, Datasets: datasets}
}

func cloneBootstrapDataset(dataset domain.Dataset) domain.Dataset {
	dataset.Labels = maps.Clone(dataset.Labels)
	dataset.DefaultTableExpirationMs = domain.CloneOptionalInt64(dataset.DefaultTableExpirationMs)
	dataset.DefaultPartitionExpirationMs = domain.CloneOptionalInt64(dataset.DefaultPartitionExpirationMs)
	return dataset
}
