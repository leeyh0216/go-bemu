package application

// CatalogService coordinates metadata with the replaceable Warehouse port.
// Official resource semantics:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
//
// Physical storage is created before catalog publication and compensated if
// publication fails. Deletes remove physical state before metadata so a
// successful response never leaves queryable storage behind an absent resource.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type CatalogService struct {
	catalog   ports.CatalogRepository
	warehouse ports.WarehouseAdmin
	clock     ports.Clock
}

func NewCatalogService(catalog ports.CatalogRepository, warehouse ports.WarehouseAdmin, clock ports.Clock) *CatalogService {
	return &CatalogService{catalog: catalog, warehouse: warehouse, clock: clock}
}

func (s *CatalogService) CreateProject(ctx context.Context, project domain.Project) (domain.Project, error) {
	if err := project.Validate(); err != nil {
		return domain.Project{}, err
	}
	now := s.clock.Now()
	project.CreatedAt = now
	project.UpdatedAt = now
	if err := s.catalog.CreateProject(ctx, project); err != nil {
		return domain.Project{}, err
	}
	return project, nil
}

func (s *CatalogService) GetProject(ctx context.Context, id string) (domain.Project, error) {
	return s.catalog.GetProject(ctx, id)
}

func (s *CatalogService) ListProjects(ctx context.Context) ([]domain.Project, error) {
	return s.catalog.ListProjects(ctx)
}

func (s *CatalogService) DeleteProject(ctx context.Context, id string) error {
	datasets, err := s.catalog.ListDatasets(ctx, id)
	if err != nil {
		return err
	}
	for _, dataset := range datasets {
		if err := s.warehouse.DropDataset(ctx, id, dataset.ID); err != nil {
			return fmt.Errorf("drop dataset storage: %w", err)
		}
	}
	return s.catalog.DeleteProject(ctx, id)
}

func (s *CatalogService) CreateDataset(ctx context.Context, dataset domain.Dataset) (domain.Dataset, error) {
	if err := dataset.Validate(); err != nil {
		return domain.Dataset{}, err
	}
	if _, err := s.catalog.GetProject(ctx, dataset.ProjectID); err != nil {
		return domain.Dataset{}, err
	}
	if _, err := s.catalog.GetDataset(ctx, dataset.ProjectID, dataset.ID); err == nil {
		return domain.Dataset{}, fmt.Errorf("%w: dataset %s/%s", domain.ErrConflict, dataset.ProjectID, dataset.ID)
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.Dataset{}, err
	}
	if dataset.Location == "" {
		dataset.Location = "US"
	}
	now := s.clock.Now()
	dataset.CreatedAt = now
	dataset.UpdatedAt = now
	if err := s.warehouse.CreateDataset(ctx, dataset.ProjectID, dataset.ID); err != nil {
		return domain.Dataset{}, err
	}
	if err := s.catalog.CreateDataset(ctx, dataset); err != nil {
		_ = s.warehouse.DropDataset(ctx, dataset.ProjectID, dataset.ID)
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func (s *CatalogService) GetDataset(ctx context.Context, projectID, datasetID string) (domain.Dataset, error) {
	return s.catalog.GetDataset(ctx, projectID, datasetID)
}

func (s *CatalogService) ListDatasets(ctx context.Context, projectID string) ([]domain.Dataset, error) {
	return s.catalog.ListDatasets(ctx, projectID)
}

func (s *CatalogService) DeleteDataset(ctx context.Context, projectID, datasetID string, deleteContents bool) error {
	if _, err := s.catalog.GetDataset(ctx, projectID, datasetID); err != nil {
		return err
	}
	tables, err := s.catalog.ListTables(ctx, projectID, datasetID)
	if err != nil {
		return err
	}
	if len(tables) != 0 && !deleteContents {
		return fmt.Errorf("%w: dataset %s/%s is not empty; set deleteContents=true", domain.ErrConflict, projectID, datasetID)
	}
	if err := s.warehouse.DropDataset(ctx, projectID, datasetID); err != nil {
		return err
	}
	return s.catalog.DeleteDataset(ctx, projectID, datasetID)
}

func (s *CatalogService) CreateTable(ctx context.Context, table domain.Table) (domain.Table, error) {
	if err := table.Validate(); err != nil {
		return domain.Table{}, err
	}
	dataset, err := s.catalog.GetDataset(ctx, table.ProjectID, table.DatasetID)
	if err != nil {
		return domain.Table{}, err
	}
	if _, err := s.catalog.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID); err == nil {
		return domain.Table{}, fmt.Errorf("%w: table %s/%s/%s", domain.ErrConflict, table.ProjectID, table.DatasetID, table.ID)
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.Table{}, err
	}
	if table.Type == "" {
		table.Type = "TABLE"
	}
	now := s.clock.Now()
	table.Location = dataset.Location
	if table.ExpirationTime == nil && dataset.DefaultTableExpirationMs != nil {
		expiration := now.Add(time.Duration(*dataset.DefaultTableExpirationMs) * time.Millisecond)
		table.ExpirationTime = &expiration
	}
	if table.TimePartitioning != nil && table.TimePartitioning.ExpirationMs == 0 && dataset.DefaultPartitionExpirationMs != nil {
		table.TimePartitioning.ExpirationMs = *dataset.DefaultPartitionExpirationMs
	}
	table.CreatedAt = now
	table.UpdatedAt = now
	if err := s.warehouse.CreateTable(ctx, table); err != nil {
		return domain.Table{}, err
	}
	if err := s.catalog.CreateTable(ctx, table); err != nil {
		_ = s.warehouse.DropTable(ctx, table.ProjectID, table.DatasetID, table.ID)
		return domain.Table{}, err
	}
	return table, nil
}

func (s *CatalogService) GetTable(ctx context.Context, projectID, datasetID, tableID string) (domain.Table, error) {
	return s.catalog.GetTable(ctx, projectID, datasetID, tableID)
}

func (s *CatalogService) ListTables(ctx context.Context, projectID, datasetID string) ([]domain.Table, error) {
	return s.catalog.ListTables(ctx, projectID, datasetID)
}

func (s *CatalogService) DeleteTable(ctx context.Context, projectID, datasetID, tableID string) error {
	if _, err := s.catalog.GetTable(ctx, projectID, datasetID, tableID); err != nil {
		return err
	}
	if err := s.warehouse.DropTable(ctx, projectID, datasetID, tableID); err != nil && !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	return s.catalog.DeleteTable(ctx, projectID, datasetID, tableID)
}
