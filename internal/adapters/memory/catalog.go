package memory

// The in-memory adapter implements the catalog outbound port for tests and
// ephemeral runs. Repository values are cloned at every boundary so callers
// cannot mutate persisted labels, nested schemas, partition metadata, or
// clustering fields without an explicit repository operation.
// Official resource model: https://cloud.google.com/bigquery/docs/reference/rest/v2

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type CatalogRepository struct {
	mu       sync.RWMutex
	projects map[string]domain.Project
	datasets map[string]domain.Dataset
	tables   map[string]domain.Table
}

var _ ports.CatalogRepository = (*CatalogRepository)(nil)

func NewCatalogRepository() *CatalogRepository {
	return &CatalogRepository{
		projects: make(map[string]domain.Project),
		datasets: make(map[string]domain.Dataset),
		tables:   make(map[string]domain.Table),
	}
}

func datasetKey(projectID, datasetID string) string { return projectID + "/" + datasetID }
func tableKey(projectID, datasetID, tableID string) string {
	return datasetKey(projectID, datasetID) + "/" + tableID
}

func (r *CatalogRepository) CreateProject(_ context.Context, p domain.Project) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.projects[p.ID]; ok {
		return fmt.Errorf("%w: project %s", domain.ErrConflict, p.ID)
	}
	r.projects[p.ID] = cloneProject(p)
	return nil
}

func (r *CatalogRepository) GetProject(_ context.Context, id string) (domain.Project, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.projects[id]
	if !ok {
		return domain.Project{}, fmt.Errorf("%w: project %s", domain.ErrNotFound, id)
	}
	return cloneProject(p), nil
}

func (r *CatalogRepository) ListProjects(_ context.Context) ([]domain.Project, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.Project, 0, len(r.projects))
	for _, p := range r.projects {
		out = append(out, cloneProject(p))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *CatalogRepository) DeleteProject(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.projects[id]; !ok {
		return fmt.Errorf("%w: project %s", domain.ErrNotFound, id)
	}
	for key, dataset := range r.datasets {
		if dataset.ProjectID == id {
			delete(r.datasets, key)
		}
	}
	for key, table := range r.tables {
		if table.ProjectID == id {
			delete(r.tables, key)
		}
	}
	delete(r.projects, id)
	return nil
}

func (r *CatalogRepository) CreateDataset(_ context.Context, d domain.Dataset) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.projects[d.ProjectID]; !ok {
		return fmt.Errorf("%w: project %s", domain.ErrNotFound, d.ProjectID)
	}
	key := datasetKey(d.ProjectID, d.ID)
	if _, ok := r.datasets[key]; ok {
		return fmt.Errorf("%w: dataset %s", domain.ErrConflict, key)
	}
	r.datasets[key] = cloneDataset(d)
	return nil
}

func (r *CatalogRepository) UpdateDataset(_ context.Context, dataset domain.Dataset) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := datasetKey(dataset.ProjectID, dataset.ID)
	if _, ok := r.datasets[key]; !ok {
		return fmt.Errorf("%w: dataset %s", domain.ErrNotFound, key)
	}
	r.datasets[key] = cloneDataset(dataset)
	return nil
}

func (r *CatalogRepository) GetDataset(_ context.Context, projectID, datasetID string) (domain.Dataset, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.datasets[datasetKey(projectID, datasetID)]
	if !ok {
		return domain.Dataset{}, fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, projectID, datasetID)
	}
	return cloneDataset(d), nil
}

func (r *CatalogRepository) ListDatasets(_ context.Context, projectID string) ([]domain.Dataset, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.projects[projectID]; !ok {
		return nil, fmt.Errorf("%w: project %s", domain.ErrNotFound, projectID)
	}
	out := make([]domain.Dataset, 0)
	for _, d := range r.datasets {
		if d.ProjectID == projectID {
			out = append(out, cloneDataset(d))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *CatalogRepository) DeleteDataset(_ context.Context, projectID, datasetID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := datasetKey(projectID, datasetID)
	if _, ok := r.datasets[key]; !ok {
		return fmt.Errorf("%w: dataset %s", domain.ErrNotFound, key)
	}
	for tableKey, table := range r.tables {
		if table.ProjectID == projectID && table.DatasetID == datasetID {
			delete(r.tables, tableKey)
		}
	}
	delete(r.datasets, key)
	return nil
}

func (r *CatalogRepository) CreateTable(_ context.Context, t domain.Table) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.datasets[datasetKey(t.ProjectID, t.DatasetID)]; !ok {
		return fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, t.ProjectID, t.DatasetID)
	}
	key := tableKey(t.ProjectID, t.DatasetID, t.ID)
	if _, ok := r.tables[key]; ok {
		return fmt.Errorf("%w: table %s", domain.ErrConflict, key)
	}
	r.tables[key] = cloneTable(t)
	return nil
}

func (r *CatalogRepository) UpdateTable(_ context.Context, table domain.Table) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := tableKey(table.ProjectID, table.DatasetID, table.ID)
	if _, ok := r.tables[key]; !ok {
		return fmt.Errorf("%w: table %s", domain.ErrNotFound, key)
	}
	r.tables[key] = cloneTable(table)
	return nil
}

func (r *CatalogRepository) GetTable(_ context.Context, projectID, datasetID, tableID string) (domain.Table, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tables[tableKey(projectID, datasetID, tableID)]
	if !ok {
		return domain.Table{}, fmt.Errorf("%w: table %s/%s/%s", domain.ErrNotFound, projectID, datasetID, tableID)
	}
	return cloneTable(t), nil
}

func (r *CatalogRepository) ListTables(_ context.Context, projectID, datasetID string) ([]domain.Table, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.datasets[datasetKey(projectID, datasetID)]; !ok {
		return nil, fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, projectID, datasetID)
	}
	out := make([]domain.Table, 0)
	for _, t := range r.tables {
		if t.ProjectID == projectID && t.DatasetID == datasetID {
			out = append(out, cloneTable(t))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *CatalogRepository) DeleteTable(_ context.Context, projectID, datasetID, tableID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := tableKey(projectID, datasetID, tableID)
	if _, ok := r.tables[key]; !ok {
		return fmt.Errorf("%w: table %s", domain.ErrNotFound, key)
	}
	delete(r.tables, key)
	return nil
}

func cloneProject(project domain.Project) domain.Project { return project }

func cloneDataset(dataset domain.Dataset) domain.Dataset {
	clone := dataset
	clone.Labels = cloneStringMap(dataset.Labels)
	clone.DefaultTableExpirationMs = cloneInt64Pointer(dataset.DefaultTableExpirationMs)
	clone.DefaultPartitionExpirationMs = cloneInt64Pointer(dataset.DefaultPartitionExpirationMs)
	return clone
}

func cloneTable(table domain.Table) domain.Table {
	clone := table
	clone.Schema = cloneFields(table.Schema)
	clone.ClusteringFields = append([]string(nil), table.ClusteringFields...)
	clone.Labels = cloneStringMap(table.Labels)
	if table.ExpirationTime != nil {
		expiration := *table.ExpirationTime
		clone.ExpirationTime = &expiration
	}
	if table.TimePartitioning != nil {
		partitioning := *table.TimePartitioning
		clone.TimePartitioning = &partitioning
	}
	if table.RangePartitioning != nil {
		partitioning := *table.RangePartitioning
		clone.RangePartitioning = &partitioning
	}
	return clone
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneFields(fields []domain.Field) []domain.Field {
	clone := make([]domain.Field, len(fields))
	for index, field := range fields {
		clone[index] = field
		clone[index].Precision = cloneInt64Pointer(field.Precision)
		clone[index].Scale = cloneInt64Pointer(field.Scale)
		clone[index].Fields = cloneFields(field.Fields)
	}
	return clone
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}
