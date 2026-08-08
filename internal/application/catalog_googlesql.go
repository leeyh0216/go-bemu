package application

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

var _ ports.GoogleSQLCatalogReader = (*CatalogService)(nil)

// GoogleSQLCatalogSnapshot returns one owned canonical catalog revision for a
// single parser/analyzer admission. It uses the catalog mutation lock so DDL,
// REST mutations, and lazy expiration cannot expose a mixed schema revision.
func (s *CatalogService) GoogleSQLCatalogSnapshot(ctx context.Context) (ports.GoogleSQLCatalogSnapshot, error) {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return ports.GoogleSQLCatalogSnapshot{}, err
	}
	defer s.resourceMutationMu.Unlock()

	projects, err := s.catalog.ListProjects(ctx)
	if err != nil {
		return ports.GoogleSQLCatalogSnapshot{}, err
	}
	snapshot := ports.GoogleSQLCatalogSnapshot{
		Projects: make([]ports.GoogleSQLProjectSnapshot, len(projects)),
	}
	for projectIndex, project := range projects {
		if err := ctx.Err(); err != nil {
			return ports.GoogleSQLCatalogSnapshot{}, err
		}
		datasets, err := s.catalog.ListDatasets(ctx, project.ID)
		if err != nil {
			return ports.GoogleSQLCatalogSnapshot{}, err
		}
		projectSnapshot := ports.GoogleSQLProjectSnapshot{
			Project: project, Datasets: make([]ports.GoogleSQLDatasetSnapshot, len(datasets)),
		}
		for datasetIndex, dataset := range datasets {
			tables, err := s.listTablesLocked(ctx, project.ID, dataset.ID)
			if err != nil {
				return ports.GoogleSQLCatalogSnapshot{}, err
			}
			ownedTables := make([]domain.Table, len(tables))
			for tableIndex, table := range tables {
				ownedTables[tableIndex] = cloneGoogleSQLCatalogTable(table)
			}
			projectSnapshot.Datasets[datasetIndex] = ports.GoogleSQLDatasetSnapshot{
				Dataset: cloneGoogleSQLCatalogDataset(dataset), Tables: ownedTables,
			}
		}
		snapshot.Projects[projectIndex] = projectSnapshot
	}
	return snapshot, nil
}

func cloneGoogleSQLCatalogDataset(dataset domain.Dataset) domain.Dataset {
	clone := dataset
	clone.Labels = cloneGoogleSQLCatalogLabels(dataset.Labels)
	clone.DefaultTableExpirationMs = domain.CloneOptionalInt64(dataset.DefaultTableExpirationMs)
	clone.DefaultPartitionExpirationMs = domain.CloneOptionalInt64(dataset.DefaultPartitionExpirationMs)
	return clone
}

func cloneGoogleSQLCatalogTable(table domain.Table) domain.Table {
	clone := table
	clone.Schema = domain.CloneFields(table.Schema)
	clone.Labels = cloneGoogleSQLCatalogLabels(table.Labels)
	clone.ClusteringFields = append([]string(nil), table.ClusteringFields...)
	if table.ExpirationTime != nil {
		value := *table.ExpirationTime
		clone.ExpirationTime = &value
	}
	if table.TimePartitioning != nil {
		value := *table.TimePartitioning
		clone.TimePartitioning = &value
	}
	if table.RangePartitioning != nil {
		value := *table.RangePartitioning
		clone.RangePartitioning = &value
	}
	return clone
}

func cloneGoogleSQLCatalogLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	clone := make(map[string]string, len(labels))
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}
