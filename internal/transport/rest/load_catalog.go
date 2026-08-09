package rest

import (
	"context"
	"errors"
	"fmt"

	catalogDomain "github.com/leeyh0216/go-bemu/internal/domain"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type loadTableCatalog struct {
	catalog LoadCatalogUseCases
}

type LoadCatalogUseCases interface {
	GetTable(context.Context, string, string, string) (catalogDomain.Table, error)
	GetDataset(context.Context, string, string) (catalogDomain.Dataset, error)
	PublishMaterializedTable(context.Context, catalogDomain.Table) error
	PublishLoadedTableSchema(context.Context, catalogDomain.TableReference, []catalogDomain.Field, []catalogDomain.Field) error
}

// NewLoadTableCatalog adapts the existing catalog application boundary to the
// load-job bounded context without coupling that context to REST DTOs.
func NewLoadTableCatalog(catalog LoadCatalogUseCases) loadports.TableCatalog {
	return &loadTableCatalog{catalog: catalog}
}

func (c *loadTableCatalog) GetDataset(ctx context.Context, projectID, datasetID string) (loadDomain.Dataset, error) {
	dataset, err := c.catalog.GetDataset(ctx, projectID, datasetID)
	if err != nil {
		return loadDomain.Dataset{}, mapCatalogLoadError(err)
	}
	return loadDomain.Dataset{
		Location:                     dataset.Location,
		DefaultPartitionExpirationMs: catalogDomain.CloneOptionalInt64(dataset.DefaultPartitionExpirationMs),
	}, nil
}

func (c *loadTableCatalog) PublishTable(ctx context.Context, table loadDomain.Table) error {
	err := c.catalog.PublishMaterializedTable(ctx, catalogDomain.Table{
		ProjectID:         table.Reference.ProjectID,
		DatasetID:         table.Reference.DatasetID,
		ID:                table.Reference.TableID,
		Location:          table.Location,
		Schema:            catalogDomain.CloneFields(table.Schema),
		TimePartitioning:  cloneLoadTimePartitioning(table.TimePartitioning),
		RangePartitioning: cloneLoadRangePartitioning(table.RangePartitioning),
		ClusteringFields:  cloneLoadOptionalStrings(table.ClusteringFields),
	})
	return mapCatalogLoadError(err)
}

func (c *loadTableCatalog) PublishSchemaUpdate(
	ctx context.Context,
	reference loadDomain.TableReference,
	expected, updated []loadDomain.Field,
) error {
	err := c.catalog.PublishLoadedTableSchema(ctx, catalogDomain.TableReference{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID, TableID: reference.TableID,
	}, catalogDomain.CloneFields(expected), catalogDomain.CloneFields(updated))
	return mapCatalogLoadError(err)
}

func (c *loadTableCatalog) GetTable(ctx context.Context, reference loadDomain.TableReference) (loadDomain.Table, error) {
	table, err := c.catalog.GetTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
	if err != nil {
		return loadDomain.Table{}, mapCatalogLoadError(err)
	}
	return loadDomain.Table{
		Reference: reference, Location: table.Location, Schema: loadFieldsFromCatalog(table.Schema),
		TimePartitioning:  cloneLoadTimePartitioning(table.TimePartitioning),
		RangePartitioning: cloneLoadRangePartitioning(table.RangePartitioning),
		ClusteringFields:  cloneLoadOptionalStrings(table.ClusteringFields),
	}, nil
}

func cloneLoadTimePartitioning(value *catalogDomain.TimePartitioning) *catalogDomain.TimePartitioning {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneLoadRangePartitioning(value *catalogDomain.RangePartitioning) *loadDomain.RangePartitioning {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneLoadOptionalStrings(values []string) []string {
	if values == nil {
		return nil
	}
	return append(make([]string, 0, len(values)), values...)
}

func loadFieldsFromCatalog(fields []catalogDomain.Field) []loadDomain.Field {
	result := make([]loadDomain.Field, len(fields))
	for index, field := range fields {
		result[index] = loadDomain.Field{
			Name: field.Name, Type: field.Type, Mode: field.Mode,
			Description: field.Description, Precision: catalogDomain.CloneOptionalInt64(field.Precision), Scale: catalogDomain.CloneOptionalInt64(field.Scale), RoundingMode: field.RoundingMode,
			Fields: loadFieldsFromCatalog(field.Fields),
		}
	}
	return result
}

func mapCatalogLoadError(err error) error {
	switch {
	case errors.Is(err, catalogDomain.ErrNotFound):
		return fmt.Errorf("%w: destination table", loadDomain.ErrNotFound)
	case errors.Is(err, catalogDomain.ErrInvalid):
		return fmt.Errorf("%w: destination table", loadDomain.ErrInvalid)
	case errors.Is(err, catalogDomain.ErrConflict):
		return fmt.Errorf("%w: destination table", loadDomain.ErrConflict)
	case errors.Is(err, catalogDomain.ErrPrecondition):
		return fmt.Errorf("%w: destination table", loadDomain.ErrPrecondition)
	case errors.Is(err, catalogDomain.ErrUnsupported):
		return fmt.Errorf("%w: destination table", loadDomain.ErrUnsupported)
	default:
		return err
	}
}

var _ loadports.TableCatalog = (*loadTableCatalog)(nil)
