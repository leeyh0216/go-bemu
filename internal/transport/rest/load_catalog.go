package rest

import (
	"context"
	"errors"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/application"
	catalogDomain "github.com/leeyh0216/go-bemu/internal/domain"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type loadTableCatalog struct {
	catalog CatalogUseCases
}

// NewLoadTableCatalog adapts the existing catalog application boundary to the
// load-job bounded context without coupling that context to REST DTOs.
func NewLoadTableCatalog(catalog CatalogUseCases) loadports.TableCatalog {
	return &loadTableCatalog{catalog: catalog}
}

func (c *loadTableCatalog) GetTable(ctx context.Context, reference loadDomain.TableReference) (loadDomain.Table, error) {
	table, err := c.catalog.GetTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
	if err != nil {
		return loadDomain.Table{}, mapCatalogLoadError(err)
	}
	return loadDomain.Table{
		Reference: reference, Location: table.Location, Schema: loadFieldsFromCatalog(table.Schema),
	}, nil
}

func (c *loadTableCatalog) CreateTable(ctx context.Context, table loadDomain.Table) (loadDomain.Table, error) {
	created, err := c.catalog.CreateTable(ctx, catalogDomain.Table{
		ProjectID: table.Reference.ProjectID, DatasetID: table.Reference.DatasetID, ID: table.Reference.TableID,
		Location: table.Location, Type: "TABLE", Schema: loadFieldsToCatalog(table.Schema),
	})
	if err != nil {
		return loadDomain.Table{}, mapCatalogLoadError(err)
	}
	return loadDomain.Table{Reference: table.Reference, Location: created.Location, Schema: loadFieldsFromCatalog(created.Schema)}, nil
}

func (c *loadTableCatalog) DeleteTable(ctx context.Context, reference loadDomain.TableReference) error {
	return mapCatalogLoadError(c.catalog.DeleteTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID))
}

func (c *loadTableCatalog) UpdateSchema(ctx context.Context, reference loadDomain.TableReference, schema []loadDomain.Field) (loadDomain.Table, error) {
	updated, err := c.catalog.UpdateTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID, application.TablePatch{
		Schema: application.PatchValue[[]catalogDomain.Field]{Set: true, Value: loadFieldsToCatalog(schema)},
	})
	if err != nil {
		return loadDomain.Table{}, mapCatalogLoadError(err)
	}
	return loadDomain.Table{Reference: reference, Location: updated.Location, Schema: loadFieldsFromCatalog(updated.Schema)}, nil
}

func loadFieldsFromCatalog(fields []catalogDomain.Field) []loadDomain.Field {
	result := make([]loadDomain.Field, len(fields))
	for index, field := range fields {
		result[index] = loadDomain.Field{
			Name: field.Name, Type: field.Type, Mode: field.Mode,
			Fields: loadFieldsFromCatalog(field.Fields),
		}
	}
	return result
}

func loadFieldsToCatalog(fields []loadDomain.Field) []catalogDomain.Field {
	result := make([]catalogDomain.Field, len(fields))
	for index, field := range fields {
		result[index] = catalogDomain.Field{Name: field.Name, Type: field.Type, Mode: field.Mode, Fields: loadFieldsToCatalog(field.Fields)}
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
	default:
		return err
	}
}

var _ loadports.TableCatalog = (*loadTableCatalog)(nil)
var _ loadports.DestinationTableCatalog = (*loadTableCatalog)(nil)
var _ loadports.SchemaEvolutionCatalog = (*loadTableCatalog)(nil)
