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

func loadFieldsFromCatalog(fields []catalogDomain.Field) []loadDomain.Field {
	result := make([]loadDomain.Field, len(fields))
	for index, field := range fields {
		result[index] = loadDomain.Field{
			Name: field.Name, Type: field.Type, Mode: field.Mode,
			Description: field.Description, Precision: catalogDomain.CloneOptionalInt64(field.Precision), Scale: catalogDomain.CloneOptionalInt64(field.Scale),
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
	default:
		return err
	}
}

var _ loadports.TableCatalog = (*loadTableCatalog)(nil)
