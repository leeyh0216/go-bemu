package application

import (
	"context"
	"fmt"
	"reflect"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

// PublishLoadedTableSchema records a schema update whose physical mutation and
// data append were already committed by the load engine. The expected schema
// prevents a concurrent metadata writer from being overwritten.
func (s *CatalogService) PublishLoadedTableSchema(
	ctx context.Context,
	reference domain.TableReference,
	expected, updated []domain.Field,
) error {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()

	current, err := s.getTableLocked(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
	if err != nil {
		return err
	}
	if sameLoadSchema(current.Schema, updated) {
		return nil
	}
	if !sameLoadSchema(current.Schema, expected) {
		return fmt.Errorf("%w: destination schema changed before load publication", domain.ErrPrecondition)
	}
	evolution, err := domain.ValidateSchemaUpdate(current.Schema, updated, domain.SchemaEvolutionOptions{
		AllowFieldAddition: true, AllowFieldRelaxation: true,
	})
	if err != nil {
		return err
	}
	if len(evolution.Additions) == 0 && len(evolution.Relaxations) == 0 {
		return fmt.Errorf("%w: load schema publication contains no update", domain.ErrInvalid)
	}
	current.Schema = domain.CloneFields(updated)
	current.UpdatedAt = s.clock.Now()
	if err := current.Validate(); err != nil {
		return err
	}
	return s.catalog.UpdateTable(ctx, current)
}

func samePublishedLoadTable(existing, desired domain.Table) bool {
	if existing.ProjectID != desired.ProjectID || existing.DatasetID != desired.DatasetID || existing.ID != desired.ID ||
		(existing.Location != desired.Location && desired.Location != "") || existing.Type != "TABLE" {
		return false
	}
	return sameLoadSchema(existing.Schema, desired.Schema) &&
		reflect.DeepEqual(existing.TimePartitioning, desired.TimePartitioning) &&
		reflect.DeepEqual(existing.RangePartitioning, desired.RangePartitioning) &&
		sameOptionalStringSlice(existing.ClusteringFields, desired.ClusteringFields)
}

func sameLoadSchema(left, right []domain.Field) bool {
	return reflect.DeepEqual(canonicalLoadSchema(left), canonicalLoadSchema(right))
}

func canonicalLoadSchema(fields []domain.Field) []domain.Field {
	if len(fields) == 0 {
		return nil
	}
	result := make([]domain.Field, len(fields))
	for index, field := range fields {
		result[index] = field
		result[index].Precision = domain.CloneOptionalInt64(field.Precision)
		result[index].Scale = domain.CloneOptionalInt64(field.Scale)
		result[index].Fields = canonicalLoadSchema(field.Fields)
	}
	return result
}

func sameOptionalStringSlice(left, right []string) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	return reflect.DeepEqual(left, right)
}
