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
	if !reflect.DeepEqual(current.Schema, domain.CloneFields(expected)) {
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
