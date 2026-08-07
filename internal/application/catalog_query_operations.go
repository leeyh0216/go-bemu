package application

// Query-operation catalog coordination holds the same process-wide mutation
// gate used by table create/update/delete across the complete backend
// transaction. Canonical partition metadata therefore cannot be replaced after
// validation and before the physical delete/insert commits.
//
// BigQuery DML statements are atomic:
// https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

var _ ports.QueryOperationCatalog = (*CatalogService)(nil)

func (s *CatalogService) WithCanonicalTables(
	ctx context.Context,
	destinationReference domain.TableReference,
	sourceReference domain.TableReference,
	execute func(destination domain.Table, source domain.Table) (domain.QueryResult, error),
) (domain.QueryResult, error) {
	if execute == nil {
		return domain.QueryResult{}, fmt.Errorf("%w: canonical table operation callback is required", domain.ErrPrecondition)
	}
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return domain.QueryResult{}, err
	}
	defer s.resourceMutationMu.Unlock()
	destination, err := s.getTableLocked(ctx, destinationReference.ProjectID, destinationReference.DatasetID, destinationReference.TableID)
	if err != nil {
		return domain.QueryResult{}, err
	}
	source, err := s.getTableLocked(ctx, sourceReference.ProjectID, sourceReference.DatasetID, sourceReference.TableID)
	if err != nil {
		return domain.QueryResult{}, err
	}
	return execute(destination, source)
}
