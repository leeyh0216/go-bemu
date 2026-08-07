package application

// Official PATCH semantics and mutable fields:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/patch
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch
//   - https://cloud.google.com/bigquery/docs/managing-table-schemas#add_columns
//
// PatchValue distinguishes an omitted property from an explicit JSON null or
// zero. The transport owns JSON shape; this application input owns update
// intent and keeps HTTP-specific field detection out of the domain.

import (
	"context"
	"fmt"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

const CapabilityRESTMetadataPatchV1 = "CAP-REST-METADATA-PATCH-V1"

type PatchValue[T any] struct {
	Set   bool
	Value T
}

type DatasetPatch struct {
	FriendlyName                 PatchValue[string]
	Description                  PatchValue[string]
	Labels                       PatchValue[map[string]string]
	DefaultTableExpirationMs     PatchValue[*int64]
	DefaultPartitionExpirationMs PatchValue[*int64]
}

type TablePatch struct {
	FriendlyName   PatchValue[string]
	Description    PatchValue[string]
	Labels         PatchValue[map[string]string]
	ExpirationTime PatchValue[*time.Time]
	Schema         PatchValue[[]domain.Field]
}

func (s *CatalogService) UpdateDataset(ctx context.Context, projectID, datasetID string, patch DatasetPatch) (domain.Dataset, error) {
	s.resourceMutationMu.Lock()
	defer s.resourceMutationMu.Unlock()

	dataset, err := s.catalog.GetDataset(ctx, projectID, datasetID)
	if err != nil {
		return domain.Dataset{}, err
	}
	if patch.FriendlyName.Set {
		dataset.FriendlyName = patch.FriendlyName.Value
	}
	if patch.Description.Set {
		dataset.Description = patch.Description.Value
	}
	if patch.Labels.Set {
		dataset.Labels = copyStringMap(patch.Labels.Value)
	}
	if patch.DefaultTableExpirationMs.Set {
		dataset.DefaultTableExpirationMs = copyInt64Pointer(patch.DefaultTableExpirationMs.Value)
	}
	if patch.DefaultPartitionExpirationMs.Set {
		dataset.DefaultPartitionExpirationMs = copyInt64Pointer(patch.DefaultPartitionExpirationMs.Value)
	}
	if err := dataset.Validate(); err != nil {
		return domain.Dataset{}, err
	}
	dataset.UpdatedAt = s.clock.Now()
	if err := s.catalog.UpdateDataset(ctx, dataset); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func (s *CatalogService) UpdateTable(ctx context.Context, projectID, datasetID, tableID string, patch TablePatch) (domain.Table, error) {
	s.resourceMutationMu.Lock()
	defer s.resourceMutationMu.Unlock()

	table, err := s.catalog.GetTable(ctx, projectID, datasetID, tableID)
	if err != nil {
		return domain.Table{}, err
	}
	if tableExpired(table, s.clock.Now()) {
		if _, cleanupErr := s.removeExpiredTableLocked(ctx, projectID, datasetID, tableID); cleanupErr != nil {
			return domain.Table{}, cleanupErr
		}
		return domain.Table{}, fmt.Errorf("%w: table %s/%s/%s expired", domain.ErrNotFound, projectID, datasetID, tableID)
	}
	if patch.FriendlyName.Set {
		table.FriendlyName = patch.FriendlyName.Value
	}
	if patch.Description.Set {
		table.Description = patch.Description.Value
	}
	if patch.Labels.Set {
		table.Labels = copyStringMap(patch.Labels.Value)
	}
	if patch.ExpirationTime.Set {
		table.ExpirationTime = copyTimePointer(patch.ExpirationTime.Value)
	}
	var additions []domain.SchemaAddition
	if patch.Schema.Set {
		additions, err = domain.ValidateSchemaEvolution(table.Schema, patch.Schema.Value)
		if err != nil {
			return domain.Table{}, err
		}
		table.Schema = copyFields(patch.Schema.Value)
	}
	if err := table.Validate(); err != nil {
		return domain.Table{}, err
	}
	if len(additions) != 0 {
		if err := s.warehouse.ApplySchemaAdditions(ctx, table, additions); err != nil {
			return domain.Table{}, err
		}
	}
	table.UpdatedAt = s.clock.Now()
	if err := s.catalog.UpdateTable(ctx, table); err != nil {
		return domain.Table{}, err
	}
	return table, nil
}

func copyStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func copyInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func copyTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func copyFields(fields []domain.Field) []domain.Field {
	clone := make([]domain.Field, len(fields))
	for index, field := range fields {
		clone[index] = field
		clone[index].Fields = copyFields(field.Fields)
	}
	return clone
}
