package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/state"
)

// DDLCommand remains an application alias for callers that execute semantic
// commands directly. The canonical model is owned by the domain package.
type DDLCommand = domain.DDLCommand

// ExecuteDDL shares CatalogService's canonical resource mutation boundary with
// REST. SQL text has already been reduced to a bounded semantic command.
func (s *CatalogService) ExecuteDDL(ctx context.Context, command DDLCommand) error {
	switch command.Kind {
	case "CREATE_TABLE":
		_, err := s.CreateTable(ctx, domain.Table{ProjectID: command.Table.ProjectID, DatasetID: command.Table.DatasetID, ID: command.Table.TableID, Type: "TABLE", Schema: command.Schema})
		return err
	case "DROP_TABLE":
		return s.DeleteTable(ctx, command.Table.ProjectID, command.Table.DatasetID, command.Table.TableID)
	case "ADD_COLUMN", "RENAME_COLUMN", "DROP_COLUMN", "ALTER_COLUMN_TYPE":
		_, err := s.applyDDLTableSchema(ctx, command)
		return err
	default:
		return ddlUnsupported("unsupported DDL command")
	}
}

func (s *CatalogService) applyDDLTableSchema(ctx context.Context, command DDLCommand) (domain.Table, error) {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return domain.Table{}, err
	}
	defer s.resourceMutationMu.Unlock()
	before, err := s.getTableLocked(ctx, command.Table.ProjectID, command.Table.DatasetID, command.Table.TableID)
	if err != nil {
		return domain.Table{}, err
	}
	if s.mutationJournal == nil && (command.Kind == "DROP_COLUMN" || command.Kind == "ALTER_COLUMN_TYPE") {
		return domain.Table{}, ddlUnsupported("DROP COLUMN and SET DATA TYPE require a durable canonical mutation journal")
	}
	after := before
	after.Schema = copyFields(before.Schema)
	index := -1
	if command.Kind != "ADD_COLUMN" {
		for i := range after.Schema {
			if strings.EqualFold(after.Schema[i].Name, command.Name) {
				index = i
				break
			}
		}
		if index < 0 {
			return domain.Table{}, fmt.Errorf("%w: column %s", domain.ErrNotFound, command.Name)
		}
	}
	switch command.Kind {
	case "ADD_COLUMN":
		for _, field := range after.Schema {
			if strings.EqualFold(field.Name, command.Field.Name) {
				return domain.Table{}, fmt.Errorf("%w: column %s", domain.ErrConflict, command.Field.Name)
			}
		}
		after.Schema = append(after.Schema, command.Field)
	case "RENAME_COLUMN":
		for _, field := range after.Schema {
			if strings.EqualFold(field.Name, command.NewName) {
				return domain.Table{}, fmt.Errorf("%w: column %s", domain.ErrConflict, command.NewName)
			}
		}
		after.Schema[index].Name = command.NewName
	case "DROP_COLUMN":
		after.Schema = append(after.Schema[:index], after.Schema[index+1:]...)
	case "ALTER_COLUMN_TYPE":
		current := after.Schema[index]
		replacement := command.Field
		replacement.Name = current.Name
		replacement.Mode = current.Mode
		replacement.Description = current.Description
		after.Schema[index] = replacement
	}
	after.UpdatedAt = s.clock.Now()
	if !after.UpdatedAt.After(before.UpdatedAt) {
		after.UpdatedAt = before.UpdatedAt.Add(time.Nanosecond)
	}
	if err := after.Validate(); err != nil {
		return domain.Table{}, err
	}
	if planner, ok := s.warehouse.(ports.SchemaPlanner); ok {
		if err := planner.ValidateSchema(after.Schema); err != nil {
			return domain.Table{}, err
		}
	}
	planner, ok := s.warehouse.(ports.TableSchemaPlanner)
	if !ok {
		return domain.Table{}, ddlUnsupported("configured engine cannot plan table schema changes")
	}
	mutator, ok := s.warehouse.(ports.TableSchemaMutator)
	if !ok {
		return domain.Table{}, ddlUnsupported("configured engine cannot apply table schema changes")
	}
	if err := requireDDLTableChangeCapability(s.warehouse, command.Kind); err != nil {
		return domain.Table{}, err
	}
	plan, err := planner.PlanTableChange(before, after)
	if err != nil {
		return domain.Table{}, err
	}
	if s.mutationJournal != nil {
		return s.applyDurableDDLTableSchema(ctx, plan, mutator)
	}
	if err := mutator.ApplyTableSchemaChange(ctx, plan); err != nil {
		return domain.Table{}, err
	}
	if err := s.catalog.UpdateTable(ctx, after); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.compensationTimeout)
		reverse, planErr := planner.PlanTableChange(after, before)
		cleanupErr := planErr
		if planErr == nil {
			cleanupErr = mutator.ApplyTableSchemaChange(cleanupCtx, reverse)
		}
		cancel()
		if cleanupErr != nil {
			return domain.Table{}, errors.Join(err, fmt.Errorf("compensate unpublished DDL schema change: %w", cleanupErr))
		}
		return domain.Table{}, err
	}
	return after, nil
}

func (s *CatalogService) applyDurableDDLTableSchema(
	ctx context.Context,
	plan ports.TableSchemaChangePlan,
	mutator ports.TableSchemaMutator,
) (domain.Table, error) {
	mutationID, err := newDDLMutationID()
	if err != nil {
		return domain.Table{}, err
	}
	preparedAt := s.clock.Now()
	record, err := s.mutationJournal.Begin(ctx, state.BeginMutation{
		ID: mutationID, ResourceKey: state.TableResourceKey(plan.Before), Kind: state.MutationKindTableSchema,
		ExpectedCanonicalRevision: state.TableRevision(plan.Before),
		BeforePhysicalFingerprint: plan.BeforePhysicalFingerprint,
		AfterPhysicalFingerprint:  plan.AfterPhysicalFingerprint,
		TableChange:               state.TableChange{Before: plan.Before, After: plan.After},
		PreparedAt:                preparedAt,
	})
	if err != nil {
		return domain.Table{}, err
	}
	if err := mutator.ApplyTableSchemaChange(ctx, plan); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.compensationTimeout)
		defer cancel()
		matchesAfter, inspectAfterErr := mutator.TableSchemaMatches(cleanupCtx, plan.After)
		if inspectAfterErr != nil {
			return domain.Table{}, errors.Join(err, fmt.Errorf("inspect failed DDL mutation after schema: %w", inspectAfterErr))
		}
		if matchesAfter {
			return domain.Table{}, err
		}
		matchesBefore, inspectErr := mutator.TableSchemaMatches(cleanupCtx, plan.Before)
		if inspectErr == nil && matchesBefore {
			_, markErr := s.mutationJournal.MarkFailed(cleanupCtx, record.ID, state.Failure{
				Code: "physical.apply_failed", Digest: state.Fingerprint([]byte(err.Error())),
			}, mutationCompletedAt(record, s.clock.Now()))
			if markErr != nil {
				return domain.Table{}, errors.Join(err, fmt.Errorf("record failed DDL mutation: %w", markErr))
			}
		} else if inspectErr != nil {
			return domain.Table{}, errors.Join(err, fmt.Errorf("inspect failed DDL mutation: %w", inspectErr))
		}
		return domain.Table{}, err
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.compensationTimeout)
	defer cancel()
	if _, err := s.mutationJournal.CommitTableChange(publishCtx, record.ID, mutationCompletedAt(record, s.clock.Now())); err != nil {
		return domain.Table{}, fmt.Errorf("publish durable DDL mutation: %w", err)
	}
	return plan.After, nil
}

func requireDDLTableChangeCapability(warehouse ports.WarehouseAdmin, kind domain.DDLKind) error {
	provider, ok := warehouse.(ports.EngineCapabilityProvider)
	if !ok {
		return nil
	}
	capabilities := provider.EngineCapabilities().TableSchemaChanges
	supported := map[domain.DDLKind]bool{
		domain.DDLAddColumn: capabilities.AddColumn, domain.DDLDropColumn: capabilities.DropColumn,
		domain.DDLRenameColumn: capabilities.RenameColumn, domain.DDLAlterColumnType: capabilities.AlterColumnType,
	}[kind]
	if !supported || !capabilities.Transactional || !capabilities.InspectBeforeAfter {
		return ddlUnsupported("configured engine does not advertise the required table schema change semantics")
	}
	return nil
}

func newDDLMutationID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate DDL mutation ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func ddlUnsupported(reason string) error {
	return fmt.Errorf("%w: %s; capability=query.ddl.catalog-sync-v1", domain.ErrUnsupported, reason)
}
