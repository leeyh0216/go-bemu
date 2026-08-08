package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

var _ ports.DDLExecutor = (*CatalogService)(nil)

// ExecuteDDL applies one parser-validated semantic command under the same
// mutation lock used by the Tables API. Persistent cross-store recovery is a
// separate state-journal concern; this method never stores an engine plan.
func (s *CatalogService) ExecuteDDL(ctx context.Context, command domain.DDLCommand, correlationID string) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if s.ddlStorage == nil {
		return unsupportedDDL("planned DDL storage is not configured")
	}
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()

	correlationID = ddlCorrelationID(correlationID, command)
	switch command.Kind() {
	case domain.DDLCreateTable:
		return s.executeDDLCreateTable(ctx, command, correlationID)
	case domain.DDLDropTable:
		return s.executeDDLDropTable(ctx, command, correlationID)
	case domain.DDLTruncateTable:
		return s.executeDDLTruncateTable(ctx, command, correlationID)
	case domain.DDLAddColumn, domain.DDLDropColumn, domain.DDLRenameColumn, domain.DDLAlterColumnType:
		return s.executeDDLAlterTable(ctx, command, correlationID)
	default:
		return unsupportedDDL("DDL command kind is not supported")
	}
}

func (s *CatalogService) executeDDLCreateTable(
	ctx context.Context,
	command domain.DDLCommand,
	correlationID string,
) error {
	reference := command.Table()
	dataset, err := s.catalog.GetDataset(ctx, reference.ProjectID, reference.DatasetID)
	if err != nil {
		return err
	}
	if existing, err := s.catalog.GetTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID); err == nil {
		if !tableExpired(existing, s.clock.Now()) {
			return fmt.Errorf("%w: table %s/%s/%s", domain.ErrConflict, reference.ProjectID, reference.DatasetID, reference.TableID)
		}
		if _, err := s.removeExpiredTableLocked(ctx, reference.ProjectID, reference.DatasetID, reference.TableID); err != nil {
			return err
		}
	} else if !errors.Is(err, domain.ErrNotFound) {
		return err
	}

	now := s.clock.Now()
	table := domain.Table{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID, ID: reference.TableID,
		Type: "TABLE", Schema: command.Schema(), Location: dataset.Location, CreatedAt: now, UpdatedAt: now,
	}
	if dataset.DefaultTableExpirationMs != nil {
		expiration := now.Add(time.Duration(*dataset.DefaultTableExpirationMs) * time.Millisecond)
		table.ExpirationTime = &expiration
	}
	if err := table.Validate(); err != nil {
		return err
	}
	generation := ddlMetadataGeneration(table.UpdatedAt)
	mutation, err := engine.NewTableMutation(engine.TableMutationDescriptor{
		Kind: engine.TableMutationCreate, Target: reference, AfterSchema: table.Schema,
		CorrelationID: correlationID, Generation: generation,
	})
	if err != nil {
		return err
	}
	plan, err := s.ddlStorage.PlanTableMutation(ctx, mutation)
	if err != nil {
		return err
	}
	if err := s.ddlStorage.ApplyTableMutation(ctx, plan, mutation); err != nil {
		return err
	}
	if err := s.catalog.CreateTable(ctx, table); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.compensationTimeout)
		cleanupErr := s.warehouse.DropTable(cleanupCtx, reference.ProjectID, reference.DatasetID, reference.TableID)
		cancel()
		if cleanupErr != nil {
			return errors.Join(err, fmt.Errorf("compensate unpublished DDL table storage: %w", cleanupErr))
		}
		return err
	}
	return nil
}

func (s *CatalogService) executeDDLDropTable(
	ctx context.Context,
	command domain.DDLCommand,
	correlationID string,
) error {
	reference := command.Table()
	table, err := s.getTableLocked(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
	if err != nil {
		return err
	}
	expected := ddlMetadataGeneration(table.UpdatedAt)
	mutation, err := engine.NewTableMutation(engine.TableMutationDescriptor{
		Kind: engine.TableMutationDrop, Target: reference, BeforeSchema: table.Schema,
		CorrelationID: correlationID, ExpectedGeneration: expected, Generation: expected + 1,
	})
	if err != nil {
		return err
	}
	plan, err := s.ddlStorage.PlanTableMutation(ctx, mutation)
	if err != nil {
		return err
	}
	if err := s.ddlStorage.ApplyTableMutation(ctx, plan, mutation); err != nil {
		return err
	}
	return s.catalog.DeleteTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
}

func (s *CatalogService) executeDDLAlterTable(
	ctx context.Context,
	command domain.DDLCommand,
	correlationID string,
) error {
	reference := command.Table()
	before, err := s.getTableLocked(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
	if err != nil {
		return err
	}
	after := before
	after.Schema = domain.CloneFields(before.Schema)
	updatedSchema, change, kind, err := applyDDLFieldChange(command, after.Schema)
	if err != nil {
		return err
	}
	after.Schema = updatedSchema
	after.UpdatedAt = nextDDLMetadataTime(before.UpdatedAt, s.clock.Now())
	if err := after.Validate(); err != nil {
		return err
	}
	expected := ddlMetadataGeneration(before.UpdatedAt)
	generation := ddlMetadataGeneration(after.UpdatedAt)
	mutation, err := engine.NewTableMutation(engine.TableMutationDescriptor{
		Kind: kind, Target: reference, BeforeSchema: before.Schema, AfterSchema: after.Schema,
		FieldChanges: []engine.FieldChangeDescriptor{change}, CorrelationID: correlationID,
		ExpectedGeneration: expected, Generation: generation,
	})
	if err != nil {
		return err
	}
	plan, err := s.ddlStorage.PlanTableMutation(ctx, mutation)
	if err != nil {
		return err
	}
	if err := s.ddlStorage.ApplyTableMutation(ctx, plan, mutation); err != nil {
		return err
	}
	return s.catalog.UpdateTable(ctx, after)
}

func applyDDLFieldChange(
	command domain.DDLCommand,
	fields []domain.Field,
) ([]domain.Field, engine.FieldChangeDescriptor, engine.TableMutationKind, error) {
	index := -1
	if command.Kind() != domain.DDLAddColumn {
		for fieldIndex := range fields {
			if strings.EqualFold(fields[fieldIndex].Name, command.Name()) {
				index = fieldIndex
				break
			}
		}
		if index < 0 {
			return nil, engine.FieldChangeDescriptor{}, "", fmt.Errorf("%w: column %s", domain.ErrNotFound, command.Name())
		}
	}
	switch command.Kind() {
	case domain.DDLAddColumn:
		field := command.Field()
		for _, existing := range fields {
			if strings.EqualFold(existing.Name, field.Name) {
				return nil, engine.FieldChangeDescriptor{}, "", fmt.Errorf("%w: column %s", domain.ErrConflict, field.Name)
			}
		}
		fields = append(fields, field)
		return fields, engine.FieldChangeDescriptor{Path: []string{field.Name}, After: field}, engine.TableMutationAddColumn, nil
	case domain.DDLDropColumn:
		before := fields[index]
		copy(fields[index:], fields[index+1:])
		fields = fields[:len(fields)-1]
		return fields, engine.FieldChangeDescriptor{Path: []string{before.Name}, Before: before}, engine.TableMutationDropColumn, nil
	case domain.DDLRenameColumn:
		for fieldIndex := range fields {
			if fieldIndex != index && strings.EqualFold(fields[fieldIndex].Name, command.NewName()) {
				return nil, engine.FieldChangeDescriptor{}, "", fmt.Errorf("%w: column %s", domain.ErrConflict, command.NewName())
			}
		}
		before := fields[index]
		fields[index].Name = command.NewName()
		return fields, engine.FieldChangeDescriptor{
			Path: []string{before.Name}, Before: before, After: fields[index],
		}, engine.TableMutationRenameColumn, nil
	case domain.DDLAlterColumnType:
		before := fields[index]
		replacement := command.Field()
		if strings.EqualFold(before.Mode, "REPEATED") || len(before.Fields) != 0 ||
			strings.EqualFold(replacement.Mode, "REPEATED") || len(replacement.Fields) != 0 {
			return nil, engine.FieldChangeDescriptor{}, "", unsupportedDDL("SET DATA TYPE supports top-level scalar columns only")
		}
		replacement.Name = before.Name
		replacement.Mode = before.Mode
		replacement.Description = before.Description
		fields[index] = replacement
		return fields, engine.FieldChangeDescriptor{
			Path: []string{before.Name}, Before: before, After: replacement,
		}, engine.TableMutationChangeColumnType, nil
	default:
		return nil, engine.FieldChangeDescriptor{}, "", unsupportedDDL("ALTER TABLE action is not supported")
	}
}

func (s *CatalogService) executeDDLTruncateTable(
	ctx context.Context,
	command domain.DDLCommand,
	correlationID string,
) error {
	reference := command.Table()
	before, err := s.getTableLocked(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
	if err != nil {
		return err
	}
	after := before
	after.UpdatedAt = nextDDLMetadataTime(before.UpdatedAt, s.clock.Now())
	expected := ddlMetadataGeneration(before.UpdatedAt)
	generation := ddlMetadataGeneration(after.UpdatedAt)
	replacement, err := engine.NewDataReplacement(engine.DataReplacementDescriptor{
		Scope: engine.DataReplacementTable, Target: reference, Schema: before.Schema,
		CorrelationID: correlationID, ExpectedGeneration: expected, Generation: generation,
		SourceFingerprint: ddlFingerprint("truncate-source", reference, expected),
		ResultFingerprint: ddlFingerprint("truncate-empty", reference, generation),
	})
	if err != nil {
		return err
	}
	plan, err := s.ddlStorage.PlanTableTruncation(ctx, replacement)
	if err != nil {
		return err
	}
	if err := s.ddlStorage.ApplyTableTruncation(ctx, plan, replacement); err != nil {
		return err
	}
	return s.catalog.UpdateTable(ctx, after)
}

func ddlMetadataGeneration(value time.Time) uint64 {
	nanoseconds := value.UTC().UnixNano()
	if nanoseconds <= 0 {
		return 1
	}
	return uint64(nanoseconds)
}

func nextDDLMetadataTime(before, candidate time.Time) time.Time {
	if candidate.After(before) {
		return candidate
	}
	return before.Add(time.Nanosecond)
}

func ddlCorrelationID(value string, command domain.DDLCommand) string {
	reference := command.Table()
	digest := sha256.Sum256([]byte(strings.Join([]string{
		value, string(command.Kind()), reference.ProjectID, reference.DatasetID, reference.TableID,
	}, "\x00")))
	return "ddl-" + hex.EncodeToString(digest[:])
}

func ddlFingerprint(label string, reference domain.TableReference, generation uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d",
		label, reference.ProjectID, reference.DatasetID, reference.TableID, generation)))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func unsupportedDDL(reason string) error {
	return fmt.Errorf("%w: %s; capability=%s", domain.ErrUnsupported, reason, domain.GapQueryDDLCatalogSyncV1)
}
