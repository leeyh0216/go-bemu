package duckdb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

var _ ports.DDLStorage = (*Warehouse)(nil)

type duckDBDDLAdapterPlanner struct{ warehouse *Warehouse }

func (planner duckDBDDLAdapterPlanner) PlanTableMutation(
	ctx context.Context,
	mutation engine.TableMutation,
) (engine.PlanProof, error) {
	if planner.warehouse == nil || planner.warehouse.db == nil {
		return engine.PlanProof{}, fmt.Errorf("DuckDB DDL planner is unavailable")
	}
	target := mutation.Target()
	exists, actualFingerprint, err := inspectDDLTable(ctx, planner.warehouse.db, target)
	if err != nil {
		return engine.PlanProof{}, err
	}

	var before engine.PhysicalTableState
	if mutation.Kind() == engine.TableMutationCreate {
		if exists {
			return engine.PlanProof{}, fmt.Errorf("physical table already exists")
		}
		before, err = engine.NewPhysicalTableState(engine.PhysicalTableStateDescriptor{
			Target: target, Provenance: engine.PhysicalStateVirgin,
		})
	} else {
		if !exists || mutation.ExpectedGeneration() == 0 {
			return engine.PlanProof{}, fmt.Errorf("physical table is absent or has no canonical generation")
		}
		expectedFingerprint, fingerprintErr := expectedDDLPhysicalFingerprint(
			ctx, planner.warehouse.db, mutation.BeforeSchema(),
		)
		if fingerprintErr != nil {
			return engine.PlanProof{}, fingerprintErr
		}
		if actualFingerprint != expectedFingerprint {
			return engine.PlanProof{}, fmt.Errorf("physical table shape differs from canonical metadata")
		}
		before, err = engine.NewPhysicalTableState(engine.PhysicalTableStateDescriptor{
			Target: target, Exists: true, Generation: mutation.ExpectedGeneration(),
			LogicalShapeFingerprint: mutation.BeforeShapeFingerprint(), PhysicalShapeFingerprint: actualFingerprint,
			MarkerFingerprint: previousDDLMarker(target, mutation.ExpectedGeneration()),
			Provenance:        engine.PhysicalStateManaged,
		})
	}
	if err != nil {
		return engine.PlanProof{}, err
	}

	var after engine.PhysicalTableState
	strategy := engine.PlanStrategyAlterInPlace
	switch mutation.Kind() {
	case engine.TableMutationCreate:
		strategy = engine.PlanStrategyCreateTable
		fingerprint, fingerprintErr := expectedDDLPhysicalFingerprint(ctx, planner.warehouse.db, mutation.AfterSchema())
		if fingerprintErr != nil {
			return engine.PlanProof{}, fingerprintErr
		}
		after, err = managedDDLState(mutation, fingerprint, mutation.AfterShapeFingerprint())
	case engine.TableMutationDrop:
		strategy = engine.PlanStrategyDropTable
		after, err = engine.NewPhysicalTableState(engine.PhysicalTableStateDescriptor{
			Target: target, Generation: mutation.Generation(),
			MarkerFingerprint: mutation.GenerationMarkerFingerprint(), Provenance: engine.PhysicalStateTombstone,
		})
	default:
		fingerprint, fingerprintErr := expectedDDLPhysicalFingerprint(ctx, planner.warehouse.db, mutation.AfterSchema())
		if fingerprintErr != nil {
			return engine.PlanProof{}, fingerprintErr
		}
		after, err = managedDDLState(mutation, fingerprint, mutation.AfterShapeFingerprint())
	}
	if err != nil {
		return engine.PlanProof{}, err
	}
	return engine.NewPlanProof(engine.PlanProofDescriptor{Before: before, After: after, Strategy: strategy})
}

func (planner duckDBDDLAdapterPlanner) PlanDataReplacement(
	ctx context.Context,
	replacement engine.DataReplacement,
) (engine.PlanProof, error) {
	if planner.warehouse == nil || planner.warehouse.db == nil || replacement.ExpectedGeneration() == 0 {
		return engine.PlanProof{}, fmt.Errorf("DuckDB replacement planner is unavailable")
	}
	exists, actualFingerprint, err := inspectDDLTable(ctx, planner.warehouse.db, replacement.Target())
	if err != nil {
		return engine.PlanProof{}, err
	}
	expectedFingerprint, err := expectedDDLPhysicalFingerprint(ctx, planner.warehouse.db, replacement.Schema())
	if err != nil {
		return engine.PlanProof{}, err
	}
	if !exists || actualFingerprint != expectedFingerprint {
		return engine.PlanProof{}, fmt.Errorf("physical table shape differs from canonical metadata")
	}
	before, err := engine.NewPhysicalTableState(engine.PhysicalTableStateDescriptor{
		Target: replacement.Target(), Exists: true, Generation: replacement.ExpectedGeneration(),
		LogicalShapeFingerprint: replacement.ShapeFingerprint(), PhysicalShapeFingerprint: actualFingerprint,
		MarkerFingerprint: previousDDLMarker(replacement.Target(), replacement.ExpectedGeneration()),
		Provenance:        engine.PhysicalStateManaged,
	})
	if err != nil {
		return engine.PlanProof{}, err
	}
	after, err := engine.NewPhysicalTableState(engine.PhysicalTableStateDescriptor{
		Target: replacement.Target(), Exists: true, Generation: replacement.Generation(),
		LogicalShapeFingerprint: replacement.ShapeFingerprint(), PhysicalShapeFingerprint: expectedFingerprint,
		MarkerFingerprint: replacement.GenerationMarkerFingerprint(), Provenance: engine.PhysicalStateManaged,
	})
	if err != nil {
		return engine.PlanProof{}, err
	}
	strategy := engine.PlanStrategyReplaceTableData
	if replacement.Scope() == engine.DataReplacementPartitions {
		strategy = engine.PlanStrategyReplacePartitions
	}
	return engine.NewPlanProof(engine.PlanProofDescriptor{Before: before, After: after, Strategy: strategy})
}

func managedDDLState(
	mutation engine.TableMutation,
	physicalFingerprint string,
	logicalFingerprint string,
) (engine.PhysicalTableState, error) {
	return engine.NewPhysicalTableState(engine.PhysicalTableStateDescriptor{
		Target: mutation.Target(), Exists: true, Generation: mutation.Generation(),
		LogicalShapeFingerprint: logicalFingerprint, PhysicalShapeFingerprint: physicalFingerprint,
		MarkerFingerprint: mutation.GenerationMarkerFingerprint(), Provenance: engine.PhysicalStateManaged,
	})
}

func (w *Warehouse) PlanTableMutation(ctx context.Context, mutation engine.TableMutation) (engine.TablePlan, error) {
	if w == nil || w.ddlPlanner == nil {
		return engine.TablePlan{}, fmt.Errorf("%w: DuckDB DDL planner is not configured", domain.ErrPrecondition)
	}
	return w.ddlPlanner.PlanTableChange(ctx, mutation, engine.PlanRequirements{
		Transactions: []engine.TransactionScope{engine.TransactionScopeSingleTable},
		Inspection:   []engine.InspectionScope{engine.InspectionTableShape},
		DDLGuarantee: engine.DDLGuaranteeAtomicPhysicalStatement,
	})
}

func (w *Warehouse) ApplyTableMutation(
	ctx context.Context,
	plan engine.TablePlan,
	mutation engine.TableMutation,
) (err error) {
	if w == nil || w.ddlPlanner == nil {
		return fmt.Errorf("%w: DuckDB DDL planner is not configured", domain.ErrPrecondition)
	}
	statement, err := tableMutationStatement(mutation)
	if err != nil {
		return err
	}
	// Reject forged, stale, or foreign plans before opening a transaction.
	if err := w.ddlPlanner.ValidateApplyStart(plan, mutation, plan.Proof().Before()); err != nil {
		return err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return sanitizedDDLBackendError(ctx, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	current, err := inspectDDLStateForProof(ctx, tx, mutation.Target(), mutation.BeforeSchema(), plan.Proof().Before())
	if err != nil {
		return err
	}
	if err := w.ddlPlanner.ValidateApplyStart(plan, mutation, current); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, statement); err != nil {
		return sanitizedDDLMutationError(ctx, err)
	}
	current, err = inspectDDLStateForProof(ctx, tx, mutation.Target(), mutation.AfterSchema(), plan.Proof().After())
	if err != nil {
		return err
	}
	if err := w.ddlPlanner.ValidateApplyResult(plan, current); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return sanitizedDDLBackendError(ctx, err)
	}
	return nil
}

func (w *Warehouse) PlanTableTruncation(
	ctx context.Context,
	replacement engine.DataReplacement,
) (engine.DataReplacementPlan, error) {
	if w == nil || w.ddlPlanner == nil {
		return engine.DataReplacementPlan{}, fmt.Errorf("%w: DuckDB DDL planner is not configured", domain.ErrPrecondition)
	}
	if replacement.Scope() != engine.DataReplacementTable {
		return engine.DataReplacementPlan{}, fmt.Errorf("%w: DDL truncation requires a complete table replacement", domain.ErrInvalid)
	}
	return w.ddlPlanner.PlanDataReplacement(ctx, replacement)
}

func (w *Warehouse) ApplyTableTruncation(
	ctx context.Context,
	plan engine.DataReplacementPlan,
	replacement engine.DataReplacement,
) (err error) {
	if w == nil || w.ddlPlanner == nil {
		return fmt.Errorf("%w: DuckDB DDL planner is not configured", domain.ErrPrecondition)
	}
	if replacement.Scope() != engine.DataReplacementTable {
		return fmt.Errorf("%w: DDL truncation requires a complete table replacement", domain.ErrInvalid)
	}
	if err := w.ddlPlanner.ValidateDataReplacementStart(plan, replacement, plan.Proof().Before()); err != nil {
		return err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return sanitizedDDLBackendError(ctx, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	current, err := inspectDDLStateForProof(ctx, tx, replacement.Target(), replacement.Schema(), plan.Proof().Before())
	if err != nil {
		return err
	}
	if err := w.ddlPlanner.ValidateDataReplacementStart(plan, replacement, current); err != nil {
		return err
	}
	tableName := quotedDDLTable(replacement.Target())
	if _, err = tx.ExecContext(ctx, "DELETE FROM "+tableName); err != nil {
		return sanitizedDDLMutationError(ctx, err)
	}
	var remaining int64
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM "+tableName).Scan(&remaining); err != nil || remaining != 0 {
		if err == nil {
			err = errors.New("truncate result is not empty")
		}
		return sanitizedDDLBackendError(ctx, err)
	}
	current, err = inspectDDLStateForProof(ctx, tx, replacement.Target(), replacement.Schema(), plan.Proof().After())
	if err != nil {
		return err
	}
	if err := w.ddlPlanner.ValidateDataReplacementResult(plan, current); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return sanitizedDDLBackendError(ctx, err)
	}
	return nil
}

type ddlQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type ddlPhysicalColumn struct {
	name     string
	typeName string
	nullable bool
}

func inspectDDLTable(
	ctx context.Context,
	queryer ddlQueryer,
	target domain.TableReference,
) (bool, string, error) {
	var tableCount int
	if err := queryer.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'`,
		physicalSchema(target.ProjectID, target.DatasetID), target.TableID,
	).Scan(&tableCount); err != nil {
		return false, "", err
	}
	if tableCount == 0 {
		return false, "", nil
	}
	rows, err := queryer.QueryContext(ctx, `SELECT column_name, data_type, is_nullable
        FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position`,
		physicalSchema(target.ProjectID, target.DatasetID), target.TableID,
	)
	if err != nil {
		return false, "", err
	}
	columns := make([]ddlPhysicalColumn, 0)
	for rows.Next() {
		var column ddlPhysicalColumn
		var nullable string
		if err := rows.Scan(&column.name, &column.typeName, &nullable); err != nil {
			_ = rows.Close()
			return false, "", err
		}
		column.nullable = nullable == "YES"
		columns = append(columns, column)
	}
	if err := rows.Close(); err != nil {
		return false, "", err
	}
	if err := rows.Err(); err != nil {
		return false, "", err
	}
	return true, ddlPhysicalFingerprint(columns), nil
}

func expectedDDLPhysicalFingerprint(ctx context.Context, queryer ddlQueryer, fields []domain.Field) (string, error) {
	columns := make([]ddlPhysicalColumn, 0, len(fields))
	for _, field := range fields {
		physicalType, err := duckDBType(field)
		if err != nil {
			return "", err
		}
		var normalized string
		if err := queryer.QueryRowContext(ctx, "SELECT typeof(CAST(NULL AS "+physicalType+"))").Scan(&normalized); err != nil {
			return "", err
		}
		columns = append(columns, ddlPhysicalColumn{
			name: field.Name, typeName: normalized, nullable: !strings.EqualFold(field.Mode, "REQUIRED"),
		})
	}
	return ddlPhysicalFingerprint(columns), nil
}

func ddlPhysicalFingerprint(columns []ddlPhysicalColumn) string {
	digest := sha256.New()
	for _, column := range columns {
		fmt.Fprintf(digest, "%d:%s\x00%d:%s\x00%t\n", len(column.name), column.name,
			len(column.typeName), strings.ToUpper(column.typeName), column.nullable)
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil))
}

func previousDDLMarker(target domain.TableReference, generation uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"%d:%s\x00%d:%s\x00%d:%s\x00%d", len(target.ProjectID), target.ProjectID,
		len(target.DatasetID), target.DatasetID, len(target.TableID), target.TableID, generation,
	)))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func inspectDDLStateForProof(
	ctx context.Context,
	queryer ddlQueryer,
	target domain.TableReference,
	schema []domain.Field,
	expected engine.PhysicalTableState,
) (engine.PhysicalTableState, error) {
	exists, fingerprint, err := inspectDDLTable(ctx, queryer, target)
	if err != nil {
		return engine.PhysicalTableState{}, sanitizedDDLBackendError(ctx, err)
	}
	if exists != expected.Exists() {
		return engine.PhysicalTableState{}, fmt.Errorf("%w: physical DDL state changed after planning", domain.ErrPrecondition)
	}
	if exists {
		expectedFingerprint, err := expectedDDLPhysicalFingerprint(ctx, queryer, schema)
		if err != nil {
			return engine.PhysicalTableState{}, sanitizedDDLBackendError(ctx, err)
		}
		if fingerprint != expectedFingerprint || fingerprint != expected.PhysicalShapeFingerprint() {
			return engine.PhysicalTableState{}, fmt.Errorf("%w: physical DDL shape changed after planning", domain.ErrPrecondition)
		}
	}
	return engine.NewPhysicalTableState(engine.PhysicalTableStateDescriptor{
		Target: target, Exists: exists, Generation: expected.Generation(),
		LogicalShapeFingerprint: expected.LogicalShapeFingerprint(), PhysicalShapeFingerprint: fingerprint,
		MarkerFingerprint: expected.MarkerFingerprint(), Provenance: expected.Provenance(),
	})
}

func tableMutationStatement(mutation engine.TableMutation) (string, error) {
	tableName := quotedDDLTable(mutation.Target())
	switch mutation.Kind() {
	case engine.TableMutationCreate:
		return ddlCreateTableStatement(mutation.Target(), mutation.AfterSchema())
	case engine.TableMutationDrop:
		return "DROP TABLE " + tableName, nil
	}
	changes := mutation.FieldChanges()
	if len(changes) != 1 || len(changes[0].Path()) != 1 {
		return "", fmt.Errorf("%w: DuckDB DDL supports one top-level field change", domain.ErrUnsupported)
	}
	change := changes[0]
	switch mutation.Kind() {
	case engine.TableMutationAddColumn:
		field := change.After()
		physicalType, err := duckDBType(field)
		if err != nil {
			return "", err
		}
		statement := "ALTER TABLE " + tableName + " ADD COLUMN " + quoteIdentifier(field.Name) + " " + physicalType
		if strings.EqualFold(field.Mode, "REQUIRED") {
			statement += " NOT NULL"
		}
		return statement, nil
	case engine.TableMutationDropColumn:
		return "ALTER TABLE " + tableName + " DROP COLUMN " + quoteIdentifier(change.Before().Name), nil
	case engine.TableMutationRenameColumn:
		return "ALTER TABLE " + tableName + " RENAME COLUMN " + quoteIdentifier(change.Before().Name) +
			" TO " + quoteIdentifier(change.After().Name), nil
	case engine.TableMutationChangeColumnType:
		physicalType, err := duckDBType(change.After())
		if err != nil {
			return "", err
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + quoteIdentifier(change.Before().Name) +
			" TYPE " + physicalType, nil
	default:
		return "", fmt.Errorf("%w: DuckDB DDL mutation is unsupported", domain.ErrUnsupported)
	}
}

func ddlCreateTableStatement(target domain.TableReference, fields []domain.Field) (string, error) {
	columns := make([]string, 0, len(fields))
	for _, field := range fields {
		physicalType, err := duckDBType(field)
		if err != nil {
			return "", err
		}
		column := quoteIdentifier(field.Name) + " " + physicalType
		if strings.EqualFold(field.Mode, "REQUIRED") {
			column += " NOT NULL"
		}
		columns = append(columns, column)
	}
	return "CREATE TABLE " + quotedDDLTable(target) + " (" + strings.Join(columns, ", ") + ")", nil
}

func quotedDDLTable(target domain.TableReference) string {
	return quoteIdentifier(physicalSchema(target.ProjectID, target.DatasetID)) + "." + quoteIdentifier(target.TableID)
}

func sanitizedDDLMutationError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: engine rejected the planned table mutation; capability=%s",
		domain.ErrInvalid, domain.GapQueryDDLCatalogSyncV1)
}

func sanitizedDDLBackendError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: engine could not apply the planned table mutation; capability=%s",
		domain.ErrPrecondition, domain.GapQueryDDLCatalogSyncV1)
}
