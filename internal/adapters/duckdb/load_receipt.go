package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

const loadReceiptSchema = "_bqemu_load"

type loadReceipt struct {
	PlanFingerprint string
	Result          loadports.LoadResult
}

func (w *Warehouse) InspectLoadMutation(
	ctx context.Context,
	mutationID string,
) (loadports.LoadMutationReceipt, bool, error) {
	if !loadDomain.ValidLoadMutationID(mutationID) {
		return loadports.LoadMutationReceipt{}, false, fmt.Errorf("%w: invalid load mutation identity", loadDomain.ErrInvalid)
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return loadports.LoadMutationReceipt{}, false, fmt.Errorf("begin load mutation inspection: %w", err)
	}
	defer tx.Rollback()
	if err := ensureLoadReceiptStore(ctx, tx); err != nil {
		return loadports.LoadMutationReceipt{}, false, err
	}
	receipt, found, err := readLoadReceipt(ctx, tx, mutationID)
	if err != nil {
		return loadports.LoadMutationReceipt{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return loadports.LoadMutationReceipt{}, false, fmt.Errorf("commit load mutation inspection: %w", err)
	}
	if !found {
		return loadports.LoadMutationReceipt{}, false, nil
	}
	return loadports.LoadMutationReceipt{
		PlanFingerprint: receipt.PlanFingerprint,
		Result:          receipt.Result,
	}, true, nil
}

func ensureLoadReceiptStore(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdentifier(loadReceiptSchema)); err != nil {
		return fmt.Errorf("create load receipt schema: %w", err)
	}
	statement := `CREATE TABLE IF NOT EXISTS ` + quoteIdentifier(loadReceiptSchema) + `.` + quoteIdentifier("receipts") + ` (
    mutation_id VARCHAR PRIMARY KEY,
    plan_fingerprint VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    table_id VARCHAR NOT NULL,
    output_rows BIGINT NOT NULL CHECK (output_rows >= 0),
    created_destination BOOLEAN NOT NULL,
    updated_destination BOOLEAN NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
)`
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("create load receipt table: %w", err)
	}
	return nil
}

func readLoadReceipt(ctx context.Context, tx *sql.Tx, mutationID string) (loadReceipt, bool, error) {
	var receipt loadReceipt
	err := tx.QueryRowContext(ctx, `SELECT plan_fingerprint, output_rows, created_destination, updated_destination
FROM `+quoteIdentifier(loadReceiptSchema)+`.`+quoteIdentifier("receipts")+` WHERE mutation_id = ?`, mutationID).Scan(
		&receipt.PlanFingerprint, &receipt.Result.OutputRows,
		&receipt.Result.CreatedDestination, &receipt.Result.UpdatedDestination,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return loadReceipt{}, false, nil
	}
	if err != nil {
		return loadReceipt{}, false, fmt.Errorf("read load mutation receipt: %w", err)
	}
	return receipt, true, nil
}

func insertLoadReceipt(
	ctx context.Context,
	tx *sql.Tx,
	mutationID, planFingerprint string,
	table loadDomain.TableReference,
	result loadports.LoadResult,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO `+quoteIdentifier(loadReceiptSchema)+`.`+quoteIdentifier("receipts")+`
    (mutation_id, plan_fingerprint, project_id, dataset_id, table_id,
     output_rows, created_destination, updated_destination)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, mutationID, planFingerprint,
		table.ProjectID, table.DatasetID, table.TableID, result.OutputRows,
		result.CreatedDestination, result.UpdatedDestination)
	if err != nil {
		return fmt.Errorf("record load mutation receipt: %w", err)
	}
	return nil
}

func (w *Warehouse) DiscardLoadedTable(
	ctx context.Context,
	mutationID string,
	table loadDomain.TableReference,
) (err error) {
	started := observability.LogSideEffectStart(ctx, "duckdb", "discard_load_mutation",
		"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.TableID,
		"mutation_id", mutationID, "transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "discard_load_mutation", started, err,
			"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.TableID,
			"mutation_id", mutationID, "transaction_mode", "explicit")
	}()
	if !loadDomain.ValidLoadMutationID(mutationID) {
		return fmt.Errorf("%w: invalid load mutation identity", loadDomain.ErrInvalid)
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin load mutation discard: %w", err)
	}
	defer tx.Rollback()
	if err := ensureLoadReceiptStore(ctx, tx); err != nil {
		return err
	}
	destination := quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + "." + quoteIdentifier(table.TableID)
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+destination); err != nil {
		return fmt.Errorf("discard unpublished load destination: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+quoteIdentifier(loadReceiptSchema)+"."+quoteIdentifier("receipts")+" WHERE mutation_id = ?", mutationID); err != nil {
		return fmt.Errorf("discard load mutation receipt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit load mutation discard: %w", err)
	}
	return nil
}
