package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/state"
)

var _ state.CanonicalMutationJournal = (*Store)(nil)

// CommitTableChange publishes the immutable After value from a PREPARED
// journal record and marks that record APPLIED in the same SQLite transaction.
func (s *Store) CommitTableChange(
	ctx context.Context,
	mutationID string,
	completedAt time.Time,
) (state.Mutation, error) {
	if !validMutationID(mutationID) {
		return state.Mutation{}, fmt.Errorf("%w: mutation_id", state.ErrInvalid)
	}
	if completedAt.IsZero() {
		return state.Mutation{}, fmt.Errorf("%w: completed_at", state.ErrInvalid)
	}
	completedAt = completedAt.UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return state.Mutation{}, fmt.Errorf("begin canonical mutation commit: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	record, err := scanMutation(tx.QueryRowContext(ctx,
		"SELECT "+mutationColumns+" FROM mutation_journal WHERE mutation_id = ?", mutationID))
	if errors.Is(err, sql.ErrNoRows) {
		return state.Mutation{}, state.ErrNotFound
	}
	if err != nil {
		return state.Mutation{}, fmt.Errorf("read canonical mutation: %w", err)
	}
	if record.State != state.MutationPrepared {
		return state.Mutation{}, state.ErrInvalidTransition
	}
	if completedAt.Before(record.PreparedAt) {
		return state.Mutation{}, fmt.Errorf("%w: completed_at", state.ErrInvalid)
	}

	before := record.TableChange.Before
	current, err := getTableInTransaction(ctx, tx, before.ProjectID, before.DatasetID, before.ID)
	if err != nil {
		return state.Mutation{}, err
	}
	if state.TableRevision(current) != record.ExpectedCanonicalRevision || !reflect.DeepEqual(current, before) {
		return state.Mutation{}, state.ErrConflict
	}

	after := record.TableChange.After
	result, err := updateTableMetadata(ctx, tx, after)
	if err != nil {
		return state.Mutation{}, err
	}
	if err := requireAffected(result, "canonical mutation table"); err != nil {
		return state.Mutation{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM table_fields
        WHERE project_id = ? AND dataset_id = ? AND table_id = ?`, after.ProjectID, after.DatasetID, after.ID); err != nil {
		return state.Mutation{}, fmt.Errorf("delete canonical mutation fields: %w", err)
	}
	if err := insertFields(ctx, tx, after); err != nil {
		return state.Mutation{}, err
	}

	result, err = tx.ExecContext(ctx, `UPDATE mutation_journal
        SET state = 'APPLIED', failure_code = '', failure_digest = '', updated_at = ?, completed_at = ?
        WHERE mutation_id = ? AND state = 'PREPARED'`,
		encodeJournalTime(completedAt), encodeJournalTime(completedAt), mutationID)
	if err != nil {
		return state.Mutation{}, fmt.Errorf("apply canonical mutation journal transition: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return state.Mutation{}, fmt.Errorf("inspect canonical mutation journal transition: %w", err)
	}
	if affected != 1 {
		return state.Mutation{}, state.ErrInvalidTransition
	}
	if err := tx.Commit(); err != nil {
		return state.Mutation{}, fmt.Errorf("commit canonical mutation: %w", err)
	}
	return s.Get(ctx, mutationID)
}

func getTableInTransaction(
	ctx context.Context,
	tx *sql.Tx,
	projectID, datasetID, tableID string,
) (domain.Table, error) {
	table, err := scanTable(tx.QueryRowContext(ctx, tableSelect+
		` WHERE project_id = ? AND dataset_id = ? AND table_id = ?`, projectID, datasetID, tableID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Table{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Table{}, fmt.Errorf("read canonical mutation table: %w", err)
	}
	table.Schema, err = loadFields(ctx, tx, projectID, datasetID, tableID)
	if err != nil {
		return domain.Table{}, err
	}
	return table, nil
}
