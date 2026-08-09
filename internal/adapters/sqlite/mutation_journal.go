package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/leeyh0216/go-bemu/internal/state"
)

var _ state.MutationJournal = (*Store)(nil)

const mutationColumns = "mutation_id, resource_key, mutation_kind, " +
	"expected_canonical_revision, before_physical_fingerprint, " +
	"after_physical_fingerprint, canonical_before_json, canonical_after_json, " +
	"state, failure_code, failure_digest, " +
	"prepared_at, updated_at, completed_at"

func (s *Store) Begin(ctx context.Context, intent state.BeginMutation) (state.Mutation, error) {
	if err := intent.Validate(); err != nil {
		return state.Mutation{}, err
	}
	canonicalBefore, err := json.Marshal(intent.TableChange.Before)
	if err != nil {
		return state.Mutation{}, fmt.Errorf("encode mutation before table: %w", err)
	}
	canonicalAfter, err := json.Marshal(intent.TableChange.After)
	if err != nil {
		return state.Mutation{}, fmt.Errorf("encode mutation after table: %w", err)
	}
	preparedAt := intent.PreparedAt.UTC()
	result, err := s.db.ExecContext(ctx, "INSERT INTO mutation_journal ("+
		"mutation_id, resource_key, mutation_kind, expected_canonical_revision, "+
		"before_physical_fingerprint, after_physical_fingerprint, "+
		"canonical_before_json, canonical_after_json, state, "+
		"failure_code, failure_digest, prepared_at, updated_at, completed_at"+
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PREPARED', '', '', ?, ?, NULL) "+
		"ON CONFLICT(mutation_id) DO NOTHING",
		intent.ID, intent.ResourceKey, string(intent.Kind), intent.ExpectedCanonicalRevision,
		intent.BeforePhysicalFingerprint, intent.AfterPhysicalFingerprint,
		string(canonicalBefore), string(canonicalAfter),
		encodeJournalTime(preparedAt), encodeJournalTime(preparedAt),
	)
	if err != nil {
		return state.Mutation{}, fmt.Errorf("begin state mutation: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return state.Mutation{}, fmt.Errorf("inspect state mutation insert: %w", err)
	}
	record, err := s.Get(ctx, intent.ID)
	if err != nil {
		return state.Mutation{}, err
	}
	if affected == 0 && !record.SameIntent(intent) {
		return state.Mutation{}, state.ErrConflict
	}
	return record, nil
}

func (s *Store) Get(ctx context.Context, mutationID string) (state.Mutation, error) {
	if !validMutationID(mutationID) {
		return state.Mutation{}, fmt.Errorf("%w: mutation_id", state.ErrInvalid)
	}
	record, err := scanMutation(s.db.QueryRowContext(ctx,
		"SELECT "+mutationColumns+" FROM mutation_journal WHERE mutation_id = ?", mutationID))
	if errors.Is(err, sql.ErrNoRows) {
		return state.Mutation{}, state.ErrNotFound
	}
	if err != nil {
		return state.Mutation{}, fmt.Errorf("get state mutation: %w", err)
	}
	return record, nil
}

func (s *Store) ListPending(ctx context.Context, limit int) ([]state.Mutation, error) {
	if limit < 1 || limit > state.MaxPendingList {
		return nil, fmt.Errorf("%w: pending_limit", state.ErrInvalid)
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+mutationColumns+
		" FROM mutation_journal WHERE state = 'PREPARED' "+
		"ORDER BY prepared_at, mutation_id LIMIT ?", limit)
	if err != nil {
		return nil, fmt.Errorf("list pending state mutations: %w", err)
	}
	defer rows.Close()
	result := make([]state.Mutation, 0)
	for rows.Next() {
		record, err := scanMutation(rows)
		if err != nil {
			return nil, fmt.Errorf("scan pending state mutation: %w", err)
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending state mutations: %w", err)
	}
	return result, nil
}

func (s *Store) MarkFailed(
	ctx context.Context,
	mutationID string,
	failure state.Failure,
	completedAt time.Time,
) (state.Mutation, error) {
	if !validMutationID(mutationID) {
		return state.Mutation{}, fmt.Errorf("%w: mutation_id", state.ErrInvalid)
	}
	if err := failure.Validate(); err != nil {
		return state.Mutation{}, err
	}
	if completedAt.IsZero() {
		return state.Mutation{}, fmt.Errorf("%w: completed_at", state.ErrInvalid)
	}
	return s.markFailed(ctx, mutationID, failure, completedAt.UTC())
}

func (s *Store) markFailed(
	ctx context.Context,
	mutationID string,
	failure state.Failure,
	completedAt time.Time,
) (state.Mutation, error) {
	current, err := s.Get(ctx, mutationID)
	if err != nil {
		return state.Mutation{}, err
	}
	if current.State != state.MutationPrepared {
		return state.Mutation{}, state.ErrInvalidTransition
	}
	if completedAt.Before(current.PreparedAt) {
		return state.Mutation{}, fmt.Errorf("%w: completed_at", state.ErrInvalid)
	}

	result, err := s.db.ExecContext(ctx, "UPDATE mutation_journal "+
		"SET state = ?, failure_code = ?, failure_digest = ?, updated_at = ?, completed_at = ? "+
		"WHERE mutation_id = ? AND state = 'PREPARED'",
		string(state.MutationFailed), failure.Code, failure.Digest, encodeJournalTime(completedAt),
		encodeJournalTime(completedAt), mutationID,
	)
	if err != nil {
		return state.Mutation{}, fmt.Errorf("transition state mutation: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return state.Mutation{}, fmt.Errorf("inspect state mutation transition: %w", err)
	}
	if affected != 1 {
		return state.Mutation{}, state.ErrInvalidTransition
	}
	return s.Get(ctx, mutationID)
}

func scanMutation(scanner rowScanner) (state.Mutation, error) {
	var record state.Mutation
	var kind, mutationState, preparedAt, updatedAt string
	var canonicalBefore, canonicalAfter string
	var completedAt sql.NullString
	if err := scanner.Scan(
		&record.ID, &record.ResourceKey, &kind, &record.ExpectedCanonicalRevision,
		&record.BeforePhysicalFingerprint, &record.AfterPhysicalFingerprint,
		&canonicalBefore, &canonicalAfter,
		&mutationState, &record.Failure.Code, &record.Failure.Digest,
		&preparedAt, &updatedAt, &completedAt,
	); err != nil {
		return state.Mutation{}, err
	}
	record.Kind = state.MutationKind(kind)
	record.State = state.MutationState(mutationState)
	if err := json.Unmarshal([]byte(canonicalBefore), &record.TableChange.Before); err != nil {
		return state.Mutation{}, errors.New("decode state mutation before table")
	}
	if err := json.Unmarshal([]byte(canonicalAfter), &record.TableChange.After); err != nil {
		return state.Mutation{}, errors.New("decode state mutation after table")
	}
	var err error
	if record.PreparedAt, err = decodeJournalTime(preparedAt); err != nil {
		return state.Mutation{}, errors.New("decode state mutation prepared time")
	}
	if record.UpdatedAt, err = decodeJournalTime(updatedAt); err != nil {
		return state.Mutation{}, errors.New("decode state mutation updated time")
	}
	if completedAt.Valid {
		if record.CompletedAt, err = decodeJournalTime(completedAt.String); err != nil {
			return state.Mutation{}, errors.New("decode state mutation completed time")
		}
	}
	if err := record.Validate(); err != nil {
		return state.Mutation{}, errors.New("persisted state mutation is invalid")
	}
	return record, nil
}

func validMutationID(value string) bool {
	if value == "" || len(value) > state.MaxMutationIDBytes {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func encodeJournalTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func decodeJournalTime(value string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04:05.000000000Z", value)
}
