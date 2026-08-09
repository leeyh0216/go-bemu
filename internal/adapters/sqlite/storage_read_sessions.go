package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
	githubsqlite3 "github.com/mattn/go-sqlite3"
)

var _ readports.SessionStateRepository = (*Store)(nil)

func (s *Store) CreateSession(ctx context.Context, record readdomain.SessionRecord) error {
	if err := validateReadSessionRecord(record); err != nil {
		return err
	}
	selectedFields := slices.Clone(record.SelectedFields)
	if selectedFields == nil {
		selectedFields = []string{}
	}
	selectedFieldsJSON, err := json.Marshal(selectedFields)
	if err != nil {
		return fmt.Errorf("encode Storage Read selected fields: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Read session create: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `INSERT INTO storage_read_sessions (
        session_name, table_reference, data_format, selected_fields_json,
        row_restriction_digest, row_restriction_bytes, filter_predicate_count,
        filter_logical_operator_count, stream_count, created_at_ns, expires_at_ns,
        snapshot_time_ns, retained_row_count, retained_bytes, estimated_bytes_scanned,
        schema_fingerprint, lifecycle_state, lifecycle_updated_at_ns
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Name, record.Table, record.Format.String(), string(selectedFieldsJSON),
		record.RowRestrictionDigest, record.RowRestrictionBytes, record.FilterShape.PredicateCount,
		record.FilterShape.LogicalOperatorCount, len(record.Streams), record.CreatedAt.UnixNano(),
		record.ExpireTime.UnixNano(), nullableUnixNano(record.SnapshotTime), record.RetainedRowCount,
		record.RetainedBytes, record.EstimatedBytesScanned, record.SchemaFingerprint,
		string(record.Lifecycle), record.LifecycleUpdatedAt.UnixNano(),
	)
	if err != nil {
		return translateReadSessionConstraint(err)
	}
	for index, stream := range record.Streams {
		if _, err := tx.ExecContext(ctx, `INSERT INTO storage_read_streams
            (stream_name, session_name, stream_index, start_offset, end_offset)
            VALUES (?, ?, ?, ?, ?)`,
			stream.Name, record.Name, index, stream.StartOffset, stream.EndOffset,
		); err != nil {
			return translateReadSessionConstraint(err)
		}
	}
	var persistedCount int
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM storage_read_streams WHERE session_name = ?", record.Name,
	).Scan(&persistedCount); err != nil {
		return fmt.Errorf("verify Storage Read stream count: %w", err)
	}
	if persistedCount != len(record.Streams) {
		return fmt.Errorf("%w: persisted Storage Read stream count", readports.ErrSessionStateConflict)
	}
	if err := tx.Commit(); err != nil {
		return translateReadSessionConstraint(err)
	}
	return nil
}

func (s *Store) TransitionSessions(
	ctx context.Context,
	names []string,
	from readdomain.SessionLifecycle,
	to readdomain.SessionLifecycle,
	at time.Time,
) error {
	if len(names) == 0 {
		return nil
	}
	if from != readdomain.SessionActive ||
		(to != readdomain.SessionExpired && to != readdomain.SessionUnavailable) || at.IsZero() {
		return fmt.Errorf("%w: Storage Read lifecycle transition", readports.ErrSessionStateConflict)
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%w: empty Storage Read session name", readports.ErrSessionStateConflict)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("%w: duplicate Storage Read session name", readports.ErrSessionStateConflict)
		}
		seen[name] = struct{}{}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Read lifecycle transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, name := range names {
		result, err := tx.ExecContext(ctx, `UPDATE storage_read_sessions
            SET lifecycle_state = ?, lifecycle_updated_at_ns = max(lifecycle_updated_at_ns, created_at_ns, ?)
            WHERE session_name = ? AND lifecycle_state = ?`,
			string(to), at.UTC().UnixNano(), name, string(from),
		)
		if err != nil {
			return translateReadSessionConstraint(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect Storage Read lifecycle transition: %w", err)
		}
		if affected == 1 {
			continue
		}
		var lifecycle string
		err = tx.QueryRowContext(ctx,
			"SELECT lifecycle_state FROM storage_read_sessions WHERE session_name = ?", name,
		).Scan(&lifecycle)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: session %s", readports.ErrSessionStateNotFound, name)
		}
		if err != nil {
			return fmt.Errorf("read Storage Read lifecycle after conflict: %w", err)
		}
		if lifecycle != string(to) {
			return fmt.Errorf("%w: session %s is %s", readports.ErrSessionStateConflict, name, lifecycle)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Read lifecycle transition: %w", err)
	}
	return nil
}

func (s *Store) ReconcileActive(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		return 0, fmt.Errorf("%w: Storage Read reconciliation time", readports.ErrSessionStateConflict)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE storage_read_sessions
        SET lifecycle_state = CASE
                WHEN expires_at_ns <= ? THEN 'EXPIRED'
                ELSE 'UNAVAILABLE'
            END,
            lifecycle_updated_at_ns = max(lifecycle_updated_at_ns, created_at_ns, ?)
        WHERE lifecycle_state = 'ACTIVE'`, now.UTC().UnixNano(), now.UTC().UnixNano())
	if err != nil {
		return 0, fmt.Errorf("reconcile active Storage Read sessions: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect Storage Read reconciliation: %w", err)
	}
	return affected, nil
}

func (s *Store) GetStream(ctx context.Context, name string) (readdomain.PersistedStream, error) {
	var stream readdomain.PersistedStream
	var lifecycle string
	var expiresAtNS int64
	err := s.db.QueryRowContext(ctx, `SELECT
        st.stream_name, st.session_name, se.lifecycle_state, se.expires_at_ns
        FROM storage_read_streams st
        JOIN storage_read_sessions se ON se.session_name = st.session_name
        WHERE st.stream_name = ?`, name).Scan(
		&stream.Name, &stream.Session, &lifecycle, &expiresAtNS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return readdomain.PersistedStream{}, readports.ErrSessionStateNotFound
	}
	if err != nil {
		return readdomain.PersistedStream{}, fmt.Errorf("get Storage Read stream: %w", err)
	}
	stream.Lifecycle = readdomain.SessionLifecycle(lifecycle)
	stream.ExpiresAt = time.Unix(0, expiresAtNS).UTC()
	return stream, nil
}

func validateReadSessionRecord(record readdomain.SessionRecord) error {
	if strings.TrimSpace(record.Name) == "" || strings.TrimSpace(record.Table) == "" {
		return fmt.Errorf("%w: Storage Read session identity", readports.ErrSessionStateConflict)
	}
	if record.Format != readdomain.FormatArrow && record.Format != readdomain.FormatAvro {
		return fmt.Errorf("%w: Storage Read data format", readports.ErrSessionStateConflict)
	}
	if record.Lifecycle != readdomain.SessionActive || record.CreatedAt.IsZero() ||
		record.ExpireTime.IsZero() || !record.ExpireTime.After(record.CreatedAt) ||
		record.LifecycleUpdatedAt.Before(record.CreatedAt) {
		return fmt.Errorf("%w: Storage Read lifecycle metadata", readports.ErrSessionStateConflict)
	}
	if len(record.Streams) == 0 || record.RetainedRowCount < 0 || record.RetainedBytes < 0 ||
		record.EstimatedBytesScanned < 0 || record.RowRestrictionBytes < 0 ||
		record.FilterShape.PredicateCount < 0 || record.FilterShape.LogicalOperatorCount < 0 {
		return fmt.Errorf("%w: Storage Read structural metadata", readports.ErrSessionStateConflict)
	}
	if !validSHA256Fingerprint(record.RowRestrictionDigest) || !validSHA256Fingerprint(record.SchemaFingerprint) {
		return fmt.Errorf("%w: Storage Read fingerprint", readports.ErrSessionStateConflict)
	}
	for _, field := range record.SelectedFields {
		if strings.TrimSpace(field) == "" {
			return fmt.Errorf("%w: empty selected field", readports.ErrSessionStateConflict)
		}
	}
	for index, stream := range record.Streams {
		if !strings.HasPrefix(stream.Name, record.Name+"/streams/") ||
			stream.StartOffset < 0 || stream.EndOffset < stream.StartOffset {
			return fmt.Errorf("%w: stream %d", readports.ErrSessionStateConflict, index)
		}
		if index > 0 && stream.StartOffset != record.Streams[index-1].EndOffset {
			return fmt.Errorf("%w: non-contiguous stream %d", readports.ErrSessionStateConflict, index)
		}
	}
	if record.Streams[0].StartOffset != 0 || record.Streams[len(record.Streams)-1].EndOffset != record.RetainedRowCount {
		return fmt.Errorf("%w: Storage Read stream range", readports.ErrSessionStateConflict)
	}
	return nil
}

func translateReadSessionConstraint(err error) error {
	if err == nil {
		return nil
	}
	var sqliteError githubsqlite3.Error
	if errors.As(err, &sqliteError) && sqliteError.Code == githubsqlite3.ErrConstraint {
		return fmt.Errorf("%w: %v", readports.ErrSessionStateConflict, err)
	}
	return fmt.Errorf("persist Storage Read session state: %w", err)
}

func validSHA256Fingerprint(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[7:] {
		isDigit := character >= '0' && character <= '9'
		isLowerHex := character >= 'a' && character <= 'f'
		if !isDigit && !isLowerHex {
			return false
		}
	}
	return true
}

func nullableUnixNano(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().UnixNano()
}
