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
)

type readSessionRepository struct{ db *sql.DB }

var _ readports.SessionStateRepository = (*readSessionRepository)(nil)

func (r *readSessionRepository) CreateSession(ctx context.Context, record readdomain.SessionRecord) error {
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

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Read session create: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO bqemu_read_sessions (
    session_name, table_reference, data_format, selected_fields_json,
    row_restriction_digest, row_restriction_bytes, stream_count,
    created_at_ns, expires_at_ns, snapshot_time_ns, retained_row_count,
    retained_bytes, estimated_bytes_scanned, schema_fingerprint,
    lifecycle_state, lifecycle_updated_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Name, record.Table, record.Format.String(), string(selectedFieldsJSON),
		record.RowRestrictionDigest, record.RowRestrictionBytes, len(record.Streams),
		record.CreatedAt.UTC().UnixNano(), record.ExpireTime.UTC().UnixNano(), nullableUnixNano(record.SnapshotTime),
		record.RetainedRowCount, record.RetainedBytes, record.EstimatedBytesScanned,
		record.SchemaFingerprint, string(record.Lifecycle), record.LifecycleUpdatedAt.UTC().UnixNano(),
	)
	if err != nil {
		return translateReadSessionError(err)
	}
	for index, stream := range record.Streams {
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_read_streams
    (stream_name, session_name, stream_index, start_offset, end_offset)
    VALUES (?, ?, ?, ?, ?)`,
			stream.Name, record.Name, index, stream.StartOffset, stream.EndOffset,
		); err != nil {
			return translateReadSessionError(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return translateReadSessionError(err)
	}
	return nil
}

func (r *readSessionRepository) TransitionSessions(
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
		return readports.ErrSessionStateConflict
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return readports.ErrSessionStateConflict
		}
		if _, duplicate := seen[name]; duplicate {
			return readports.ErrSessionStateConflict
		}
		seen[name] = struct{}{}
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Read lifecycle transition: %w", err)
	}
	defer tx.Rollback()
	for _, name := range names {
		result, err := tx.ExecContext(ctx, `UPDATE bqemu_read_sessions
SET lifecycle_state = ?, lifecycle_updated_at_ns = max(lifecycle_updated_at_ns, created_at_ns, ?)
WHERE session_name = ? AND lifecycle_state = ?`,
			string(to), at.UTC().UnixNano(), name, string(from),
		)
		if err != nil {
			return translateReadSessionError(err)
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
			"SELECT lifecycle_state FROM bqemu_read_sessions WHERE session_name = ?", name,
		).Scan(&lifecycle)
		if errors.Is(err, sql.ErrNoRows) {
			return readports.ErrSessionStateNotFound
		}
		if err != nil {
			return fmt.Errorf("read Storage Read lifecycle after conflict: %w", err)
		}
		if lifecycle != string(to) {
			return readports.ErrSessionStateConflict
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Read lifecycle transition: %w", err)
	}
	return nil
}

func (r *readSessionRepository) ReconcileActive(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		return 0, readports.ErrSessionStateConflict
	}
	result, err := r.db.ExecContext(ctx, `UPDATE bqemu_read_sessions
SET lifecycle_state = CASE WHEN expires_at_ns <= ? THEN 'EXPIRED' ELSE 'UNAVAILABLE' END,
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

func (r *readSessionRepository) GetStream(ctx context.Context, name string) (readdomain.PersistedStream, error) {
	var stream readdomain.PersistedStream
	var lifecycle string
	var expiresAt int64
	err := r.db.QueryRowContext(ctx, `SELECT st.stream_name, st.session_name,
    se.lifecycle_state, se.expires_at_ns
FROM bqemu_read_streams st
JOIN bqemu_read_sessions se ON se.session_name = st.session_name
WHERE st.stream_name = ?`, name).Scan(&stream.Name, &stream.Session, &lifecycle, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return readdomain.PersistedStream{}, readports.ErrSessionStateNotFound
	}
	if err != nil {
		return readdomain.PersistedStream{}, fmt.Errorf("get Storage Read stream: %w", err)
	}
	stream.Lifecycle = readdomain.SessionLifecycle(lifecycle)
	stream.ExpiresAt = time.Unix(0, expiresAt).UTC()
	return stream, nil
}

func validateReadSessionRecord(record readdomain.SessionRecord) error {
	if strings.TrimSpace(record.Name) == "" || strings.TrimSpace(record.Table) == "" {
		return readports.ErrSessionStateConflict
	}
	if record.Format != readdomain.FormatArrow && record.Format != readdomain.FormatAvro {
		return readports.ErrSessionStateConflict
	}
	if record.Lifecycle != readdomain.SessionActive || record.CreatedAt.IsZero() ||
		record.ExpireTime.IsZero() || !record.ExpireTime.After(record.CreatedAt) ||
		record.LifecycleUpdatedAt.Before(record.CreatedAt) {
		return readports.ErrSessionStateConflict
	}
	if len(record.Streams) == 0 || record.RetainedRowCount < 0 || record.RetainedBytes < 0 ||
		record.EstimatedBytesScanned < 0 || record.RowRestrictionBytes < 0 {
		return readports.ErrSessionStateConflict
	}
	if !validSHA256Fingerprint(record.RowRestrictionDigest) || !validSHA256Fingerprint(record.SchemaFingerprint) {
		return readports.ErrSessionStateConflict
	}
	for _, field := range record.SelectedFields {
		if strings.TrimSpace(field) == "" {
			return readports.ErrSessionStateConflict
		}
	}
	seen := make(map[string]struct{}, len(record.Streams))
	for index, stream := range record.Streams {
		if !strings.HasPrefix(stream.Name, record.Name+"/streams/") ||
			stream.StartOffset < 0 || stream.EndOffset < stream.StartOffset {
			return readports.ErrSessionStateConflict
		}
		if _, duplicate := seen[stream.Name]; duplicate {
			return readports.ErrSessionStateConflict
		}
		seen[stream.Name] = struct{}{}
		if index > 0 && stream.StartOffset != record.Streams[index-1].EndOffset {
			return readports.ErrSessionStateConflict
		}
	}
	if record.Streams[0].StartOffset != 0 || record.Streams[len(record.Streams)-1].EndOffset != record.RetainedRowCount {
		return readports.ErrSessionStateConflict
	}
	return nil
}

func translateReadSessionError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "constraint") {
		return readports.ErrSessionStateConflict
	}
	return fmt.Errorf("persist Storage Read session state: %w", err)
}

func validSHA256Fingerprint(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[7:] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
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
