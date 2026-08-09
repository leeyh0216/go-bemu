package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	writedomain "github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

var _ writeports.StreamRepository = (*Store)(nil)

const storageWriteTimeFormat = "2006-01-02T15:04:05.000000000Z"

func (s *Store) CreateWriteStream(ctx context.Context, record writedomain.StreamRecord, maxPending int64) error {
	if err := validateStorageWriteRecord(record); err != nil {
		return err
	}
	tx, err := s.beginStorageWriteTx(ctx, "create stream")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if record.Stream.Type == writedomain.StreamTypePending && maxPending > 0 {
		var count int64
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM storage_write_streams
			WHERE stream_type = 'PENDING' AND stream_state NOT IN ('COMMITTED', 'FAILED')`).Scan(&count); err != nil {
			return fmt.Errorf("count pending Storage Write streams before creation: %w", err)
		}
		if count >= maxPending {
			return writeports.ErrResourceExhausted
		}
	}
	if err := insertStorageWriteStream(ctx, tx, record); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write stream creation: %w", err)
	}
	return nil
}

func (s *Store) GetWriteStream(ctx context.Context, name string) (writedomain.StreamRecord, error) {
	if s == nil || s.db == nil {
		return writedomain.StreamRecord{}, fmt.Errorf("sqlite state store is not open")
	}
	return scanStorageWriteStream(s.db.QueryRowContext(ctx, storageWriteSelect+" WHERE stream_name = ?", name))
}

func (s *Store) ListWriteStreams(ctx context.Context) ([]writedomain.StreamRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite state store is not open")
	}
	rows, err := s.db.QueryContext(ctx, storageWriteSelect+" ORDER BY stream_name")
	if err != nil {
		return nil, fmt.Errorf("list Storage Write streams: %w", err)
	}
	defer rows.Close()
	result := make([]writedomain.StreamRecord, 0)
	for rows.Next() {
		record, err := scanStorageWriteStream(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Storage Write streams: %w", err)
	}
	return result, nil
}

func (s *Store) CountActivePendingStreams(ctx context.Context) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("sqlite state store is not open")
	}
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM storage_write_streams
		WHERE stream_type = 'PENDING' AND stream_state NOT IN ('COMMITTED', 'FAILED')`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active pending Storage Write streams: %w", err)
	}
	return count, nil
}

func (s *Store) SaveWriteStream(ctx context.Context, expectedRevision int64, record writedomain.StreamRecord) error {
	if err := validateStorageWriteRecord(record); err != nil {
		return err
	}
	tx, err := s.beginStorageWriteTx(ctx, "save stream")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := updateStorageWriteStream(ctx, tx, expectedRevision, record); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write stream update: %w", err)
	}
	return nil
}

func (s *Store) SaveWriteStreams(ctx context.Context, expected map[string]int64, records []writedomain.StreamRecord) error {
	if len(records) == 0 {
		return fmt.Errorf("at least one Storage Write stream is required")
	}
	tx, err := s.beginStorageWriteTx(ctx, "save streams")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		if err := validateStorageWriteRecord(record); err != nil {
			return err
		}
		if _, duplicate := seen[record.Stream.Name]; duplicate {
			return fmt.Errorf("duplicate Storage Write stream %q", record.Stream.Name)
		}
		seen[record.Stream.Name] = struct{}{}
		revision, ok := expected[record.Stream.Name]
		if !ok {
			return fmt.Errorf("expected revision is missing for Storage Write stream %q", record.Stream.Name)
		}
		if err := updateStorageWriteStream(ctx, tx, revision, record); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write stream batch update: %w", err)
	}
	return nil
}

func (s *Store) DeleteWriteStream(ctx context.Context, name string, expectedRevision int64) error {
	tx, err := s.beginStorageWriteTx(ctx, "delete stream")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, "DELETE FROM storage_write_streams WHERE stream_name = ? AND revision = ?", name, expectedRevision)
	if err != nil {
		return fmt.Errorf("delete Storage Write stream: %w", err)
	}
	if err := requireOneStorageWriteRow(result); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write stream deletion: %w", err)
	}
	return nil
}

func (s *Store) PrepareAppend(ctx context.Context, expectedRevision int64, record writedomain.StreamRecord, receipt writedomain.AppendReceipt) error {
	if receipt.State != writedomain.AppendReceiptPrepared {
		return fmt.Errorf("new Storage Write receipt must be PREPARED")
	}
	return s.changeAppend(ctx, "prepare", expectedRevision, record, receipt, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO storage_write_append_receipts (
			stream_name, start_offset, row_count, staged_bytes, schema_fingerprint,
			payload_digest, receipt_state, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, receipt.StreamName, receipt.StartOffset,
			receipt.RowCount, receipt.StagedBytes, receipt.SchemaFingerprint, receipt.PayloadDigest,
			string(receipt.State), encodeStorageWriteTime(receipt.CreatedAt), encodeStorageWriteTime(receipt.UpdatedAt))
		if err != nil {
			if isSQLiteConstraint(err) {
				return fmt.Errorf("%w: append receipt already exists", writeports.ErrReceiptConflict)
			}
			return fmt.Errorf("insert Storage Write append receipt: %w", err)
		}
		return nil
	})
}

func (s *Store) CompleteAppend(ctx context.Context, expectedRevision int64, record writedomain.StreamRecord, receipt writedomain.AppendReceipt) error {
	if receipt.State != writedomain.AppendReceiptApplied {
		return fmt.Errorf("completed Storage Write receipt must be APPLIED")
	}
	return s.changeAppend(ctx, "complete", expectedRevision, record, receipt, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE storage_write_append_receipts
			SET receipt_state = 'APPLIED', updated_at = ?
			WHERE stream_name = ? AND start_offset = ? AND receipt_state = 'PREPARED'
			AND row_count = ? AND staged_bytes = ? AND schema_fingerprint = ? AND payload_digest = ?`,
			encodeStorageWriteTime(receipt.UpdatedAt), receipt.StreamName, receipt.StartOffset,
			receipt.RowCount, receipt.StagedBytes, receipt.SchemaFingerprint, receipt.PayloadDigest)
		if err != nil {
			return fmt.Errorf("apply Storage Write append receipt: %w", err)
		}
		return requireOneStorageWriteReceipt(result)
	})
}

func (s *Store) AbortAppend(ctx context.Context, expectedRevision int64, record writedomain.StreamRecord, receipt writedomain.AppendReceipt) error {
	return s.changeAppend(ctx, "abort", expectedRevision, record, receipt, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM storage_write_append_receipts
			WHERE stream_name = ? AND start_offset = ? AND receipt_state = 'PREPARED'
			AND row_count = ? AND staged_bytes = ? AND schema_fingerprint = ? AND payload_digest = ?`,
			receipt.StreamName, receipt.StartOffset, receipt.RowCount, receipt.StagedBytes,
			receipt.SchemaFingerprint, receipt.PayloadDigest)
		if err != nil {
			return fmt.Errorf("delete prepared Storage Write receipt: %w", err)
		}
		return requireOneStorageWriteReceipt(result)
	})
}

func (s *Store) GetWriteAppendReceipt(ctx context.Context, stream string, offset int64) (writedomain.AppendReceipt, error) {
	if s == nil || s.db == nil {
		return writedomain.AppendReceipt{}, fmt.Errorf("sqlite state store is not open")
	}
	return scanStorageWriteReceipt(s.db.QueryRowContext(ctx, storageWriteReceiptSelect+" WHERE stream_name = ? AND start_offset = ?", stream, offset))
}

func (s *Store) ListWriteAppendReceipts(ctx context.Context, stream string) ([]writedomain.AppendReceipt, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite state store is not open")
	}
	rows, err := s.db.QueryContext(ctx, storageWriteReceiptSelect+" WHERE stream_name = ? ORDER BY start_offset", stream)
	if err != nil {
		return nil, fmt.Errorf("list Storage Write receipts: %w", err)
	}
	defer rows.Close()
	result := make([]writedomain.AppendReceipt, 0)
	for rows.Next() {
		receipt, err := scanStorageWriteReceipt(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Storage Write receipts: %w", err)
	}
	return result, nil
}

func (s *Store) changeAppend(ctx context.Context, action string, expectedRevision int64, record writedomain.StreamRecord, receipt writedomain.AppendReceipt, change func(context.Context, *sql.Tx) error) error {
	if err := validateStorageWriteRecord(record); err != nil {
		return err
	}
	if err := validateStorageWriteReceipt(receipt); err != nil {
		return err
	}
	if receipt.StreamName != record.Stream.Name {
		return fmt.Errorf("Storage Write receipt belongs to another stream")
	}
	tx, err := s.beginStorageWriteTx(ctx, action+" append")
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := change(ctx, tx); err != nil {
		return err
	}
	if err := updateStorageWriteStream(ctx, tx, expectedRevision, record); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write append %s: %w", action, err)
	}
	return nil
}

const storageWriteSelect = `SELECT
	stream_name, project_id, dataset_id, table_id, stream_type, stream_state,
	location, schema_json, table_fingerprint, writer_descriptor, writer_fingerprint,
	row_count, next_offset, created_at, updated_at, committed_at,
	failure_code, failure_digest, operation_kind, operation_token,
	cleanup_state, cleanup_attempts, revision
	FROM storage_write_streams`

const storageWriteReceiptSelect = `SELECT stream_name, start_offset, row_count,
	staged_bytes, schema_fingerprint, payload_digest, receipt_state, created_at, updated_at
	FROM storage_write_append_receipts`

type storageWriteScanner interface{ Scan(...any) error }

func scanStorageWriteStream(scanner storageWriteScanner) (writedomain.StreamRecord, error) {
	var record writedomain.StreamRecord
	var streamType, streamState, schemaJSON, createdAt, updatedAt string
	var committedAt sql.NullString
	var writerDescriptor []byte
	var operation, cleanup string
	err := scanner.Scan(
		&record.Stream.Name, &record.Stream.Parent.ProjectID, &record.Stream.Parent.DatasetID,
		&record.Stream.Parent.TableID, &streamType, &streamState, &record.Stream.Location,
		&schemaJSON, &record.Stream.TableFingerprint, &writerDescriptor,
		&record.Stream.SchemaFingerprint, &record.Stream.RowCount, &record.Stream.NextOffset,
		&createdAt, &updatedAt, &committedAt, &record.Stream.FailureCode,
		&record.Stream.FailureDigest, &operation, &record.OperationToken, &cleanup,
		&record.CleanupAttempts, &record.Revision,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return writedomain.StreamRecord{}, writeports.ErrStreamNotFound
	}
	if err != nil {
		return writedomain.StreamRecord{}, fmt.Errorf("scan Storage Write stream: %w", err)
	}
	record.Stream.Type = writedomain.StreamType(streamType)
	record.Stream.State = writedomain.StreamState(streamState)
	record.Operation = writedomain.StreamOperation(operation)
	record.CleanupState = writedomain.CleanupState(cleanup)
	record.WriterDescriptor = append([]byte(nil), writerDescriptor...)
	if err := json.Unmarshal([]byte(schemaJSON), &record.Stream.Schema); err != nil {
		return writedomain.StreamRecord{}, fmt.Errorf("decode Storage Write schema: %w", err)
	}
	var errTime error
	record.Stream.CreateTime, errTime = decodeStorageWriteTime(createdAt)
	if errTime != nil {
		return writedomain.StreamRecord{}, fmt.Errorf("decode Storage Write creation time: %w", errTime)
	}
	record.Stream.LastActivity, errTime = decodeStorageWriteTime(updatedAt)
	if errTime != nil {
		return writedomain.StreamRecord{}, fmt.Errorf("decode Storage Write update time: %w", errTime)
	}
	if committedAt.Valid {
		value, err := decodeStorageWriteTime(committedAt.String)
		if err != nil {
			return writedomain.StreamRecord{}, fmt.Errorf("decode Storage Write commit time: %w", err)
		}
		record.Stream.CommitTime = &value
	}
	return record, nil
}

func scanStorageWriteReceipt(scanner storageWriteScanner) (writedomain.AppendReceipt, error) {
	var receipt writedomain.AppendReceipt
	var state, createdAt, updatedAt string
	err := scanner.Scan(&receipt.StreamName, &receipt.StartOffset, &receipt.RowCount,
		&receipt.StagedBytes, &receipt.SchemaFingerprint, &receipt.PayloadDigest,
		&state, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return writedomain.AppendReceipt{}, writeports.ErrReceiptNotFound
	}
	if err != nil {
		return writedomain.AppendReceipt{}, fmt.Errorf("scan Storage Write receipt: %w", err)
	}
	receipt.State = writedomain.AppendReceiptState(state)
	var parseErr error
	receipt.CreatedAt, parseErr = decodeStorageWriteTime(createdAt)
	if parseErr != nil {
		return writedomain.AppendReceipt{}, fmt.Errorf("decode Storage Write receipt creation time: %w", parseErr)
	}
	receipt.UpdatedAt, parseErr = decodeStorageWriteTime(updatedAt)
	if parseErr != nil {
		return writedomain.AppendReceipt{}, fmt.Errorf("decode Storage Write receipt update time: %w", parseErr)
	}
	return receipt, nil
}

func insertStorageWriteStream(ctx context.Context, tx *sql.Tx, record writedomain.StreamRecord) error {
	values, err := storageWriteRecordValues(record)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO storage_write_streams (
		stream_name, project_id, dataset_id, table_id, stream_type, stream_state,
		location, schema_json, table_fingerprint, writer_descriptor, writer_fingerprint,
		row_count, next_offset, created_at, updated_at, committed_at, failure_code,
		failure_digest, operation_kind, operation_token, cleanup_state, cleanup_attempts, revision
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, values...)
	if err != nil {
		if isSQLiteConstraint(err) {
			return fmt.Errorf("%w: %s", writeports.ErrStreamExists, record.Stream.Name)
		}
		return fmt.Errorf("insert Storage Write stream: %w", err)
	}
	return nil
}

func updateStorageWriteStream(ctx context.Context, tx *sql.Tx, expectedRevision int64, record writedomain.StreamRecord) error {
	values, err := storageWriteRecordValues(record)
	if err != nil {
		return err
	}
	// The primary key is immutable; omit the first value from the SET list.
	args := append(values[1:], record.Stream.Name, expectedRevision)
	result, err := tx.ExecContext(ctx, `UPDATE storage_write_streams SET
		project_id = ?, dataset_id = ?, table_id = ?, stream_type = ?, stream_state = ?,
		location = ?, schema_json = ?, table_fingerprint = ?, writer_descriptor = ?, writer_fingerprint = ?,
		row_count = ?, next_offset = ?, created_at = ?, updated_at = ?, committed_at = ?, failure_code = ?,
		failure_digest = ?, operation_kind = ?, operation_token = ?, cleanup_state = ?, cleanup_attempts = ?, revision = ?
		WHERE stream_name = ? AND revision = ?`, args...)
	if err != nil {
		return fmt.Errorf("update Storage Write stream: %w", err)
	}
	return requireOneStorageWriteRow(result)
}

func storageWriteRecordValues(record writedomain.StreamRecord) ([]any, error) {
	schema, err := json.Marshal(record.Stream.Schema)
	if err != nil {
		return nil, fmt.Errorf("encode Storage Write schema: %w", err)
	}
	return []any{
		record.Stream.Name, record.Stream.Parent.ProjectID, record.Stream.Parent.DatasetID,
		record.Stream.Parent.TableID, string(record.Stream.Type), string(record.Stream.State),
		record.Stream.Location, string(schema), record.Stream.TableFingerprint,
		record.WriterDescriptor, record.Stream.SchemaFingerprint, record.Stream.RowCount,
		record.Stream.NextOffset, encodeStorageWriteTime(record.Stream.CreateTime),
		encodeStorageWriteTime(record.Stream.LastActivity), nullableStorageWriteTime(record.Stream.CommitTime),
		record.Stream.FailureCode, record.Stream.FailureDigest, string(record.Operation),
		record.OperationToken, string(record.CleanupState), record.CleanupAttempts, record.Revision,
	}, nil
}

func validateStorageWriteRecord(record writedomain.StreamRecord) error {
	if _, canonical, isDefault, err := writedomain.ParseStreamName(record.Stream.Name); err != nil || canonical != record.Stream.Name {
		return fmt.Errorf("invalid canonical Storage Write stream name")
	} else if isDefault != (record.Stream.Type == writedomain.StreamTypeDefault) {
		return fmt.Errorf("Storage Write stream name/type mismatch")
	}
	if record.Stream.Parent.Name() == "" || record.Stream.CreateTime.IsZero() || record.Stream.LastActivity.IsZero() {
		return fmt.Errorf("Storage Write stream identity and timestamps are required")
	}
	if record.Stream.TableFingerprint == "" || record.Stream.Location == "" || record.Revision <= 0 {
		return fmt.Errorf("Storage Write stream fingerprint, location, and revision are required")
	}
	if record.Stream.RowCount < 0 || record.Stream.NextOffset != record.Stream.RowCount {
		return fmt.Errorf("Storage Write stream row count/offset invariant is invalid")
	}
	if record.CleanupState == "" {
		return fmt.Errorf("Storage Write cleanup state is required")
	}
	if (record.Operation == writedomain.StreamOperationNone) != (record.OperationToken == "") {
		return fmt.Errorf("Storage Write operation token invariant is invalid")
	}
	return nil
}

func validateStorageWriteReceipt(receipt writedomain.AppendReceipt) error {
	if receipt.StreamName == "" || receipt.StartOffset < 0 || receipt.RowCount <= 0 || receipt.StagedBytes <= 0 {
		return fmt.Errorf("Storage Write receipt identity and counts are invalid")
	}
	if receipt.SchemaFingerprint == "" || receipt.PayloadDigest == "" || receipt.CreatedAt.IsZero() || receipt.UpdatedAt.IsZero() {
		return fmt.Errorf("Storage Write receipt fingerprints and timestamps are required")
	}
	return nil
}

func (s *Store) beginStorageWriteTx(ctx context.Context, operation string) (*sql.Tx, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite state store is not open")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin Storage Write %s transaction: %w", operation, err)
	}
	return tx, nil
}

func requireOneStorageWriteRow(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect Storage Write stream update: %w", err)
	}
	if affected != 1 {
		return writeports.ErrStreamConflict
	}
	return nil
}

func requireOneStorageWriteReceipt(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect Storage Write receipt update: %w", err)
	}
	if affected != 1 {
		return writeports.ErrReceiptConflict
	}
	return nil
}

func encodeStorageWriteTime(value time.Time) string {
	return value.UTC().Format(storageWriteTimeFormat)
}

func decodeStorageWriteTime(value string) (time.Time, error) {
	return time.Parse(storageWriteTimeFormat, value)
}

func nullableStorageWriteTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return encodeStorageWriteTime(*value)
}

func isSQLiteConstraint(err error) bool {
	return err != nil && (errors.Is(err, writeports.ErrStreamExists) ||
		containsFold(err.Error(), "constraint failed") || containsFold(err.Error(), "unique constraint"))
}

func containsFold(value, fragment string) bool {
	if len(fragment) > len(value) {
		return false
	}
	for index := 0; index+len(fragment) <= len(value); index++ {
		match := true
		for offset := range fragment {
			left, right := value[index+offset], fragment[offset]
			if left >= 'A' && left <= 'Z' {
				left += 'a' - 'A'
			}
			if right >= 'A' && right <= 'Z' {
				right += 'a' - 'A'
			}
			if left != right {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
