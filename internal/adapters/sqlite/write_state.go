package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

var _ ports.StateRepository = (*writeStateRepository)(nil)

type writeStateRepository struct {
	db *sql.DB
}

func (r *writeStateRepository) ReconcileStartup(ctx context.Context, at time.Time) (domain.StartupSnapshot, error) {
	if r == nil || r.db == nil {
		return domain.StartupSnapshot{}, errors.New("Storage Write state repository is closed")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.StartupSnapshot{}, fmt.Errorf("begin Storage Write startup reconciliation: %w", err)
	}
	defer tx.Rollback()
	updatedAt := at.UTC().UnixNano()
	if _, err := tx.ExecContext(ctx, `UPDATE bqemu_write_append_receipts
SET receipt_phase = 'UNRESOLVED', updated_at_ns = ?
WHERE receipt_phase = 'PREPARED'`, updatedAt); err != nil {
		return domain.StartupSnapshot{}, fmt.Errorf("classify prepared Storage Write receipts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE bqemu_write_commit_groups
SET commit_phase = 'UNRESOLVED', updated_at_ns = ?
WHERE commit_phase = 'PREPARED'`, updatedAt); err != nil {
		return domain.StartupSnapshot{}, fmt.Errorf("classify prepared Storage Write commits: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE bqemu_write_streams
SET operation_phase = 'UNRESOLVED', revision = revision + 1
WHERE operation_phase = 'PREPARED'`); err != nil {
		return domain.StartupSnapshot{}, fmt.Errorf("classify prepared Storage Write streams: %w", err)
	}
	snapshot, err := loadWriteSnapshot(ctx, tx)
	if err != nil {
		return domain.StartupSnapshot{}, err
	}
	if err := validateWriteSnapshot(snapshot); err != nil {
		return domain.StartupSnapshot{}, fmt.Errorf("reconcile Storage Write ledger: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.StartupSnapshot{}, fmt.Errorf("commit Storage Write startup reconciliation: %w", err)
	}
	return snapshot, nil
}

func (r *writeStateRepository) CreateStream(ctx context.Context, record domain.StreamRecord) error {
	if err := validateWriteStreamRecord(record); err != nil {
		return err
	}
	values, err := writeStreamValues(record)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO bqemu_write_streams (
    stream_name, project_id, dataset_id, table_id, stream_type, stream_state,
    create_time_ns, commit_time_ns, location, schema_json, row_count, next_offset,
    writer_schema_fingerprint, last_activity_ns, operation_kind, operation_phase,
    operation_token, cleanup_phase, cleanup_attempts, revision
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, values...)
	if err != nil {
		if sqliteConstraint(err) {
			return fmt.Errorf("%w: create stream", ports.ErrStateConflict)
		}
		return fmt.Errorf("create Storage Write stream: %w", err)
	}
	return nil
}

func (r *writeStateRepository) GetStream(ctx context.Context, name string) (domain.StreamRecord, error) {
	return scanWriteStreamRow(r.db.QueryRowContext(ctx, writeStreamSelect+" WHERE stream_name = ?", name))
}

func (r *writeStateRepository) UpdateStream(ctx context.Context, expectedRevision int64, record domain.StreamRecord) error {
	if err := validateWriteStreamRecord(record); err != nil {
		return err
	}
	return updateWriteStream(ctx, r.db, expectedRevision, record)
}

func (r *writeStateRepository) DeleteStream(ctx context.Context, name string, expectedRevision int64) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM bqemu_write_streams
WHERE stream_name = ? AND revision = ? AND operation_kind = 'NONE'`, name, expectedRevision)
	if err != nil {
		if sqliteConstraint(err) {
			return fmt.Errorf("%w: stream has retained commit metadata", ports.ErrStateConflict)
		}
		return fmt.Errorf("delete Storage Write stream: %w", err)
	}
	return requireWriteRow(result, ports.ErrStateConflict)
}

func (r *writeStateRepository) PrepareAppend(
	ctx context.Context,
	expectedRevision int64,
	record domain.StreamRecord,
	receipt domain.AppendReceipt,
) error {
	if record.Operation != domain.OperationAppend || record.OperationPhase != domain.OperationPhasePrepared ||
		receipt.Phase != domain.ReceiptPrepared {
		return errors.New("Storage Write append preparation requires PREPARED stream and receipt state")
	}
	if err := validateAppendChange(record, receipt); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Write append preparation: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO bqemu_write_append_receipts (
    stream_name, start_offset, row_count, schema_fingerprint, payload_digest,
    receipt_phase, created_at_ns, updated_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, receipt.StreamName, receipt.StartOffset,
		receipt.RowCount, receipt.SchemaFingerprint, receipt.PayloadDigest, string(receipt.Phase),
		receipt.CreatedAt.UTC().UnixNano(), receipt.UpdatedAt.UTC().UnixNano())
	if err != nil {
		if sqliteConstraint(err) {
			existing, getErr := scanWriteReceipt(tx.QueryRowContext(ctx,
				writeReceiptSelect+" WHERE stream_name = ? AND start_offset = ?", receipt.StreamName, receipt.StartOffset))
			if getErr == nil && sameWriteReceiptIdentity(existing, receipt) &&
				(existing.Phase == domain.ReceiptPrepared || existing.Phase == domain.ReceiptUnresolved) {
				return ports.ErrExactReceipt
			}
			return fmt.Errorf("%w: offset %d", ports.ErrReceiptConflict, receipt.StartOffset)
		}
		return fmt.Errorf("insert Storage Write append receipt: %w", err)
	}
	if err := updateWriteStream(ctx, tx, expectedRevision, record); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write append preparation: %w", err)
	}
	return nil
}

func (r *writeStateRepository) MarkAppendUnresolved(
	ctx context.Context,
	expectedRevision int64,
	record domain.StreamRecord,
	receipt domain.AppendReceipt,
) error {
	if record.OperationPhase != domain.OperationPhaseUnresolved || receipt.Phase != domain.ReceiptUnresolved {
		return errors.New("Storage Write unresolved append requires UNRESOLVED stream and receipt state")
	}
	return r.changeAppend(ctx, "mark unresolved", expectedRevision, record, receipt, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE bqemu_write_append_receipts
SET receipt_phase = 'UNRESOLVED', updated_at_ns = ?
WHERE stream_name = ? AND start_offset = ? AND receipt_phase = 'PREPARED'
  AND row_count = ? AND schema_fingerprint = ? AND payload_digest = ?`,
			receipt.UpdatedAt.UTC().UnixNano(), receipt.StreamName, receipt.StartOffset,
			receipt.RowCount, receipt.SchemaFingerprint, receipt.PayloadDigest)
		if err != nil {
			return fmt.Errorf("mark Storage Write append unresolved: %w", err)
		}
		return requireWriteRow(result, ports.ErrReceiptConflict)
	})
}

func (r *writeStateRepository) CompleteAppend(
	ctx context.Context,
	expectedRevision int64,
	record domain.StreamRecord,
	receipt domain.AppendReceipt,
) error {
	if record.Operation != domain.OperationNone || receipt.Phase != domain.ReceiptApplied {
		return errors.New("Storage Write append completion requires settled stream and APPLIED receipt state")
	}
	return r.changeAppend(ctx, "complete", expectedRevision, record, receipt, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE bqemu_write_append_receipts
SET receipt_phase = 'APPLIED', updated_at_ns = ?
WHERE stream_name = ? AND start_offset = ? AND receipt_phase IN ('PREPARED', 'UNRESOLVED')
  AND row_count = ? AND schema_fingerprint = ? AND payload_digest = ?`,
			receipt.UpdatedAt.UTC().UnixNano(), receipt.StreamName, receipt.StartOffset,
			receipt.RowCount, receipt.SchemaFingerprint, receipt.PayloadDigest)
		if err != nil {
			return fmt.Errorf("complete Storage Write append receipt: %w", err)
		}
		return requireWriteRow(result, ports.ErrReceiptConflict)
	})
}

func (r *writeStateRepository) AbortAppend(
	ctx context.Context,
	expectedRevision int64,
	record domain.StreamRecord,
	receipt domain.AppendReceipt,
) error {
	if record.Operation != domain.OperationNone {
		return errors.New("Storage Write append abort requires a settled stream")
	}
	return r.changeAppend(ctx, "abort", expectedRevision, record, receipt, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM bqemu_write_append_receipts
WHERE stream_name = ? AND start_offset = ? AND receipt_phase IN ('PREPARED', 'UNRESOLVED')
  AND row_count = ? AND schema_fingerprint = ? AND payload_digest = ?`,
			receipt.StreamName, receipt.StartOffset, receipt.RowCount,
			receipt.SchemaFingerprint, receipt.PayloadDigest)
		if err != nil {
			return fmt.Errorf("abort Storage Write append receipt: %w", err)
		}
		return requireWriteRow(result, ports.ErrReceiptConflict)
	})
}

func (r *writeStateRepository) changeAppend(
	ctx context.Context,
	action string,
	expectedRevision int64,
	record domain.StreamRecord,
	receipt domain.AppendReceipt,
	change func(*sql.Tx) error,
) error {
	if err := validateAppendChange(record, receipt); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Write append %s: %w", action, err)
	}
	defer tx.Rollback()
	if err := change(tx); err != nil {
		return err
	}
	if err := updateWriteStream(ctx, tx, expectedRevision, record); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write append %s: %w", action, err)
	}
	return nil
}

func (r *writeStateRepository) PrepareCommit(
	ctx context.Context,
	expected map[string]int64,
	records []domain.StreamRecord,
	group domain.CommitGroup,
) error {
	if group.Phase != domain.CommitPrepared {
		return errors.New("new Storage Write commit group must be PREPARED")
	}
	if err := validateCommitChange(records, group, domain.OperationPhasePrepared); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Write commit preparation: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO bqemu_write_commit_groups (
    group_id, parent_reference, member_count, expected_row_count, commit_phase,
    created_at_ns, updated_at_ns, commit_time_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)`, group.ID, group.Parent.Name(), len(group.Members),
		group.ExpectedRowCount, string(group.Phase), group.CreatedAt.UTC().UnixNano(), group.UpdatedAt.UTC().UnixNano())
	if err != nil {
		if sqliteConstraint(err) {
			return fmt.Errorf("%w: group %s", ports.ErrCommitGroupConflict, group.ID)
		}
		return fmt.Errorf("insert Storage Write commit group: %w", err)
	}
	for index, member := range group.Members {
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_write_commit_members
    (group_id, member_index, stream_name, expected_row_count) VALUES (?, ?, ?, ?)`,
			group.ID, index, member.StreamName, member.ExpectedRowCount); err != nil {
			if sqliteConstraint(err) {
				return fmt.Errorf("%w: commit membership", ports.ErrCommitGroupConflict)
			}
			return fmt.Errorf("insert Storage Write commit member: %w", err)
		}
	}
	if err := updateWriteStreams(ctx, tx, expected, records); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write commit preparation: %w", err)
	}
	return nil
}

func (r *writeStateRepository) MarkCommitUnresolved(
	ctx context.Context,
	expected map[string]int64,
	records []domain.StreamRecord,
	group domain.CommitGroup,
) error {
	if group.Phase != domain.CommitUnresolved {
		return errors.New("Storage Write unresolved commit group must be UNRESOLVED")
	}
	return r.changeCommit(ctx, "mark unresolved", expected, records, group,
		domain.OperationPhaseUnresolved, []domain.CommitPhase{domain.CommitPrepared})
}

func (r *writeStateRepository) CompleteCommit(
	ctx context.Context,
	expected map[string]int64,
	records []domain.StreamRecord,
	group domain.CommitGroup,
) error {
	if group.Phase != domain.CommitApplied || group.CommitTime == nil {
		return errors.New("completed Storage Write commit group must be APPLIED with a commit time")
	}
	return r.changeCommit(ctx, "complete", expected, records, group,
		domain.OperationPhaseNone, []domain.CommitPhase{domain.CommitPrepared, domain.CommitUnresolved})
}

func (r *writeStateRepository) AbortCommit(
	ctx context.Context,
	expected map[string]int64,
	records []domain.StreamRecord,
	group domain.CommitGroup,
) error {
	if group.Phase != domain.CommitAborted || group.CommitTime != nil {
		return errors.New("aborted Storage Write commit group must be ABORTED without a commit time")
	}
	return r.changeCommit(ctx, "abort", expected, records, group,
		domain.OperationPhaseNone, []domain.CommitPhase{domain.CommitPrepared, domain.CommitUnresolved})
}

func (r *writeStateRepository) changeCommit(
	ctx context.Context,
	action string,
	expected map[string]int64,
	records []domain.StreamRecord,
	group domain.CommitGroup,
	recordPhase domain.OperationPhase,
	from []domain.CommitPhase,
) error {
	if err := validateCommitChange(records, group, recordPhase); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Write commit %s: %w", action, err)
	}
	defer tx.Rollback()
	placeholders := make([]string, len(from))
	args := []any{string(group.Phase), group.UpdatedAt.UTC().UnixNano(), writeNullableUnixNano(group.CommitTime), group.ID}
	for index, phase := range from {
		placeholders[index] = "?"
		args = append(args, string(phase))
	}
	statement := `UPDATE bqemu_write_commit_groups
SET commit_phase = ?, updated_at_ns = ?, commit_time_ns = ?
WHERE group_id = ? AND commit_phase IN (` + strings.Join(placeholders, ", ") + `)`
	result, err := tx.ExecContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("%s Storage Write commit group: %w", action, err)
	}
	if err := requireWriteRow(result, ports.ErrCommitGroupConflict); err != nil {
		return err
	}
	if err := updateWriteStreams(ctx, tx, expected, records); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write commit %s: %w", action, err)
	}
	return nil
}

const writeStreamSelect = `SELECT stream_name, project_id, dataset_id, table_id,
stream_type, stream_state, create_time_ns, commit_time_ns, location, schema_json,
row_count, next_offset, writer_schema_fingerprint, last_activity_ns,
operation_kind, operation_phase, operation_token, cleanup_phase,
cleanup_attempts, revision FROM bqemu_write_streams`

const writeReceiptSelect = `SELECT stream_name, start_offset, row_count,
schema_fingerprint, payload_digest, receipt_phase, created_at_ns, updated_at_ns
FROM bqemu_write_append_receipts`

type writeScanner interface {
	Scan(...any) error
}

type writeExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// Keep integer timestamp holders explicit without weakening the domain type.
func scanWriteStreamRow(scanner writeScanner) (domain.StreamRecord, error) {
	var record domain.StreamRecord
	var streamType, streamState, schemaJSON string
	var createTime, lastActivity int64
	var commitTime sql.NullInt64
	var operation, operationPhase, cleanupPhase string
	err := scanner.Scan(&record.Stream.Name, &record.Stream.Parent.ProjectID,
		&record.Stream.Parent.DatasetID, &record.Stream.Parent.TableID, &streamType,
		&streamState, &createTime, &commitTime, &record.Stream.Location, &schemaJSON,
		&record.Stream.RowCount, &record.Stream.NextOffset,
		&record.Stream.SchemaFingerprint, &lastActivity, &operation, &operationPhase,
		&record.OperationToken, &cleanupPhase, &record.CleanupAttempts, &record.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.StreamRecord{}, ports.ErrStateNotFound
	}
	if err != nil {
		return domain.StreamRecord{}, fmt.Errorf("scan Storage Write stream: %w", err)
	}
	record.Stream.Type = domain.StreamType(streamType)
	record.Stream.State = domain.StreamState(streamState)
	record.Stream.CreateTime = time.Unix(0, createTime).UTC()
	record.Stream.LastActivity = time.Unix(0, lastActivity).UTC()
	return decodeScannedWriteStream(record, commitTime, schemaJSON, operation, operationPhase, cleanupPhase)
}

func decodeScannedWriteStream(
	record domain.StreamRecord,
	commitTime sql.NullInt64,
	schemaJSON, operation, operationPhase, cleanupPhase string,
) (domain.StreamRecord, error) {
	if err := json.Unmarshal([]byte(schemaJSON), &record.Stream.Schema); err != nil {
		return domain.StreamRecord{}, fmt.Errorf("decode Storage Write schema: %w", err)
	}
	if commitTime.Valid {
		value := time.Unix(0, commitTime.Int64).UTC()
		record.Stream.CommitTime = &value
	}
	record.Operation = domain.OperationKind(operation)
	record.OperationPhase = domain.OperationPhase(operationPhase)
	record.CleanupPhase = domain.CleanupPhase(cleanupPhase)
	return record, nil
}

func scanWriteReceipt(scanner writeScanner) (domain.AppendReceipt, error) {
	var receipt domain.AppendReceipt
	var phase string
	var createdAt, updatedAt int64
	err := scanner.Scan(&receipt.StreamName, &receipt.StartOffset, &receipt.RowCount,
		&receipt.SchemaFingerprint, &receipt.PayloadDigest, &phase, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.AppendReceipt{}, ports.ErrStateNotFound
	}
	if err != nil {
		return domain.AppendReceipt{}, fmt.Errorf("scan Storage Write append receipt: %w", err)
	}
	receipt.Phase = domain.ReceiptPhase(phase)
	receipt.CreatedAt = time.Unix(0, createdAt).UTC()
	receipt.UpdatedAt = time.Unix(0, updatedAt).UTC()
	return receipt, nil
}

func writeStreamValues(record domain.StreamRecord) ([]any, error) {
	schemaJSON, err := json.Marshal(record.Stream.Schema)
	if err != nil {
		return nil, fmt.Errorf("encode Storage Write schema: %w", err)
	}
	return []any{
		record.Stream.Name, record.Stream.Parent.ProjectID, record.Stream.Parent.DatasetID,
		record.Stream.Parent.TableID, string(record.Stream.Type), string(record.Stream.State),
		record.Stream.CreateTime.UTC().UnixNano(), writeNullableUnixNano(record.Stream.CommitTime),
		record.Stream.Location, string(schemaJSON), record.Stream.RowCount, record.Stream.NextOffset,
		record.Stream.SchemaFingerprint, record.Stream.LastActivity.UTC().UnixNano(),
		string(record.Operation), string(record.OperationPhase), record.OperationToken,
		string(record.CleanupPhase), record.CleanupAttempts, record.Revision,
	}, nil
}

func updateWriteStream(ctx context.Context, execer writeExecer, expectedRevision int64, record domain.StreamRecord) error {
	values, err := writeStreamValues(record)
	if err != nil {
		return err
	}
	args := append(values[5:], record.Stream.Name, expectedRevision)
	result, err := execer.ExecContext(ctx, `UPDATE bqemu_write_streams SET
    stream_state = ?, create_time_ns = ?, commit_time_ns = ?, location = ?, schema_json = ?,
    row_count = ?, next_offset = ?, writer_schema_fingerprint = ?, last_activity_ns = ?,
    operation_kind = ?, operation_phase = ?, operation_token = ?, cleanup_phase = ?,
    cleanup_attempts = ?, revision = ?
WHERE stream_name = ? AND revision = ?`, args...)
	if err != nil {
		if sqliteConstraint(err) {
			return fmt.Errorf("%w: invalid stream transition", ports.ErrStateConflict)
		}
		return fmt.Errorf("update Storage Write stream: %w", err)
	}
	return requireWriteRow(result, ports.ErrStateConflict)
}

func updateWriteStreams(ctx context.Context, tx *sql.Tx, expected map[string]int64, records []domain.StreamRecord) error {
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		if _, duplicate := seen[record.Stream.Name]; duplicate {
			return fmt.Errorf("%w: duplicate stream in update", ports.ErrStateConflict)
		}
		seen[record.Stream.Name] = struct{}{}
		revision, ok := expected[record.Stream.Name]
		if !ok {
			return fmt.Errorf("%w: expected revision is missing", ports.ErrStateConflict)
		}
		if err := validateWriteStreamRecord(record); err != nil {
			return err
		}
		if err := updateWriteStream(ctx, tx, revision, record); err != nil {
			return err
		}
	}
	return nil
}

func validateWriteStreamRecord(record domain.StreamRecord) error {
	table, canonical, isDefault, err := domain.ParseStreamName(record.Stream.Name)
	if err != nil || canonical != record.Stream.Name || table != record.Stream.Parent {
		return errors.New("invalid canonical Storage Write stream identity")
	}
	if isDefault != (record.Stream.Type == domain.StreamTypeDefault) {
		return errors.New("Storage Write stream name and type do not match")
	}
	if record.Stream.CreateTime.IsZero() || record.Stream.LastActivity.IsZero() ||
		record.Stream.Location == "" || record.Revision <= 0 {
		return errors.New("Storage Write stream timestamps, location, and revision are required")
	}
	if record.Stream.RowCount < 0 || record.Stream.NextOffset != record.Stream.RowCount {
		return errors.New("Storage Write stream offset ledger is invalid")
	}
	for _, field := range record.Stream.Schema.Fields {
		if err := field.Validate(); err != nil {
			return fmt.Errorf("validate Storage Write schema: %w", err)
		}
	}
	if (record.Operation == domain.OperationNone) !=
		(record.OperationPhase == domain.OperationPhaseNone && record.OperationToken == "") {
		return errors.New("Storage Write operation phase/token invariant is invalid")
	}
	if record.Operation != domain.OperationNone &&
		(record.OperationPhase == domain.OperationPhaseNone || record.OperationToken == "") {
		return errors.New("active Storage Write operation requires a phase and token")
	}
	if record.CleanupPhase != domain.CleanupActive && record.CleanupPhase != domain.CleanupPending {
		return errors.New("Storage Write cleanup phase is invalid")
	}
	return nil
}

func validateAppendChange(record domain.StreamRecord, receipt domain.AppendReceipt) error {
	if err := validateWriteStreamRecord(record); err != nil {
		return err
	}
	if receipt.StreamName != record.Stream.Name || receipt.StartOffset < 0 || receipt.RowCount <= 0 ||
		receipt.SchemaFingerprint == "" || receipt.PayloadDigest == "" ||
		receipt.CreatedAt.IsZero() || receipt.UpdatedAt.Before(receipt.CreatedAt) {
		return errors.New("Storage Write append receipt identity is invalid")
	}
	return nil
}

func validateCommitChange(records []domain.StreamRecord, group domain.CommitGroup, recordPhase domain.OperationPhase) error {
	if group.ID == "" || len(group.Members) == 0 || group.CreatedAt.IsZero() || group.UpdatedAt.Before(group.CreatedAt) {
		return errors.New("Storage Write commit group identity is invalid")
	}
	if _, err := domain.ParseTableName(group.Parent.Name()); err != nil {
		return errors.New("Storage Write commit group parent is invalid")
	}
	if len(records) != len(group.Members) {
		return errors.New("Storage Write commit group membership is incomplete")
	}
	var expectedRows int64
	for index, member := range group.Members {
		record := records[index]
		if member.StreamName != record.Stream.Name || member.ExpectedRowCount != record.Stream.RowCount ||
			record.Stream.Parent != group.Parent {
			return errors.New("Storage Write commit member does not match its stream")
		}
		if recordPhase == domain.OperationPhaseNone {
			if record.Operation != domain.OperationNone {
				return errors.New("settled Storage Write commit stream retains an operation")
			}
		} else if record.Operation != domain.OperationCommit || record.OperationPhase != recordPhase || record.OperationToken != group.ID {
			return errors.New("Storage Write commit stream operation does not match its group")
		}
		expectedRows += member.ExpectedRowCount
	}
	if expectedRows != group.ExpectedRowCount {
		return errors.New("Storage Write commit group expected row count is invalid")
	}
	return nil
}

func loadWriteSnapshot(ctx context.Context, queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (domain.StartupSnapshot, error) {
	var snapshot domain.StartupSnapshot
	rows, err := queryer.QueryContext(ctx, writeStreamSelect+" ORDER BY stream_name")
	if err != nil {
		return snapshot, fmt.Errorf("list Storage Write streams: %w", err)
	}
	for rows.Next() {
		record, scanErr := scanWriteStreamRow(rows)
		if scanErr != nil {
			rows.Close()
			return snapshot, scanErr
		}
		snapshot.Streams = append(snapshot.Streams, record)
	}
	if err := closeWriteRows(rows, "Storage Write streams"); err != nil {
		return snapshot, err
	}
	rows, err = queryer.QueryContext(ctx, writeReceiptSelect+" ORDER BY stream_name, start_offset")
	if err != nil {
		return snapshot, fmt.Errorf("list Storage Write receipts: %w", err)
	}
	for rows.Next() {
		receipt, scanErr := scanWriteReceipt(rows)
		if scanErr != nil {
			rows.Close()
			return snapshot, scanErr
		}
		snapshot.Receipts = append(snapshot.Receipts, receipt)
	}
	if err := closeWriteRows(rows, "Storage Write receipts"); err != nil {
		return snapshot, err
	}
	groups, err := loadWriteCommitGroups(ctx, queryer)
	if err != nil {
		return snapshot, err
	}
	snapshot.CommitGroups = groups
	return snapshot, nil
}

func loadWriteCommitGroups(ctx context.Context, queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) ([]domain.CommitGroup, error) {
	rows, err := queryer.QueryContext(ctx, `SELECT group_id, parent_reference, member_count,
expected_row_count, commit_phase, created_at_ns, updated_at_ns, commit_time_ns
FROM bqemu_write_commit_groups ORDER BY group_id`)
	if err != nil {
		return nil, fmt.Errorf("list Storage Write commit groups: %w", err)
	}
	groups := make([]domain.CommitGroup, 0)
	memberCounts := make(map[string]int)
	for rows.Next() {
		var group domain.CommitGroup
		var parent, phase string
		var memberCount int
		var createdAt, updatedAt int64
		var commitTime sql.NullInt64
		if err := rows.Scan(&group.ID, &parent, &memberCount, &group.ExpectedRowCount,
			&phase, &createdAt, &updatedAt, &commitTime); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan Storage Write commit group: %w", err)
		}
		parsedParent, err := domain.ParseTableName(parent)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("decode Storage Write commit parent: %w", err)
		}
		group.Parent = parsedParent
		group.Phase = domain.CommitPhase(phase)
		group.CreatedAt = time.Unix(0, createdAt).UTC()
		group.UpdatedAt = time.Unix(0, updatedAt).UTC()
		if commitTime.Valid {
			value := time.Unix(0, commitTime.Int64).UTC()
			group.CommitTime = &value
		}
		memberCounts[group.ID] = memberCount
		groups = append(groups, group)
	}
	if err := closeWriteRows(rows, "Storage Write commit groups"); err != nil {
		return nil, err
	}
	byID := make(map[string]*domain.CommitGroup, len(groups))
	for index := range groups {
		byID[groups[index].ID] = &groups[index]
	}
	rows, err = queryer.QueryContext(ctx, `SELECT group_id, member_index, stream_name,
expected_row_count FROM bqemu_write_commit_members ORDER BY group_id, member_index`)
	if err != nil {
		return nil, fmt.Errorf("list Storage Write commit members: %w", err)
	}
	for rows.Next() {
		var groupID, streamName string
		var index int
		var expectedRows int64
		if err := rows.Scan(&groupID, &index, &streamName, &expectedRows); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan Storage Write commit member: %w", err)
		}
		group := byID[groupID]
		if group == nil || index != len(group.Members) {
			rows.Close()
			return nil, errors.New("Storage Write commit membership ordering is invalid")
		}
		group.Members = append(group.Members, domain.CommitMember{StreamName: streamName, ExpectedRowCount: expectedRows})
	}
	if err := closeWriteRows(rows, "Storage Write commit members"); err != nil {
		return nil, err
	}
	for _, group := range groups {
		if len(group.Members) != memberCounts[group.ID] {
			return nil, fmt.Errorf("Storage Write commit group %s member count is inconsistent", group.ID)
		}
	}
	return groups, nil
}

func validateWriteSnapshot(snapshot domain.StartupSnapshot) error {
	streams := make(map[string]domain.StreamRecord, len(snapshot.Streams))
	for _, record := range snapshot.Streams {
		if err := validateWriteStreamRecord(record); err != nil {
			return err
		}
		if _, duplicate := streams[record.Stream.Name]; duplicate {
			return errors.New("duplicate Storage Write stream in startup snapshot")
		}
		streams[record.Stream.Name] = record
	}
	receiptsByStream := make(map[string][]domain.AppendReceipt)
	for _, receipt := range snapshot.Receipts {
		if _, exists := streams[receipt.StreamName]; !exists {
			return errors.New("Storage Write receipt references a missing stream")
		}
		receiptsByStream[receipt.StreamName] = append(receiptsByStream[receipt.StreamName], receipt)
	}
	groups := make(map[string]domain.CommitGroup, len(snapshot.CommitGroups))
	for _, group := range snapshot.CommitGroups {
		groups[group.ID] = group
		var total int64
		for _, member := range group.Members {
			record, exists := streams[member.StreamName]
			if !exists || record.Stream.Parent != group.Parent || record.Stream.RowCount != member.ExpectedRowCount {
				return fmt.Errorf("Storage Write commit group %s does not match its stream ledger", group.ID)
			}
			total += member.ExpectedRowCount
		}
		if total != group.ExpectedRowCount {
			return fmt.Errorf("Storage Write commit group %s expected row count is inconsistent", group.ID)
		}
	}
	for name, record := range streams {
		receipts := receiptsByStream[name]
		sort.Slice(receipts, func(i, j int) bool { return receipts[i].StartOffset < receipts[j].StartOffset })
		var appliedOffset int64
		unresolved := 0
		for _, receipt := range receipts {
			switch receipt.Phase {
			case domain.ReceiptApplied:
				if receipt.StartOffset != appliedOffset {
					return fmt.Errorf("Storage Write receipt continuity failed for stream %s", name)
				}
				appliedOffset += receipt.RowCount
			case domain.ReceiptUnresolved:
				unresolved++
				if receipt.StartOffset != record.Stream.NextOffset {
					return fmt.Errorf("Storage Write unresolved receipt offset failed for stream %s", name)
				}
			default:
				return fmt.Errorf("Storage Write startup retained receipt phase %s", receipt.Phase)
			}
		}
		if appliedOffset != record.Stream.RowCount || unresolved > 1 {
			return fmt.Errorf("Storage Write receipt total failed for stream %s", name)
		}
		switch record.Operation {
		case domain.OperationNone:
			if unresolved != 0 {
				return fmt.Errorf("settled Storage Write stream %s retains an unresolved receipt", name)
			}
		case domain.OperationAppend:
			offset, err := strconv.ParseInt(record.OperationToken, 10, 64)
			if err != nil || offset != record.Stream.NextOffset || unresolved != 1 || record.OperationPhase != domain.OperationPhaseUnresolved {
				return fmt.Errorf("Storage Write append reconciliation failed for stream %s", name)
			}
		case domain.OperationCommit:
			group, exists := groups[record.OperationToken]
			if !exists || group.Phase != domain.CommitUnresolved || record.OperationPhase != domain.OperationPhaseUnresolved ||
				!commitGroupContains(group, name) {
				return fmt.Errorf("Storage Write commit reconciliation failed for stream %s", name)
			}
		default:
			return fmt.Errorf("Storage Write stream %s has unknown operation %s", name, record.Operation)
		}
	}
	return nil
}

func commitGroupContains(group domain.CommitGroup, stream string) bool {
	for _, member := range group.Members {
		if member.StreamName == stream {
			return true
		}
	}
	return false
}

func sameWriteReceiptIdentity(left, right domain.AppendReceipt) bool {
	return left.StreamName == right.StreamName && left.StartOffset == right.StartOffset &&
		left.RowCount == right.RowCount && left.SchemaFingerprint == right.SchemaFingerprint &&
		left.PayloadDigest == right.PayloadDigest
}

func requireWriteRow(result sql.Result, conflict error) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect Storage Write compare-and-swap: %w", err)
	}
	if affected != 1 {
		return conflict
	}
	return nil
}

func closeWriteRows(rows *sql.Rows, label string) error {
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close %s: %w", label, err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s: %w", label, err)
	}
	return nil
}

func writeNullableUnixNano(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().UnixNano()
}

func sqliteConstraint(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "constraint")
}
