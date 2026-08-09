package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type jobKind string

const (
	queryJobKind          jobKind = "QUERY"
	loadJobKind           jobKind = "LOAD"
	interruptedJobMessage         = "job execution was interrupted by emulator restart"
)

var (
	errJobIdentityNotFound = errors.New("sqlite job identity not found")
	errJobKindConflict     = errors.New("sqlite job identity belongs to another job kind")
	errJobStateConflict    = errors.New("sqlite job state transition conflicts with persisted state")
	errJobDetailsNotFound  = errors.New("sqlite job details not found")
)

type persistedJobIdentity struct {
	projectID           string
	location            string
	jobID               string
	kind                jobKind
	configurationDigest string
	state               string
	errorReason         sql.NullString
	errorMessage        sql.NullString
	createdAt           time.Time
	startedAt           *time.Time
	endedAt             *time.Time
}

func insertJobIdentity(
	ctx context.Context,
	tx *sql.Tx,
	identity persistedJobIdentity,
) (bool, error) {
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO job_identities (
		project_id, location_key, location, job_id, job_kind, configuration_digest,
		state, error_reason, error_message, created_at, started_at, ended_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		identity.projectID, jobLocationKey(identity.location), identity.location,
		identity.jobID, string(identity.kind), identity.configurationDigest,
		identity.state, nullableStringValue(identity.errorReason), nullableStringValue(identity.errorMessage),
		encodeJobTime(identity.createdAt), nullableTimeValue(identity.startedAt), nullableTimeValue(identity.endedAt),
	)
	if err != nil {
		return false, fmt.Errorf("insert job identity: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect job identity insert: %w", err)
	}
	return affected == 1, nil
}

func getJobIdentity(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	projectID, location, jobID string,
) (persistedJobIdentity, error) {
	return scanJobIdentity(queryer.QueryRowContext(ctx, `SELECT
		project_id, location, job_id, job_kind, configuration_digest, state,
		error_reason, error_message, created_at, started_at, ended_at
	FROM job_identities
	WHERE project_id = ? AND location_key = ? AND job_id = ?`,
		projectID, jobLocationKey(location), jobID,
	))
}

func scanJobIdentity(scanner rowScanner) (persistedJobIdentity, error) {
	var identity persistedJobIdentity
	var kind, createdAt string
	var startedAt, endedAt sql.NullString
	if err := scanner.Scan(
		&identity.projectID, &identity.location, &identity.jobID, &kind,
		&identity.configurationDigest, &identity.state,
		&identity.errorReason, &identity.errorMessage,
		&createdAt, &startedAt, &endedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return persistedJobIdentity{}, errJobIdentityNotFound
		}
		return persistedJobIdentity{}, err
	}
	identity.kind = jobKind(kind)
	var err error
	identity.createdAt, err = decodeJobTime(createdAt)
	if err != nil {
		return persistedJobIdentity{}, fmt.Errorf("decode job creation time: %w", err)
	}
	identity.startedAt, err = decodeNullableJobTime(startedAt)
	if err != nil {
		return persistedJobIdentity{}, fmt.Errorf("decode job start time: %w", err)
	}
	identity.endedAt, err = decodeNullableJobTime(endedAt)
	if err != nil {
		return persistedJobIdentity{}, fmt.Errorf("decode job end time: %w", err)
	}
	return identity, nil
}

func updateJobIdentity(
	ctx context.Context,
	tx *sql.Tx,
	identity persistedJobIdentity,
) error {
	current, err := getJobIdentity(ctx, tx, identity.projectID, identity.location, identity.jobID)
	if err != nil {
		return err
	}
	if current.kind != identity.kind {
		return errJobKindConflict
	}
	if current.configurationDigest != identity.configurationDigest {
		return errJobStateConflict
	}
	if !validPersistedJobTransition(current.state, identity.state) {
		return errJobStateConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE job_identities SET
		state = ?, error_reason = ?, error_message = ?, started_at = ?, ended_at = ?
	WHERE project_id = ? AND location_key = ? AND job_id = ?
		AND job_kind = ? AND configuration_digest = ? AND state = ?`,
		identity.state, nullableStringValue(identity.errorReason), nullableStringValue(identity.errorMessage),
		nullableTimeValue(identity.startedAt), nullableTimeValue(identity.endedAt),
		identity.projectID, jobLocationKey(identity.location), identity.jobID,
		string(identity.kind), identity.configurationDigest, current.state,
	)
	if err != nil {
		return fmt.Errorf("update job identity: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect job identity update: %w", err)
	}
	if affected != 1 {
		return errJobStateConflict
	}
	return nil
}

func validPersistedJobTransition(current, next string) bool {
	if current == next {
		return true
	}
	return current == "PENDING" && next == "RUNNING" || current == "RUNNING" && next == "DONE"
}

// ReconcileInterruptedJobs converts process-owned work left PENDING or RUNNING
// by a previous process into a stable terminal failure. It must run during
// startup before query/load admission begins; completed resources are immutable
// and are never changed by this operation.
func (s *Store) ReconcileInterruptedJobs(ctx context.Context, now time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("sqlite state store is not open")
	}
	if now.IsZero() {
		return 0, fmt.Errorf("job reconciliation time is required")
	}
	now = now.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin interrupted job reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var latestActivity sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT max(COALESCE(started_at, created_at))
		FROM job_identities WHERE state IN ('PENDING', 'RUNNING')`).Scan(&latestActivity); err != nil {
		return 0, fmt.Errorf("inspect interrupted job times: %w", err)
	}
	if latestActivity.Valid {
		latest, err := decodeJobTime(latestActivity.String)
		if err != nil {
			return 0, fmt.Errorf("decode interrupted job time: %w", err)
		}
		if now.Before(latest) {
			return 0, fmt.Errorf("job reconciliation time precedes persisted job activity")
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE job_identities SET
		state = 'DONE',
		error_reason = 'backendError',
		error_message = ?,
		started_at = COALESCE(started_at, created_at),
		ended_at = ?
	WHERE state IN ('PENDING', 'RUNNING')`, interruptedJobMessage, encodeJobTime(now))
	if err != nil {
		return 0, fmt.Errorf("reconcile interrupted jobs: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect interrupted job reconciliation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit interrupted job reconciliation: %w", err)
	}
	return affected, nil
}

func jobLocationKey(location string) string {
	return strings.ToUpper(location)
}

func validJobID(value string) bool {
	if value == "" || len(value) > 1024 {
		return false
	}
	for _, character := range value {
		if character == '_' || character == '-' || character >= 'A' && character <= 'Z' ||
			character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}

func validJobLocation(value string) bool {
	if value == "" || len(value) > 1024 {
		return false
	}
	for _, character := range value {
		if character == '-' || character >= 'A' && character <= 'Z' ||
			character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}

func encodeJobTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func decodeJobTime(value string) (time.Time, error) {
	parsed, err := time.Parse("2006-01-02T15:04:05.000000000Z", value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func decodeNullableJobTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	parsed, err := decodeJobTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func nullableTimeValue(value *time.Time) any {
	if value == nil {
		return nil
	}
	return encodeJobTime(*value)
}

func nullableString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: true}
}

func nullableStringValue(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}
