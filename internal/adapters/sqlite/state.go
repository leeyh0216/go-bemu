package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/ports"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
	_ "modernc.org/sqlite"
)

const (
	driverName = "sqlite"

	// This value is deliberately independent from SQLite's user_version. The
	// ledger is checked before a repository is exposed, so changing an applied
	// migration requires a new version rather than editing history in place.
	baselineChecksum    = "ca3040ec8a716e9acafc71179d924902c9d4cac608a779143864fe3de2d9fce3"
	jobMetadataChecksum = "4e0c30c547b0b23ca5a6e134e89e2679fa2cac71d0913dd6b061091c6b3c3dce"
	readSessionChecksum = "b1be01ae19ebfc1b9337e696107ad9106f371b93776886b342bd33feddb8c54d"
)

const migrationLedgerDDL = `CREATE TABLE IF NOT EXISTS bqemu_schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    name TEXT NOT NULL UNIQUE,
    checksum TEXT NOT NULL CHECK (length(checksum) = 64),
    applied_at TEXT NOT NULL
) STRICT`

type migration struct {
	version    int
	name       string
	checksum   string
	statements []string
}

type appliedMigration struct {
	version  int
	name     string
	checksum string
}

var migrations = []migration{
	{
		version:  1,
		name:     "catalog_baseline",
		checksum: baselineChecksum,
		statements: []string{
			`CREATE TABLE bqemu_projects (
    project_id TEXT PRIMARY KEY,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT`,
			`CREATE TABLE bqemu_datasets (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    location TEXT NOT NULL,
    labels_present INTEGER NOT NULL CHECK (labels_present IN (0, 1)),
    default_table_expiration_ms INTEGER,
    default_partition_expiration_ms INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    hidden INTEGER NOT NULL CHECK (hidden IN (0, 1)),
    PRIMARY KEY (project_id, dataset_id),
    FOREIGN KEY (project_id) REFERENCES bqemu_projects(project_id) ON DELETE CASCADE
) STRICT`,
			`CREATE TABLE bqemu_dataset_labels (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    label_key TEXT NOT NULL,
    label_value TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, label_key),
    FOREIGN KEY (project_id, dataset_id)
        REFERENCES bqemu_datasets(project_id, dataset_id) ON DELETE CASCADE
) STRICT`,
			`CREATE TABLE bqemu_tables (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    labels_present INTEGER NOT NULL CHECK (labels_present IN (0, 1)),
    table_type TEXT NOT NULL,
    location TEXT NOT NULL,
    expiration_time TEXT,
    time_partitioning_present INTEGER NOT NULL CHECK (time_partitioning_present IN (0, 1)),
    time_partitioning_type TEXT,
    time_partitioning_field TEXT,
    time_partitioning_expiration_ms INTEGER,
    range_partitioning_present INTEGER NOT NULL CHECK (range_partitioning_present IN (0, 1)),
    range_partitioning_field TEXT,
    range_start INTEGER,
    range_end INTEGER,
    range_interval INTEGER,
    clustering_present INTEGER NOT NULL CHECK (clustering_present IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id),
    FOREIGN KEY (project_id, dataset_id)
        REFERENCES bqemu_datasets(project_id, dataset_id) ON DELETE CASCADE,
    CHECK ((expiration_time IS NULL) OR length(expiration_time) > 0),
    CHECK ((time_partitioning_present = 0 AND time_partitioning_type IS NULL
            AND time_partitioning_field IS NULL AND time_partitioning_expiration_ms IS NULL)
        OR (time_partitioning_present = 1 AND time_partitioning_type IS NOT NULL
            AND time_partitioning_field IS NOT NULL AND time_partitioning_expiration_ms IS NOT NULL)),
    CHECK ((range_partitioning_present = 0 AND range_partitioning_field IS NULL
            AND range_start IS NULL AND range_end IS NULL AND range_interval IS NULL)
        OR (range_partitioning_present = 1 AND range_partitioning_field IS NOT NULL
            AND range_start IS NOT NULL AND range_end IS NOT NULL AND range_interval IS NOT NULL))
) STRICT`,
			`CREATE TABLE bqemu_table_labels (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    label_key TEXT NOT NULL,
    label_value TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id, label_key),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES bqemu_tables(project_id, dataset_id, table_id) ON DELETE CASCADE
) STRICT`,
			`CREATE TABLE bqemu_table_clustering_fields (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    field_name TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id, ordinal),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES bqemu_tables(project_id, dataset_id, table_id) ON DELETE CASCADE
) STRICT`,
			`CREATE TABLE bqemu_table_fields (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    field_path TEXT NOT NULL,
    parent_path TEXT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    field_name TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_mode TEXT NOT NULL,
    description TEXT NOT NULL,
    precision INTEGER,
    scale INTEGER,
    rounding_mode TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id, field_path),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES bqemu_tables(project_id, dataset_id, table_id) ON DELETE CASCADE,
    FOREIGN KEY (project_id, dataset_id, table_id, parent_path)
        REFERENCES bqemu_table_fields(project_id, dataset_id, table_id, field_path) ON DELETE CASCADE,
    CHECK (length(field_path) > 0),
    CHECK (length(field_name) > 0),
    CHECK (precision IS NULL OR (precision >= 1 AND precision <= 38)),
    CHECK (scale IS NULL OR (scale >= 0 AND scale <= 38)),
    CHECK (rounding_mode IN ('', 'ROUNDING_MODE_UNSPECIFIED',
        'ROUND_HALF_AWAY_FROM_ZERO', 'ROUND_HALF_EVEN'))
) STRICT`,
			`CREATE UNIQUE INDEX bqemu_table_fields_sibling_ordinal
ON bqemu_table_fields (
    project_id, dataset_id, table_id, ifnull(parent_path, ''), ordinal
)`,
			`CREATE TRIGGER bqemu_table_fields_parent_insert
BEFORE INSERT ON bqemu_table_fields
WHEN NEW.parent_path IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM bqemu_table_fields AS parent
        WHERE parent.project_id = NEW.project_id
          AND parent.dataset_id = NEW.dataset_id
          AND parent.table_id = NEW.table_id
          AND parent.field_path = NEW.parent_path
          AND upper(parent.field_type) IN ('RECORD', 'STRUCT')
    ) THEN RAISE(ABORT, 'nested field requires a RECORD or STRUCT parent') END;
END`,
		},
	},
	{
		version:  2,
		name:     "job_metadata",
		checksum: jobMetadataChecksum,
		statements: []string{
			`CREATE TABLE bqemu_query_jobs (
    project_id TEXT NOT NULL,
    location_key TEXT NOT NULL,
    location TEXT NOT NULL,
    job_id TEXT NOT NULL,
    configuration_version INTEGER NOT NULL CHECK (configuration_version = 1),
    configuration_json TEXT NOT NULL CHECK (json_valid(configuration_json)),
    configuration_digest TEXT NOT NULL CHECK (length(configuration_digest) = 64),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'DONE')),
    error_reason TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    result_present INTEGER NOT NULL CHECK (result_present IN (0, 1)),
    result_schema_json TEXT,
    result_row_count INTEGER NOT NULL CHECK (result_row_count >= 0),
    affected_rows INTEGER NOT NULL,
    PRIMARY KEY (project_id, location_key, job_id),
    CHECK (length(location_key) > 0 AND length(location) > 0 AND length(job_id) > 0),
    CHECK ((error_reason IS NULL) = (error_message IS NULL)),
    CHECK (result_schema_json IS NULL OR json_valid(result_schema_json)),
    CHECK ((result_present = 0 AND result_schema_json IS NULL
            AND result_row_count = 0 AND affected_rows = 0)
        OR (result_present = 1 AND result_schema_json IS NOT NULL)),
    CHECK ((state = 'PENDING' AND started_at IS NULL AND ended_at IS NULL)
        OR (state = 'RUNNING' AND started_at IS NOT NULL AND ended_at IS NULL)
        OR (state = 'DONE' AND ended_at IS NOT NULL))
) STRICT`,
			`CREATE INDEX bqemu_query_jobs_list
ON bqemu_query_jobs (project_id, location_key, created_at DESC, job_id)`,
			`CREATE TABLE bqemu_load_jobs (
    project_id TEXT NOT NULL,
    location_key TEXT NOT NULL,
    location TEXT NOT NULL,
    job_id TEXT NOT NULL,
    configuration_version INTEGER NOT NULL CHECK (configuration_version = 1),
    configuration_json TEXT NOT NULL CHECK (json_valid(configuration_json)),
    configuration_digest TEXT NOT NULL CHECK (length(configuration_digest) = 64),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'DONE')),
    error_reason TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    input_files INTEGER NOT NULL CHECK (input_files >= 0),
    input_bytes INTEGER NOT NULL CHECK (input_bytes >= 0),
    output_rows INTEGER NOT NULL CHECK (output_rows >= 0),
    output_bytes INTEGER NOT NULL CHECK (output_bytes >= 0),
    PRIMARY KEY (project_id, location_key, job_id),
    CHECK (length(location_key) > 0 AND length(location) > 0 AND length(job_id) > 0),
    CHECK ((error_reason IS NULL) = (error_message IS NULL)),
    CHECK ((state = 'PENDING' AND started_at IS NULL AND ended_at IS NULL)
        OR (state = 'RUNNING' AND started_at IS NOT NULL AND ended_at IS NULL)
        OR (state = 'DONE' AND ended_at IS NOT NULL))
) STRICT`,
			`CREATE INDEX bqemu_load_jobs_list
ON bqemu_load_jobs (project_id, location_key, created_at DESC, job_id)`,
		},
	},
	{
		version:  3,
		name:     "storage_read_sessions",
		checksum: readSessionChecksum,
		statements: []string{
			`CREATE TABLE bqemu_read_sessions (
    session_name TEXT PRIMARY KEY,
    table_reference TEXT NOT NULL,
    data_format TEXT NOT NULL CHECK (data_format IN ('ARROW', 'AVRO')),
    selected_fields_json TEXT NOT NULL
        CHECK (json_valid(selected_fields_json) AND json_type(selected_fields_json) = 'array'),
    row_restriction_digest TEXT NOT NULL,
    row_restriction_bytes INTEGER NOT NULL CHECK (row_restriction_bytes >= 0),
    stream_count INTEGER NOT NULL CHECK (stream_count > 0),
    created_at_ns INTEGER NOT NULL,
    expires_at_ns INTEGER NOT NULL CHECK (expires_at_ns > created_at_ns),
    snapshot_time_ns INTEGER,
    retained_row_count INTEGER NOT NULL CHECK (retained_row_count >= 0),
    retained_bytes INTEGER NOT NULL CHECK (retained_bytes >= 0),
    estimated_bytes_scanned INTEGER NOT NULL CHECK (estimated_bytes_scanned >= 0),
    schema_fingerprint TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('ACTIVE', 'EXPIRED', 'UNAVAILABLE')),
    lifecycle_updated_at_ns INTEGER NOT NULL CHECK (lifecycle_updated_at_ns >= created_at_ns),
    CHECK (length(session_name) BETWEEN 1 AND 2048),
    CHECK (length(table_reference) BETWEEN 1 AND 2048),
    CHECK (length(row_restriction_digest) = 71 AND substr(row_restriction_digest, 1, 7) = 'sha256:'
        AND substr(row_restriction_digest, 8) NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(schema_fingerprint) = 71 AND substr(schema_fingerprint, 1, 7) = 'sha256:'
        AND substr(schema_fingerprint, 8) NOT GLOB '*[^0-9a-f]*')
) STRICT`,
			`CREATE TABLE bqemu_read_streams (
    stream_name TEXT PRIMARY KEY,
    session_name TEXT NOT NULL,
    stream_index INTEGER NOT NULL CHECK (stream_index >= 0),
    start_offset INTEGER NOT NULL CHECK (start_offset >= 0),
    end_offset INTEGER NOT NULL CHECK (end_offset >= start_offset),
    FOREIGN KEY (session_name) REFERENCES bqemu_read_sessions(session_name) ON DELETE CASCADE,
    UNIQUE (session_name, stream_index),
    CHECK (length(stream_name) BETWEEN 1 AND 2304)
) STRICT`,
			`CREATE INDEX bqemu_read_sessions_lifecycle
ON bqemu_read_sessions (lifecycle_state, expires_at_ns, session_name)`,
			`CREATE INDEX bqemu_read_streams_session
ON bqemu_read_streams (session_name, stream_index)`,
			`CREATE TRIGGER bqemu_read_session_identity_immutable
BEFORE UPDATE ON bqemu_read_sessions
WHEN OLD.session_name <> NEW.session_name
    OR OLD.table_reference <> NEW.table_reference
    OR OLD.data_format <> NEW.data_format
    OR OLD.selected_fields_json <> NEW.selected_fields_json
    OR OLD.row_restriction_digest <> NEW.row_restriction_digest
    OR OLD.row_restriction_bytes <> NEW.row_restriction_bytes
    OR OLD.stream_count <> NEW.stream_count
    OR OLD.created_at_ns <> NEW.created_at_ns
    OR OLD.expires_at_ns <> NEW.expires_at_ns
    OR OLD.snapshot_time_ns IS NOT NEW.snapshot_time_ns
    OR OLD.retained_row_count <> NEW.retained_row_count
    OR OLD.retained_bytes <> NEW.retained_bytes
    OR OLD.estimated_bytes_scanned <> NEW.estimated_bytes_scanned
    OR OLD.schema_fingerprint <> NEW.schema_fingerprint
BEGIN
    SELECT RAISE(ABORT, 'Storage Read session metadata is immutable');
END`,
			`CREATE TRIGGER bqemu_read_session_lifecycle_transition
BEFORE UPDATE OF lifecycle_state ON bqemu_read_sessions
WHEN OLD.lifecycle_state <> NEW.lifecycle_state
    AND NOT (OLD.lifecycle_state = 'ACTIVE' AND NEW.lifecycle_state IN ('EXPIRED', 'UNAVAILABLE'))
BEGIN
    SELECT RAISE(ABORT, 'Storage Read session lifecycle transition is invalid');
END`,
			`CREATE TRIGGER bqemu_read_stream_immutable
BEFORE UPDATE ON bqemu_read_streams
BEGIN
    SELECT RAISE(ABORT, 'Storage Read stream metadata is immutable');
END`,
		},
	},
}

var requiredSchemaObjects = map[string]string{
	"bqemu_schema_migrations":                 "table",
	"bqemu_projects":                          "table",
	"bqemu_datasets":                          "table",
	"bqemu_dataset_labels":                    "table",
	"bqemu_tables":                            "table",
	"bqemu_table_labels":                      "table",
	"bqemu_table_clustering_fields":           "table",
	"bqemu_table_fields":                      "table",
	"bqemu_table_fields_sibling_ordinal":      "index",
	"bqemu_table_fields_parent_insert":        "trigger",
	"bqemu_query_jobs":                        "table",
	"bqemu_query_jobs_list":                   "index",
	"bqemu_load_jobs":                         "table",
	"bqemu_load_jobs_list":                    "index",
	"bqemu_read_sessions":                     "table",
	"bqemu_read_streams":                      "table",
	"bqemu_read_sessions_lifecycle":           "index",
	"bqemu_read_streams_session":              "index",
	"bqemu_read_session_identity_immutable":   "trigger",
	"bqemu_read_session_lifecycle_transition": "trigger",
	"bqemu_read_stream_immutable":             "trigger",
}

// Repositories owns the SQLite connection but exposes only context-specific
// repository ports. Application services never receive the storage
// implementation or its transaction primitives.
type Repositories struct {
	db           *sql.DB
	catalog      ports.CatalogRepository
	queryJobs    ports.JobRepository
	loadJobs     loadports.JobRepository
	readSessions readports.SessionStateRepository
}

// Open creates or verifies BQEMU state at path. The returned facades are safe
// for concurrent use; callers must Close the owning repository bundle.
func Open(ctx context.Context, path string) (*Repositories, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("SQLite state path is required")
	}
	if err := verifyCompiledMigrations(); err != nil {
		return nil, err
	}

	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, fmt.Errorf("open BQEMU SQLite state: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	closeWithError := func(openErr error) (*Repositories, error) {
		return nil, errors.Join(openErr, db.Close())
	}
	if err := db.PingContext(ctx); err != nil {
		return closeWithError(fmt.Errorf("ping BQEMU SQLite state: %w", err))
	}
	if err := configureConnection(ctx, db); err != nil {
		return closeWithError(err)
	}
	if err := applyMigrations(ctx, db); err != nil {
		return closeWithError(err)
	}
	if err := checkIntegrity(ctx, db); err != nil {
		return closeWithError(err)
	}
	if err := reconcileInterruptedJobs(ctx, db, time.Now().UTC()); err != nil {
		return closeWithError(err)
	}

	return &Repositories{
		db: db, catalog: &catalogRepository{db: db},
		queryJobs: newQueryJobRepository(db), loadJobs: &loadJobRepository{db: db},
		readSessions: &readSessionRepository{db: db},
	}, nil
}

func (r *Repositories) Catalog() ports.CatalogRepository               { return r.catalog }
func (r *Repositories) QueryJobs() ports.JobRepository                 { return r.queryJobs }
func (r *Repositories) LoadJobs() loadports.JobRepository              { return r.loadJobs }
func (r *Repositories) ReadSessions() readports.SessionStateRepository { return r.readSessions }

func reconcileInterruptedJobs(ctx context.Context, db *sql.DB, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin interrupted job reconciliation: %w", err)
	}
	defer tx.Rollback()
	endedAt := encodeTime(now)
	updates := []struct {
		statement string
		message   string
	}{
		{`UPDATE bqemu_query_jobs SET state = 'DONE', error_reason = 'stopped',
    error_message = ?, ended_at = ? WHERE state IN ('PENDING', 'RUNNING')`,
			"query job was interrupted by emulator restart"},
		{`UPDATE bqemu_load_jobs SET state = 'DONE', error_reason = 'backendError',
    error_message = ?, ended_at = ? WHERE state IN ('PENDING', 'RUNNING')`,
			"load job was interrupted by emulator restart"},
	}
	for _, update := range updates {
		if _, err := tx.ExecContext(ctx, update.statement, update.message, endedAt); err != nil {
			return fmt.Errorf("reconcile interrupted BQEMU jobs: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit interrupted job reconciliation: %w", err)
	}
	return nil
}

func (r *Repositories) Check(ctx context.Context) error {
	if r == nil || r.db == nil {
		return errors.New("SQLite repositories are closed")
	}
	return checkIntegrity(ctx, r.db)
}

func (r *Repositories) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	db := r.db
	r.db = nil
	return db.Close()
}

func configureConnection(ctx context.Context, db *sql.DB) error {
	statements := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = FULL",
		"PRAGMA journal_mode = WAL",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure BQEMU SQLite state: %w", err)
		}
	}
	var foreignKeys int
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		return fmt.Errorf("verify BQEMU SQLite foreign keys: enabled=%d: %w", foreignKeys, err)
	}
	var synchronous int
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("verify BQEMU SQLite synchronous mode: %w", err)
	}
	// SQLite's FULL value is 2. Values below it weaken crash durability.
	if synchronous < 2 {
		return fmt.Errorf("verify BQEMU SQLite synchronous mode: got %d, require FULL", synchronous)
	}
	return nil
}

func applyMigrations(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, migrationLedgerDDL); err != nil {
		return fmt.Errorf("create BQEMU SQLite migration ledger: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin BQEMU SQLite migration: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT version, name, checksum
FROM bqemu_schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read BQEMU SQLite migration ledger: %w", err)
	}
	applied, err := scanAppliedMigrations(rows)
	if err != nil {
		return err
	}
	if len(applied) > len(migrations) {
		return fmt.Errorf("BQEMU SQLite state uses unknown migration version %d", applied[len(migrations)].version)
	}
	for index, item := range applied {
		expected := migrations[index]
		if item.version != expected.version || item.name != expected.name || item.checksum != expected.checksum {
			return fmt.Errorf("BQEMU SQLite migration %d checksum or identity mismatch", item.version)
		}
	}
	if len(applied) == 0 {
		var existing int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema
WHERE type = 'table' AND name LIKE 'bqemu_%' AND name <> 'bqemu_schema_migrations'`).Scan(&existing); err != nil {
			return fmt.Errorf("inspect unversioned BQEMU SQLite schema: %w", err)
		}
		if existing != 0 {
			return errors.New("BQEMU SQLite state contains an unversioned schema")
		}
	}

	for _, item := range migrations[len(applied):] {
		for _, statement := range item.statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply BQEMU SQLite migration %d (%s): %w", item.version, item.name, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_schema_migrations
    (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`,
			item.version, item.name, item.checksum, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("record BQEMU SQLite migration %d: %w", item.version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit BQEMU SQLite migrations: %w", err)
	}
	return nil
}

func checkIntegrity(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT version, name, checksum
FROM bqemu_schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read BQEMU SQLite migration ledger: %w", err)
	}
	applied, err := scanAppliedMigrations(rows)
	if err != nil {
		return err
	}
	if len(applied) != len(migrations) {
		return fmt.Errorf("BQEMU SQLite migration ledger has %d versions; require %d", len(applied), len(migrations))
	}
	for index, item := range applied {
		expected := migrations[index]
		if item.version != expected.version || item.name != expected.name || item.checksum != expected.checksum {
			return fmt.Errorf("BQEMU SQLite migration %d checksum or identity mismatch", item.version)
		}
	}

	rows, err = db.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return fmt.Errorf("run BQEMU SQLite quick_check: %w", err)
	}
	var failures []string
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			rows.Close()
			return fmt.Errorf("scan BQEMU SQLite quick_check: %w", err)
		}
		if result != "ok" {
			failures = append(failures, result)
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close BQEMU SQLite quick_check: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("run BQEMU SQLite quick_check: %w", err)
	}
	if len(failures) != 0 {
		return fmt.Errorf("BQEMU SQLite quick_check failed: %s", strings.Join(failures, "; "))
	}

	foreignRows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("run BQEMU SQLite foreign_key_check: %w", err)
	}
	if foreignRows.Next() {
		foreignRows.Close()
		return errors.New("BQEMU SQLite foreign_key_check found an invalid reference")
	}
	if err := foreignRows.Close(); err != nil {
		return fmt.Errorf("close BQEMU SQLite foreign_key_check: %w", err)
	}
	if err := foreignRows.Err(); err != nil {
		return fmt.Errorf("run BQEMU SQLite foreign_key_check: %w", err)
	}

	schemaRows, err := db.QueryContext(ctx, `SELECT name, type FROM sqlite_schema
WHERE name LIKE 'bqemu_%'`)
	if err != nil {
		return fmt.Errorf("inspect BQEMU SQLite schema: %w", err)
	}
	found := make(map[string]string, len(requiredSchemaObjects))
	for schemaRows.Next() {
		var name, objectType string
		if err := schemaRows.Scan(&name, &objectType); err != nil {
			schemaRows.Close()
			return fmt.Errorf("scan BQEMU SQLite schema: %w", err)
		}
		found[name] = objectType
	}
	if err := schemaRows.Close(); err != nil {
		return fmt.Errorf("close BQEMU SQLite schema: %w", err)
	}
	if err := schemaRows.Err(); err != nil {
		return fmt.Errorf("inspect BQEMU SQLite schema: %w", err)
	}
	for name, objectType := range requiredSchemaObjects {
		if found[name] != objectType {
			return fmt.Errorf("BQEMU SQLite schema object %s is missing or has the wrong type", name)
		}
	}
	for name := range found {
		if _, ok := requiredSchemaObjects[name]; !ok {
			return fmt.Errorf("BQEMU SQLite schema contains unknown object %s", name)
		}
	}
	return nil
}

func scanAppliedMigrations(rows *sql.Rows) ([]appliedMigration, error) {
	var applied []appliedMigration
	for rows.Next() {
		var item appliedMigration
		if err := rows.Scan(&item.version, &item.name, &item.checksum); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan BQEMU SQLite migration ledger: %w", err)
		}
		applied = append(applied, item)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close BQEMU SQLite migration ledger: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read BQEMU SQLite migration ledger: %w", err)
	}
	return applied, nil
}

func verifyCompiledMigrations() error {
	for _, item := range migrations {
		actual := checksumMigration(item)
		if actual != item.checksum {
			return fmt.Errorf("compiled BQEMU SQLite migration %d checksum mismatch: got %s", item.version, actual)
		}
	}
	return nil
}

func checksumMigration(item migration) string {
	hash := sha256.New()
	hash.Write([]byte(migrationLedgerDDL))
	hash.Write([]byte{0})
	fmt.Fprintf(hash, "%d\x00%s\x00", item.version, item.name)
	for _, statement := range item.statements {
		hash.Write([]byte(statement))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
