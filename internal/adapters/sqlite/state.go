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

	"github.com/leeyh0216/go-bemu/internal/ports"
	_ "modernc.org/sqlite"
)

const (
	driverName = "sqlite"

	// This value is deliberately independent from SQLite's user_version. The
	// ledger is checked before a repository is exposed, so changing an applied
	// migration requires a new version rather than editing history in place.
	baselineChecksum = "ca3040ec8a716e9acafc71179d924902c9d4cac608a779143864fe3de2d9fce3"
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

var migrations = []migration{{
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
}}

var requiredSchemaObjects = map[string]string{
	"bqemu_schema_migrations":            "table",
	"bqemu_projects":                     "table",
	"bqemu_datasets":                     "table",
	"bqemu_dataset_labels":               "table",
	"bqemu_tables":                       "table",
	"bqemu_table_labels":                 "table",
	"bqemu_table_clustering_fields":      "table",
	"bqemu_table_fields":                 "table",
	"bqemu_table_fields_sibling_ordinal": "index",
	"bqemu_table_fields_parent_insert":   "trigger",
}

// Repositories owns the SQLite connection but exposes only context-specific
// repository ports. Application services receive Catalog(), never the storage
// implementation or its transaction primitives.
type Repositories struct {
	db      *sql.DB
	catalog ports.CatalogRepository
}

// Open creates or verifies BQEMU state at path. The returned catalog facade is
// safe for concurrent use; callers must Close the owning repository bundle.
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

	return &Repositories{db: db, catalog: &catalogRepository{db: db}}, nil
}

func (r *Repositories) Catalog() ports.CatalogRepository { return r.catalog }

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
