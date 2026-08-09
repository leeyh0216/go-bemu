package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/ports"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
	_ "modernc.org/sqlite"
)

const driverName = "sqlite"

var requiredSchemaObjects = map[string]string{
	gooseMigrationTable:                       "table",
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
	"bqemu_write_streams":                     "table",
	"bqemu_write_append_receipts":             "table",
	"bqemu_write_receipts_stream":             "index",
	"bqemu_write_commit_groups":               "table",
	"bqemu_write_commit_members":              "table",
	"bqemu_write_commit_members_stream":       "index",
	"bqemu_write_streams_cleanup":             "index",
	"bqemu_write_stream_identity_immutable":   "trigger",
	"bqemu_write_stream_transition":           "trigger",
	"bqemu_write_receipt_identity_immutable":  "trigger",
	"bqemu_write_receipt_transition":          "trigger",
	"bqemu_write_commit_identity_immutable":   "trigger",
	"bqemu_write_commit_transition":           "trigger",
	"bqemu_write_commit_member_immutable":     "trigger",
	"bqemu_load_mutations":                    "table",
	"bqemu_load_mutations_recovery":           "index",
	"bqemu_load_mutation_identity_immutable":  "trigger",
	"bqemu_load_mutation_transition":          "trigger",
}

// Repositories owns the SQLite connection but exposes only context-specific
// repository ports. Application services never receive the storage
// implementation or its transaction primitives.
type Repositories struct {
	db            *sql.DB
	catalog       ports.CatalogRepository
	queryJobs     ports.JobRepository
	loadJobs      loadports.JobRepository
	loadMutations loadports.MutationJournal
	readSessions  readports.SessionStateRepository
	writeState    writeports.StateRepository
}

// Open creates or verifies BQEMU state at path. The returned facades are safe
// for concurrent use; callers must Close the owning repository bundle.
func Open(ctx context.Context, path string) (*Repositories, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("SQLite state path is required")
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
	if err := reconcileInterruptedQueryJobs(ctx, db, time.Now().UTC()); err != nil {
		return closeWithError(err)
	}

	return &Repositories{
		db: db, catalog: &catalogRepository{db: db},
		queryJobs: newQueryJobRepository(db), loadJobs: &loadJobRepository{db: db},
		loadMutations: newLoadMutationJournal(db),
		readSessions:  &readSessionRepository{db: db},
		writeState:    &writeStateRepository{db: db},
	}, nil
}

func (r *Repositories) Catalog() ports.CatalogRepository               { return r.catalog }
func (r *Repositories) QueryJobs() ports.JobRepository                 { return r.queryJobs }
func (r *Repositories) LoadJobs() loadports.JobRepository              { return r.loadJobs }
func (r *Repositories) LoadMutations() loadports.MutationJournal       { return r.loadMutations }
func (r *Repositories) ReadSessions() readports.SessionStateRepository { return r.readSessions }
func (r *Repositories) WriteState() writeports.StateRepository         { return r.writeState }

func reconcileInterruptedQueryJobs(ctx context.Context, db *sql.DB, now time.Time) error {
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

func checkIntegrity(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA quick_check")
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
