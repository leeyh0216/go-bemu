package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
)

const gooseMigrationTable = "bqemu_goose_db_version"

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

func applyMigrations(ctx context.Context, db *sql.DB) error {
	if err := importLegacyMigrationHistory(ctx, db); err != nil {
		return err
	}
	migrationFS, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded SQLite migrations: %w", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectSQLite3,
		db,
		migrationFS,
		goose.WithTableName(gooseMigrationTable),
	)
	if err != nil {
		return fmt.Errorf("configure SQLite migrations: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply SQLite migrations: %w", err)
	}
	if err := validateGooseMigrationHistory(ctx, db, provider.ListSources()); err != nil {
		return err
	}
	return nil
}

func validateGooseMigrationHistory(ctx context.Context, db *sql.DB, sources []*goose.Source) error {
	var maximum int64
	for _, source := range sources {
		if source.Version > maximum {
			maximum = source.Version
		}
	}
	var applied sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT max(version_id)
FROM bqemu_goose_db_version WHERE is_applied = 1`).Scan(&applied); err != nil {
		return fmt.Errorf("read Goose SQLite migration history: %w", err)
	}
	if !applied.Valid || applied.Int64 > maximum {
		return fmt.Errorf("Goose SQLite state uses unsupported migration version %d", applied.Int64)
	}
	return nil
}

// importLegacyMigrationHistory is a one-time bridge for databases written by
// the pre-Goose runtime. It imports only a contiguous legacy prefix, then the
// regular Goose SQL migrations own all subsequent schema evolution.
func importLegacyMigrationHistory(ctx context.Context, db *sql.DB) error {
	legacy, err := sqliteTableExists(ctx, db, "bqemu_schema_migrations")
	if err != nil {
		return err
	}
	gooseHistory, err := sqliteTableExists(ctx, db, gooseMigrationTable)
	if err != nil {
		return err
	}
	if !legacy || gooseHistory {
		return nil
	}
	rows, err := db.QueryContext(ctx, `SELECT version FROM bqemu_schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read legacy SQLite migration history: %w", err)
	}
	defer rows.Close()
	versions := make([]int, 0, 5)
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("scan legacy SQLite migration history: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate legacy SQLite migration history: %w", err)
	}
	if len(versions) == 0 {
		return errors.New("legacy SQLite migration history is empty")
	}
	for index, version := range versions {
		if version != index+1 || version > 5 {
			return fmt.Errorf("legacy SQLite migration history is not a supported contiguous prefix")
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin legacy SQLite migration history import: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE bqemu_goose_db_version (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL,
    is_applied INTEGER NOT NULL,
    tstamp TIMESTAMP DEFAULT (datetime('now'))
)`); err != nil {
		return fmt.Errorf("create Goose SQLite migration history: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_goose_db_version (version_id, is_applied) VALUES (0, 1)`); err != nil {
		return fmt.Errorf("initialize Goose SQLite migration history: %w", err)
	}
	for _, version := range versions {
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_goose_db_version (version_id, is_applied) VALUES (?, 1)`, version); err != nil {
			return fmt.Errorf("import legacy SQLite migration version %d: %w", version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit legacy SQLite migration history import: %w", err)
	}
	return nil
}

func sqliteTableExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (
    SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?
)`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect SQLite table %s: %w", name, err)
	}
	return exists == 1, nil
}
