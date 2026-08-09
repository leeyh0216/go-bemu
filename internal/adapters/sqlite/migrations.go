package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version  int
	name     string
	sql      string
	checksum string
}

func readMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded sqlite migrations: %w", err)
	}
	migrations := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("sqlite migration %q has no numeric prefix", entry.Name())
		}
		version, err := strconv.Atoi(prefix)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("sqlite migration %q has invalid version", entry.Name())
		}
		contents, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read sqlite migration %q: %w", entry.Name(), err)
		}
		digest := sha256.Sum256(contents)
		migrations = append(migrations, migration{
			version: version, name: entry.Name(), sql: string(contents),
			checksum: hex.EncodeToString(digest[:]),
		})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	for index, migration := range migrations {
		expectedVersion := index + 1
		if migration.version != expectedVersion {
			return nil, fmt.Errorf("sqlite migration sequence has version %d, expected %d", migration.version, expectedVersion)
		}
	}
	return migrations, nil
}

func applyMigrations(ctx context.Context, db *sql.DB) error {
	migrations, err := readMigrations()
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        checksum TEXT NOT NULL,
        applied_at TEXT NOT NULL
    )`); err != nil {
		return fmt.Errorf("create sqlite migration ledger: %w", err)
	}

	applied := make(map[int]migration)
	rows, err := tx.QueryContext(ctx, "SELECT version, name, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return fmt.Errorf("read sqlite migration ledger: %w", err)
	}
	for rows.Next() {
		var record migration
		if err := rows.Scan(&record.version, &record.name, &record.checksum); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan sqlite migration ledger: %w", err)
		}
		applied[record.version] = record
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close sqlite migration ledger: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite migration ledger: %w", err)
	}

	var userVersion int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&userVersion); err != nil {
		return fmt.Errorf("read sqlite schema version: %w", err)
	}
	if userVersion != len(applied) {
		return fmt.Errorf("sqlite schema version %d does not match %d applied migrations", userVersion, len(applied))
	}
	if userVersion > len(migrations) {
		return fmt.Errorf("sqlite schema version %d is newer than supported version %d", userVersion, len(migrations))
	}

	for _, candidate := range migrations {
		if record, ok := applied[candidate.version]; ok {
			if record.name != candidate.name || record.checksum != candidate.checksum {
				return fmt.Errorf("sqlite migration %d checksum or name does not match embedded migration", candidate.version)
			}
			continue
		}
		if candidate.version != userVersion+1 {
			return fmt.Errorf("sqlite migration %d is missing before version %d", userVersion+1, candidate.version)
		}
		if _, err := tx.ExecContext(ctx, candidate.sql); err != nil {
			return fmt.Errorf("apply sqlite migration %q: %w", candidate.name, err)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
			candidate.version, candidate.name, candidate.checksum,
		); err != nil {
			return fmt.Errorf("record sqlite migration %q: %w", candidate.name, err)
		}
		userVersion = candidate.version
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version = "+strconv.Itoa(userVersion)); err != nil {
			return fmt.Errorf("record sqlite schema version %d: %w", userVersion, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite migrations: %w", err)
	}
	return nil
}
