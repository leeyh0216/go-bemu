package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const driverName = "sqlite3"

// Config contains the SQLite settings that affect durability and lock waits.
// The application layer can map these values from its runtime configuration
// without exposing SQLite details to catalog use cases.
type Config struct {
	DataSourceName string
	BusyTimeout    time.Duration
	JournalMode    string
	Synchronous    string
}

// DefaultConfig returns the production defaults for a state database. Tests
// may override them explicitly, including when using an in-memory database.
func DefaultConfig(dataSourceName string) Config {
	return Config{
		DataSourceName: dataSourceName,
		BusyTimeout:    5 * time.Second,
		JournalMode:    "WAL",
		Synchronous:    "NORMAL",
	}
}

// Store owns BQEMU canonical state. Physical table rows remain in the query
// engine and must not be persisted in this database.
type Store struct {
	db *sql.DB
}

// Open connects to the state database, configures SQLite, verifies all applied
// migration checksums, and applies pending embedded migrations.
func Open(ctx context.Context, config Config) (*Store, error) {
	if strings.TrimSpace(config.DataSourceName) == "" {
		return nil, fmt.Errorf("sqlite state data source name is required")
	}
	if config.BusyTimeout.Milliseconds() <= 0 {
		return nil, fmt.Errorf("sqlite busy timeout must be at least one millisecond")
	}
	journalMode, err := pragmaToken("journal mode", config.JournalMode, "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF")
	if err != nil {
		return nil, err
	}
	synchronous, err := pragmaToken("synchronous mode", config.Synchronous, "OFF", "NORMAL", "FULL", "EXTRA")
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverName, config.DataSourceName)
	if err != nil {
		return nil, fmt.Errorf("open sqlite state database: %w", err)
	}
	// Pragmas such as foreign_keys are connection-local. A single pooled
	// connection guarantees every repository operation uses the configured
	// connection and also gives SQLite's single-writer model predictable order.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	closeWithError := func(cause error) (*Store, error) {
		_ = db.Close()
		return nil, cause
	}
	if err := db.PingContext(ctx); err != nil {
		return closeWithError(fmt.Errorf("ping sqlite state database: %w", err))
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return closeWithError(fmt.Errorf("enable sqlite foreign keys: %w", err))
	}
	busyMilliseconds := config.BusyTimeout.Milliseconds()
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", busyMilliseconds)); err != nil {
		return closeWithError(fmt.Errorf("configure sqlite busy timeout: %w", err))
	}
	var effectiveJournalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = "+journalMode).Scan(&effectiveJournalMode); err != nil {
		return closeWithError(fmt.Errorf("configure sqlite journal mode: %w", err))
	}
	if !strings.EqualFold(effectiveJournalMode, journalMode) && !isMemoryDataSource(config.DataSourceName) {
		return closeWithError(fmt.Errorf("sqlite journal mode is %q, expected %q", effectiveJournalMode, journalMode))
	}
	if _, err := db.ExecContext(ctx, "PRAGMA synchronous = "+synchronous); err != nil {
		return closeWithError(fmt.Errorf("configure sqlite synchronous mode: %w", err))
	}

	if err := applyMigrations(ctx, db); err != nil {
		return closeWithError(err)
	}
	return &Store{db: db}, nil
}

func pragmaToken(name, value string, allowed ...string) (string, error) {
	value = strings.ToUpper(strings.TrimSpace(value))
	for _, candidate := range allowed {
		if value == candidate {
			return value, nil
		}
	}
	return "", fmt.Errorf("unsupported sqlite %s %q", name, value)
}

func isMemoryDataSource(dataSourceName string) bool {
	dataSourceName = strings.ToLower(dataSourceName)
	return dataSourceName == ":memory:" || strings.Contains(dataSourceName, "mode=memory")
}

// Ping verifies that the state database connection remains available.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite state store is not open")
	}
	return s.db.PingContext(ctx)
}

// Close releases the state database connection.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
