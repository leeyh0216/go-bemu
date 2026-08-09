package duckdb

// PENDING rows are materialized into opaque DuckDB staging tables rather than
// retained as decoded Go objects. A receipt catalog makes AppendRows retries
// idempotent, while BatchCommitWriteStreams moves every selected stream into
// the destination and removes its staging state in one DuckDB transaction.
//
// Official PENDING stream and retry semantics:
//   - https://cloud.google.com/bigquery/docs/write-api-batch
//   - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows
// DuckDB transaction semantics:
//   - https://duckdb.org/docs/stable/sql/statements/transactions

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/leeyh0216/go-bemu/internal/observability"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

const (
	storageWriteInternalSchema = "_bqemu_storage_write"
	// Retained as a migration guard and for compatibility assertions. New
	// versions keep receipt metadata exclusively in SQLite.
	storageWriteReceiptTable = "pending_receipts"
)

// StorageWriteCoordinatorConfig keeps operation-count queueing independent
// from byte admission. QueueCapacity alone cannot cap memory because one queued
// AppendRows operation can be close to the protocol's 20 MiB request limit.
type StorageWriteCoordinatorConfig struct {
	QueueCapacity             int
	QueueWaitTimeout          time.Duration
	OperationTimeout          time.Duration
	MaxInFlightBytes          int64
	MaxInFlightBytesPerStream int64
	MaxStagedBytes            int64
	MaxStagedBytesPerStream   int64
}

func (c StorageWriteCoordinatorConfig) validate() error {
	if c.QueueCapacity <= 0 {
		return fmt.Errorf("Storage Write operation queue capacity must be positive")
	}
	if c.QueueWaitTimeout <= 0 || c.OperationTimeout <= 0 {
		return fmt.Errorf("Storage Write queue wait and operation timeouts must be positive")
	}
	if c.MaxInFlightBytesPerStream <= 0 || c.MaxInFlightBytes < c.MaxInFlightBytesPerStream {
		return fmt.Errorf("Storage Write in-flight byte limits must satisfy 0 < per-stream <= global")
	}
	if c.MaxStagedBytesPerStream <= 0 || c.MaxStagedBytes < c.MaxStagedBytesPerStream {
		return fmt.Errorf("Storage Write staged byte limits must satisfy 0 < per-stream <= global")
	}
	return nil
}

type storageWriteByteAdmission struct {
	mu           sync.Mutex
	maxGlobal    int64
	maxPerStream int64
	global       int64
	byStream     map[string]int64
}

func newStorageWriteByteAdmission(config StorageWriteCoordinatorConfig) *storageWriteByteAdmission {
	return &storageWriteByteAdmission{
		maxGlobal: config.MaxInFlightBytes, maxPerStream: config.MaxInFlightBytesPerStream,
		byStream: make(map[string]int64),
	}
}

// acquire is deliberately fail-fast. Waiting after protobuf decoding would
// retain the rejected payload in every blocked bidi handler and defeat the heap
// bound; callers receive RESOURCE_EXHAUSTED and may retry with backoff.
func (a *storageWriteByteAdmission) acquire(stream string, bytes int64) (func(), error) {
	if bytes <= 0 {
		return nil, fmt.Errorf("%w: append admission size must be positive", writeports.ErrResourceExhausted)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	streamBytes := a.byStream[stream]
	if bytes > a.maxGlobal-a.global {
		return nil, fmt.Errorf("%w: global in-flight byte limit exceeded", writeports.ErrResourceExhausted)
	}
	if bytes > a.maxPerStream-streamBytes {
		return nil, fmt.Errorf("%w: per-stream in-flight byte limit exceeded", writeports.ErrResourceExhausted)
	}
	a.global += bytes
	a.byStream[stream] = streamBytes + bytes
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			a.global -= bytes
			remaining := a.byStream[stream] - bytes
			if remaining == 0 {
				delete(a.byStream, stream)
			} else {
				a.byStream[stream] = remaining
			}
			a.mu.Unlock()
		})
	}, nil
}

func (a *storageWriteByteAdmission) snapshot(stream string) (global, perStream int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.global, a.byStream[stream]
}

func callRelease(release func()) {
	if release != nil {
		release()
	}
}

func batchInFlightBytes(batch writeports.AppendBatch) int64 {
	minimum := int64(len(batch.Descriptor) + serializedRowsBytes(batch.Rows) + len(batch.Rows))
	if batch.WireBytes > minimum {
		return batch.WireBytes
	}
	return minimum
}

func batchStagedBytes(batch writeports.AppendBatch) int64 {
	// Account at least one byte per row so default-valued proto messages cannot
	// create an unbounded receipt/row count under a zero-byte payload estimate.
	return int64(serializedRowsBytes(batch.Rows) + len(batch.Rows))
}

func storageWriteStreamFingerprint(stream string) string {
	return observability.Digest([]byte(stream))
}

func (c *StorageWriteCoordinator) initializeStaging(ctx context.Context) (err error) {
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_initialize_staging",
		"transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_initialize_staging", started, err,
			"transaction_mode", "explicit")
	}()
	if _, err := c.warehouse.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdentifier(storageWriteInternalSchema)); err != nil {
		return fmt.Errorf("create Storage Write internal schema: %w", err)
	}
	var legacyReceipts int
	if err := c.warehouse.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = ? AND table_name = 'pending_receipts'`, storageWriteInternalSchema).Scan(&legacyReceipts); err != nil {
		return fmt.Errorf("inspect legacy Storage Write metadata: %w", err)
	}
	if legacyReceipts != 0 {
		return fmt.Errorf("Storage Write state drift: DuckDB contains the legacy pending_receipts metadata table; restore a matching SQLite state database or remove both state files")
	}
	return nil
}

func (c *StorageWriteCoordinator) stagePending(ctx context.Context, batch writeports.AppendBatch) (created bool, resultErr error) {
	tx, err := c.warehouse.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin pending stage transaction: %w", err)
	}
	defer func() {
		if resultErr != nil || !created {
			_ = tx.Rollback()
		}
	}()

	pendingTable := storageWriteStagingTable(batch.StreamName)
	appliedTable := storageWriteAppliedTable(batch.StreamName)
	appliedExists, appliedRows, err := storageWritePhysicalTable(ctx, tx, appliedTable)
	if err != nil {
		return false, err
	}
	if appliedExists {
		if appliedRows == int64(len(batch.Rows)) {
			return false, nil
		}
		return false, fmt.Errorf("Storage Write applied row drift for stream fingerprint %s: got %d rows, expected %d",
			storageWriteStreamFingerprint(batch.StreamName), appliedRows, len(batch.Rows))
	}
	pendingExists, nextOffset, err := storageWritePhysicalTable(ctx, tx, pendingTable)
	if err != nil {
		return false, err
	}
	if nextOffset == batch.StartOffset+int64(len(batch.Rows)) {
		return false, nil
	}
	if nextOffset != batch.StartOffset {
		return false, fmt.Errorf("Storage Write physical offset drift for stream fingerprint %s: got %d rows, expected %d",
			storageWriteStreamFingerprint(batch.StreamName), nextOffset, batch.StartOffset)
	}
	if !pendingExists && batch.StartOffset != 0 {
		return false, fmt.Errorf("Storage Write staging table is missing at offset %d for stream fingerprint %s",
			batch.StartOffset, storageWriteStreamFingerprint(batch.StreamName))
	}
	_, streamStagedBytes := c.stagedSnapshot(batch.StreamName)
	stagedBytes := batchStagedBytes(batch)
	if err := c.checkStagedAdmission(streamStagedBytes, stagedBytes); err != nil {
		return false, err
	}
	prepared, err := c.prepareBatch(ctx, tx, batch)
	if err != nil {
		return false, err
	}
	destination := quoteIdentifier(physicalSchema(batch.Table.ProjectID, batch.Table.DatasetID)) + "." + quoteIdentifier(batch.Table.TableID)
	createStatement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s AS SELECT * FROM %s WHERE FALSE",
		quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(pendingTable), destination)
	if _, err := tx.ExecContext(ctx, createStatement); err != nil {
		return false, fmt.Errorf("create pending stream staging table: %w", err)
	}
	if err := insertPreparedBatchInto(ctx, tx, prepared, storageWriteInternalSchema, pendingTable); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit pending stage transaction: %w", err)
	}
	c.addStagedBytes(batch.StreamName, stagedBytes)
	return true, nil
}

func (c *StorageWriteCoordinator) checkStagedAdmission(streamUsed, requested int64) error {
	globalUsed := c.stagedBytes.Load()
	if requested <= 0 {
		return fmt.Errorf("%w: staged batch size must be positive", writeports.ErrResourceExhausted)
	}
	if requested > c.config.MaxStagedBytes-globalUsed {
		return fmt.Errorf("%w: global staged byte limit exceeded", writeports.ErrResourceExhausted)
	}
	if requested > c.config.MaxStagedBytesPerStream-streamUsed {
		return fmt.Errorf("%w: per-stream staged byte limit exceeded", writeports.ErrResourceExhausted)
	}
	return nil
}

func storageWriteStagingTable(stream string) string {
	sum := sha256.Sum256([]byte(stream))
	return "stream_" + hex.EncodeToString(sum[:])
}

func storageWriteAppliedTable(stream string) string {
	sum := sha256.Sum256([]byte(stream))
	return "applied_" + hex.EncodeToString(sum[:])
}

// commitPending uses one transaction for destination inserts, receipt removal,
// and staging-table removal. A fault or canceled transaction therefore leaves
// destination visibility and all retry state unchanged.
func (c *StorageWriteCoordinator) commitPending(ctx context.Context, request writeports.CommitRequest) (resultErr error) {
	tx, err := c.warehouse.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pending stream transaction: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()
	for _, stream := range request.StreamNames {
		expectedRows, ok := request.RowCounts[stream]
		if !ok || expectedRows < 0 {
			return fmt.Errorf("expected row count is missing for stream fingerprint %s", storageWriteStreamFingerprint(stream))
		}
		stagingTable := storageWriteStagingTable(stream)
		appliedTable := storageWriteAppliedTable(stream)
		pendingExists, pendingRows, err := storageWritePhysicalTable(ctx, tx, stagingTable)
		if err != nil {
			return err
		}
		appliedExists, appliedRows, err := storageWritePhysicalTable(ctx, tx, appliedTable)
		if err != nil {
			return err
		}
		if pendingExists && appliedExists {
			return fmt.Errorf("Storage Write state drift: pending and applied rows both exist for stream fingerprint %s", storageWriteStreamFingerprint(stream))
		}
		if appliedExists {
			if appliedRows != expectedRows {
				return fmt.Errorf("Storage Write applied row drift for stream fingerprint %s: got %d, expected %d",
					storageWriteStreamFingerprint(stream), appliedRows, expectedRows)
			}
			continue
		}
		if !pendingExists && expectedRows != 0 {
			return fmt.Errorf("Storage Write staging rows are missing for stream fingerprint %s: expected %d", storageWriteStreamFingerprint(stream), expectedRows)
		}
		if pendingExists && pendingRows != expectedRows {
			return fmt.Errorf("Storage Write staged row drift for stream fingerprint %s: got %d, expected %d",
				storageWriteStreamFingerprint(stream), pendingRows, expectedRows)
		}
		destination := quoteIdentifier(physicalSchema(request.Parent.ProjectID, request.Parent.DatasetID)) + "." + quoteIdentifier(request.Parent.TableID)
		if !pendingExists {
			statement := fmt.Sprintf("CREATE TABLE %s.%s AS SELECT * FROM %s WHERE FALSE",
				quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(stagingTable), destination)
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("create empty pending stream marker: %w", err)
			}
		}
		columns, err := storageWriteStagingColumns(ctx, tx, stagingTable)
		if err != nil {
			return err
		}
		quotedColumns := make([]string, len(columns))
		for index, column := range columns {
			quotedColumns[index] = quoteIdentifier(column)
		}
		columnList := strings.Join(quotedColumns, ", ")
		staging := quoteIdentifier(storageWriteInternalSchema) + "." + quoteIdentifier(stagingTable)
		statement := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", destination, columnList, columnList, staging)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply pending stream staging: %w", err)
		}
		rename := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", staging, quoteIdentifier(appliedTable))
		if _, err := tx.ExecContext(ctx, rename); err != nil {
			return fmt.Errorf("mark committed stream rows as applied: %w", err)
		}
	}
	if c.beforeCommit != nil {
		if err := c.beforeCommit(); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pending stream transaction: %w", err)
	}
	return nil
}

func storageWriteStagingColumns(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position`, storageWriteInternalSchema, table)
	if err != nil {
		return nil, fmt.Errorf("describe pending stream staging: %w", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan pending stream staging: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending stream staging: %w", err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("pending stream staging table is missing")
	}
	return columns, nil
}

func (c *StorageWriteCoordinator) discardPending(ctx context.Context, stream string) (resultErr error) {
	tx, err := c.warehouse.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pending stream discard: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()
	staging := quoteIdentifier(storageWriteInternalSchema) + "." + quoteIdentifier(storageWriteStagingTable(stream))
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+staging); err != nil {
		return fmt.Errorf("drop discarded stream staging: %w", err)
	}
	applied := quoteIdentifier(storageWriteInternalSchema) + "." + quoteIdentifier(storageWriteAppliedTable(stream))
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+applied); err != nil {
		return fmt.Errorf("drop discarded applied stream rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pending stream discard: %w", err)
	}
	_, stagedBytes := c.stagedSnapshot(stream)
	c.releaseStagedBytes(stream, stagedBytes)
	return nil
}

func storageWritePhysicalTable(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, table string) (bool, int64, error) {
	var count int
	if err := queryer.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?`, storageWriteInternalSchema, table).Scan(&count); err != nil {
		return false, 0, fmt.Errorf("inspect Storage Write physical table: %w", err)
	}
	if count == 0 {
		return false, 0, nil
	}
	var rows int64
	statement := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(table))
	if err := queryer.QueryRowContext(ctx, statement).Scan(&rows); err != nil {
		return false, 0, fmt.Errorf("count Storage Write physical rows: %w", err)
	}
	return true, rows, nil
}

func (c *StorageWriteCoordinator) inspectPhysical(ctx context.Context, expected []writeports.PhysicalExpectation) (map[string]writeports.PhysicalStreamState, error) {
	knownTables := make(map[string]string, len(expected)*2)
	for _, item := range expected {
		knownTables[storageWriteStagingTable(item.StreamName)] = item.StreamName
		knownTables[storageWriteAppliedTable(item.StreamName)] = item.StreamName
	}
	rows, err := c.warehouse.db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables
		WHERE table_schema = ? ORDER BY table_name`, storageWriteInternalSchema)
	if err != nil {
		return nil, fmt.Errorf("list Storage Write physical state: %w", err)
	}
	var physicalTables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan Storage Write physical state: %w", err)
		}
		physicalTables = append(physicalTables, table)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close Storage Write physical state: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Storage Write physical state: %w", err)
	}
	for _, table := range physicalTables {
		if _, known := knownTables[table]; !known {
			return nil, fmt.Errorf("Storage Write state drift: DuckDB contains unowned staging table %q; restore the matching SQLite and DuckDB files together", table)
		}
	}

	result := make(map[string]writeports.PhysicalStreamState, len(expected))
	restoredBytes := make(map[string]int64, len(expected))
	for _, item := range expected {
		pendingExists, pendingRows, err := storageWritePhysicalTable(ctx, c.warehouse.db, storageWriteStagingTable(item.StreamName))
		if err != nil {
			return nil, err
		}
		appliedExists, appliedRows, err := storageWritePhysicalTable(ctx, c.warehouse.db, storageWriteAppliedTable(item.StreamName))
		if err != nil {
			return nil, err
		}
		if pendingExists && appliedExists {
			return nil, fmt.Errorf("Storage Write state drift: pending and applied rows both exist for stream fingerprint %s", storageWriteStreamFingerprint(item.StreamName))
		}
		result[item.StreamName] = writeports.PhysicalStreamState{
			PendingExists: pendingExists, PendingRows: pendingRows,
			AppliedExists: appliedExists, AppliedRows: appliedRows,
		}
		if pendingExists || appliedExists {
			restoredBytes[item.StreamName] = item.StagedBytes
		}
	}
	c.resetStagedBytes()
	for stream, bytes := range restoredBytes {
		c.addStagedBytes(stream, bytes)
	}
	return result, nil
}

func (c *StorageWriteCoordinator) acknowledgeApplied(ctx context.Context, streams []string) (resultErr error) {
	tx, err := c.warehouse.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Write acknowledgement: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()
	for _, stream := range streams {
		table := quoteIdentifier(storageWriteInternalSchema) + "." + quoteIdentifier(storageWriteAppliedTable(stream))
		if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return fmt.Errorf("drop acknowledged Storage Write applied rows: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write acknowledgement: %w", err)
	}
	for _, stream := range streams {
		_, bytes := c.stagedSnapshot(stream)
		c.releaseStagedBytes(stream, bytes)
	}
	return nil
}

func (c *StorageWriteCoordinator) cleanupAllStaging(ctx context.Context) error {
	tx, err := c.warehouse.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Write staging cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteIdentifier(storageWriteInternalSchema)+" CASCADE"); err != nil {
		return fmt.Errorf("drop Storage Write staging catalog: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write staging cleanup: %w", err)
	}
	c.resetStagedBytes()
	return nil
}

func (c *StorageWriteCoordinator) addStagedBytes(stream string, bytes int64) {
	c.stagedMu.Lock()
	c.stagedByStream[stream] += bytes
	c.stagedBytes.Add(bytes)
	c.stagedMu.Unlock()
}

func (c *StorageWriteCoordinator) releaseStagedBytes(stream string, bytes int64) {
	c.stagedMu.Lock()
	remaining := c.stagedByStream[stream] - bytes
	if remaining <= 0 {
		delete(c.stagedByStream, stream)
	} else {
		c.stagedByStream[stream] = remaining
	}
	c.stagedBytes.Add(-bytes)
	c.stagedMu.Unlock()
}

func (c *StorageWriteCoordinator) resetStagedBytes() {
	c.stagedMu.Lock()
	c.stagedByStream = make(map[string]int64)
	c.stagedBytes.Store(0)
	c.stagedMu.Unlock()
}

func (c *StorageWriteCoordinator) stagedSnapshot(stream string) (global, perStream int64) {
	c.stagedMu.Lock()
	perStream = c.stagedByStream[stream]
	global = c.stagedBytes.Load()
	c.stagedMu.Unlock()
	return global, perStream
}
