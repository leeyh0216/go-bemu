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
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/observability"
	writedomain "github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

const (
	storageWriteInternalSchema = "_bqemu_storage_write"
	storageWriteReceiptTable   = "pending_receipts"
)

type storageWriteByteAdmission struct {
	mu           sync.Mutex
	maxGlobal    int64
	maxPerStream int64
	global       int64
	byStream     map[string]int64
}

func newStorageWriteByteAdmission(config writeports.CoordinatorConfig) *storageWriteByteAdmission {
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

type stageReceipt struct {
	streamName        string
	startOffset       int64
	rowCount          int64
	stagedBytes       int64
	table             writedomain.TableReference
	schemaFingerprint string
	payloadDigest     string
}

func (r stageReceipt) matches(batch writeports.AppendBatch) bool {
	return r.streamName == batch.StreamName && r.startOffset == batch.StartOffset &&
		r.rowCount == int64(len(batch.Rows)) && r.table == batch.Table &&
		r.schemaFingerprint == batch.SchemaFingerprint && r.payloadDigest == batch.PayloadDigest
}

func (c *StorageWriteCoordinator) initializeStaging(ctx context.Context) (err error) {
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_initialize_staging",
		"transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_initialize_staging", started, err,
			"transaction_mode", "explicit")
	}()
	tx, err := c.warehouse.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Storage Write staging initialization: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteIdentifier(storageWriteInternalSchema)+" CASCADE"); err != nil {
		return fmt.Errorf("clear stale Storage Write staging: %w", err)
	}
	if err := createStorageWriteCatalog(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Storage Write staging initialization: %w", err)
	}
	return nil
}

func createStorageWriteCatalog(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, "CREATE SCHEMA "+quoteIdentifier(storageWriteInternalSchema)); err != nil {
		return fmt.Errorf("create Storage Write internal schema: %w", err)
	}
	statement := fmt.Sprintf(`CREATE TABLE %s.%s (
		stream_name VARCHAR NOT NULL,
		start_offset BIGINT NOT NULL,
		row_count BIGINT NOT NULL,
		staged_bytes BIGINT NOT NULL,
		project_id VARCHAR NOT NULL,
		dataset_id VARCHAR NOT NULL,
		table_id VARCHAR NOT NULL,
		schema_fingerprint VARCHAR NOT NULL,
		payload_digest VARCHAR NOT NULL,
		PRIMARY KEY (stream_name, start_offset)
	)`, quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(storageWriteReceiptTable))
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("create Storage Write receipt catalog: %w", err)
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

	receipt, found, err := findStageReceipt(ctx, tx, batch.StreamName, batch.StartOffset)
	if err != nil {
		return false, err
	}
	if found {
		// AppendRows offsets are retry tokens. If a worker committed staging but
		// its acknowledgement was lost, the application ledger is still behind;
		// the exact receipt must reconcile successfully without inserting again.
		// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows
		if receipt.matches(batch) {
			return false, nil
		}
		return false, fmt.Errorf("coordinator receipt conflict at offset %d for stream fingerprint %s",
			batch.StartOffset, storageWriteStreamFingerprint(batch.StreamName))
	}

	nextOffset, streamStagedBytes, err := pendingStreamTotals(ctx, tx, batch.StreamName)
	if err != nil {
		return false, err
	}
	if batch.StartOffset != nextOffset {
		return false, fmt.Errorf("coordinator offset invariant: got %d, want %d", batch.StartOffset, nextOffset)
	}
	stagedBytes := batchStagedBytes(batch)
	if err := c.checkStagedAdmission(streamStagedBytes, stagedBytes); err != nil {
		return false, err
	}
	prepared, err := c.prepareBatch(ctx, tx, batch)
	if err != nil {
		return false, err
	}
	stagingTable := storageWriteStagingTable(batch.StreamName)
	destination := quoteIdentifier(physicalSchema(batch.Table.ProjectID, batch.Table.DatasetID)) + "." + quoteIdentifier(batch.Table.TableID)
	destinationColumns := make([]string, len(prepared.destinationColumns))
	for index, column := range prepared.destinationColumns {
		destinationColumns[index] = quoteIdentifier(column)
	}
	createStatement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s AS SELECT %s FROM %s WHERE FALSE",
		quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(stagingTable),
		strings.Join(destinationColumns, ", "), destination)
	if _, err := tx.ExecContext(ctx, createStatement); err != nil {
		return false, fmt.Errorf("create pending stream staging table: %w", err)
	}
	if err := insertPreparedBatchInto(ctx, tx, prepared, storageWriteInternalSchema, stagingTable); err != nil {
		return false, err
	}
	receiptStatement := fmt.Sprintf(`INSERT INTO %s.%s
		(stream_name, start_offset, row_count, staged_bytes, project_id, dataset_id, table_id, schema_fingerprint, payload_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(storageWriteReceiptTable))
	if _, err := tx.ExecContext(ctx, receiptStatement,
		batch.StreamName, batch.StartOffset, len(batch.Rows), stagedBytes,
		batch.Table.ProjectID, batch.Table.DatasetID, batch.Table.TableID,
		batch.SchemaFingerprint, batch.PayloadDigest,
	); err != nil {
		return false, fmt.Errorf("record pending stream receipt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit pending stage transaction: %w", err)
	}
	c.addStagedBytes(batch.StreamName, stagedBytes)
	return true, nil
}

func findStageReceipt(ctx context.Context, tx *sql.Tx, stream string, offset int64) (stageReceipt, bool, error) {
	statement := fmt.Sprintf(`SELECT stream_name, start_offset, row_count, staged_bytes,
		project_id, dataset_id, table_id, schema_fingerprint, payload_digest
		FROM %s.%s WHERE stream_name = ? AND start_offset = ?`,
		quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(storageWriteReceiptTable))
	var receipt stageReceipt
	err := tx.QueryRowContext(ctx, statement, stream, offset).Scan(
		&receipt.streamName, &receipt.startOffset, &receipt.rowCount, &receipt.stagedBytes,
		&receipt.table.ProjectID, &receipt.table.DatasetID, &receipt.table.TableID,
		&receipt.schemaFingerprint, &receipt.payloadDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return stageReceipt{}, false, nil
	}
	if err != nil {
		return stageReceipt{}, false, fmt.Errorf("read pending stream receipt: %w", err)
	}
	return receipt, true, nil
}

func pendingStreamTotals(ctx context.Context, tx *sql.Tx, stream string) (rows, bytes int64, err error) {
	statement := fmt.Sprintf("SELECT COALESCE(SUM(row_count), 0), COALESCE(SUM(staged_bytes), 0) FROM %s.%s WHERE stream_name = ?",
		quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(storageWriteReceiptTable))
	if err := tx.QueryRowContext(ctx, statement, stream).Scan(&rows, &bytes); err != nil {
		return 0, 0, fmt.Errorf("read pending stream totals: %w", err)
	}
	return rows, bytes, nil
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
	released := make(map[string]int64, len(request.StreamNames))
	for _, stream := range request.StreamNames {
		count, rows, bytes, err := pendingStreamReceiptSummary(ctx, tx, stream, request.Parent)
		if err != nil {
			return err
		}
		if count == 0 {
			continue
		}
		if len(request.ExpectedRowCounts) != 0 {
			expected, exists := request.ExpectedRowCounts[stream]
			if !exists || expected != rows {
				return fmt.Errorf("pending stream row-count proof mismatch for stream fingerprint %s: got %d, expected %d",
					storageWriteStreamFingerprint(stream), rows, expected)
			}
		}
		stagingTable := storageWriteStagingTable(stream)
		columns, err := storageWriteStagingColumns(ctx, tx, stagingTable)
		if err != nil {
			return err
		}
		quotedColumns := make([]string, len(columns))
		for index, column := range columns {
			quotedColumns[index] = quoteIdentifier(column)
		}
		columnList := strings.Join(quotedColumns, ", ")
		destination := quoteIdentifier(physicalSchema(request.Parent.ProjectID, request.Parent.DatasetID)) + "." + quoteIdentifier(request.Parent.TableID)
		staging := quoteIdentifier(storageWriteInternalSchema) + "." + quoteIdentifier(stagingTable)
		statement := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", destination, columnList, columnList, staging)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply pending stream staging: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "DROP TABLE "+staging); err != nil {
			return fmt.Errorf("drop committed stream staging: %w", err)
		}
		deleteStatement := fmt.Sprintf("DELETE FROM %s.%s WHERE stream_name = ?",
			quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(storageWriteReceiptTable))
		if _, err := tx.ExecContext(ctx, deleteStatement, stream); err != nil {
			return fmt.Errorf("delete committed stream receipts: %w", err)
		}
		released[stream] = bytes
	}
	if c.beforeCommit != nil {
		if err := c.beforeCommit(); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pending stream transaction: %w", err)
	}
	for stream, bytes := range released {
		c.releaseStagedBytes(stream, bytes)
	}
	return nil
}

func pendingStreamReceiptSummary(ctx context.Context, tx *sql.Tx, stream string, parent writedomain.TableReference) (count, rows, bytes int64, err error) {
	wrongTableStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s WHERE stream_name = ?
		AND (project_id <> ? OR dataset_id <> ? OR table_id <> ?)`,
		quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(storageWriteReceiptTable))
	var wrongTable int64
	if err := tx.QueryRowContext(ctx, wrongTableStatement, stream, parent.ProjectID, parent.DatasetID, parent.TableID).Scan(&wrongTable); err != nil {
		return 0, 0, 0, fmt.Errorf("validate pending stream destination: %w", err)
	}
	if wrongTable != 0 {
		return 0, 0, 0, fmt.Errorf("stream fingerprint %s belongs to another table", storageWriteStreamFingerprint(stream))
	}
	statement := fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(row_count), 0), COALESCE(SUM(staged_bytes), 0) FROM %s.%s WHERE stream_name = ?",
		quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(storageWriteReceiptTable))
	if err := tx.QueryRowContext(ctx, statement, stream).Scan(&count, &rows, &bytes); err != nil {
		return 0, 0, 0, fmt.Errorf("read pending stream commit summary: %w", err)
	}
	return count, rows, bytes, nil
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
	_, bytes, err := pendingStreamTotals(ctx, tx, stream)
	if err != nil {
		return err
	}
	staging := quoteIdentifier(storageWriteInternalSchema) + "." + quoteIdentifier(storageWriteStagingTable(stream))
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+staging); err != nil {
		return fmt.Errorf("drop discarded stream staging: %w", err)
	}
	statement := fmt.Sprintf("DELETE FROM %s.%s WHERE stream_name = ?",
		quoteIdentifier(storageWriteInternalSchema), quoteIdentifier(storageWriteReceiptTable))
	if _, err := tx.ExecContext(ctx, statement, stream); err != nil {
		return fmt.Errorf("delete discarded stream receipts: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pending stream discard: %w", err)
	}
	c.releaseStagedBytes(stream, bytes)
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
