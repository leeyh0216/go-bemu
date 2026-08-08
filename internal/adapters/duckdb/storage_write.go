package duckdb

// Storage Write rows are decoded from the official self-contained ProtoSchema
// descriptor and ProtoRows wire bytes at this outbound adapter. DuckDB remains
// invisible to the domain/application packages.
//
// Official ProtoRows schema/data mapping:
// https://cloud.google.com/bigquery/docs/write-api#data_type_conversions
// ProtoSchema self-contained descriptor contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#protoschema
// Atomic PENDING stream commit contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#batchcommitwritestreamsresponse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	writedomain "github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	_ writeports.Coordinator                    = (*StorageWriteCoordinator)(nil)
	_ interface{ Close(context.Context) error } = (*StorageWriteCoordinator)(nil)
)

var errStorageWriteCoordinatorClosed = errors.New("DuckDB Storage Write coordinator is closed")

// StorageWriteCoordinator presents concurrent logical operations to callers but
// executes all DuckDB work on one queue. This is an implementation constraint,
// not a stream-count constraint: 2, 8, or 16 task streams may negotiate in
// parallel while database transactions remain serialized.
type StorageWriteCoordinator struct {
	warehouse   *Warehouse
	resolver    writeports.TableSchemaResolver
	config      writeports.CoordinatorConfig
	admission   *storageWriteByteAdmission
	queue       chan coordinatorOperation
	stop        context.CancelFunc
	done        chan struct{}
	closed      atomic.Bool
	stagedBytes atomic.Int64
	stagedMu    sync.Mutex

	// The worker owns stagedByStream. Payload rows and receipts live in hidden
	// DuckDB tables; this map contains byte counters only. Tests may set the fault
	// hooks before submitting operations to exercise acknowledgement/transaction
	// boundaries without exposing those seams through the outbound port.
	stagedByStream  map[string]int64
	afterStage      func()
	beforeCommit    func() error
	scheduleTimeout func(time.Duration, func()) func()
	submissionMu    sync.RWMutex
	closeOnce       sync.Once
	closeErr        error
}

type coordinatorOperation struct {
	ctx       context.Context
	callerCtx context.Context
	fn        func(context.Context) (any, error)
	out       chan coordinatorResult
	release   func()
}

type coordinatorResult struct {
	value any
	err   error
}

type preparedBatch struct {
	streamName         string
	table              writedomain.TableReference
	startOffset        int64
	columns            []string
	destinationColumns []string
	columnTypes        []string
	rows               [][]any
}

type tableLayout struct {
	schema  writedomain.TableSchema
	columns []columnLayout
}

type columnLayout struct {
	field      writedomain.Field
	duckDBType string
	isNullable bool
}

func NewStorageWriteCoordinator(
	ctx context.Context,
	warehouse *Warehouse,
	resolver writeports.TableSchemaResolver,
	config writeports.CoordinatorConfig,
) (*StorageWriteCoordinator, error) {
	if ctx == nil {
		return nil, fmt.Errorf("initialization context is required")
	}
	if warehouse == nil {
		return nil, fmt.Errorf("warehouse is required")
	}
	if resolver == nil {
		return nil, fmt.Errorf("Storage Write table schema resolver is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	workerContext, cancel := context.WithCancel(context.Background())
	coordinator := &StorageWriteCoordinator{
		warehouse: warehouse, resolver: resolver, config: config, admission: newStorageWriteByteAdmission(config),
		queue: make(chan coordinatorOperation, config.QueueCapacity), stop: cancel, done: make(chan struct{}),
		stagedByStream: make(map[string]int64),
		scheduleTimeout: func(timeout time.Duration, expire func()) func() {
			timer := time.AfterFunc(timeout, expire)
			return func() { timer.Stop() }
		},
	}
	if err := coordinator.initializeStaging(ctx); err != nil {
		cancel()
		return nil, err
	}
	go coordinator.run(workerContext)
	return coordinator, nil
}

func (c *StorageWriteCoordinator) DescribeTable(ctx context.Context, table writedomain.TableReference) (writedomain.TableSchema, error) {
	value, err := c.submit(ctx, func(operationContext context.Context) (any, error) {
		layout, err := c.describeTable(operationContext, c.warehouse.db, table)
		return layout.schema, err
	})
	if err != nil {
		return writedomain.TableSchema{}, err
	}
	return value.(writedomain.TableSchema), nil
}

func (c *StorageWriteCoordinator) AppendDefault(ctx context.Context, batch writeports.AppendBatch) (err error) {
	admissionBytes := batchInFlightBytes(batch)
	globalInFlight, streamInFlight := c.admission.snapshot(batch.StreamName)
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_append_default",
		"stream", batch.StreamName, "stream_fingerprint", storageWriteStreamFingerprint(batch.StreamName), "table", batch.Table.Name(), "start_offset", batch.StartOffset,
		"row_count", len(batch.Rows), "row_bytes", serializedRowsBytes(batch.Rows),
		"rows", batch.Rows, "descriptor", batch.Descriptor,
		"schema_fingerprint", batch.SchemaFingerprint, "payload_digest", batch.PayloadDigest,
		"admission_bytes", admissionBytes, "global_in_flight_bytes", globalInFlight,
		"stream_in_flight_bytes", streamInFlight, "transaction_mode", "explicit")
	defer func() {
		globalInFlight, streamInFlight := c.admission.snapshot(batch.StreamName)
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_append_default", started, err,
			"stream_fingerprint", storageWriteStreamFingerprint(batch.StreamName), "table", batch.Table.Name(), "start_offset", batch.StartOffset,
			"row_count", len(batch.Rows), "schema_fingerprint", batch.SchemaFingerprint,
			"payload_digest", batch.PayloadDigest, "admission_bytes", admissionBytes,
			"global_in_flight_bytes", globalInFlight, "stream_in_flight_bytes", streamInFlight,
			"transaction_mode", "explicit")
	}()
	_, err = c.submitBatch(ctx, batch, func(operationContext context.Context) (_ any, resultErr error) {
		prepared, prepareErr := c.prepareBatch(operationContext, c.warehouse.db, batch)
		if prepareErr != nil {
			return nil, prepareErr
		}
		tx, beginErr := c.warehouse.db.BeginTx(operationContext, nil)
		if beginErr != nil {
			return nil, fmt.Errorf("begin default append transaction: %w", beginErr)
		}
		defer func() {
			if resultErr != nil {
				_ = tx.Rollback()
			}
		}()
		if insertErr := insertPreparedBatch(operationContext, tx, prepared); insertErr != nil {
			return nil, insertErr
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, fmt.Errorf("commit default append transaction: %w", commitErr)
		}
		return nil, nil
	})
	return err
}

func (c *StorageWriteCoordinator) StagePending(ctx context.Context, batch writeports.AppendBatch) (err error) {
	admissionBytes := batchInFlightBytes(batch)
	globalInFlight, streamInFlight := c.admission.snapshot(batch.StreamName)
	globalStaged, streamStaged := c.stagedSnapshot(batch.StreamName)
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_stage_pending",
		"stream", batch.StreamName, "stream_fingerprint", storageWriteStreamFingerprint(batch.StreamName), "table", batch.Table.Name(), "start_offset", batch.StartOffset,
		"row_count", len(batch.Rows), "row_bytes", serializedRowsBytes(batch.Rows),
		"rows", batch.Rows, "descriptor", batch.Descriptor,
		"schema_fingerprint", batch.SchemaFingerprint, "payload_digest", batch.PayloadDigest,
		"admission_bytes", admissionBytes, "global_in_flight_bytes", globalInFlight,
		"stream_in_flight_bytes", streamInFlight, "global_staged_bytes", globalStaged,
		"stream_staged_bytes", streamStaged,
		"transaction_mode", "duckdb_staging")
	defer func() {
		globalInFlight, streamInFlight := c.admission.snapshot(batch.StreamName)
		globalStaged, streamStaged := c.stagedSnapshot(batch.StreamName)
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_stage_pending", started, err,
			"stream_fingerprint", storageWriteStreamFingerprint(batch.StreamName), "table", batch.Table.Name(), "start_offset", batch.StartOffset,
			"row_count", len(batch.Rows), "schema_fingerprint", batch.SchemaFingerprint,
			"payload_digest", batch.PayloadDigest, "admission_bytes", admissionBytes,
			"global_in_flight_bytes", globalInFlight, "stream_in_flight_bytes", streamInFlight,
			"global_staged_bytes", globalStaged, "stream_staged_bytes", streamStaged,
			"transaction_mode", "duckdb_staging")
	}()
	_, err = c.submitBatch(ctx, batch, func(operationContext context.Context) (any, error) {
		created, stageErr := c.stagePending(operationContext, batch)
		if stageErr == nil && created && c.afterStage != nil {
			c.afterStage()
		}
		return nil, stageErr
	})
	return err
}

func (c *StorageWriteCoordinator) CommitPending(ctx context.Context, request writeports.CommitRequest) (err error) {
	streamFingerprint := observability.Digest([]byte(strings.Join(request.StreamNames, "\n")))
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_commit_pending",
		"table", request.Parent.Name(), "stream_count", len(request.StreamNames),
		"stream_set_fingerprint", streamFingerprint,
		"transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_commit_pending", started, err,
			"table", request.Parent.Name(), "stream_count", len(request.StreamNames),
			"stream_set_fingerprint", streamFingerprint,
			"transaction_mode", "explicit")
	}()
	_, err = c.submit(ctx, func(operationContext context.Context) (any, error) {
		return nil, c.commitPending(operationContext, request)
	})
	return err
}

func (c *StorageWriteCoordinator) DiscardPending(ctx context.Context, streamName string) error {
	globalStaged, streamStaged := c.stagedSnapshot(streamName)
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_discard_pending",
		"stream_fingerprint", storageWriteStreamFingerprint(streamName),
		"global_staged_bytes", globalStaged, "stream_staged_bytes", streamStaged,
		"transaction_mode", "explicit")
	_, err := c.submit(ctx, func(operationContext context.Context) (any, error) {
		return nil, c.discardPending(operationContext, streamName)
	})
	globalStaged, streamStaged = c.stagedSnapshot(streamName)
	observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_discard_pending", started, err,
		"stream_fingerprint", storageWriteStreamFingerprint(streamName),
		"global_staged_bytes", globalStaged, "stream_staged_bytes", streamStaged,
		"transaction_mode", "explicit")
	return err
}

func (c *StorageWriteCoordinator) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		// Taking the exclusive submission lock waits for every public submitter
		// that already passed admission to finish enqueueing. Marking closed here
		// rejects all later calls; the internal cleanup operation can then be put
		// behind the complete pre-close FIFO without a post-cleanup append race.
		c.submissionMu.Lock()
		c.closed.Store(true)
		c.submissionMu.Unlock()
		started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_cleanup_staging",
			"global_staged_bytes", c.stagedBytes.Load(), "transaction_mode", "explicit")
		_, c.closeErr = c.submitInternal(ctx, func(operationContext context.Context) (any, error) {
			return nil, c.cleanupAllStaging(operationContext)
		})
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_cleanup_staging", started, c.closeErr,
			"global_staged_bytes", c.stagedBytes.Load(), "transaction_mode", "explicit")
		c.stop()
	})
	select {
	case <-c.done:
		return c.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *StorageWriteCoordinator) submit(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	return c.submitOperation(ctx, fn, nil)
}

func (c *StorageWriteCoordinator) submitBatch(ctx context.Context, batch writeports.AppendBatch, fn func(context.Context) (any, error)) (any, error) {
	release, err := c.admission.acquire(batch.StreamName, batchInFlightBytes(batch))
	if err != nil {
		return nil, err
	}
	return c.submitOperation(ctx, fn, release)
}

func (c *StorageWriteCoordinator) submitOperation(ctx context.Context, fn func(context.Context) (any, error), release func()) (any, error) {
	if ctx == nil {
		callRelease(release)
		return nil, fmt.Errorf("operation context is required")
	}
	c.submissionMu.RLock()
	if c.closed.Load() {
		c.submissionMu.RUnlock()
		callRelease(release)
		return nil, errStorageWriteCoordinatorClosed
	}
	operationContext, cancel := context.WithCancelCause(ctx)
	operation := coordinatorOperation{
		ctx: operationContext, callerCtx: ctx,
		fn: fn, out: make(chan coordinatorResult, 1), release: release,
	}
	queueTimer := time.NewTimer(c.config.QueueWaitTimeout)
	defer queueTimer.Stop()
	select {
	case c.queue <- operation:
		c.submissionMu.RUnlock()
	case <-ctx.Done():
		c.submissionMu.RUnlock()
		cancel(ctx.Err())
		callRelease(release)
		return nil, ctx.Err()
	case <-queueTimer.C:
		c.submissionMu.RUnlock()
		cancel(writeports.ErrQueueWaitTimeout)
		callRelease(release)
		return nil, fmt.Errorf("%w after %s", writeports.ErrQueueWaitTimeout, c.config.QueueWaitTimeout)
	case <-c.done:
		c.submissionMu.RUnlock()
		cancel(errStorageWriteCoordinatorClosed)
		callRelease(release)
		return nil, errStorageWriteCoordinatorClosed
	}
	stopOperationTimer := c.scheduleTimeout(c.config.OperationTimeout, func() {
		cancel(writeports.ErrOperationTimeout)
	})
	value, err := waitCoordinatorResult(c.done, operation)
	stopOperationTimer()
	cancel(nil)
	return value, err
}

func (c *StorageWriteCoordinator) submitInternal(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("operation context is required")
	}
	operation := coordinatorOperation{ctx: ctx, callerCtx: ctx, fn: fn, out: make(chan coordinatorResult, 1)}
	select {
	case c.queue <- operation:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, errStorageWriteCoordinatorClosed
	}
	return waitCoordinatorResult(c.done, operation)
}

func waitCoordinatorResult(done <-chan struct{}, operation coordinatorOperation) (any, error) {
	// Prefer an acknowledgement that is already buffered when completion and a
	// deadline become observable together. PENDING retries are receipt-backed;
	// DEFAULT streams remain intentionally at-least-once on truly ambiguous
	// commit/deadline races.
	select {
	case result := <-operation.out:
		return result.value, result.err
	default:
	}
	select {
	case result := <-operation.out:
		return result.value, result.err
	case <-operation.ctx.Done():
		return nil, coordinatorOperationContextError(operation)
	case <-done:
		return nil, errStorageWriteCoordinatorClosed
	}
}

func coordinatorOperationContextError(operation coordinatorOperation) error {
	if operation.callerCtx != nil {
		if err := operation.callerCtx.Err(); err != nil {
			return err
		}
	}
	if errors.Is(context.Cause(operation.ctx), writeports.ErrOperationTimeout) {
		return fmt.Errorf("%w after configured deadline", writeports.ErrOperationTimeout)
	}
	if cause := context.Cause(operation.ctx); cause != nil {
		return cause
	}
	return fmt.Errorf("%w after configured deadline", writeports.ErrOperationTimeout)
}

func (c *StorageWriteCoordinator) run(ctx context.Context) {
	defer func() {
		for {
			select {
			case operation := <-c.queue:
				callRelease(operation.release)
				operation.out <- coordinatorResult{err: errStorageWriteCoordinatorClosed}
			default:
				close(c.done)
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case operation := <-c.queue:
			if err := operation.ctx.Err(); err != nil {
				callRelease(operation.release)
				operation.out <- coordinatorResult{err: coordinatorOperationContextError(operation)}
				continue
			}
			value, err := operation.fn(operation.ctx)
			if err != nil && operation.ctx.Err() != nil {
				err = coordinatorOperationContextError(operation)
			}
			callRelease(operation.release)
			operation.out <- coordinatorResult{value: value, err: err}
		}
	}
}

type storageWriteQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (c *StorageWriteCoordinator) prepareBatch(ctx context.Context, queryer storageWriteQueryer, batch writeports.AppendBatch) (preparedBatch, error) {
	layout, err := c.describeTable(ctx, queryer, batch.Table)
	if err != nil {
		return preparedBatch{}, err
	}
	message, err := messageDescriptor(batch.Descriptor)
	if err != nil {
		return preparedBatch{}, invalidRowsError("invalid ProtoSchema: %v", err)
	}
	columnsByName := make(map[string]columnLayout, len(layout.columns))
	for _, column := range layout.columns {
		columnsByName[strings.ToLower(column.field.Name)] = column
	}
	fields := message.Fields()
	columns := make([]string, fields.Len())
	columnTypes := make([]string, fields.Len())
	targets := make([]columnLayout, fields.Len())
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		target, exists := columnsByName[strings.ToLower(string(field.Name()))]
		if !exists {
			return preparedBatch{}, invalidRowsError("ProtoSchema field %q is not present in destination table", field.Name())
		}
		columns[index] = target.field.Name
		columnTypes[index] = target.duckDBType
		targets[index] = target
	}
	decodedRows := make([][]any, len(batch.Rows))
	for rowIndex, serialized := range batch.Rows {
		row := dynamicpb.NewMessage(message)
		if err := proto.Unmarshal(serialized, row); err != nil {
			return preparedBatch{}, invalidRowsError("decode ProtoRow %d: %v", rowIndex, err)
		}
		if err := proto.CheckInitialized(row); err != nil {
			return preparedBatch{}, invalidRowsError("ProtoRow %d misses a required field: %v", rowIndex, err)
		}
		if len(row.GetUnknown()) != 0 {
			return preparedBatch{}, invalidRowsError("ProtoRow %d contains fields absent from ProtoSchema", rowIndex)
		}
		values := make([]any, fields.Len())
		for fieldIndex := 0; fieldIndex < fields.Len(); fieldIndex++ {
			field := fields.Get(fieldIndex)
			if !field.IsList() && !row.Has(field) {
				if !targets[fieldIndex].isNullable {
					return preparedBatch{}, invalidRowsError("required destination column %q is absent", targets[fieldIndex].field.Name)
				}
				values[fieldIndex] = nil
				continue
			}
			converted, convertErr := convertProtoValue(field, row.Get(field), targets[fieldIndex])
			if convertErr != nil {
				return preparedBatch{}, invalidRowsError("convert ProtoRow %d field %q: %v", rowIndex, field.Name(), convertErr)
			}
			bindable, bindErr := storageWriteBindableValue(targets[fieldIndex].field, converted)
			if bindErr != nil {
				return preparedBatch{}, invalidRowsError("bind ProtoRow %d field %q: %v", rowIndex, field.Name(), bindErr)
			}
			values[fieldIndex] = bindable
		}
		decodedRows[rowIndex] = values
	}
	destinationColumns := make([]string, len(layout.columns))
	for index, column := range layout.columns {
		destinationColumns[index] = column.field.Name
	}
	return preparedBatch{
		streamName: batch.StreamName, table: batch.Table, startOffset: batch.StartOffset,
		columns: columns, destinationColumns: destinationColumns, columnTypes: columnTypes, rows: decodedRows,
	}, nil
}

func invalidRowsError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", writeports.ErrInvalidRows, fmt.Sprintf(format, args...))
}

func (c *StorageWriteCoordinator) describeTable(ctx context.Context, queryer storageWriteQueryer, table writedomain.TableReference) (tableLayout, error) {
	catalogTable, err := c.resolver.GetTable(ctx, table.ProjectID, table.DatasetID, table.TableID)
	if err != nil {
		if errors.Is(err, catalogdomain.ErrNotFound) {
			return tableLayout{}, fmt.Errorf("%w: %s", writeports.ErrTableNotFound, table.Name())
		}
		return tableLayout{}, fmt.Errorf("resolve canonical Storage Write destination: %w", err)
	}
	if catalogTable.ProjectID != table.ProjectID || catalogTable.DatasetID != table.DatasetID || catalogTable.ID != table.TableID {
		return tableLayout{}, fmt.Errorf("canonical Storage Write resolver returned a different table for %s", table.Name())
	}
	if catalogTable.Type != "" && !strings.EqualFold(catalogTable.Type, "TABLE") {
		return tableLayout{}, fmt.Errorf("Storage Write destination %s has unsupported table type %q", table.Name(), catalogTable.Type)
	}
	if err := catalogTable.Validate(); err != nil {
		if errors.Is(err, catalogdomain.ErrUnsupported) {
			return tableLayout{}, fmt.Errorf("%w: %v", writeports.ErrUnsupportedSchema, err)
		}
		return tableLayout{}, fmt.Errorf("validate canonical Storage Write destination: %w", err)
	}

	rows, err := queryer.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`, physicalSchema(table.ProjectID, table.DatasetID), table.TableID)
	if err != nil {
		return tableLayout{}, fmt.Errorf("describe Storage Write destination: %w", err)
	}
	type physicalColumn struct {
		name, dataType, nullable string
	}
	physicalColumns := make([]physicalColumn, 0, len(catalogTable.Schema))
	for rows.Next() {
		var name, dataType, nullable string
		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			_ = rows.Close()
			return tableLayout{}, fmt.Errorf("scan Storage Write destination schema: %w", err)
		}
		if catalogdomain.IsPartitionPseudoColumn(name) {
			continue
		}
		physicalColumns = append(physicalColumns, physicalColumn{name: name, dataType: dataType, nullable: nullable})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return tableLayout{}, fmt.Errorf("read Storage Write destination schema: %w", err)
	}
	if err := rows.Close(); err != nil {
		return tableLayout{}, fmt.Errorf("close Storage Write destination schema: %w", err)
	}
	if len(physicalColumns) == 0 {
		return tableLayout{}, fmt.Errorf("%w: %s", writeports.ErrTableNotFound, table.Name())
	}
	if len(physicalColumns) != len(catalogTable.Schema) {
		return tableLayout{}, fmt.Errorf("canonical/physical Storage Write schema mismatch for %s: catalog=%d physical=%d", table.Name(), len(catalogTable.Schema), len(physicalColumns))
	}

	expectedTypes, err := canonicalDuckDBStorageWriteTypes(ctx, queryer, catalogTable.Schema)
	if err != nil {
		return tableLayout{}, err
	}
	layout := tableLayout{schema: writedomain.TableSchema{Fields: make([]writedomain.Field, 0, len(catalogTable.Schema))}}
	for index, catalogField := range catalogTable.Schema {
		physical := physicalColumns[index]
		if !strings.EqualFold(physical.name, catalogField.Name) || !equalDuckDBTypeName(physical.dataType, expectedTypes[index]) {
			return tableLayout{}, fmt.Errorf("canonical/physical Storage Write schema mismatch for %s column %q", table.Name(), catalogField.Name)
		}
		actualNullable := strings.EqualFold(physical.nullable, "YES")
		if !actualNullable && !strings.EqualFold(physical.nullable, "NO") {
			return tableLayout{}, fmt.Errorf("DuckDB returned unknown nullability %q for %s column %q", physical.nullable, table.Name(), catalogField.Name)
		}
		expectedNullable := !strings.EqualFold(catalogField.Mode, "REQUIRED")
		if actualNullable != expectedNullable {
			return tableLayout{}, fmt.Errorf("canonical/physical Storage Write nullability mismatch for %s column %q", table.Name(), catalogField.Name)
		}
		field := canonicalStorageWriteField(catalogField)
		layout.schema.Fields = append(layout.schema.Fields, field)
		layout.columns = append(layout.columns, columnLayout{field: field, duckDBType: physical.dataType, isNullable: actualNullable})
	}
	return layout, nil
}

func canonicalDuckDBStorageWriteTypes(ctx context.Context, queryer storageWriteQueryer, fields []catalogdomain.Field) ([]string, error) {
	expressions := make([]string, len(fields))
	for index, field := range fields {
		dataType, err := duckDBType(field)
		if err != nil {
			return nil, fmt.Errorf("map canonical Storage Write column %q: %w", field.Name, err)
		}
		expressions[index] = "typeof(CAST(NULL AS " + dataType + "))"
	}
	row := queryer.QueryRowContext(ctx, "SELECT "+strings.Join(expressions, ", "))
	result := make([]string, len(fields))
	destinations := make([]any, len(fields))
	for index := range result {
		destinations[index] = &result[index]
	}
	if err := row.Scan(destinations...); err != nil {
		return nil, fmt.Errorf("normalize canonical Storage Write physical types: %w", err)
	}
	return result, nil
}

func equalDuckDBTypeName(left, right string) bool {
	normalize := func(value string) string {
		return strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	}
	return normalize(left) == normalize(right)
}

func canonicalStorageWriteField(field catalogdomain.Field) writedomain.Field {
	result := field
	result.Type = strings.ToUpper(result.Type)
	switch result.Type {
	case "BOOLEAN":
		result.Type = "BOOL"
	case "INTEGER":
		result.Type = "INT64"
	case "FLOAT":
		result.Type = "FLOAT64"
	case "RECORD":
		result.Type = "STRUCT"
	}
	if result.Mode == "" {
		result.Mode = "NULLABLE"
	} else {
		result.Mode = strings.ToUpper(result.Mode)
	}
	result.Precision = catalogdomain.CloneOptionalInt64(field.Precision)
	result.Scale = catalogdomain.CloneOptionalInt64(field.Scale)
	result.Fields = make([]catalogdomain.Field, len(field.Fields))
	for index, nested := range field.Fields {
		result.Fields[index] = canonicalStorageWriteField(nested)
	}
	return result
}

func messageDescriptor(serialized []byte) (protoreflect.MessageDescriptor, error) {
	if len(serialized) == 0 {
		return nil, fmt.Errorf("ProtoSchema descriptor is empty")
	}
	var message descriptorpb.DescriptorProto
	if err := proto.Unmarshal(serialized, &message); err != nil {
		return nil, fmt.Errorf("decode ProtoSchema descriptor: %w", err)
	}
	if message.GetName() == "" {
		return nil, fmt.Errorf("ProtoSchema root message name is required")
	}
	fileName, syntax := "bqemu_storage_write.proto", "proto2"
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: &fileName, Syntax: &syntax, MessageType: []*descriptorpb.DescriptorProto{&message},
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("validate self-contained ProtoSchema: %w", err)
	}
	if file.Messages().Len() != 1 {
		return nil, fmt.Errorf("ProtoSchema must define exactly one root message")
	}
	return file.Messages().Get(0), nil
}

func convertProtoValue(field protoreflect.FieldDescriptor, value protoreflect.Value, target columnLayout) (any, error) {
	targetRepeated := strings.EqualFold(target.field.Mode, "REPEATED")
	if field.IsList() != targetRepeated {
		return nil, fmt.Errorf("protobuf repeated mode does not match destination mode %s", target.field.Mode)
	}
	if field.IsList() {
		list := value.List()
		result := make([]any, list.Len())
		for index := 0; index < list.Len(); index++ {
			converted, err := convertProtoScalar(field, list.Get(index), target)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	}
	return convertProtoScalar(field, value, target)
}

func convertProtoScalar(field protoreflect.FieldDescriptor, value protoreflect.Value, target columnLayout) (any, error) {
	targetType := strings.ToUpper(target.field.Type)
	if field.Kind() == protoreflect.MessageKind {
		if targetType != "STRUCT" && targetType != "RECORD" {
			return nil, fmt.Errorf("protobuf message does not match destination type %s", target.field.Type)
		}
		message := value.Message()
		canonicalByName := make(map[string]writedomain.Field, len(target.field.Fields))
		seenCanonical := make(map[string]struct{}, len(target.field.Fields))
		result := make(map[string]any, len(target.field.Fields))
		for _, nested := range target.field.Fields {
			canonicalByName[strings.ToLower(nested.Name)] = nested
			result[nested.Name] = nil
		}
		for index := 0; index < message.Descriptor().Fields().Len(); index++ {
			nested := message.Descriptor().Fields().Get(index)
			canonical, exists := canonicalByName[strings.ToLower(string(nested.Name()))]
			if !exists {
				return nil, fmt.Errorf("protobuf field %q is absent from destination STRUCT %q", nested.Name(), target.field.Name)
			}
			seenCanonical[strings.ToLower(canonical.Name)] = struct{}{}
			nestedTarget := columnLayout{field: canonical, isNullable: !strings.EqualFold(canonical.Mode, "REQUIRED")}
			if !nested.IsList() && !message.Has(nested) {
				if !nestedTarget.isNullable {
					return nil, fmt.Errorf("required destination STRUCT field %q is absent", canonical.Name)
				}
				continue
			}
			converted, err := convertProtoValue(nested, message.Get(nested), nestedTarget)
			if err != nil {
				return nil, err
			}
			result[canonical.Name] = converted
		}
		for _, canonical := range target.field.Fields {
			if strings.EqualFold(canonical.Mode, "REQUIRED") {
				if _, exists := seenCanonical[strings.ToLower(canonical.Name)]; !exists {
					return nil, fmt.Errorf("ProtoSchema omits required destination STRUCT field %q", canonical.Name)
				}
			}
		}
		return result, nil
	}
	if targetType == "STRUCT" || targetType == "RECORD" {
		return nil, fmt.Errorf("destination type %s requires a protobuf message", target.field.Type)
	}
	switch targetType {
	case "DATE":
		days, ok := signedProtoInteger(field.Kind(), value)
		if !ok {
			return nil, fmt.Errorf("DATE requires an int32 day count")
		}
		return time.Unix(0, 0).UTC().AddDate(0, 0, int(days)), nil
	case "TIMESTAMP":
		micros, ok := signedProtoInteger(field.Kind(), value)
		if !ok {
			return nil, fmt.Errorf("TIMESTAMP requires int64 epoch microseconds")
		}
		return time.UnixMicro(micros).UTC(), nil
	case "DATETIME":
		packed, ok := signedProtoInteger(field.Kind(), value)
		if !ok {
			return nil, fmt.Errorf("DATETIME requires packed int64 civil time")
		}
		return decodePackedDateTimeMicros(packed)
	case "NUMERIC", "BIGNUMERIC":
		if field.Kind() != protoreflect.StringKind {
			return nil, fmt.Errorf("%s requires a protobuf string", targetType)
		}
		return target.field.NormalizeDecimalValue(value.String())
	}
	switch field.Kind() {
	case protoreflect.BoolKind:
		return value.Bool(), nil
	case protoreflect.StringKind:
		return value.String(), nil
	case protoreflect.BytesKind:
		return append([]byte(nil), value.Bytes()...), nil
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return value.Float(), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int(), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return value.Uint(), nil
	case protoreflect.EnumKind:
		return int32(value.Enum()), nil
	default:
		return nil, fmt.Errorf("unsupported protobuf kind %s", field.Kind())
	}
}

func storageWriteBindableValue(field writedomain.Field, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if strings.EqualFold(field.Mode, "REPEATED") || strings.EqualFold(field.Type, "STRUCT") || strings.EqualFold(field.Type, "RECORD") {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode complex value as JSON: %w", err)
		}
		return string(encoded), nil
	}
	if strings.EqualFold(field.Type, "NUMERIC") || strings.EqualFold(field.Type, "BIGNUMERIC") {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("decimal value has Go type %T", value)
		}
		return field.NormalizeDecimalValue(text)
	}
	return value, nil
}

func signedProtoInteger(kind protoreflect.Kind, value protoreflect.Value) (int64, bool) {
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int(), true
	default:
		return 0, false
	}
}

// decodePackedDateTimeMicros mirrors the bit layout used by Google's
// CivilTimeEncoder for DATETIME ProtoRows.
// Source: https://github.com/googleapis/java-bigquerystorage/blob/main/google-cloud-bigquerystorage/src/main/java/com/google/cloud/bigquery/storage/v1/CivilTimeEncoder.java
func decodePackedDateTimeMicros(packed int64) (time.Time, error) {
	if packed < 0 || uint64(packed) > 0x0fffffffffffffff {
		return time.Time{}, fmt.Errorf("packed DATETIME is out of range")
	}
	micros := int(packed & 0xfffff)
	seconds := packed >> 20
	second := int(seconds & 0x3f)
	minute := int((seconds >> 6) & 0x3f)
	hour := int((seconds >> 12) & 0x1f)
	day := int((seconds >> 17) & 0x1f)
	month := time.Month((seconds >> 22) & 0x0f)
	year := int((seconds >> 26) & 0x3fff)
	if year < 1 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59 || micros > 999999 {
		return time.Time{}, fmt.Errorf("packed DATETIME contains invalid civil fields")
	}
	value := time.Date(year, month, day, hour, minute, second, micros*1000, time.UTC)
	if value.Year() != year || value.Month() != month || value.Day() != day {
		return time.Time{}, fmt.Errorf("packed DATETIME contains an invalid date")
	}
	return value, nil
}

func insertPreparedBatch(ctx context.Context, executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, batch preparedBatch) error {
	return insertPreparedBatchInto(ctx, executor, batch,
		physicalSchema(batch.table.ProjectID, batch.table.DatasetID), batch.table.TableID)
}

func insertPreparedBatchInto(ctx context.Context, executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, batch preparedBatch, targetSchema, targetTable string) error {
	if len(batch.rows) == 0 {
		return nil
	}
	columnNames := make([]string, len(batch.columns))
	placeholders := make([]string, len(batch.columns))
	for index, column := range batch.columns {
		columnNames[index] = quoteIdentifier(column)
		placeholders[index] = "?"
		physicalType := strings.ToUpper(strings.TrimSpace(batch.columnTypes[index]))
		if strings.HasPrefix(physicalType, "DECIMAL(") {
			placeholders[index] = "CAST(? AS " + batch.columnTypes[index] + ")"
		} else if strings.HasSuffix(physicalType, "[]") || strings.HasPrefix(physicalType, "STRUCT(") {
			placeholders[index] = "CAST(CAST(? AS JSON) AS " + batch.columnTypes[index] + ")"
		}
	}
	statement := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		quoteIdentifier(targetSchema), quoteIdentifier(targetTable),
		strings.Join(columnNames, ", "), strings.Join(placeholders, ", "))
	for rowIndex, row := range batch.rows {
		if _, err := executor.ExecContext(ctx, statement, row...); err != nil {
			return fmt.Errorf("insert staged ProtoRow %d at offset %d: %w", rowIndex, batch.startOffset+int64(rowIndex), err)
		}
	}
	return nil
}

func serializedRowsBytes(rows [][]byte) int {
	total := 0
	for _, row := range rows {
		total += len(row)
	}
	return total
}
