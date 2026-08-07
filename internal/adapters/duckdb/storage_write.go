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
// Spark 0.44.2 descriptor mapping:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/ProtobufUtils.java

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	warehouse *Warehouse
	queue     chan coordinatorOperation
	stop      context.CancelFunc
	done      chan struct{}
	closed    atomic.Bool

	// The worker owns staged. Tests may set afterStage or beforeCommit before
	// submitting any operation to inject faults at acknowledgement/transaction
	// boundaries without exposing those seams through the outbound port.
	staged       map[string][]preparedBatch
	afterStage   func()
	beforeCommit func() error
	closeOnce    sync.Once
}

type coordinatorOperation struct {
	ctx context.Context
	fn  func(context.Context) (any, error)
	out chan coordinatorResult
}

type coordinatorResult struct {
	value any
	err   error
}

type preparedBatch struct {
	streamName        string
	table             writedomain.TableReference
	startOffset       int64
	columns           []string
	rows              [][]any
	schemaFingerprint string
	payloadDigest     string
}

type stageReceipt struct {
	streamName        string
	startOffset       int64
	rowCount          int
	schemaFingerprint string
	payloadDigest     string
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

func NewStorageWriteCoordinator(warehouse *Warehouse, queueCapacity int) (*StorageWriteCoordinator, error) {
	if warehouse == nil {
		return nil, fmt.Errorf("warehouse is required")
	}
	if queueCapacity <= 0 {
		return nil, fmt.Errorf("Storage Write queue capacity must be positive")
	}
	workerContext, cancel := context.WithCancel(context.Background())
	coordinator := &StorageWriteCoordinator{
		warehouse: warehouse, queue: make(chan coordinatorOperation, queueCapacity),
		stop: cancel, done: make(chan struct{}), staged: make(map[string][]preparedBatch),
	}
	go coordinator.run(workerContext)
	return coordinator, nil
}

func (c *StorageWriteCoordinator) DescribeTable(ctx context.Context, table writedomain.TableReference) (writedomain.TableSchema, error) {
	value, err := c.submit(ctx, func(operationContext context.Context) (any, error) {
		layout, err := c.describeTable(operationContext, table)
		return layout.schema, err
	})
	if err != nil {
		return writedomain.TableSchema{}, err
	}
	return value.(writedomain.TableSchema), nil
}

func (c *StorageWriteCoordinator) AppendDefault(ctx context.Context, batch writeports.AppendBatch) (err error) {
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_append_default",
		"stream", batch.StreamName, "table", batch.Table.Name(), "start_offset", batch.StartOffset,
		"row_count", len(batch.Rows), "row_bytes", serializedRowsBytes(batch.Rows),
		"schema_fingerprint", batch.SchemaFingerprint, "payload_digest", batch.PayloadDigest,
		"transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_append_default", started, err,
			"stream", batch.StreamName, "table", batch.Table.Name(), "start_offset", batch.StartOffset,
			"row_count", len(batch.Rows), "schema_fingerprint", batch.SchemaFingerprint,
			"payload_digest", batch.PayloadDigest, "transaction_mode", "explicit")
	}()
	_, err = c.submit(ctx, func(operationContext context.Context) (_ any, resultErr error) {
		prepared, prepareErr := c.prepareBatch(operationContext, batch)
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
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_stage_pending",
		"stream", batch.StreamName, "table", batch.Table.Name(), "start_offset", batch.StartOffset,
		"row_count", len(batch.Rows), "row_bytes", serializedRowsBytes(batch.Rows),
		"schema_fingerprint", batch.SchemaFingerprint, "payload_digest", batch.PayloadDigest,
		"transaction_mode", "memory_stage")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_stage_pending", started, err,
			"stream", batch.StreamName, "table", batch.Table.Name(), "start_offset", batch.StartOffset,
			"row_count", len(batch.Rows), "schema_fingerprint", batch.SchemaFingerprint,
			"payload_digest", batch.PayloadDigest, "transaction_mode", "memory_stage")
	}()
	_, err = c.submit(ctx, func(operationContext context.Context) (any, error) {
		requestedReceipt := receiptForAppend(batch)
		var stagedRows int64
		for _, existing := range c.staged[batch.StreamName] {
			if existing.startOffset == batch.StartOffset {
				// AppendRows callers commonly retry after an acknowledgement is
				// lost. The public offset ledger cannot advance until it receives
				// that acknowledgement, so an identical durable staging receipt
				// must be acknowledged again instead of being appended twice.
				// A different receipt at the same offset remains a conflict.
				// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows
				if existing.receipt() == requestedReceipt && existing.table == batch.Table {
					return nil, nil
				}
				return nil, fmt.Errorf("coordinator receipt conflict at offset %d for stream %s", batch.StartOffset, batch.StreamName)
			}
			stagedRows += int64(len(existing.rows))
		}
		if batch.StartOffset != stagedRows {
			return nil, fmt.Errorf("coordinator offset invariant: got %d, want %d", batch.StartOffset, stagedRows)
		}
		prepared, prepareErr := c.prepareBatch(operationContext, batch)
		if prepareErr != nil {
			return nil, prepareErr
		}
		c.staged[batch.StreamName] = append(c.staged[batch.StreamName], prepared)
		if c.afterStage != nil {
			c.afterStage()
		}
		return nil, nil
	})
	return err
}

func receiptForAppend(batch writeports.AppendBatch) stageReceipt {
	return stageReceipt{
		streamName: batch.StreamName, startOffset: batch.StartOffset, rowCount: len(batch.Rows),
		schemaFingerprint: batch.SchemaFingerprint, payloadDigest: batch.PayloadDigest,
	}
}

func (b preparedBatch) receipt() stageReceipt {
	return stageReceipt{
		streamName: b.streamName, startOffset: b.startOffset, rowCount: len(b.rows),
		schemaFingerprint: b.schemaFingerprint, payloadDigest: b.payloadDigest,
	}
}

func (c *StorageWriteCoordinator) CommitPending(ctx context.Context, request writeports.CommitRequest) (err error) {
	streamFingerprint := observability.Digest([]byte(strings.Join(request.StreamNames, "\n")))
	started := observability.LogSideEffectStart(ctx, "duckdb", "storage_write_commit_pending",
		"table", request.Parent.Name(), "stream_count", len(request.StreamNames),
		"stream_set_fingerprint", streamFingerprint, "commit_time", request.CommitTime,
		"transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "storage_write_commit_pending", started, err,
			"table", request.Parent.Name(), "stream_count", len(request.StreamNames),
			"stream_set_fingerprint", streamFingerprint, "commit_time", request.CommitTime,
			"transaction_mode", "explicit")
	}()
	_, err = c.submit(ctx, func(operationContext context.Context) (_ any, resultErr error) {
		batches := make([]preparedBatch, 0)
		for _, streamName := range request.StreamNames {
			for _, batch := range c.staged[streamName] {
				if batch.table != request.Parent {
					return nil, fmt.Errorf("stream %s belongs to another table", streamName)
				}
				batches = append(batches, batch)
			}
		}
		tx, beginErr := c.warehouse.db.BeginTx(operationContext, nil)
		if beginErr != nil {
			return nil, fmt.Errorf("begin pending stream transaction: %w", beginErr)
		}
		defer func() {
			if resultErr != nil {
				_ = tx.Rollback()
			}
		}()
		for _, batch := range batches {
			if insertErr := insertPreparedBatch(operationContext, tx, batch); insertErr != nil {
				return nil, insertErr
			}
		}
		if c.beforeCommit != nil {
			if injectedErr := c.beforeCommit(); injectedErr != nil {
				return nil, injectedErr
			}
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, fmt.Errorf("commit pending stream transaction: %w", commitErr)
		}
		for _, streamName := range request.StreamNames {
			delete(c.staged, streamName)
		}
		return nil, nil
	})
	return err
}

func (c *StorageWriteCoordinator) DiscardPending(ctx context.Context, streamName string) error {
	_, err := c.submit(ctx, func(context.Context) (any, error) {
		delete(c.staged, streamName)
		return nil, nil
	})
	return err
}

func (c *StorageWriteCoordinator) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.stop()
	})
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *StorageWriteCoordinator) submit(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("operation context is required")
	}
	if c.closed.Load() {
		return nil, errStorageWriteCoordinatorClosed
	}
	operation := coordinatorOperation{ctx: ctx, fn: fn, out: make(chan coordinatorResult, 1)}
	select {
	case c.queue <- operation:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, errStorageWriteCoordinatorClosed
	}
	select {
	case result := <-operation.out:
		return result.value, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, errStorageWriteCoordinatorClosed
	}
}

func (c *StorageWriteCoordinator) run(ctx context.Context) {
	defer close(c.done)
	for {
		select {
		case <-ctx.Done():
			return
		case operation := <-c.queue:
			if err := operation.ctx.Err(); err != nil {
				operation.out <- coordinatorResult{err: err}
				continue
			}
			value, err := operation.fn(operation.ctx)
			operation.out <- coordinatorResult{value: value, err: err}
		}
	}
}

func (c *StorageWriteCoordinator) prepareBatch(ctx context.Context, batch writeports.AppendBatch) (preparedBatch, error) {
	layout, err := c.describeTable(ctx, batch.Table)
	if err != nil {
		return preparedBatch{}, err
	}
	message, err := messageDescriptor(batch.Descriptor)
	if err != nil {
		return preparedBatch{}, err
	}
	columnsByName := make(map[string]columnLayout, len(layout.columns))
	for _, column := range layout.columns {
		columnsByName[strings.ToLower(column.field.Name)] = column
	}
	fields := message.Fields()
	columns := make([]string, fields.Len())
	targets := make([]columnLayout, fields.Len())
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		target, exists := columnsByName[strings.ToLower(string(field.Name()))]
		if !exists {
			return preparedBatch{}, fmt.Errorf("ProtoSchema field %q is not present in destination table", field.Name())
		}
		columns[index] = target.field.Name
		targets[index] = target
	}
	decodedRows := make([][]any, len(batch.Rows))
	for rowIndex, serialized := range batch.Rows {
		row := dynamicpb.NewMessage(message)
		if err := proto.Unmarshal(serialized, row); err != nil {
			return preparedBatch{}, fmt.Errorf("decode ProtoRow %d: %w", rowIndex, err)
		}
		if err := proto.CheckInitialized(row); err != nil {
			return preparedBatch{}, fmt.Errorf("ProtoRow %d misses a required field: %w", rowIndex, err)
		}
		if len(row.GetUnknown()) != 0 {
			return preparedBatch{}, fmt.Errorf("ProtoRow %d contains fields absent from ProtoSchema", rowIndex)
		}
		values := make([]any, fields.Len())
		for fieldIndex := 0; fieldIndex < fields.Len(); fieldIndex++ {
			field := fields.Get(fieldIndex)
			if !field.IsList() && !row.Has(field) {
				if !targets[fieldIndex].isNullable {
					return preparedBatch{}, fmt.Errorf("required destination column %q is absent", targets[fieldIndex].field.Name)
				}
				values[fieldIndex] = nil
				continue
			}
			converted, convertErr := convertProtoValue(field, row.Get(field), targets[fieldIndex])
			if convertErr != nil {
				return preparedBatch{}, fmt.Errorf("convert ProtoRow %d field %q: %w", rowIndex, field.Name(), convertErr)
			}
			values[fieldIndex] = converted
		}
		decodedRows[rowIndex] = values
	}
	return preparedBatch{
		streamName: batch.StreamName, table: batch.Table, startOffset: batch.StartOffset,
		columns: columns, rows: decodedRows, schemaFingerprint: batch.SchemaFingerprint,
		payloadDigest: batch.PayloadDigest,
	}, nil
}

func (c *StorageWriteCoordinator) describeTable(ctx context.Context, table writedomain.TableReference) (tableLayout, error) {
	rows, err := c.warehouse.db.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`, physicalSchema(table.ProjectID, table.DatasetID), table.TableID)
	if err != nil {
		return tableLayout{}, fmt.Errorf("describe Storage Write destination: %w", err)
	}
	defer rows.Close()
	var layout tableLayout
	for rows.Next() {
		var name, dataType, nullable string
		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			return tableLayout{}, fmt.Errorf("scan Storage Write destination schema: %w", err)
		}
		field := duckDBStorageWriteField(name, dataType, nullable)
		layout.schema.Fields = append(layout.schema.Fields, field)
		layout.columns = append(layout.columns, columnLayout{
			field: field, duckDBType: dataType, isNullable: strings.EqualFold(nullable, "YES"),
		})
	}
	if err := rows.Err(); err != nil {
		return tableLayout{}, fmt.Errorf("read Storage Write destination schema: %w", err)
	}
	if len(layout.columns) == 0 {
		return tableLayout{}, fmt.Errorf("%w: %s", writeports.ErrTableNotFound, table.Name())
	}
	return layout, nil
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
		message := value.Message()
		result := make(map[string]any, message.Descriptor().Fields().Len())
		for index := 0; index < message.Descriptor().Fields().Len(); index++ {
			nested := message.Descriptor().Fields().Get(index)
			if !nested.IsList() && !message.Has(nested) {
				result[string(nested.Name())] = nil
				continue
			}
			nestedTarget := columnLayout{field: writedomain.Field{Name: string(nested.Name()), Type: protoKindFallbackType(nested.Kind()), Mode: "NULLABLE"}, isNullable: true}
			converted, err := convertProtoValue(nested, message.Get(nested), nestedTarget)
			if err != nil {
				return nil, err
			}
			result[string(nested.Name())] = converted
		}
		return result, nil
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
	case "GEOGRAPHY":
		if field.Kind() != protoreflect.BytesKind {
			return nil, fmt.Errorf("GEOGRAPHY requires encoded bytes")
		}
		return string(value.Bytes()), nil
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
// CivilTimeEncoder and Spark's DATETIME ProtoRows mapping.
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
	if len(batch.rows) == 0 {
		return nil
	}
	columnNames := make([]string, len(batch.columns))
	placeholders := make([]string, len(batch.columns))
	for index, column := range batch.columns {
		columnNames[index] = quoteIdentifier(column)
		placeholders[index] = "?"
	}
	statement := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		quoteIdentifier(physicalSchema(batch.table.ProjectID, batch.table.DatasetID)),
		quoteIdentifier(batch.table.TableID), strings.Join(columnNames, ", "), strings.Join(placeholders, ", "))
	for rowIndex, row := range batch.rows {
		if _, err := executor.ExecContext(ctx, statement, row...); err != nil {
			return fmt.Errorf("insert staged ProtoRow %d at offset %d: %w", rowIndex, batch.startOffset+int64(rowIndex), err)
		}
	}
	return nil
}

func duckDBStorageWriteField(name, dataType, nullable string) writedomain.Field {
	mode := "REQUIRED"
	if strings.EqualFold(nullable, "YES") {
		mode = "NULLABLE"
	}
	normalized := strings.ToUpper(strings.TrimSpace(dataType))
	if strings.HasSuffix(normalized, "[]") {
		mode = "REPEATED"
		normalized = strings.TrimSpace(strings.TrimSuffix(normalized, "[]"))
	}
	fieldType := "STRING"
	switch {
	case normalized == "BOOLEAN" || normalized == "BOOL":
		fieldType = "BOOL"
	case strings.Contains(normalized, "INT"):
		fieldType = "INT64"
	case normalized == "DOUBLE" || normalized == "FLOAT" || normalized == "REAL":
		fieldType = "FLOAT64"
	case strings.HasPrefix(normalized, "DECIMAL") || strings.HasPrefix(normalized, "NUMERIC"):
		fieldType = "NUMERIC"
	case normalized == "BLOB" || normalized == "BYTEA":
		fieldType = "BYTES"
	case normalized == "DATE":
		fieldType = "DATE"
	case normalized == "TIME":
		fieldType = "TIME"
	case strings.Contains(normalized, "TIMESTAMP WITH TIME ZONE") || normalized == "TIMESTAMPTZ":
		fieldType = "TIMESTAMP"
	case strings.HasPrefix(normalized, "TIMESTAMP"):
		fieldType = "DATETIME"
	case normalized == "JSON":
		fieldType = "JSON"
	case strings.HasPrefix(normalized, "STRUCT"):
		fieldType = "STRUCT"
	}
	return writedomain.Field{Name: name, Type: fieldType, Mode: mode}
}

func protoKindFallbackType(kind protoreflect.Kind) string {
	switch kind {
	case protoreflect.BoolKind:
		return "BOOL"
	case protoreflect.BytesKind:
		return "BYTES"
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return "FLOAT64"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "INT64"
	case protoreflect.MessageKind:
		return "STRUCT"
	default:
		return "STRING"
	}
}

func serializedRowsBytes(rows [][]byte) int {
	total := 0
	for _, row := range rows {
		total += len(row)
	}
	return total
}
