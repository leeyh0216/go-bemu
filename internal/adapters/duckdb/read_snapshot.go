package duckdb

// DuckDBReadSnapshotMaterializer is the outbound adapter that turns one
// statement-level DuckDB result into the immutable ordinal space shared by all
// Storage Read streams. Source tables are never queried once per stream.
//
// Protocol sources:
//   - snapshot consistency and multiple streams: https://cloud.google.com/bigquery/docs/reference/storage#key_features
//   - selected_fields and row_restriction: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession.tablereadoptions
// DuckDB sources:
//   - transaction isolation: https://duckdb.org/docs/stable/connect/concurrency
//   - database/sql query lifetime: https://pkg.go.dev/database/sql#Rows

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

type ReadTableSchemaResolver interface {
	GetTable(context.Context, string, string, string) (catalogdomain.Table, error)
}

const storageReadMaterializeOperation = "storage_read.snapshot.materialize"

// Request-derived failures are classified before they cross the outbound
// port. INVALID_ARGUMENT means the projection/filter cannot be valid for this
// table, NOT_FOUND means the catalog resource is absent, and UNIMPLEMENTED
// means the official request shape is valid but this adapter does not support
// it yet. DuckDB query, staging, and codec failures remain unclassified so the
// application reports INTERNAL without exposing backend details.
//
// Official contracts:
//   - status semantics: https://grpc.io/docs/guides/status-codes/
//   - projection/filter options: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession.tablereadoptions
//   - snapshot_time: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession.tablemodifiers
func classifiedReadSnapshotError(code readdomain.ErrorCode, cause error) error {
	return readdomain.NewError(code, storageReadMaterializeOperation, cause)
}

type ReadSnapshotConfig struct {
	TempDir              string
	TempFilePattern      string
	SpillThresholdBytes  int64
	MaxRowBytes          int64
	MaxBatchBytes        int
	MaxSchemaBytes       int
	MaxSnapshotBytes     int64
	MaxSnapshotRows      int64
	ProtocolModelVersion string
}

type DuckDBReadSnapshotMaterializer struct {
	warehouse *Warehouse
	resolver  ReadTableSchemaResolver
	config    ReadSnapshotConfig
}

var _ readports.SnapshotMaterializer = (*DuckDBReadSnapshotMaterializer)(nil)

func NewReadSnapshotMaterializer(warehouse *Warehouse, resolver ReadTableSchemaResolver, config ReadSnapshotConfig) (*DuckDBReadSnapshotMaterializer, error) {
	if warehouse == nil || warehouse.db == nil || resolver == nil {
		return nil, fmt.Errorf("DuckDB warehouse and read schema resolver are required")
	}
	if strings.TrimSpace(config.TempDir) == "" || strings.TrimSpace(config.TempFilePattern) == "" {
		return nil, fmt.Errorf("read snapshot temp directory and file pattern are required")
	}
	if config.SpillThresholdBytes < 0 || config.MaxRowBytes <= 0 || config.MaxBatchBytes <= 0 || config.MaxSchemaBytes <= 0 || config.MaxSnapshotBytes <= 0 || config.MaxSnapshotRows <= 0 {
		return nil, fmt.Errorf("read snapshot spill threshold must be non-negative and limits must be positive")
	}
	if strings.TrimSpace(config.ProtocolModelVersion) == "" {
		return nil, fmt.Errorf("read snapshot protocol model version is required")
	}
	info, err := os.Stat(config.TempDir)
	if err != nil {
		return nil, fmt.Errorf("inspect read snapshot temp directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("read snapshot temp path %q is not a directory", config.TempDir)
	}
	return &DuckDBReadSnapshotMaterializer{warehouse: warehouse, resolver: resolver, config: config}, nil
}

func (m *DuckDBReadSnapshotMaterializer) Materialize(ctx context.Context, request readdomain.MaterializeRequest) (_ readports.ReadSnapshot, resultErr error) {
	const operation = storageReadMaterializeOperation
	projectID, datasetID, tableID, err := parseStorageReadTable(request.Table)
	if err != nil {
		return nil, classifiedReadSnapshotError(readdomain.ErrorInvalidArgument, err)
	}
	if request.SnapshotTime != nil {
		return nil, classifiedReadSnapshotError(readdomain.ErrorUnimplemented,
			fmt.Errorf("historical snapshot_time is not supported by the DuckDB adapter"))
	}

	resolveStarted := observability.LogSideEffectStart(ctx, "duckdb", "resolve_read_schema",
		"operation_name", operation, "model_version", m.config.ProtocolModelVersion,
		"project_id", projectID, "dataset_id", datasetID, "table_id", tableID)
	table, err := m.resolver.GetTable(ctx, projectID, datasetID, tableID)
	observability.LogSideEffectEnd(ctx, "duckdb", "resolve_read_schema", resolveStarted, err,
		"operation_name", operation, "model_version", m.config.ProtocolModelVersion,
		"project_id", projectID, "dataset_id", datasetID, "table_id", tableID)
	if err != nil {
		cause := fmt.Errorf("resolve Storage Read table schema: %w", err)
		if errors.Is(err, catalogdomain.ErrNotFound) {
			return nil, classifiedReadSnapshotError(readdomain.ErrorNotFound, cause)
		}
		return nil, cause
	}
	if table.ProjectID != projectID || table.DatasetID != datasetID || table.ID != tableID {
		return nil, fmt.Errorf("schema resolver returned table %s:%s.%s for %s", table.ProjectID, table.DatasetID, table.ID, request.Table)
	}
	if table.Type != "" && !strings.EqualFold(table.Type, "TABLE") {
		return nil, classifiedReadSnapshotError(readdomain.ErrorUnimplemented,
			fmt.Errorf("Storage Read supports physical TABLE resources, got %q", table.Type))
	}
	fields, err := projectReadFields(table.Schema, request.SelectedFields)
	if err != nil {
		return nil, classifiedReadSnapshotError(readdomain.ErrorInvalidArgument, err)
	}
	filterSQL, filterArgs, filterShape, err := compileRowRestrictionWithShape(request.RowRestriction, table.Schema)
	if err != nil {
		return nil, classifiedReadSnapshotError(readdomain.ErrorInvalidArgument,
			fmt.Errorf("compile row restriction: %w", err))
	}

	arrowSchema, referenceSchema, err := referenceReadSchema(request.Format, fields)
	if err != nil {
		return nil, err
	}
	if len(referenceSchema.Serialized) > m.config.MaxSchemaBytes {
		return nil, classifiedReadSnapshotError(readdomain.ErrorResourceExhausted,
			fmt.Errorf("Storage Read reference schema is %d bytes, exceeds configured max %d", len(referenceSchema.Serialized), m.config.MaxSchemaBytes))
	}
	statement := materializeReadStatement(projectID, datasetID, tableID, fields, filterSQL)
	queryStarted := observability.LogSideEffectStart(ctx, "duckdb", "materialize_read_snapshot",
		"operation_name", operation, "model_version", m.config.ProtocolModelVersion,
		"project_id", projectID, "dataset_id", datasetID, "table_id", tableID,
		"format", request.Format.String(), "selected_field_count", len(fields),
		"statement_bytes", len(statement), "statement_digest", observability.Digest([]byte(statement)),
		"restriction_bytes", len(request.RowRestriction), "restriction_digest", observability.Digest([]byte(request.RowRestriction)),
		"spill_threshold_bytes", m.config.SpillThresholdBytes,
		"max_snapshot_bytes", m.config.MaxSnapshotBytes)
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "materialize_read_snapshot", queryStarted, resultErr,
			"operation_name", operation, "model_version", m.config.ProtocolModelVersion,
			"project_id", projectID, "dataset_id", datasetID, "table_id", tableID,
			"format", request.Format.String())
	}()

	rows, err := m.warehouse.db.QueryContext(ctx, statement, filterArgs...)
	if err != nil {
		return nil, fmt.Errorf("query read snapshot: %w", err)
	}
	stager := newSnapshotStager(m.config)
	rowsClosed := false
	defer func() {
		if !rowsClosed {
			resultErr = errors.Join(resultErr, rows.Close())
		}
		if resultErr != nil {
			resultErr = errors.Join(resultErr, stager.abort(ctx))
		}
	}()
	for rows.Next() {
		if int64(stager.rowCount()) >= m.config.MaxSnapshotRows {
			return nil, fmt.Errorf("read snapshot exceeds configured max rows %d", m.config.MaxSnapshotRows)
		}
		raw := make([]any, len(fields))
		destinations := make([]any, len(fields))
		for index := range raw {
			destinations[index] = &raw[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("scan read snapshot row %d: %w", stager.rowCount(), err)
		}
		normalized, err := normalizeSnapshotRow(fields, raw)
		if err != nil {
			return nil, fmt.Errorf("normalize read snapshot row %d: %w", stager.rowCount(), err)
		}
		encoded, err := encodeSnapshotRow(normalized)
		if err != nil {
			return nil, err
		}
		if int64(len(encoded)) > m.config.MaxRowBytes {
			return nil, fmt.Errorf("read snapshot row %d is %d bytes, exceeds configured max %d", stager.rowCount(), len(encoded), m.config.MaxRowBytes)
		}
		if err := stager.append(ctx, encoded); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate read snapshot: %w", err)
	}
	closeErr := rows.Close()
	rowsClosed = true
	if closeErr != nil {
		return nil, fmt.Errorf("close read snapshot query: %w", closeErr)
	}
	storage, err := stager.finish(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := &duckDBReadSnapshot{
		metadata: readdomain.SnapshotMetadata{
			Schema:         referenceSchema,
			RowCount:       int64(storage.rowCount()),
			EstimatedBytes: storage.encodedBytes,
			RetainedBytes:  storage.retainedBytes,
			SelectedFields: canonicalReadFieldNames(fields),
			FilterShape:    filterShape,
		},
		format:        request.Format,
		fields:        cloneCatalogFields(fields),
		arrowSchema:   arrowSchema,
		memoryRows:    storage.memoryRows,
		spillPath:     storage.spillPath,
		spillRows:     storage.spillRows,
		maxBatchBytes: m.config.MaxBatchBytes,
		modelVersion:  m.config.ProtocolModelVersion,
	}
	return snapshot, nil
}

func canonicalReadFieldNames(fields []catalogdomain.Field) []string {
	names := make([]string, len(fields))
	for index, field := range fields {
		names[index] = field.Name
	}
	return names
}

func parseStorageReadTable(resource string) (string, string, string, error) {
	parts := strings.Split(resource, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "datasets" || parts[4] != "tables" ||
		parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", "", fmt.Errorf("table must have the form projects/{project}/datasets/{dataset}/tables/{table}")
	}
	return parts[1], parts[3], parts[5], nil
}

func projectReadFields(schema []catalogdomain.Field, selected []string) ([]catalogdomain.Field, error) {
	if len(selected) == 0 {
		return cloneCatalogFields(schema), nil
	}
	projected := make([]catalogdomain.Field, 0, len(selected))
	indexes := make(map[string]int, len(selected))
	for _, requested := range selected {
		parts := strings.Split(strings.TrimSpace(requested), ".")
		if len(parts) == 0 || parts[0] == "" {
			return nil, fmt.Errorf("selected_field %q is invalid", requested)
		}
		field, full, err := projectReadFieldPath(schema, parts)
		if err != nil {
			return nil, fmt.Errorf("selected_field %q: %w", requested, err)
		}
		key := strings.ToLower(field.Name)
		if index, exists := indexes[key]; exists {
			projected[index] = mergeProjectedReadField(projected[index], field, full)
			continue
		}
		indexes[key] = len(projected)
		projected = append(projected, field)
	}
	return projected, nil
}

func projectReadFieldPath(schema []catalogdomain.Field, path []string) (catalogdomain.Field, bool, error) {
	for _, field := range schema {
		if !strings.EqualFold(field.Name, path[0]) {
			continue
		}
		if len(path) == 1 {
			return cloneCatalogField(field), true, nil
		}
		if !strings.EqualFold(field.Type, "RECORD") && !strings.EqualFold(field.Type, "STRUCT") {
			return catalogdomain.Field{}, false, fmt.Errorf("cannot select a nested field from non-STRUCT field %q", field.Name)
		}
		child, _, err := projectReadFieldPath(field.Fields, path[1:])
		if err != nil {
			return catalogdomain.Field{}, false, err
		}
		result := cloneCatalogField(field)
		result.Fields = []catalogdomain.Field{child}
		return result, false, nil
	}
	return catalogdomain.Field{}, false, fmt.Errorf("does not exist")
}

func mergeProjectedReadField(existing, incoming catalogdomain.Field, incomingFull bool) catalogdomain.Field {
	if incomingFull || !isProjectedStruct(existing) {
		return incoming
	}
	if !isProjectedStruct(incoming) {
		return existing
	}
	for _, child := range incoming.Fields {
		found := false
		for index := range existing.Fields {
			if !strings.EqualFold(existing.Fields[index].Name, child.Name) {
				continue
			}
			existing.Fields[index] = mergeProjectedReadField(existing.Fields[index], child, false)
			found = true
			break
		}
		if !found {
			existing.Fields = append(existing.Fields, child)
		}
	}
	return existing
}

func isProjectedStruct(field catalogdomain.Field) bool {
	return strings.EqualFold(field.Type, "RECORD") || strings.EqualFold(field.Type, "STRUCT")
}

func materializeReadStatement(projectID, datasetID, tableID string, fields []catalogdomain.Field, filter string) string {
	columns := make([]string, len(fields))
	for index, field := range fields {
		identifier := quoteIdentifier(field.Name)
		if isProjectedStruct(field) {
			columns[index] = materializeReadStructProjection(field, identifier) + " AS " + identifier
			continue
		}
		if strings.EqualFold(field.Type, "JSON") && !strings.EqualFold(field.Mode, "REPEATED") {
			// duckdb-go can decode JSON objects into map[string]any, which may
			// coerce JSON numbers through float64. Storage Read represents JSON as
			// a string, so casting at the SQL boundary preserves DuckDB's exact
			// serialized number text.
			// Source: https://cloud.google.com/bigquery/docs/reference/storage#schema_conversion
			columns[index] = "CAST(" + identifier + " AS VARCHAR) AS " + identifier
			continue
		}
		columns[index] = identifier
	}
	statement := "SELECT " + strings.Join(columns, ", ") + " FROM " +
		quoteIdentifier(physicalSchema(projectID, datasetID)) + "." + quoteIdentifier(tableID)
	if filter != "" {
		statement += " WHERE " + filter
	}
	return statement
}

func materializeReadStructProjection(field catalogdomain.Field, source string) string {
	if strings.EqualFold(field.Mode, "REPEATED") {
		element := cloneCatalogField(field)
		element.Mode = "REQUIRED"
		return "list_transform(" + source + ", item -> " + materializeReadStructProjection(element, "item") + ")"
	}
	children := make([]string, len(field.Fields))
	for index, child := range field.Fields {
		childSource := source + "." + quoteIdentifier(child.Name)
		value := childSource
		if isProjectedStruct(child) {
			value = materializeReadStructProjection(child, childSource)
		}
		children[index] = quoteSQLString(child.Name) + ": " + value
	}
	return "{" + strings.Join(children, ", ") + "}"
}

func cloneCatalogFields(fields []catalogdomain.Field) []catalogdomain.Field {
	result := make([]catalogdomain.Field, len(fields))
	for index, field := range fields {
		result[index] = cloneCatalogField(field)
	}
	return result
}

func cloneCatalogField(field catalogdomain.Field) catalogdomain.Field {
	field.Precision = cloneInt64Pointer(field.Precision)
	field.Scale = cloneInt64Pointer(field.Scale)
	field.Fields = cloneCatalogFields(field.Fields)
	return field
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

type stagedRowLocation struct {
	offset int64
	length uint64
}

type snapshotStorage struct {
	memoryRows    [][]byte
	spillPath     string
	spillRows     []stagedRowLocation
	encodedBytes  int64
	retainedBytes int64
}

func (s snapshotStorage) rowCount() int {
	if s.spillPath != "" {
		return len(s.spillRows)
	}
	return len(s.memoryRows)
}

type snapshotStager struct {
	config        ReadSnapshotConfig
	memoryRows    [][]byte
	spillFile     *os.File
	spillPath     string
	spillRows     []stagedRowLocation
	encodedBytes  int64
	retainedBytes int64
}

func newSnapshotStager(config ReadSnapshotConfig) *snapshotStager {
	return &snapshotStager{config: config}
}

func (s *snapshotStager) rowCount() int {
	if s.spillFile != nil {
		return len(s.spillRows)
	}
	return len(s.memoryRows)
}

func (s *snapshotStager) append(ctx context.Context, row []byte) error {
	rowBytes := int64(len(row))
	if rowBytes > s.config.MaxSnapshotBytes-s.encodedBytes {
		return classifiedReadSnapshotError(readdomain.ErrorResourceExhausted,
			fmt.Errorf("read snapshot encoded bytes exceed configured max %d", s.config.MaxSnapshotBytes))
	}
	nextBytes := s.encodedBytes + rowBytes
	willSpill := s.spillFile == nil && nextBytes > s.config.SpillThresholdBytes
	nextRetainedBytes, err := s.nextRetainedBytes(rowBytes, willSpill)
	if err != nil {
		return err
	}
	if willSpill {
		if err := s.startSpill(ctx); err != nil {
			return err
		}
	}
	if s.spillFile != nil {
		if err := s.writeSpillRow(ctx, row); err != nil {
			return err
		}
	} else {
		s.memoryRows = append(s.memoryRows, slices.Clone(row))
	}
	s.encodedBytes = nextBytes
	s.retainedBytes = nextRetainedBytes
	return nil
}

// Spill storage adds one uint64 length prefix per encoded row. Admission uses
// this retained size, not estimated bytes scanned, because it is the resource
// that remains live until session expiry or shutdown.
func (s *snapshotStager) nextRetainedBytes(rowBytes int64, willSpill bool) (int64, error) {
	const spillFrameBytes int64 = 8
	current := s.retainedBytes
	charge := rowBytes
	if s.spillFile != nil {
		remaining := s.config.MaxSnapshotBytes - current
		if remaining < spillFrameBytes || rowBytes > remaining-spillFrameBytes {
			return 0, classifiedReadSnapshotError(readdomain.ErrorResourceExhausted,
				fmt.Errorf("read snapshot retained bytes exceed configured max %d", s.config.MaxSnapshotBytes))
		}
		charge += spillFrameBytes
	} else if willSpill {
		rowCountAfterAppend := int64(len(s.memoryRows)) + 1
		nextEncodedBytes := s.encodedBytes + rowBytes
		if rowCountAfterAppend > (s.config.MaxSnapshotBytes-nextEncodedBytes)/spillFrameBytes {
			return 0, classifiedReadSnapshotError(readdomain.ErrorResourceExhausted,
				fmt.Errorf("read snapshot spill framing exceeds configured max %d", s.config.MaxSnapshotBytes))
		}
		return nextEncodedBytes + rowCountAfterAppend*spillFrameBytes, nil
	}
	if charge > s.config.MaxSnapshotBytes-current {
		return 0, classifiedReadSnapshotError(readdomain.ErrorResourceExhausted,
			fmt.Errorf("read snapshot retained bytes exceed configured max %d", s.config.MaxSnapshotBytes))
	}
	return current + charge, nil
}

func (s *snapshotStager) startSpill(ctx context.Context) (resultErr error) {
	tempDir := []byte(filepath.Clean(s.config.TempDir))
	started := observability.LogSideEffectStart(ctx, "duckdb", "create_read_snapshot_spill",
		"model_version", s.config.ProtocolModelVersion,
		"temp_dir_bytes", len(tempDir), "temp_dir_digest", observability.Digest(tempDir),
		"staged_row_count", len(s.memoryRows), "staged_bytes", s.encodedBytes)
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "create_read_snapshot_spill", started, resultErr,
			"model_version", s.config.ProtocolModelVersion,
			"temp_dir_bytes", len(tempDir), "temp_dir_digest", observability.Digest(tempDir))
	}()
	file, err := os.CreateTemp(s.config.TempDir, s.config.TempFilePattern)
	if err != nil {
		return fmt.Errorf("create read snapshot spill: %w", err)
	}
	s.spillFile = file
	s.spillPath = file.Name()
	for _, row := range s.memoryRows {
		if err := s.writeSpillRow(ctx, row); err != nil {
			return err
		}
	}
	s.memoryRows = nil
	return nil
}

func (s *snapshotStager) writeSpillRow(ctx context.Context, row []byte) (resultErr error) {
	ordinal := len(s.spillRows)
	started := observability.LogSideEffectStart(ctx, "duckdb", "write_read_snapshot_spill_row",
		"model_version", s.config.ProtocolModelVersion, "row_ordinal", ordinal,
		"row_bytes", len(row), "row_digest", observability.Digest(row))
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "write_read_snapshot_spill_row", started, resultErr,
			"model_version", s.config.ProtocolModelVersion, "row_ordinal", ordinal,
			"row_bytes", len(row), "row_digest", observability.Digest(row))
	}()
	position, err := s.spillFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("seek read snapshot spill: %w", err)
	}
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(row)))
	if _, err := s.spillFile.Write(length[:]); err != nil {
		return fmt.Errorf("write read snapshot spill length: %w", err)
	}
	if _, err := s.spillFile.Write(row); err != nil {
		return fmt.Errorf("write read snapshot spill row: %w", err)
	}
	s.spillRows = append(s.spillRows, stagedRowLocation{offset: position + int64(len(length)), length: uint64(len(row))})
	return nil
}

func (s *snapshotStager) finish(ctx context.Context) (_ snapshotStorage, resultErr error) {
	if s.spillFile != nil {
		started := observability.LogSideEffectStart(ctx, "duckdb", "close_read_snapshot_spill_writer",
			"model_version", s.config.ProtocolModelVersion, "row_count", len(s.spillRows),
			"encoded_bytes", s.encodedBytes, "retained_bytes", s.retainedBytes)
		defer func() {
			observability.LogSideEffectEnd(ctx, "duckdb", "close_read_snapshot_spill_writer", started, resultErr,
				"model_version", s.config.ProtocolModelVersion, "row_count", len(s.spillRows),
				"encoded_bytes", s.encodedBytes, "retained_bytes", s.retainedBytes)
		}()
		if err := s.spillFile.Sync(); err != nil {
			return snapshotStorage{}, fmt.Errorf("sync read snapshot spill: %w", err)
		}
		if err := s.spillFile.Close(); err != nil {
			return snapshotStorage{}, fmt.Errorf("close read snapshot spill: %w", err)
		}
		s.spillFile = nil
	}
	return snapshotStorage{
		memoryRows: slices.Clone(s.memoryRows), spillPath: s.spillPath,
		spillRows: slices.Clone(s.spillRows), encodedBytes: s.encodedBytes,
		retainedBytes: s.retainedBytes,
	}, nil
}

func (s *snapshotStager) abort(ctx context.Context) error {
	var result error
	if s.spillFile != nil {
		started := observability.LogSideEffectStart(ctx, "duckdb", "close_failed_read_snapshot_spill_writer",
			"model_version", s.config.ProtocolModelVersion, "row_count", len(s.spillRows),
			"encoded_bytes", s.encodedBytes, "retained_bytes", s.retainedBytes)
		err := s.spillFile.Close()
		observability.LogSideEffectEnd(ctx, "duckdb", "close_failed_read_snapshot_spill_writer", started, err,
			"model_version", s.config.ProtocolModelVersion, "row_count", len(s.spillRows),
			"encoded_bytes", s.encodedBytes, "retained_bytes", s.retainedBytes)
		result = errors.Join(result, err)
		s.spillFile = nil
	}
	if s.spillPath != "" {
		started := observability.LogSideEffectStart(ctx, "duckdb", "remove_failed_read_snapshot_spill",
			"model_version", s.config.ProtocolModelVersion)
		err := os.Remove(s.spillPath)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		observability.LogSideEffectEnd(ctx, "duckdb", "remove_failed_read_snapshot_spill", started, err,
			"model_version", s.config.ProtocolModelVersion)
		result = errors.Join(result, err)
	}
	return result
}

type duckDBReadSnapshot struct {
	metadata      readdomain.SnapshotMetadata
	format        readdomain.Format
	fields        []catalogdomain.Field
	arrowSchema   *arrow.Schema
	memoryRows    [][]byte
	spillPath     string
	spillRows     []stagedRowLocation
	maxBatchBytes int
	modelVersion  string

	mu      sync.Mutex
	closed  bool
	readers sync.WaitGroup
}

var _ readports.ReadSnapshot = (*duckDBReadSnapshot)(nil)

func (s *duckDBReadSnapshot) Metadata() readdomain.SnapshotMetadata {
	metadata := s.metadata
	metadata.Schema.Serialized = slices.Clone(metadata.Schema.Serialized)
	return metadata
}

func (s *duckDBReadSnapshot) OpenRange(ctx context.Context, start, end, maxRows int64) (_ readports.BatchIterator, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if start < 0 || end < start || end > s.metadata.RowCount || maxRows <= 0 {
		return nil, fmt.Errorf("invalid read snapshot range [%d,%d) or max rows %d", start, end, maxRows)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("read snapshot is closed")
	}
	s.readers.Add(1)
	s.mu.Unlock()

	iterator := &duckDBSnapshotIterator{
		ctx: ctx, snapshot: s, next: start, end: end, maxRows: maxRows, maxBytes: s.maxBatchBytes,
	}
	if s.spillPath != "" {
		started := observability.LogSideEffectStart(ctx, "duckdb", "open_read_snapshot_spill_reader",
			"model_version", s.modelVersion, "start_offset", start, "end_offset", end)
		file, err := os.Open(s.spillPath)
		observability.LogSideEffectEnd(ctx, "duckdb", "open_read_snapshot_spill_reader", started, err,
			"model_version", s.modelVersion, "start_offset", start, "end_offset", end)
		if err != nil {
			s.readers.Done()
			return nil, fmt.Errorf("open read snapshot spill: %w", err)
		}
		iterator.spillFile = file
	}
	return iterator, nil
}

func (s *duckDBReadSnapshot) Close(ctx context.Context) (resultErr error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.readers.Wait()
	if s.spillPath == "" {
		return nil
	}
	started := observability.LogSideEffectStart(ctx, "duckdb", "remove_read_snapshot_spill",
		"model_version", s.modelVersion, "row_count", s.metadata.RowCount,
		"encoded_bytes", s.metadata.EstimatedBytes, "retained_bytes", s.metadata.RetainedBytes)
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "remove_read_snapshot_spill", started, resultErr,
			"model_version", s.modelVersion, "row_count", s.metadata.RowCount,
			"encoded_bytes", s.metadata.EstimatedBytes, "retained_bytes", s.metadata.RetainedBytes)
	}()
	if err := os.Remove(s.spillPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove read snapshot spill: %w", err)
	}
	return nil
}

type duckDBSnapshotIterator struct {
	ctx       context.Context
	snapshot  *duckDBReadSnapshot
	spillFile *os.File
	next      int64
	end       int64
	maxRows   int64
	maxBytes  int
	once      sync.Once
}

var _ readports.BatchIterator = (*duckDBSnapshotIterator)(nil)

func (i *duckDBSnapshotIterator) Next(ctx context.Context) (readdomain.EncodedBatch, error) {
	if i.next >= i.end {
		return readdomain.EncodedBatch{}, io.EOF
	}
	select {
	case <-ctx.Done():
		return readdomain.EncodedBatch{}, ctx.Err()
	default:
	}
	batchEnd := i.end
	if remaining := i.end - i.next; i.maxRows < remaining {
		batchEnd = i.next + i.maxRows
	}
	rows := make([][]snapshotValue, 0, batchEnd-i.next)
	for ordinal := i.next; ordinal < batchEnd; ordinal++ {
		payload, err := i.readStagedRow(ctx, ordinal)
		if err != nil {
			return readdomain.EncodedBatch{}, err
		}
		row, err := decodeSnapshotRow(payload)
		if err != nil {
			return readdomain.EncodedBatch{}, fmt.Errorf("decode snapshot row %d: %w", ordinal, err)
		}
		rows = append(rows, row)
	}
	var serialized []byte
	for {
		started := observability.LogSideEffectStart(ctx, "duckdb", "encode_read_snapshot_batch",
			"model_version", i.snapshot.modelVersion, "format", i.snapshot.format.String(),
			"offset", i.next, "row_count", len(rows), "max_payload_bytes", i.maxBytes)
		var err error
		serialized, err = encodeReadBatch(i.snapshot.format, i.snapshot.arrowSchema, i.snapshot.fields, rows)
		observability.LogSideEffectEnd(ctx, "duckdb", "encode_read_snapshot_batch", started, err,
			"model_version", i.snapshot.modelVersion, "format", i.snapshot.format.String(),
			"offset", i.next, "row_count", len(rows), "max_payload_bytes", i.maxBytes,
			"payload_bytes", len(serialized), "payload_digest", observability.Digest(serialized))
		if err != nil {
			return readdomain.EncodedBatch{}, err
		}
		if len(serialized) <= i.maxBytes {
			break
		}
		if len(rows) == 1 {
			return readdomain.EncodedBatch{}, fmt.Errorf("encoded read row at offset %d is %d bytes, exceeds configured response payload limit %d", i.next, len(serialized), i.maxBytes)
		}
		rows = rows[:max(1, len(rows)/2)]
	}
	batchEnd = i.next + int64(len(rows))
	result := readdomain.EncodedBatch{Offset: i.next, RowCount: batchEnd - i.next, SerializedRows: serialized}
	i.next = batchEnd
	return result, nil
}

func (i *duckDBSnapshotIterator) readStagedRow(ctx context.Context, ordinal int64) (_ []byte, resultErr error) {
	if i.spillFile == nil {
		return slices.Clone(i.snapshot.memoryRows[ordinal]), nil
	}
	location := i.snapshot.spillRows[ordinal]
	started := observability.LogSideEffectStart(ctx, "duckdb", "read_snapshot_spill_row",
		"model_version", i.snapshot.modelVersion, "row_ordinal", ordinal, "row_bytes", location.length)
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "read_snapshot_spill_row", started, resultErr,
			"model_version", i.snapshot.modelVersion, "row_ordinal", ordinal, "row_bytes", location.length)
	}()
	if location.length > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("spilled row %d is too large for this platform", ordinal)
	}
	payload := make([]byte, int(location.length))
	if _, err := i.spillFile.ReadAt(payload, location.offset); err != nil {
		return nil, fmt.Errorf("read spilled snapshot row %d: %w", ordinal, err)
	}
	return payload, nil
}

func (i *duckDBSnapshotIterator) Close() (resultErr error) {
	i.once.Do(func() {
		if i.spillFile != nil {
			started := observability.LogSideEffectStart(i.ctx, "duckdb", "close_read_snapshot_spill_reader",
				"model_version", i.snapshot.modelVersion)
			resultErr = i.spillFile.Close()
			observability.LogSideEffectEnd(i.ctx, "duckdb", "close_read_snapshot_spill_reader", started, resultErr,
				"model_version", i.snapshot.modelVersion)
		}
		i.snapshot.readers.Done()
	})
	return resultErr
}

func referenceReadSchema(format readdomain.Format, fields []catalogdomain.Field) (*arrow.Schema, readdomain.ReferenceSchema, error) {
	switch format {
	case readdomain.FormatArrow:
		schema, serialized, err := buildArrowReferenceSchema(fields)
		return schema, readdomain.ReferenceSchema{Format: format, Serialized: serialized}, err
	case readdomain.FormatAvro:
		serialized, err := buildAvroReferenceSchema(fields)
		return nil, readdomain.ReferenceSchema{Format: format, Serialized: serialized}, err
	default:
		return nil, readdomain.ReferenceSchema{}, fmt.Errorf("unsupported Storage Read format %s", format)
	}
}

func encodeReadBatch(format readdomain.Format, arrowSchema *arrow.Schema, fields []catalogdomain.Field, rows [][]snapshotValue) ([]byte, error) {
	switch format {
	case readdomain.FormatArrow:
		return encodeArrowRecordBatch(arrowSchema, fields, rows)
	case readdomain.FormatAvro:
		return encodeAvroRows(fields, rows)
	default:
		return nil, fmt.Errorf("unsupported Storage Read format %s", format)
	}
}
