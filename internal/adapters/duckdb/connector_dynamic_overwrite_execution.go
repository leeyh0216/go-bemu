package duckdb

// Atomic execution for the parsed Spark dynamic time-partition overwrite.
// The connector script computes distinct non-NULL source partitions, deletes
// only matching target partitions, and inserts every source row. These effects
// execute in one DuckDB transaction; the raw script is never executed.
//
// Sources:
//   - connector 0.44.2 ARRAY_AGG(DISTINCT ... IGNORE NULLS) and MERGE template:
//     https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870
//   - BigQuery time-partition field and granularity rules:
//     https://cloud.google.com/bigquery/docs/creating-partitioned-tables
//   - DATE_TRUNC and TIMESTAMP_TRUNC semantics:
//     https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_trunc
//     https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_trunc
//   - DuckDB transaction semantics:
//     https://duckdb.org/docs/stable/sql/statements/transactions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

var _ ports.QueryOperationEngine = (*Warehouse)(nil)

func (w *Warehouse) ExecuteQueryOperation(
	ctx context.Context,
	request ports.QueryRequest,
	operation ports.QueryOperation,
	canonicalDestination domain.Table,
	canonicalSource domain.Table,
) (result domain.QueryResult, err error) {
	parsed, matched, parseErr := parseSparkDynamicTimeOverwrite(request)
	if parseErr != nil {
		return result, parseErr
	}
	if !matched || !sameQueryOperation(parsed, operation) {
		err := fmt.Errorf("%w: semantic operation does not match the source-pinned request; model_version=%s",
			domain.ErrPrecondition, sparkDynamicTimeOverwriteModel)
		logDynamicOverwriteRejection(ctx, operation, canonicalDestination, canonicalSource, "semantic-operation-match", err)
		return result, err
	}
	partitionFieldType, err := validateDynamicTimeOverwriteTables(operation, canonicalDestination, canonicalSource)
	if err != nil {
		logDynamicOverwriteRejection(ctx, operation, canonicalDestination, canonicalSource, "canonical-table-contract", err)
		return result, err
	}

	queryBytes := []byte(request.SQL)
	operationAttrs := dynamicOverwriteSafeAttrs(operation, canonicalDestination, canonicalSource)
	operationAttrs = append(operationAttrs,
		"partition_type", operation.Granularity, "partition_field_type", partitionFieldType,
		"insert_field_count", len(operation.InsertFields), "query_bytes", len(queryBytes),
		"query_digest", observability.Digest(queryBytes), "transaction_mode", "explicit")
	transactionStarted := time.Now()
	transactionState := "not_started"
	logDynamicOverwritePhase(ctx, "dynamic_time_partition_overwrite", "pre", nil,
		append(operationAttrs, "tx_state", transactionState)...)
	defer func() {
		logDynamicOverwritePhase(ctx, "dynamic_time_partition_overwrite", "post", err,
			append(operationAttrs, "affected_rows", result.AffectedRows, "tx_state", transactionState,
				"duration_ms", time.Since(transactionStarted).Milliseconds())...)
	}()

	logDynamicOverwritePhase(ctx, "begin_dynamic_overwrite_transaction", "pre", nil,
		append(operationAttrs, "tx_state", transactionState)...)
	tx, beginErr := w.db.BeginTx(ctx, nil)
	if beginErr != nil {
		transactionState = "begin_failed"
		err = classifyDynamicOverwriteBackendError("begin transaction", beginErr)
		logDynamicOverwritePhase(ctx, "begin_dynamic_overwrite_transaction", "post", err,
			append(operationAttrs, "tx_state", transactionState)...)
		return result, err
	}
	transactionState = "active"
	logDynamicOverwritePhase(ctx, "begin_dynamic_overwrite_transaction", "post", nil,
		append(operationAttrs, "tx_state", transactionState)...)
	committed := false
	defer func() {
		if committed {
			return
		}
		logDynamicOverwritePhase(ctx, "rollback_dynamic_overwrite_transaction", "pre", nil,
			append(operationAttrs, "tx_state", transactionState)...)
		rollbackErr := tx.Rollback()
		if rollbackErr == nil {
			transactionState = "rolled_back"
			logDynamicOverwritePhase(ctx, "rollback_dynamic_overwrite_transaction", "post", nil,
				append(operationAttrs, "tx_state", transactionState)...)
			return
		}
		if errors.Is(rollbackErr, sql.ErrTxDone) {
			logDynamicOverwritePhase(ctx, "rollback_dynamic_overwrite_transaction", "post", rollbackErr,
				append(operationAttrs, "tx_state", transactionState, "rollback_outcome", "transaction_already_finalized")...)
			return
		}
		transactionState = "rollback_failed"
		classified := classifyDynamicOverwriteBackendError("rollback transaction", rollbackErr)
		err = errors.Join(err, classified)
		logDynamicOverwritePhase(ctx, "rollback_dynamic_overwrite_transaction", "post", classified,
			append(operationAttrs, "tx_state", transactionState)...)
	}()

	destination := dynamicOverwriteRelation(operation.Destination)
	source := dynamicOverwriteRelation(operation.Source)
	targetAlias := quoteIdentifier("__bqemu_dynamic_target")
	sourceAlias := quoteIdentifier("__bqemu_dynamic_source")
	targetPartition := dynamicOverwritePartitionExpression(partitionFieldType, operation.Granularity, operation.PartitionField, targetAlias)
	sourcePartition := dynamicOverwritePartitionExpression(partitionFieldType, operation.Granularity, operation.PartitionField, sourceAlias)
	partitionColumn := sourceAlias + "." + quoteIdentifier(operation.PartitionField)
	deleteStatement := "DELETE FROM " + destination + " AS " + targetAlias +
		" WHERE " + targetPartition + " IN (SELECT DISTINCT " + sourcePartition +
		" FROM " + source + " AS " + sourceAlias + " WHERE " + partitionColumn + " IS NOT NULL)"

	logDynamicOverwritePhase(ctx, "delete_dynamic_partitions", "pre", nil,
		append(operationAttrs, "tx_state", transactionState)...)
	deleteResult, deleteErr := tx.ExecContext(ctx, deleteStatement)
	if deleteErr != nil {
		transactionState = "rollback_pending"
		err = classifyDynamicOverwriteQueryError("delete touched destination partitions", deleteErr)
		logDynamicOverwritePhase(ctx, "delete_dynamic_partitions", "post", err,
			append(operationAttrs, "affected_rows", int64(0), "tx_state", transactionState)...)
		return result, err
	}
	deletedRows, rowsErr := deleteResult.RowsAffected()
	if rowsErr != nil {
		transactionState = "rollback_pending"
		err = classifyDynamicOverwriteBackendError("read deleted row count", rowsErr)
		logDynamicOverwritePhase(ctx, "delete_dynamic_partitions", "post", err,
			append(operationAttrs, "affected_rows", int64(0), "tx_state", transactionState)...)
		return result, err
	}
	logDynamicOverwritePhase(ctx, "delete_dynamic_partitions", "post", nil,
		append(operationAttrs, "affected_rows", deletedRows, "tx_state", "pending_commit")...)

	destinationFields := make([]string, len(operation.InsertFields))
	sourceFields := make([]string, len(operation.InsertFields))
	for index, field := range operation.InsertFields {
		destinationFields[index] = quoteIdentifier(field)
		sourceFields[index] = sourceAlias + "." + quoteIdentifier(field)
	}
	insertStatement := "INSERT INTO " + destination + " (" + strings.Join(destinationFields, ",") + ") SELECT " +
		strings.Join(sourceFields, ",") + " FROM " + source + " AS " + sourceAlias
	logDynamicOverwritePhase(ctx, "insert_dynamic_partition_source", "pre", nil,
		append(operationAttrs, "field_count", len(operation.InsertFields), "tx_state", transactionState)...)
	insertResult, insertErr := tx.ExecContext(ctx, insertStatement)
	if insertErr != nil {
		transactionState = "rollback_pending"
		err = classifyDynamicOverwriteQueryError("insert dynamic partition source", insertErr)
		logDynamicOverwritePhase(ctx, "insert_dynamic_partition_source", "post", err,
			append(operationAttrs, "affected_rows", int64(0), "tx_state", transactionState)...)
		return result, err
	}
	insertedRows, rowsErr := insertResult.RowsAffected()
	if rowsErr != nil {
		transactionState = "rollback_pending"
		err = classifyDynamicOverwriteBackendError("read inserted row count", rowsErr)
		logDynamicOverwritePhase(ctx, "insert_dynamic_partition_source", "post", err,
			append(operationAttrs, "affected_rows", int64(0), "tx_state", transactionState)...)
		return result, err
	}
	logDynamicOverwritePhase(ctx, "insert_dynamic_partition_source", "post", nil,
		append(operationAttrs, "affected_rows", insertedRows, "tx_state", "pending_commit")...)

	transactionState = "commit_pending"
	logDynamicOverwritePhase(ctx, "commit_dynamic_overwrite_transaction", "pre", nil,
		append(operationAttrs, "tx_state", transactionState)...)
	if commitErr := tx.Commit(); commitErr != nil {
		transactionState = "commit_failed"
		err = classifyDynamicOverwriteBackendError("commit transaction", commitErr)
		logDynamicOverwritePhase(ctx, "commit_dynamic_overwrite_transaction", "post", err,
			append(operationAttrs, "tx_state", transactionState)...)
		return result, err
	}
	committed = true
	transactionState = "committed"
	logDynamicOverwritePhase(ctx, "commit_dynamic_overwrite_transaction", "post", nil,
		append(operationAttrs, "tx_state", transactionState)...)
	result.AffectedRows = deletedRows + insertedRows
	return result, nil
}

func validateDynamicTimeOverwriteDestination(operation ports.QueryOperation, table domain.Table) (string, error) {
	if operation.Kind != ports.QueryOperationSparkDynamicTimePartitionOverwrite || operation.ModelVersion != sparkDynamicTimeOverwriteModel {
		return "", fmt.Errorf("%w: unknown semantic query operation model", domain.ErrPrecondition)
	}
	if err := table.Validate(); err != nil {
		return "", fmt.Errorf("%w: canonical destination metadata is invalid: %v", domain.ErrPrecondition, err)
	}
	if table.ProjectID != operation.Destination.ProjectID || table.DatasetID != operation.Destination.DatasetID || table.ID != operation.Destination.TableID {
		return "", fmt.Errorf("%w: canonical destination does not match parsed MERGE target", domain.ErrPrecondition)
	}
	if table.RangePartitioning != nil {
		return "", fmt.Errorf("%w: range-partition overwrite remains an explicit gap; capability=%s",
			domain.ErrUnsupported, domain.GapSparkDynamicRangePartitionOverwriteV1)
	}
	if table.TimePartitioning == nil || table.TimePartitioning.Field == "" {
		return "", fmt.Errorf("%w: dynamic time-partition overwrite requires canonical field partition metadata; capability=%s",
			domain.ErrInvalidQuery, domain.CapabilitySparkDynamicTimePartitionOverwriteV1)
	}
	partitioning := table.TimePartitioning
	partitionType := strings.ToUpper(partitioning.Type)
	if partitioning.Field != operation.PartitionField || partitionType != operation.Granularity || !validTimePartitionGranularity(partitionType) {
		return "", fmt.Errorf("%w: parsed partition expression differs from canonical partition metadata; capability=%s",
			domain.ErrInvalidQuery, domain.CapabilitySparkDynamicTimePartitionOverwriteV1)
	}

	var partitionField *domain.Field
	for index := range table.Schema {
		if table.Schema[index].Name == partitioning.Field {
			partitionField = &table.Schema[index]
			break
		}
	}
	if partitionField == nil || len(partitionField.Fields) != 0 || strings.EqualFold(partitionField.Mode, "REPEATED") {
		return "", fmt.Errorf("%w: canonical partition field must be a top-level scalar field", domain.ErrInvalidQuery)
	}
	fieldType := strings.ToUpper(partitionField.Type)
	expectedFunction := "TIMESTAMP_TRUNC"
	switch fieldType {
	case "DATE":
		expectedFunction = "DATE_TRUNC"
		if partitionType == "HOUR" {
			return "", fmt.Errorf("%w: DATE partition fields do not support HOUR granularity", domain.ErrInvalidQuery)
		}
	case "TIMESTAMP", "DATETIME":
	default:
		return "", fmt.Errorf("%w: canonical time partition field type must be DATE, TIMESTAMP, or DATETIME", domain.ErrInvalidQuery)
	}
	if operation.PartitionFunction != expectedFunction {
		return "", fmt.Errorf("%w: connector partition function differs from canonical field type; capability=%s",
			domain.ErrInvalidQuery, domain.CapabilitySparkDynamicTimePartitionOverwriteV1)
	}
	if len(operation.InsertFields) != len(table.Schema) {
		return "", fmt.Errorf("%w: connector INSERT field count differs from canonical destination schema", domain.ErrInvalidQuery)
	}
	for index, field := range table.Schema {
		if operation.InsertFields[index] != field.Name {
			return "", fmt.Errorf("%w: connector INSERT field order differs from canonical destination schema; field_index=%d",
				domain.ErrInvalidQuery, index)
		}
	}
	return fieldType, nil
}

// validateDynamicTimeOverwriteTables rejects every schema conversion before
// DuckDB can apply its wider implicit-cast rules. BigQuery DML validates source
// expressions against destination column types; the pinned connector emits a
// one-to-one field list, so the emulator requires the same canonical type,
// mode, nested name, and nested order for every selected source field.
//
// BigQuery DML type compatibility:
// https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
func validateDynamicTimeOverwriteTables(operation ports.QueryOperation, destination, source domain.Table) (string, error) {
	partitionFieldType, err := validateDynamicTimeOverwriteDestination(operation, destination)
	if err != nil {
		return "", err
	}
	if err := source.Validate(); err != nil {
		return "", fmt.Errorf("%w: canonical source metadata is invalid: %v", domain.ErrPrecondition, err)
	}
	if source.ProjectID != operation.Source.ProjectID || source.DatasetID != operation.Source.DatasetID || source.ID != operation.Source.TableID {
		return "", fmt.Errorf("%w: canonical source does not match parsed MERGE source", domain.ErrPrecondition)
	}

	sourceFields := make(map[string]domain.Field, len(source.Schema))
	for _, field := range source.Schema {
		sourceFields[strings.ToLower(field.Name)] = field
	}
	for index, destinationField := range destination.Schema {
		sourceField, exists := sourceFields[strings.ToLower(destinationField.Name)]
		if !exists {
			return "", fmt.Errorf("%w: source schema lacks a selected destination field; field_index=%d capability=%s",
				domain.ErrInvalidQuery, index, domain.CapabilitySparkDynamicTimePartitionOverwriteV1)
		}
		if !sameDynamicOverwriteFieldShape(destinationField, sourceField) {
			return "", fmt.Errorf("%w: source and destination field shapes differ; field_index=%d capability=%s fix_hint=align canonical source and destination schemas before overwrite",
				domain.ErrInvalidQuery, index, domain.CapabilitySparkDynamicTimePartitionOverwriteV1)
		}
	}
	return partitionFieldType, nil
}

func sameDynamicOverwriteFieldShape(destination, source domain.Field) bool {
	if !strings.EqualFold(destination.Name, source.Name) ||
		canonicalDynamicOverwriteType(destination.Type) != canonicalDynamicOverwriteType(source.Type) ||
		canonicalDynamicOverwriteMode(destination.Mode) != canonicalDynamicOverwriteMode(source.Mode) ||
		len(destination.Fields) != len(source.Fields) {
		return false
	}
	for index := range destination.Fields {
		if !sameDynamicOverwriteFieldShape(destination.Fields[index], source.Fields[index]) {
			return false
		}
	}
	return true
}

func canonicalDynamicOverwriteType(fieldType string) string {
	switch strings.ToUpper(fieldType) {
	case "BOOL":
		return "BOOLEAN"
	case "INTEGER":
		return "INT64"
	case "FLOAT":
		return "FLOAT64"
	case "STRUCT":
		return "RECORD"
	default:
		return strings.ToUpper(fieldType)
	}
}

func canonicalDynamicOverwriteMode(mode string) string {
	if mode == "" {
		return "NULLABLE"
	}
	return strings.ToUpper(mode)
}

func sameQueryOperation(left, right ports.QueryOperation) bool {
	if left.Kind != right.Kind || left.ModelVersion != right.ModelVersion || left.Destination != right.Destination ||
		left.Source != right.Source || left.PartitionFunction != right.PartitionFunction ||
		left.PartitionField != right.PartitionField || left.Granularity != right.Granularity ||
		len(left.InsertFields) != len(right.InsertFields) {
		return false
	}
	for index := range left.InsertFields {
		if left.InsertFields[index] != right.InsertFields[index] {
			return false
		}
	}
	return true
}

func dynamicOverwriteRelation(reference domain.TableReference) string {
	return quoteIdentifier(physicalSchema(reference.ProjectID, reference.DatasetID)) + "." + quoteIdentifier(reference.TableID)
}

func dynamicOverwritePartitionExpression(fieldType, granularity, field, alias string) string {
	column := alias + "." + quoteIdentifier(field)
	if fieldType == "TIMESTAMP" {
		// BigQuery TIMESTAMP_TRUNC defaults to UTC. DuckDB TIMESTAMPTZ truncation
		// otherwise depends on the connection timezone.
		column = "timezone('UTC', " + column + ")"
	}
	return "date_trunc('" + strings.ToLower(granularity) + "', " + column + ")"
}

func classifyDynamicOverwriteQueryError(stage string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", domain.ErrInvalidQuery, stage, err)
}

func classifyDynamicOverwriteBackendError(stage string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", domain.ErrBackend, stage, err)
}

func dynamicOverwriteSafeAttrs(operation ports.QueryOperation, destination, source domain.Table) []any {
	destinationReference := []byte(operation.Destination.ProjectID + "\x00" + operation.Destination.DatasetID + "\x00" + operation.Destination.TableID)
	sourceReference := []byte(operation.Source.ProjectID + "\x00" + operation.Source.DatasetID + "\x00" + operation.Source.TableID)
	partitionField := []byte(operation.PartitionField)
	return []any{
		"model_version", operation.ModelVersion,
		"capability", domain.CapabilitySparkDynamicTimePartitionOverwriteV1,
		"destination_reference_bytes", len(destinationReference),
		"destination_reference_fingerprint", observability.Digest(destinationReference),
		"source_reference_bytes", len(sourceReference),
		"source_reference_fingerprint", observability.Digest(sourceReference),
		"partition_field_bytes", len(partitionField),
		"partition_field_fingerprint", observability.Digest(partitionField),
		"destination_schema_fingerprint", queryDestinationSchemaDigest(destination.Schema),
		"source_schema_fingerprint", queryDestinationSchemaDigest(source.Schema),
	}
}

func logDynamicOverwritePhase(ctx context.Context, operation, stage string, err error, attrs ...any) {
	base := append(observability.ContextAttrs(ctx),
		"event", "side_effect."+stage, "component", "duckdb", "operation", operation,
	)
	if stage == "post" {
		base = append(base, "success", err == nil)
	}
	base = append(base, attrs...)
	if err != nil {
		base = append(base, observability.ErrorAttrs(err)...)
	}
	slog.InfoContext(ctx, "dynamic overwrite transaction phase", base...)
}

func logDynamicOverwriteRejection(
	ctx context.Context,
	operation ports.QueryOperation,
	destination domain.Table,
	source domain.Table,
	shape string,
	err error,
) {
	attrs := append(observability.ContextAttrs(ctx),
		"event", "boundary.reject", "boundary", "duckdb.dynamic_time_partition_overwrite",
		"model_version", sparkDynamicTimeOverwriteModel, "shape", shape,
		"capability", domain.CapabilitySparkDynamicTimePartitionOverwriteV1,
		"partition_type", operation.Granularity,
		"fix_hint", "compare canonical timePartitioning and schema with the parsed connector operation",
	)
	attrs = append(attrs, dynamicOverwriteSafeAttrs(operation, destination, source)...)
	attrs = append(attrs, observability.ErrorAttrs(err)...)
	slog.WarnContext(ctx, "dynamic overwrite rejected before transaction", attrs...)
}
