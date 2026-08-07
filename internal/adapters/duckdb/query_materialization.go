package duckdb

// Query destination transaction semantics:
//   - BigQuery JobConfigurationQuery destination and dispositions:
//     https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
//   - BigQuery query-result destinations:
//     https://cloud.google.com/bigquery/docs/writing-results
//   - Spark connector 0.44.2 copyData/materialization call sites:
//     https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java
//   - DuckDB transactions:
//     https://duckdb.org/docs/stable/sql/statements/transactions
//
// The query is evaluated exactly once into a transaction-local staging table.
// The selected write disposition then mutates the destination in that same
// transaction. This prevents a failed cast or insert from exposing a truncate.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

var queryMaterializationSequence atomic.Uint64

var _ ports.QueryMaterializer = (*Warehouse)(nil)

func (w *Warehouse) MaterializeQuery(ctx context.Context, request ports.QueryMaterializationRequest) (result ports.QueryMaterializationResult, err error) {
	statement, adapterModel, err := translateSQLWithModel(request.Query)
	if err != nil {
		return result, err
	}
	if !returnsRows(statement) {
		return result, fmt.Errorf("%w: destinationTable requires a row-producing query", domain.ErrInvalid)
	}
	if !request.DestinationExists && request.CreateDisposition == domain.CreateNever {
		return result, fmt.Errorf("%w: destination table does not exist and createDisposition is CREATE_NEVER", domain.ErrNotFound)
	}

	destination := request.Destination
	queryBytes := []byte(request.Query.SQL)
	started := observability.LogSideEffectStart(ctx, "duckdb", "materialize_query_destination",
		"project_id", destination.ProjectID, "dataset_id", destination.DatasetID, "table_id", destination.TableID,
		"write_disposition", request.WriteDisposition, "create_disposition", request.CreateDisposition,
		"destination_exists", request.DestinationExists, "query_bytes", len(queryBytes),
		"query_digest", observability.Digest(queryBytes), "statement_type", queryStatementType(request.Query.SQL),
		"destination_schema_fingerprint", queryDestinationSchemaDigest(request.DestinationSchema),
		"model_version", adapterModel, "transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "materialize_query_destination", started, err,
			"project_id", destination.ProjectID, "dataset_id", destination.DatasetID, "table_id", destination.TableID,
			"write_disposition", request.WriteDisposition, "destination_created", result.DestinationCreated,
			"row_count", len(result.QueryResult.Rows), "schema_fingerprint", queryMaterializationSchemaDigest(result.QueryResult.Columns),
			"model_version", adapterModel, "transaction_mode", "explicit")
	}()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin query materialization transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	staging := fmt.Sprintf("__bqemu_query_result_%d", queryMaterializationSequence.Add(1))
	if _, err := tx.ExecContext(ctx, "CREATE TEMP TABLE "+quoteIdentifier(staging)+" AS "+statement); err != nil {
		return result, fmt.Errorf("evaluate query destination source: %w", err)
	}
	queryResult, err := readMaterializedQueryResult(ctx, tx, staging)
	if err != nil {
		return result, err
	}
	result.QueryResult = queryResult
	if request.DestinationExists {
		if err := validateExistingQueryDestinationShape(queryResult.Columns, request.DestinationSchema, request.WriteDisposition); err != nil {
			return result, err
		}
	}

	destinationName := quoteIdentifier(physicalSchema(destination.ProjectID, destination.DatasetID)) + "." + quoteIdentifier(destination.TableID)
	if request.DestinationExists {
		if err := applyExistingQueryDestination(ctx, tx, destinationName, staging, request.WriteDisposition); err != nil {
			return result, err
		}
	} else {
		statement := "CREATE TABLE " + destinationName + " AS SELECT * FROM " + quoteIdentifier(staging)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return result, fmt.Errorf("create query destination: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE "+quoteIdentifier(staging)); err != nil {
		return result, fmt.Errorf("drop query result staging table: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit query destination: %w", err)
	}
	committed = true
	result.DestinationCreated = !request.DestinationExists
	return result, nil
}

func applyExistingQueryDestination(ctx context.Context, tx *sql.Tx, destination, staging string, disposition domain.WriteDisposition) error {
	switch disposition {
	case domain.WriteEmpty:
		var containsRows bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+destination+" LIMIT 1)").Scan(&containsRows); err != nil {
			return fmt.Errorf("inspect WRITE_EMPTY query destination: %w", err)
		}
		if containsRows {
			return fmt.Errorf("%w: WRITE_EMPTY destination contains rows", domain.ErrConflict)
		}
	case domain.WriteTruncate:
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+destination); err != nil {
			return fmt.Errorf("truncate query destination: %w", err)
		}
	case domain.WriteAppend:
	default:
		return fmt.Errorf("%w: unknown query writeDisposition %q", domain.ErrInvalid, disposition)
	}
	// BY NAME rejects missing/extra columns and lets DuckDB validate every cast.
	// With WRITE_TRUNCATE the preceding DELETE remains invisible if this insert
	// fails because both statements share the transaction.
	statement := "INSERT INTO " + destination + " BY NAME SELECT * FROM " + quoteIdentifier(staging)
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("apply %s query destination: %w", disposition, err)
	}
	return nil
}

// validateExistingQueryDestinationShape pins the currently verified Spark
// connector path: SELECT * from its temporary table into an existing table with
// the same ordered scalar schema. BigQuery WRITE_TRUNCATE can replace the schema,
// but that broader behavior is intentionally rejected until physical and catalog
// schema replacement can be committed as one recoverable operation.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
func validateExistingQueryDestinationShape(columns []domain.Column, fields []domain.Field, disposition domain.WriteDisposition) error {
	if len(columns) != len(fields) {
		return fmt.Errorf("%w: query output field count differs from destination; capability=%s writeDisposition=%s", domain.ErrPrecondition, domain.CapabilityQueryDestinationExactSchemaV1, disposition)
	}
	for index := range fields {
		field := fields[index]
		if len(field.Fields) != 0 || strings.EqualFold(field.Mode, "REPEATED") {
			return fmt.Errorf("%w: nested or repeated existing query destinations are not implemented; capability=%s field_index=%d", domain.ErrPrecondition, domain.CapabilityQueryDestinationExactSchemaV1, index)
		}
		if !strings.EqualFold(columns[index].Name, field.Name) || canonicalQueryDestinationType(columns[index].Type) != canonicalQueryDestinationType(field.Type) {
			return fmt.Errorf("%w: query output and destination schemas differ; capability=%s field_index=%d writeDisposition=%s fix_hint=use the Spark 0.44.2 SELECT-star copyData shape or pre-create an exact schema", domain.ErrPrecondition, domain.CapabilityQueryDestinationExactSchemaV1, index, disposition)
		}
	}
	return nil
}

func canonicalQueryDestinationType(value string) string {
	switch strings.ToUpper(value) {
	case "BOOL", "BOOLEAN":
		return "BOOL"
	case "INT64", "INTEGER":
		return "INT64"
	case "FLOAT64", "FLOAT":
		return "FLOAT64"
	case "STRING":
		return "STRING"
	case "BYTES":
		return "BYTES"
	case "NUMERIC":
		return "NUMERIC"
	case "DATE":
		return "DATE"
	case "DATETIME":
		return "DATETIME"
	case "TIME":
		return "TIME"
	case "TIMESTAMP":
		return "TIMESTAMP"
	case "JSON":
		return "JSON"
	default:
		return "UNSUPPORTED:" + strings.ToUpper(value)
	}
}

func readMaterializedQueryResult(ctx context.Context, tx *sql.Tx, staging string) (domain.QueryResult, error) {
	rows, err := tx.QueryContext(ctx, "SELECT * FROM "+quoteIdentifier(staging))
	if err != nil {
		return domain.QueryResult{}, fmt.Errorf("read materialized query result: %w", err)
	}
	defer rows.Close()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return domain.QueryResult{}, fmt.Errorf("read materialized result schema: %w", err)
	}
	result := domain.QueryResult{Columns: make([]domain.Column, len(columnTypes))}
	for index, columnType := range columnTypes {
		result.Columns[index] = domain.Column{Name: columnType.Name(), Type: bigQueryType(columnType.DatabaseTypeName())}
	}
	for rows.Next() {
		values := make([]any, len(columnTypes))
		destinations := make([]any, len(columnTypes))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return domain.QueryResult{}, fmt.Errorf("scan materialized result row: %w", err)
		}
		result.Rows = append(result.Rows, values)
	}
	if err := rows.Err(); err != nil {
		return domain.QueryResult{}, fmt.Errorf("read materialized result rows: %w", err)
	}
	return result, nil
}

func (w *Warehouse) DropMaterializedDestination(ctx context.Context, destination domain.TableReference) error {
	return w.DropTable(ctx, destination.ProjectID, destination.DatasetID, destination.TableID)
}

func queryMaterializationSchemaDigest(columns []domain.Column) string {
	parts := make([]string, len(columns))
	for index, column := range columns {
		parts[index] = column.Name + ":" + strings.ToUpper(column.Type)
	}
	return observability.Digest([]byte(strings.Join(parts, "\x00")))
}

func queryDestinationSchemaDigest(fields []domain.Field) string {
	parts := make([]string, len(fields))
	for index, field := range fields {
		parts[index] = field.Name + ":" + strings.ToUpper(field.Type) + ":" + strings.ToUpper(field.Mode)
	}
	return observability.Digest([]byte(strings.Join(parts, "\x00")))
}
