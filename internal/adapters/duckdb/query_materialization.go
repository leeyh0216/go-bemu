package duckdb

// Query destination helpers implement the physical side of analyzed statement
// materialization.
//   - BigQuery JobConfigurationQuery destination and dispositions:
//     https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
//   - BigQuery query-result destinations:
//     https://cloud.google.com/bigquery/docs/writing-results
//   - DuckDB transactions:
//     https://duckdb.org/docs/stable/sql/statements/transactions

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

var queryMaterializationSequence atomic.Uint64

func portableQueryFieldName(name string) bool {
	if name == "" || len(name) > 1024 || !(name[0] == '_' || name[0] >= 'A' && name[0] <= 'Z' || name[0] >= 'a' && name[0] <= 'z') {
		return false
	}
	for index := 1; index < len(name); index++ {
		value := name[index]
		if value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9' {
			continue
		}
		return false
	}
	return true
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

// validateExistingQueryDestinationShape requires the analyzed query output to
// match the existing table's ordered schema. BigQuery WRITE_TRUNCATE can replace the schema,
// but that broader behavior is intentionally rejected until physical and catalog
// schema replacement can be committed as one recoverable operation.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
func validateExistingQueryDestinationShape(columns []domain.Column, fields []domain.Field, disposition domain.WriteDisposition) error {
	if len(columns) != len(fields) {
		return fmt.Errorf("%w: query output field count differs from destination; capability=%s writeDisposition=%s", domain.ErrPrecondition, domain.CapabilityQueryDestinationExactSchemaV1, disposition)
	}
	for index := range fields {
		field := fields[index]
		if !strings.EqualFold(columns[index].Name, field.Name) {
			return fmt.Errorf("%w: query output and destination schemas differ; capability=%s field_index=%d writeDisposition=%s fix_hint=select an exact destination schema or pre-create a matching table", domain.ErrPrecondition, domain.CapabilityQueryDestinationExactSchemaV1, index, disposition)
		}
		compatible, requiresRounding := queryDestinationFieldsCompatible(columns[index], field)
		if requiresRounding {
			return fmt.Errorf("%w: capability=%s field_index=%d writeDisposition=%s decimal narrowing or rounding is not implemented", domain.ErrUnsupported, domain.CapabilityQueryDecimalRoundingV1, index, disposition)
		}
		if !compatible {
			return fmt.Errorf("%w: query output and destination schemas differ; capability=%s field_index=%d writeDisposition=%s fix_hint=select an exact destination schema or pre-create a matching table", domain.ErrPrecondition, domain.CapabilityQueryDestinationExactSchemaV1, index, disposition)
		}
	}
	return nil
}

func queryDestinationFieldsCompatible(output, destination domain.Field) (compatible, requiresRounding bool) {
	outputType := canonicalQueryDestinationType(output.Type)
	destinationType := canonicalQueryDestinationType(destination.Type)
	if (outputType == "NUMERIC" || outputType == "BIGNUMERIC") && (destinationType == "NUMERIC" || destinationType == "BIGNUMERIC") {
		outputParameters, outputErr := output.EffectiveDecimalParameters()
		destinationParameters, destinationErr := destination.EffectiveDecimalParameters()
		if outputErr != nil || destinationErr != nil || strings.EqualFold(output.Mode, "REPEATED") != strings.EqualFold(destination.Mode, "REPEATED") {
			return false, false
		}
		if outputParameters != destinationParameters {
			narrowing := outputParameters.Scale > destinationParameters.Scale ||
				outputParameters.Precision-outputParameters.Scale > destinationParameters.Precision-destinationParameters.Scale
			return false, narrowing
		}
		return true, false
	}
	if outputType != destinationType || strings.EqualFold(output.Mode, "REPEATED") != strings.EqualFold(destination.Mode, "REPEATED") || len(output.Fields) != len(destination.Fields) {
		return false, false
	}
	for index := range output.Fields {
		if !strings.EqualFold(output.Fields[index].Name, destination.Fields[index].Name) {
			return false, false
		}
		nestedCompatible, nestedRounding := queryDestinationFieldsCompatible(output.Fields[index], destination.Fields[index])
		if nestedRounding {
			return false, true
		}
		if !nestedCompatible {
			return false, false
		}
	}
	return true, false
}

func queryResultSchemaFromDestination(fields []domain.Field) []domain.Field {
	result := domain.CloneFields(fields)
	var normalize func([]domain.Field)
	normalize = func(fields []domain.Field) {
		for index := range fields {
			if !strings.EqualFold(fields[index].Mode, "REPEATED") {
				fields[index].Mode = "NULLABLE"
			}
			normalize(fields[index].Fields)
		}
	}
	normalize(result)
	return result
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
	case "BIGNUMERIC":
		return "BIGNUMERIC"
	case "RECORD", "STRUCT":
		return "RECORD"
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

func readMaterializedQueryResult(ctx context.Context, tx *sql.Tx, staging string, schemaHints []domain.Field) (domain.QueryResult, error) {
	rows, err := tx.QueryContext(ctx, "SELECT * FROM "+quoteIdentifier(staging))
	if err != nil {
		return domain.QueryResult{}, fmt.Errorf("read materialized query result: %w", err)
	}
	defer rows.Close()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return domain.QueryResult{}, fmt.Errorf("read materialized result schema: %w", err)
	}
	result := domain.QueryResult{}
	result.Columns, err = queryResultSchema(columnTypes, schemaHints)
	if err != nil {
		return domain.QueryResult{}, err
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
		normalized, normalizeErr := normalizeSnapshotRow(result.Columns, values)
		if normalizeErr != nil {
			return domain.QueryResult{}, fmt.Errorf("normalize materialized result row: %w", normalizeErr)
		}
		result.Rows = append(result.Rows, tableDataCanonicalRow(result.Columns, normalized))
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
	encoded, _ := json.Marshal(columns)
	return observability.Digest(encoded)
}

func queryDestinationSchemaDigest(fields []domain.Field) string {
	encoded, _ := json.Marshal(fields)
	return observability.Digest(encoded)
}
