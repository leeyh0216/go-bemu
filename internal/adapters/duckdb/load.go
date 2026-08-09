package duckdb

// Parquet and transaction provenance:
//   - https://duckdb.org/docs/stable/data/parquet/overview
//   - https://duckdb.org/docs/stable/sql/statements/transactions
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

var (
	loadSequence   atomic.Uint64
	decimalPattern = regexp.MustCompile(`^DECIMAL\(([0-9]+),([0-9]+)\)$`)
)

type stagingColumn struct {
	name     string
	typeName string
}

func (w *Warehouse) Load(ctx context.Context, request loadports.LoadRequest) (result loadports.LoadResult, err error) {
	if request.SourceFormat != loadDomain.FormatParquet {
		return result, fmt.Errorf("%w: DuckDB loader supports only PARQUET", loadDomain.ErrUnsupported)
	}
	if len(request.Objects) == 0 {
		return result, fmt.Errorf("%w: at least one local Parquet object is required", loadDomain.ErrInvalid)
	}
	if len(request.Schema) == 0 {
		return result, fmt.Errorf("%w: destination schema is required", loadDomain.ErrInvalid)
	}
	table := request.Destination.Reference
	started := observability.LogSideEffectStart(ctx, "duckdb", "load_parquet",
		"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.TableID,
		"file_count", len(request.Objects), "schema_fingerprint", loadSchemaDigest(request.Schema),
		"write_disposition", request.WriteDisposition, "transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "load_parquet", started, err,
			"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.TableID,
			"file_count", len(request.Objects), "schema_fingerprint", loadSchemaDigest(request.Schema),
			"write_disposition", request.WriteDisposition, "transaction_mode", "explicit",
			"output_rows", result.OutputRows)
	}()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin load transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	staging := "bqemu_load_" + strconv.FormatUint(loadSequence.Add(1), 10)
	objectList := make([]string, len(request.Objects))
	for index, object := range request.Objects {
		if strings.TrimSpace(object.Path) == "" {
			return result, fmt.Errorf("%w: local object path is empty", loadDomain.ErrInvalid)
		}
		objectList[index] = quoteSQLString(object.Path)
	}
	createStaging := fmt.Sprintf("CREATE TEMP TABLE %s AS SELECT * FROM read_parquet([%s], union_by_name=false)",
		quoteIdentifier(staging), strings.Join(objectList, ", "))
	if _, err := tx.ExecContext(ctx, createStaging); err != nil {
		return result, fmt.Errorf("read Parquet staging data: %w", err)
	}

	columns, err := describeStaging(ctx, tx, staging)
	if err != nil {
		return result, err
	}
	selectExpressions, destinationColumns, err := validateLoadShape(request.Schema, columns)
	if err != nil {
		return result, err
	}
	for _, field := range request.Schema {
		if !strings.EqualFold(normalizeMode(field.Mode), "REQUIRED") {
			continue
		}
		var nullRows int64
		statement := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NULL", quoteIdentifier(staging), quoteIdentifier(field.Name))
		if err := tx.QueryRowContext(ctx, statement).Scan(&nullRows); err != nil {
			return result, fmt.Errorf("validate required load field: %w", err)
		}
		if nullRows != 0 {
			return result, fmt.Errorf("%w: REQUIRED destination field %q contains NULL source values", loadDomain.ErrInvalid, field.Name)
		}
	}
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+quoteIdentifier(staging)).Scan(&result.OutputRows); err != nil {
		return result, fmt.Errorf("count Parquet staging rows: %w", err)
	}

	destination := quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + "." + quoteIdentifier(table.TableID)
	switch request.WriteDisposition {
	case loadDomain.WriteEmpty:
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+destination+" LIMIT 1)").Scan(&exists); err != nil {
			return result, fmt.Errorf("inspect WRITE_EMPTY destination: %w", err)
		}
		if exists {
			return result, fmt.Errorf("%w: WRITE_EMPTY destination contains rows", loadDomain.ErrPrecondition)
		}
	case loadDomain.WriteTruncate:
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+destination); err != nil {
			return result, fmt.Errorf("truncate load destination: %w", err)
		}
	case loadDomain.WriteAppend:
	default:
		return result, fmt.Errorf("%w: write disposition %q", loadDomain.ErrInvalid, request.WriteDisposition)
	}

	insert := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", destination,
		strings.Join(destinationColumns, ", "), strings.Join(selectExpressions, ", "), quoteIdentifier(staging))
	if _, err := tx.ExecContext(ctx, insert); err != nil {
		return result, fmt.Errorf("insert validated Parquet rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE "+quoteIdentifier(staging)); err != nil {
		return result, fmt.Errorf("drop load staging table: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit load transaction: %w", err)
	}
	committed = true
	return result, nil
}

func describeStaging(ctx context.Context, tx *sql.Tx, staging string) ([]stagingColumn, error) {
	rows, err := tx.QueryContext(ctx, "DESCRIBE SELECT * FROM "+quoteIdentifier(staging))
	if err != nil {
		return nil, fmt.Errorf("describe Parquet staging schema: %w", err)
	}
	defer rows.Close()
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("inspect Parquet schema description: %w", err)
	}
	if len(columnNames) < 2 {
		return nil, fmt.Errorf("%w: DuckDB returned an incomplete Parquet schema description", loadDomain.ErrInvalid)
	}
	result := make([]stagingColumn, 0)
	for rows.Next() {
		values := make([]any, len(columnNames))
		destinations := make([]any, len(columnNames))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("scan Parquet schema description: %w", err)
		}
		name, nameOK := databaseString(values[0])
		typeName, typeOK := databaseString(values[1])
		if !nameOK || !typeOK {
			return nil, fmt.Errorf("%w: DuckDB returned an invalid Parquet schema description", loadDomain.ErrInvalid)
		}
		result = append(result, stagingColumn{name: name, typeName: strings.ToUpper(typeName)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read Parquet schema description: %w", err)
	}
	return result, nil
}

func validateLoadShape(schema []loadDomain.Field, columns []stagingColumn) ([]string, []string, error) {
	if len(schema) != len(columns) {
		return nil, nil, fmt.Errorf("%w: Parquet field count does not match the destination schema", loadDomain.ErrInvalid)
	}
	byName := make(map[string]stagingColumn, len(columns))
	for _, column := range columns {
		key := strings.ToLower(column.name)
		if _, duplicate := byName[key]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate Parquet field %q", loadDomain.ErrInvalid, column.name)
		}
		byName[key] = column
	}
	selectExpressions := make([]string, len(schema))
	destinationColumns := make([]string, len(schema))
	for index, field := range schema {
		column, ok := byName[strings.ToLower(field.Name)]
		if !ok {
			return nil, nil, fmt.Errorf("%w: Parquet field %q is missing", loadDomain.ErrInvalid, field.Name)
		}
		if len(field.Fields) > 0 || strings.EqualFold(field.Mode, "REPEATED") {
			targetType, err := loadDuckDBType(field)
			if err != nil {
				return nil, nil, err
			}
			if !equivalentDuckDBLoadType(column.typeName, targetType) {
				return nil, nil, fmt.Errorf("%w: Parquet field %q has DuckDB type %s, incompatible with BigQuery %s", loadDomain.ErrInvalid, field.Name, column.typeName, field.Type)
			}
			selectExpressions[index] = "CAST(" + quoteIdentifier(column.name) + " AS " + targetType + ")"
			destinationColumns[index] = quoteIdentifier(field.Name)
			continue
		}
		targetType, err := validatedTargetType(field, column.typeName)
		if err != nil {
			return nil, nil, err
		}
		selectExpressions[index] = "CAST(" + quoteIdentifier(column.name) + " AS " + targetType + ")"
		destinationColumns[index] = quoteIdentifier(field.Name)
	}
	return selectExpressions, destinationColumns, nil
}

func loadDuckDBType(field loadDomain.Field) (string, error) {
	var result string
	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		result = "BOOLEAN"
	case "INT64", "INTEGER":
		result = "BIGINT"
	case "FLOAT64", "FLOAT":
		result = "DOUBLE"
	case "STRING", "GEOGRAPHY":
		result = "VARCHAR"
	case "BYTES":
		result = "BLOB"
	case "DATE":
		result = "DATE"
	case "TIME":
		result = "TIME"
	case "DATETIME":
		result = "TIMESTAMP"
	case "TIMESTAMP":
		result = "TIMESTAMPTZ"
	case "NUMERIC":
		result = "DECIMAL(38,9)"
	case "BIGNUMERIC":
		result = "DECIMAL(38,18)"
	case "RECORD", "STRUCT":
		if len(field.Fields) == 0 {
			return "", fmt.Errorf("%w: STRUCT field %q has no nested fields", loadDomain.ErrInvalid, field.Name)
		}
		children := make([]string, len(field.Fields))
		for index, child := range field.Fields {
			childType, err := loadDuckDBType(child)
			if err != nil {
				return "", err
			}
			children[index] = quoteIdentifier(child.Name) + " " + childType
		}
		result = "STRUCT(" + strings.Join(children, ", ") + ")"
	default:
		return "", fmt.Errorf("%w: BigQuery type %s", loadDomain.ErrInvalid, field.Type)
	}
	if strings.EqualFold(normalizeMode(field.Mode), "REPEATED") {
		result += "[]"
	}
	return result, nil
}

func equivalentDuckDBLoadType(left, right string) bool {
	normalize := func(value string) string {
		value = strings.ToUpper(value)
		value = strings.ReplaceAll(value, " ", "")
		return strings.ReplaceAll(value, "\"", "")
	}
	return normalize(left) == normalize(right)
}

func validatedTargetType(field loadDomain.Field, sourceType string) (string, error) {
	target := strings.ToUpper(field.Type)
	source := strings.ToUpper(strings.TrimSpace(sourceType))
	isInteger := source == "TINYINT" || source == "SMALLINT" || source == "INTEGER" || source == "BIGINT"
	switch target {
	case "BOOL", "BOOLEAN":
		if source == "BOOLEAN" {
			return "BOOLEAN", nil
		}
	case "INT64", "INTEGER":
		if isInteger {
			return "BIGINT", nil
		}
	case "FLOAT64", "FLOAT":
		if isInteger || source == "FLOAT" || source == "DOUBLE" || decimalPattern.MatchString(source) {
			return "DOUBLE", nil
		}
	case "NUMERIC":
		if isInteger {
			return "DECIMAL(38,9)", nil
		}
		if match := decimalPattern.FindStringSubmatch(source); match != nil {
			precision, _ := strconv.Atoi(match[1])
			scale, _ := strconv.Atoi(match[2])
			if precision <= 38 && scale <= 9 {
				return "DECIMAL(38,9)", nil
			}
		}
	case "STRING":
		if source == "VARCHAR" {
			return "VARCHAR", nil
		}
	case "BYTES":
		if source == "BLOB" {
			return "BLOB", nil
		}
	case "DATE":
		if source == "DATE" {
			return "DATE", nil
		}
	case "TIME":
		if strings.HasPrefix(source, "TIME") && !strings.Contains(source, "ZONE") {
			return "TIME", nil
		}
	case "DATETIME":
		if strings.HasPrefix(source, "TIMESTAMP") && !strings.Contains(source, "ZONE") {
			return "TIMESTAMP", nil
		}
	case "TIMESTAMP":
		if strings.HasPrefix(source, "TIMESTAMP") {
			return "TIMESTAMPTZ", nil
		}
	case "BIGNUMERIC", "GEOGRAPHY", "JSON", "RECORD", "STRUCT":
		return "", fmt.Errorf("%w: BigQuery type %s in Parquet loads", loadDomain.ErrUnsupported, target)
	default:
		return "", fmt.Errorf("%w: BigQuery type %s", loadDomain.ErrInvalid, target)
	}
	return "", fmt.Errorf("%w: Parquet field %q has DuckDB type %s, incompatible with BigQuery %s", loadDomain.ErrInvalid, field.Name, source, target)
}

func quoteSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func normalizeMode(mode string) string {
	if mode == "" {
		return "NULLABLE"
	}
	return strings.ToUpper(mode)
}

func databaseString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

func loadSchemaDigest(schema []loadDomain.Field) string {
	parts := make([]string, 0, len(schema))
	var appendField func(loadDomain.Field)
	appendField = func(field loadDomain.Field) {
		parts = append(parts, field.Name+":"+strings.ToUpper(field.Type)+":"+normalizeMode(field.Mode)+"{")
		for _, child := range field.Fields {
			appendField(child)
		}
		parts = append(parts, "}")
	}
	for _, field := range schema {
		appendField(field)
	}
	return observability.Digest([]byte(strings.Join(parts, "\x00")))
}

var _ loadports.Loader = (*Warehouse)(nil)
