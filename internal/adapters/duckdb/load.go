package duckdb

// Parquet and transaction provenance:
//   - https://duckdb.org/docs/stable/data/parquet/overview
//   - https://duckdb.org/docs/stable/sql/statements/transactions
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
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

type stagingValueCheck struct {
	predicate string
	decimal   bool
}

func (w *Warehouse) PlanLoad(ctx context.Context, request loadports.LoadPlanRequest) (loadports.LoadPlan, error) {
	if w == nil || w.loadPlanner == nil {
		return loadports.LoadPlan{}, fmt.Errorf("%w: DuckDB load planner is not configured", loadDomain.ErrPrecondition)
	}
	return w.loadPlanner.Plan(ctx, request)
}

func (w *Warehouse) ExecuteLoad(
	ctx context.Context,
	plan loadports.LoadPlan,
	objects []loadports.LocalObject,
) (result loadports.LoadResult, err error) {
	if w == nil || w.loadPlanner == nil {
		return result, fmt.Errorf("%w: DuckDB load planner is not configured", loadDomain.ErrPrecondition)
	}
	request, err := w.loadPlanner.ValidateExecution(ctx, plan, objects)
	if err != nil {
		return result, err
	}
	table := request.Destination.Reference
	started := observability.LogSideEffectStart(ctx, "duckdb", "load_parquet",
		"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.TableID,
		"file_count", len(objects), "schema_fingerprint", loadSchemaDigest(request.Destination.Schema),
		"write_disposition", request.WriteDisposition, "transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "load_parquet", started, err,
			"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.TableID,
			"file_count", len(objects), "schema_fingerprint", loadSchemaDigest(request.Destination.Schema),
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
	objectList := make([]string, len(objects))
	for index, object := range objects {
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
	selectExpressions, destinationColumns, valueChecks, err := validateLoadShape(request.Destination.Schema, columns)
	if err != nil {
		return result, err
	}
	validatedStaging := staging + "_validated"
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		"CREATE TEMP TABLE %s AS SELECT %s FROM %s",
		quoteIdentifier(validatedStaging), strings.Join(selectExpressions, ", "), quoteIdentifier(staging),
	)); err != nil {
		return result, fmt.Errorf("cast Parquet staging data to the destination schema: %w", err)
	}
	if err := validateLosslessLoadValues(ctx, tx, staging, valueChecks); err != nil {
		return result, err
	}
	if err := validateRequiredLoadValues(ctx, tx, validatedStaging, request.Destination.Schema); err != nil {
		return result, err
	}
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+quoteIdentifier(validatedStaging)).Scan(&result.OutputRows); err != nil {
		return result, fmt.Errorf("count Parquet staging rows: %w", err)
	}

	destination := quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + "." + quoteIdentifier(table.TableID)
	if request.CreateDestination {
		create, createErr := createLoadDestinationStatement(request.Destination)
		if createErr != nil {
			return result, createErr
		}
		if _, err := tx.ExecContext(ctx, create); err != nil {
			return result, fmt.Errorf("create load destination: %w", err)
		}
		result.CreatedDestination = true
	}
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
		strings.Join(destinationColumns, ", "), strings.Join(destinationColumns, ", "), quoteIdentifier(validatedStaging))
	if _, err := tx.ExecContext(ctx, insert); err != nil {
		return result, fmt.Errorf("insert validated Parquet rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE "+quoteIdentifier(validatedStaging)); err != nil {
		return result, fmt.Errorf("drop validated load staging table: %w", err)
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

func validateRequiredLoadValues(ctx context.Context, tx *sql.Tx, staging string, schema []loadDomain.Field) error {
	predicates := make([]string, 0)
	for index, field := range schema {
		predicate := requiredLoadViolation(field, quoteIdentifier(field.Name), index)
		if predicate != "" {
			predicates = append(predicates, predicate)
		}
	}
	if len(predicates) == 0 {
		return nil
	}
	var invalidRows int64
	statement := "SELECT count(*) FROM " + quoteIdentifier(staging) + " WHERE " + strings.Join(predicates, " OR ")
	if err := tx.QueryRowContext(ctx, statement).Scan(&invalidRows); err != nil {
		return fmt.Errorf("validate required load fields: %w", err)
	}
	if invalidRows != 0 {
		return fmt.Errorf("%w: Parquet values violate destination REQUIRED or REPEATED modes", loadDomain.ErrInvalid)
	}
	return nil
}

func validateLosslessLoadValues(ctx context.Context, tx *sql.Tx, staging string, checks []stagingValueCheck) error {
	for _, decimal := range []bool{true, false} {
		predicates := make([]string, 0)
		for _, check := range checks {
			if check.decimal == decimal {
				predicates = append(predicates, check.predicate)
			}
		}
		if len(predicates) == 0 {
			continue
		}
		var changedRows int64
		statement := "SELECT count(*) FROM " + quoteIdentifier(staging) + " WHERE " + strings.Join(predicates, " OR ")
		if err := tx.QueryRowContext(ctx, statement).Scan(&changedRows); err != nil {
			return fmt.Errorf("validate lossless nested Parquet conversion: %w", err)
		}
		if changedRows == 0 {
			continue
		}
		if decimal {
			return fmt.Errorf("%w: capability=%s nested Parquet decimal values require narrowing or rounding", loadDomain.ErrUnsupported, loadDomain.CapabilityDecimalRoundingV1)
		}
		return fmt.Errorf("%w: nested Parquet values cannot be converted losslessly", loadDomain.ErrInvalid)
	}
	return nil
}

func requiredLoadViolation(field loadDomain.Field, source string, depth int) string {
	if strings.EqualFold(field.Mode, "REPEATED") {
		item := fmt.Sprintf("__bqemu_load_item_%d", depth)
		itemViolation := item + " IS NULL"
		if isLoadRecord(field) {
			children := requiredLoadChildren(field.Fields, item, depth+1)
			if children != "" {
				itemViolation += " OR " + children
			}
		}
		return "(CASE WHEN " + source + " IS NULL THEN TRUE ELSE " +
			"len(list_filter(" + source + ", " + item + " -> " + itemViolation + ")) > 0 END)"
	}
	parts := make([]string, 0, 2)
	if strings.EqualFold(normalizeMode(field.Mode), "REQUIRED") {
		parts = append(parts, source+" IS NULL")
	}
	if isLoadRecord(field) {
		children := requiredLoadChildren(field.Fields, source, depth+1)
		if children != "" {
			parts = append(parts, "("+source+" IS NOT NULL AND ("+children+"))")
		}
	}
	return strings.Join(parts, " OR ")
}

func requiredLoadChildren(fields []loadDomain.Field, source string, depth int) string {
	predicates := make([]string, 0)
	for index, field := range fields {
		name := quoteIdentifier(field.Name)
		predicate := requiredLoadViolation(field, source+"."+name, depth+index)
		if predicate != "" {
			predicates = append(predicates, predicate)
		}
	}
	return strings.Join(predicates, " OR ")
}

func isLoadRecord(field loadDomain.Field) bool {
	return strings.EqualFold(field.Type, "RECORD") || strings.EqualFold(field.Type, "STRUCT")
}

func (w *Warehouse) DiscardLoadedTable(ctx context.Context, table loadDomain.TableReference) error {
	return w.DropTable(ctx, table.ProjectID, table.DatasetID, table.TableID)
}

func createLoadDestinationStatement(table loadDomain.Table) (string, error) {
	columns := make([]string, len(table.Schema))
	for index, field := range table.Schema {
		physicalType, err := duckDBType(field)
		if err != nil {
			return "", err
		}
		column := quoteIdentifier(field.Name) + " " + physicalType
		if strings.EqualFold(normalizeMode(field.Mode), "REQUIRED") {
			column += " NOT NULL"
		}
		columns[index] = column
	}
	return fmt.Sprintf(
		"CREATE TABLE %s.%s (%s)",
		quoteIdentifier(physicalSchema(table.Reference.ProjectID, table.Reference.DatasetID)),
		quoteIdentifier(table.Reference.TableID),
		strings.Join(columns, ", "),
	), nil
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

func validateLoadShape(schema []loadDomain.Field, columns []stagingColumn) ([]string, []string, []stagingValueCheck, error) {
	if len(schema) != len(columns) {
		return nil, nil, nil, fmt.Errorf("%w: Parquet field count does not match the destination schema", loadDomain.ErrInvalid)
	}
	byName := make(map[string]stagingColumn, len(columns))
	for _, column := range columns {
		key := strings.ToLower(column.name)
		if _, duplicate := byName[key]; duplicate {
			return nil, nil, nil, fmt.Errorf("%w: duplicate Parquet field %q", loadDomain.ErrInvalid, column.name)
		}
		byName[key] = column
	}
	selectExpressions := make([]string, len(schema))
	destinationColumns := make([]string, len(schema))
	valueChecks := make([]stagingValueCheck, 0)
	for index, field := range schema {
		column, ok := byName[strings.ToLower(field.Name)]
		if !ok {
			return nil, nil, nil, fmt.Errorf("%w: Parquet field %q is missing", loadDomain.ErrInvalid, field.Name)
		}
		targetType, err := validatedTargetType(field, column.typeName)
		if err != nil {
			return nil, nil, nil, err
		}
		source := quoteIdentifier(column.name)
		selectExpressions[index] = "CAST(" + source + " AS " + targetType + ") AS " + quoteIdentifier(field.Name)
		destinationColumns[index] = quoteIdentifier(field.Name)
		if isLoadRecord(field) || strings.EqualFold(field.Mode, "REPEATED") {
			valueChecks = append(valueChecks, stagingValueCheck{
				predicate: source + " IS DISTINCT FROM CAST(CAST(" + source + " AS " + targetType + ") AS " + column.typeName + ")",
				decimal:   loadFieldContainsDecimal(field),
			})
		}
	}
	return selectExpressions, destinationColumns, valueChecks, nil
}

func loadFieldContainsDecimal(field loadDomain.Field) bool {
	if strings.EqualFold(field.Type, "NUMERIC") || strings.EqualFold(field.Type, "BIGNUMERIC") {
		return true
	}
	for _, child := range field.Fields {
		if loadFieldContainsDecimal(child) {
			return true
		}
	}
	return false
}

func validatedTargetType(field loadDomain.Field, sourceType string) (string, error) {
	target := strings.ToUpper(field.Type)
	source := strings.ToUpper(strings.TrimSpace(sourceType))
	if strings.EqualFold(field.Mode, "REPEATED") {
		if !strings.HasSuffix(source, "[]") {
			return "", fmt.Errorf("%w: Parquet field %q is not a LIST", loadDomain.ErrInvalid, field.Name)
		}
		physicalType, err := duckDBType(field)
		if err != nil {
			return "", err
		}
		return physicalType, nil
	}
	if target == "RECORD" || target == "STRUCT" {
		if !strings.HasPrefix(source, "STRUCT(") {
			return "", fmt.Errorf("%w: Parquet field %q is not a STRUCT", loadDomain.ErrInvalid, field.Name)
		}
		physicalType, err := duckDBType(field)
		if err != nil {
			return "", err
		}
		return physicalType, nil
	}
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
	case "NUMERIC", "BIGNUMERIC":
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			classification := loadDomain.ErrInvalid
			if errors.Is(err, catalogdomain.ErrUnsupported) {
				classification = loadDomain.ErrUnsupported
			}
			return "", fmt.Errorf("%w: decimal target %q: %v", classification, field.Name, err)
		}
		targetType := fmt.Sprintf("DECIMAL(%d,%d)", parameters.Precision, parameters.Scale)
		if isInteger {
			return targetType, nil
		}
		if match := decimalPattern.FindStringSubmatch(source); match != nil {
			precision, _ := strconv.Atoi(match[1])
			scale, _ := strconv.Atoi(match[2])
			if scale <= int(parameters.Scale) && precision-scale <= int(parameters.Precision-parameters.Scale) {
				return targetType, nil
			}
			return "", fmt.Errorf("%w: capability=%s Parquet field %q requires decimal narrowing or rounding", loadDomain.ErrUnsupported, loadDomain.CapabilityDecimalRoundingV1, field.Name)
		}
		return "", fmt.Errorf("%w: capability=%s Parquet field %q requires an unsupported decimal conversion", loadDomain.ErrUnsupported, loadDomain.CapabilityDecimalRoundingV1, field.Name)
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
	case "GEOGRAPHY", "JSON":
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
	encoded, _ := json.Marshal(schema)
	return observability.Digest(encoded)
}

var _ loadports.Loader = (*Warehouse)(nil)
