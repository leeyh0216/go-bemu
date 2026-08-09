package duckdb

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

const (
	parquetNumericMaxScale         int64 = 9
	parquetNumericMaxIntegerDigits int64 = 29
)

// InferParquetSchema maps DuckDB's typed Parquet description to canonical
// catalog fields. LIST inference is opt-in because BigQuery assigns different
// legacy shapes when Parquet LIST logical inference is disabled.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#ParquetOptions
func (w *Warehouse) InferParquetSchema(
	ctx context.Context,
	objects []loadports.LocalObject,
	options loadports.ParquetSchemaOptions,
) ([]loadDomain.Field, error) {
	if w == nil || w.db == nil || len(objects) == 0 {
		return nil, fmt.Errorf("%w: local Parquet objects are required for schema inference", loadDomain.ErrInvalid)
	}
	for _, object := range objects {
		if strings.TrimSpace(object.Path) == "" {
			return nil, fmt.Errorf("%w: local Parquet object path is required", loadDomain.ErrInvalid)
		}
	}
	// The application supplies objects in canonical source URI order. BigQuery
	// derives a multi-file Parquet schema from the alphabetically last source;
	// execution still validates every file against that inferred schema.
	schemaObject := objects[len(objects)-1]
	rows, err := w.db.QueryContext(ctx, fmt.Sprintf(
		"DESCRIBE SELECT * FROM read_parquet([%s], union_by_name=false)", quoteSQLString(schemaObject.Path),
	))
	if err != nil {
		return nil, fmt.Errorf("describe Parquet schema: %w", err)
	}
	columns, err := scanStagingColumns(rows)
	if err != nil {
		return nil, err
	}
	fields := make([]loadDomain.Field, len(columns))
	for index, column := range columns {
		field, err := inferParquetField(column.name, column.typeName, options.EnableListInference)
		if err != nil {
			return nil, err
		}
		fields[index] = field
	}
	if err := loadDomain.ValidateSchema(fields); err != nil {
		return nil, err
	}
	return catalogdomain.CloneFields(fields), nil
}

func inferParquetField(name, input string, enableListInference bool) (loadDomain.Field, error) {
	field, err := inferParquetType(input, enableListInference)
	if err != nil {
		return loadDomain.Field{}, fmt.Errorf("infer Parquet field %q: %w", name, err)
	}
	field.Name = name
	return field, nil
}

func inferParquetType(input string, enableListInference bool) (loadDomain.Field, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return loadDomain.Field{}, fmt.Errorf("%w: empty Parquet type", loadDomain.ErrInvalid)
	}
	repeated := false
	if strings.HasSuffix(input, "[]") {
		repeated = true
		input = strings.TrimSpace(strings.TrimSuffix(input, "[]"))
	} else if strings.HasPrefix(strings.ToUpper(input), "LIST(") && strings.HasSuffix(input, ")") {
		repeated = true
		input = strings.TrimSpace(input[len("LIST(") : len(input)-1])
	}
	if repeated && !enableListInference {
		return loadDomain.Field{}, fmt.Errorf(
			"%w: capability=%s Parquet LIST schema inference requires enableListInference=true",
			loadDomain.ErrUnsupported, loadDomain.CapabilityParquetListInferenceV1,
		)
	}
	if strings.HasSuffix(input, "[]") || strings.HasPrefix(strings.ToUpper(input), "LIST(") {
		return loadDomain.Field{}, fmt.Errorf("%w: nested Parquet arrays are not a catalog field", loadDomain.ErrUnsupported)
	}
	mode := "NULLABLE"
	if repeated {
		mode = "REPEATED"
	}
	upper := strings.ToUpper(input)
	if strings.HasPrefix(upper, "STRUCT(") && strings.HasSuffix(input, ")") {
		parts, err := splitDuckDBTypeList(input[len("STRUCT(") : len(input)-1])
		if err != nil {
			return loadDomain.Field{}, fmt.Errorf("%w: %v", loadDomain.ErrInvalid, err)
		}
		children := make([]loadDomain.Field, len(parts))
		for index, part := range parts {
			childName, childType, err := splitDuckDBStructField(part)
			if err != nil {
				return loadDomain.Field{}, fmt.Errorf("%w: %v", loadDomain.ErrInvalid, err)
			}
			child, err := inferParquetField(childName, childType, enableListInference)
			if err != nil {
				return loadDomain.Field{}, err
			}
			children[index] = child
		}
		return loadDomain.Field{Type: "RECORD", Mode: mode, Fields: children}, nil
	}
	if match := queryDecimalTypePattern.FindStringSubmatch(input); match != nil {
		precision, _ := strconv.ParseInt(match[1], 10, 64)
		scale, _ := strconv.ParseInt(match[2], 10, 64)
		fieldType := "NUMERIC"
		if scale > parquetNumericMaxScale || precision-scale > parquetNumericMaxIntegerDigits {
			fieldType = "BIGNUMERIC"
		}
		field := loadDomain.Field{Type: fieldType, Mode: mode, Precision: &precision, Scale: &scale}
		if _, err := field.EffectiveDecimalParameters(); err != nil {
			return loadDomain.Field{}, err
		}
		return field, nil
	}
	fieldType := ""
	switch {
	case upper == "BOOLEAN" || upper == "BOOL":
		fieldType = "BOOLEAN"
	case upper == "TINYINT" || upper == "SMALLINT" || upper == "INTEGER" || upper == "BIGINT":
		fieldType = "INTEGER"
	case upper == "FLOAT" || upper == "DOUBLE" || upper == "REAL":
		fieldType = "FLOAT"
	case upper == "VARCHAR" || strings.HasPrefix(upper, "VARCHAR("):
		fieldType = "STRING"
	case upper == "BLOB":
		fieldType = "BYTES"
	case upper == "DATE":
		fieldType = "DATE"
	case strings.HasPrefix(upper, "TIMESTAMP"):
		fieldType = "TIMESTAMP"
	case strings.HasPrefix(upper, "TIME") && !strings.Contains(upper, "ZONE"):
		fieldType = "TIME"
	case upper == "JSON":
		fieldType = "JSON"
	default:
		return loadDomain.Field{}, fmt.Errorf("%w: Parquet type %q is not supported", loadDomain.ErrUnsupported, input)
	}
	return loadDomain.Field{Type: fieldType, Mode: mode}, nil
}
