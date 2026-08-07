package duckdb

// Arrow mappings and payload framing follow the Storage Read schema table.
// GetSchemaPayload/GetRecordBatchPayload write exactly one encapsulated IPC
// message; an Arrow stream/file wrapper is intentionally never emitted.
//
// Protocol sources:
//   - BigQuery to Arrow mappings: https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details
//   - Arrow IPC encapsulated messages: https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
)

func buildArrowReferenceSchema(fields []catalogdomain.Field) (*arrow.Schema, []byte, error) {
	arrowFields := make([]arrow.Field, len(fields))
	for index, field := range fields {
		converted, err := arrowField(field, field.Name)
		if err != nil {
			return nil, nil, err
		}
		arrowFields[index] = converted
	}
	schema := arrow.NewSchema(arrowFields, nil)
	payload := ipc.GetSchemaPayload(schema, memory.DefaultAllocator)
	defer payload.Release()
	var output bytes.Buffer
	if _, err := payload.WritePayload(&output); err != nil {
		return nil, nil, fmt.Errorf("serialize Arrow reference schema: %w", err)
	}
	return schema, output.Bytes(), nil
}

func arrowField(field catalogdomain.Field, path string) (arrow.Field, error) {
	baseType, err := arrowBaseType(field, path)
	if err != nil {
		return arrow.Field{}, err
	}
	metadata := arrowFieldMetadata(field)
	if strings.EqualFold(field.Mode, "REPEATED") {
		element := arrow.Field{Name: "item", Type: baseType, Nullable: false, Metadata: metadata}
		return arrow.Field{Name: field.Name, Type: arrow.ListOfField(element), Nullable: false, Metadata: metadata}, nil
	}
	return arrow.Field{
		Name: field.Name, Type: baseType,
		Nullable: !strings.EqualFold(field.Mode, "REQUIRED"), Metadata: metadata,
	}, nil
}

func arrowBaseType(field catalogdomain.Field, path string) (arrow.DataType, error) {
	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, nil
	case "INT64", "INTEGER":
		return arrow.PrimitiveTypes.Int64, nil
	case "FLOAT64", "FLOAT":
		return arrow.PrimitiveTypes.Float64, nil
	case "NUMERIC":
		return &arrow.Decimal128Type{Precision: 38, Scale: 9}, nil
	case "BIGNUMERIC":
		return &arrow.Decimal256Type{Precision: 76, Scale: 38}, nil
	case "STRING", "GEOGRAPHY", "JSON":
		return arrow.BinaryTypes.String, nil
	case "BYTES":
		return arrow.BinaryTypes.Binary, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "DATETIME":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "TIME":
		return &arrow.Time64Type{Unit: arrow.Microsecond}, nil
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "RECORD", "STRUCT":
		children := make([]arrow.Field, len(field.Fields))
		for index, child := range field.Fields {
			converted, err := arrowField(child, path+"."+child.Name)
			if err != nil {
				return nil, err
			}
			children[index] = converted
		}
		return arrow.StructOf(children...), nil
	default:
		return nil, fmt.Errorf("map BigQuery field %q at %s to Arrow: unsupported type", field.Type, path)
	}
}

func arrowFieldMetadata(field catalogdomain.Field) arrow.Metadata {
	keys := []string{"BIGQUERY:type", "BIGQUERY:mode"}
	values := []string{strings.ToUpper(field.Type), normalizedFieldMode(field)}
	if field.Description != "" {
		keys = append(keys, "BIGQUERY:description")
		values = append(values, field.Description)
	}
	if extension := arrowExtensionName(field); extension != "" {
		// Google clients use Arrow extension metadata to disambiguate logical
		// types that share an Arrow representation. Keep this in addition to
		// BIGQUERY:type, which makes contract drift explicit to generic readers.
		// Official client source: https://github.com/googleapis/google-cloud-python/blob/main/packages/google-cloud-bigquery/google/cloud/bigquery/_pandas_helpers.py
		keys = append(keys, "ARROW:extension:name")
		values = append(values, extension)
	}
	return arrow.NewMetadata(keys, values)
}

func arrowExtensionName(field catalogdomain.Field) string {
	switch strings.ToUpper(field.Type) {
	case "DATETIME":
		return "google:sqlType:datetime"
	case "GEOGRAPHY":
		return "google:sqlType:geography"
	case "JSON":
		return "google:sqlType:json"
	default:
		return ""
	}
}

func normalizedFieldMode(field catalogdomain.Field) string {
	if field.Mode == "" {
		return "NULLABLE"
	}
	return strings.ToUpper(field.Mode)
}

func encodeArrowRecordBatch(schema *arrow.Schema, fields []catalogdomain.Field, rows [][]snapshotValue) ([]byte, error) {
	if schema == nil {
		return nil, fmt.Errorf("Arrow schema is required")
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for rowIndex, row := range rows {
		if len(row) != len(fields) {
			return nil, fmt.Errorf("Arrow row %d has %d values, want %d", rowIndex, len(row), len(fields))
		}
		for fieldIndex, field := range fields {
			if err := appendArrowValue(builder.Field(fieldIndex), field, row[fieldIndex]); err != nil {
				return nil, fmt.Errorf("encode Arrow row %d field %q: %w", rowIndex, field.Name, err)
			}
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	payload, err := ipc.GetRecordBatchPayload(record, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, fmt.Errorf("build Arrow record-batch payload: %w", err)
	}
	defer payload.Release()
	var output bytes.Buffer
	if _, err := payload.WritePayload(&output); err != nil {
		return nil, fmt.Errorf("serialize Arrow record-batch payload: %w", err)
	}
	return output.Bytes(), nil
}

func appendArrowValue(builder array.Builder, field catalogdomain.Field, value snapshotValue) error {
	if strings.EqualFold(field.Mode, "REPEATED") {
		list, ok := builder.(*array.ListBuilder)
		if !ok {
			return fmt.Errorf("builder %T is not a ListBuilder", builder)
		}
		list.Append(true)
		elementField := field
		elementField.Mode = "REQUIRED"
		for _, child := range value.Children {
			if err := appendArrowValue(list.ValueBuilder(), elementField, child); err != nil {
				return err
			}
		}
		return nil
	}
	if value.Null {
		builder.AppendNull()
		return nil
	}
	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		builder.(*array.BooleanBuilder).Append(value.Bool)
	case "INT64", "INTEGER":
		builder.(*array.Int64Builder).Append(value.Int)
	case "FLOAT64", "FLOAT":
		builder.(*array.Float64Builder).Append(value.Float)
	case "NUMERIC":
		decimal, err := decimal128.FromString(value.Text, 38, 9)
		if err != nil {
			return fmt.Errorf("parse NUMERIC %q: %w", value.Text, err)
		}
		builder.(*array.Decimal128Builder).Append(decimal)
	case "BIGNUMERIC":
		decimal, err := decimal256.FromString(value.Text, 76, 38)
		if err != nil {
			return fmt.Errorf("parse BIGNUMERIC %q: %w", value.Text, err)
		}
		builder.(*array.Decimal256Builder).Append(decimal)
	case "STRING", "GEOGRAPHY", "JSON":
		builder.(*array.StringBuilder).Append(value.Text)
	case "BYTES":
		builder.(*array.BinaryBuilder).Append(value.Bytes)
	case "DATE":
		builder.(*array.Date32Builder).Append(arrow.Date32(value.Int))
	case "DATETIME", "TIMESTAMP":
		builder.(*array.TimestampBuilder).Append(arrow.Timestamp(value.Int))
	case "TIME":
		builder.(*array.Time64Builder).Append(arrow.Time64(value.Int))
	case "RECORD", "STRUCT":
		structBuilder, ok := builder.(*array.StructBuilder)
		if !ok {
			return fmt.Errorf("builder %T is not a StructBuilder", builder)
		}
		if len(value.Children) != len(field.Fields) {
			return fmt.Errorf("STRUCT has %d children, want %d", len(value.Children), len(field.Fields))
		}
		structBuilder.Append(true)
		for index, child := range value.Children {
			if err := appendArrowValue(structBuilder.FieldBuilder(index), field.Fields[index], child); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported Arrow field type %q", field.Type)
	}
	return nil
}
