package rest

// BigQuery's tabledata.list response uses nested f/v objects rather than plain
// JSON objects. The encoder is driven by canonical catalog schema so STRUCT and
// REPEATED values keep field order across replaceable warehouse adapters.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list#response-body

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type tableDataListResponse struct {
	Kind      string     `json:"kind"`
	ETag      string     `json:"etag,omitempty"`
	TotalRows string     `json:"totalRows"`
	PageToken string     `json:"pageToken,omitempty"`
	Rows      []tableRow `json:"rows,omitempty"`
}

func tableDataRows(rows [][]any, schema []domain.Field, format tableDataFormatOptions) ([]tableRow, error) {
	encoded := make([]tableRow, len(rows))
	for rowIndex, row := range rows {
		if len(row) != len(schema) {
			return nil, fmt.Errorf("table data row %d has %d values for %d schema fields", rowIndex, len(row), len(schema))
		}
		fields := make([]tableCell, len(schema))
		for fieldIndex, field := range schema {
			value, err := tableDataValue(field, row[fieldIndex], format)
			if err != nil {
				return nil, fmt.Errorf("encode table data row %d field %d: %w", rowIndex, fieldIndex, err)
			}
			fields[fieldIndex] = tableCell{Value: value}
		}
		encoded[rowIndex] = tableRow{Fields: fields}
	}
	return encoded, nil
}

func tableDataValue(field domain.Field, raw any, format tableDataFormatOptions) (any, error) {
	if strings.EqualFold(field.Mode, "REPEATED") {
		if raw == nil {
			return []tableCell{}, nil
		}
		value := reflect.ValueOf(raw)
		for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
			if value.IsNil() {
				return []tableCell{}, nil
			}
			value = value.Elem()
		}
		if value.Kind() != reflect.Array && value.Kind() != reflect.Slice {
			return nil, fmt.Errorf("REPEATED value has Go type %T", raw)
		}
		elementField := field
		elementField.Mode = "REQUIRED"
		elements := make([]tableCell, value.Len())
		for index := 0; index < value.Len(); index++ {
			encoded, err := tableDataValue(elementField, value.Index(index).Interface(), format)
			if err != nil {
				return nil, fmt.Errorf("array element %d: %w", index, err)
			}
			elements[index] = tableCell{Value: encoded}
		}
		return elements, nil
	}
	if raw == nil {
		return nil, nil
	}
	if strings.EqualFold(field.Type, "TIMESTAMP") {
		micros, ok := raw.(int64)
		if !ok {
			return nil, fmt.Errorf("TIMESTAMP value has Go type %T", raw)
		}
		if format.UseInt64Timestamp {
			return strconv.FormatInt(micros, 10), nil
		}
		return formatTableDataTimestamp(micros), nil
	}
	if strings.EqualFold(field.Type, "RECORD") || strings.EqualFold(field.Type, "STRUCT") {
		values, err := tableDataStruct(raw)
		if err != nil {
			return nil, err
		}
		children := make([]tableCell, len(field.Fields))
		for index, child := range field.Fields {
			rawChild, found := values[strings.ToLower(child.Name)]
			if !found {
				return nil, fmt.Errorf("STRUCT value is missing field %q", child.Name)
			}
			encoded, err := tableDataValue(child, rawChild, format)
			if err != nil {
				return nil, fmt.Errorf("nested field %q: %w", child.Name, err)
			}
			children[index] = tableCell{Value: encoded}
		}
		return tableRow{Fields: children}, nil
	}
	return encodeCell(raw), nil
}

// BigQuery's default tabledata.list timestamp representation is fractional
// Unix seconds. Official clients request epoch microseconds with
// formatOptions.useInt64Timestamp=true to avoid floating-point loss.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/FormatOptions
func formatTableDataTimestamp(micros int64) string {
	whole := micros / int64(time.Second/time.Microsecond)
	fraction := micros % int64(time.Second/time.Microsecond)
	if fraction == 0 {
		return strconv.FormatInt(whole, 10)
	}
	negativeZero := micros < 0 && whole == 0
	if fraction < 0 {
		fraction = -fraction
	}
	prefix := strconv.FormatInt(whole, 10)
	if negativeZero {
		prefix = "-0"
	}
	return prefix + "." + strings.TrimRight(fmt.Sprintf("%06d", fraction), "0")
}

func tableDataStruct(raw any) (map[string]any, error) {
	if values, ok := raw.(map[string]any); ok {
		result := make(map[string]any, len(values))
		for name, value := range values {
			result[strings.ToLower(name)] = value
		}
		return result, nil
	}
	value := reflect.ValueOf(raw)
	if value.Kind() != reflect.Map {
		return nil, fmt.Errorf("STRUCT value has Go type %T", raw)
	}
	result := make(map[string]any, value.Len())
	iterator := value.MapRange()
	for iterator.Next() {
		name, ok := iterator.Key().Interface().(string)
		if !ok {
			return nil, fmt.Errorf("STRUCT key has Go type %T", iterator.Key().Interface())
		}
		result[strings.ToLower(name)] = iterator.Value().Interface()
	}
	return result, nil
}
