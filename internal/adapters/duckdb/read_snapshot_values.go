package duckdb

// Value normalization is schema-driven. DuckDB driver values are converted
// once during snapshot materialization, so Arrow and Avro streams share the
// same immutable logical rows and cannot diverge in offset semantics.
//
// Type source: https://cloud.google.com/bigquery/docs/reference/storage#schema_conversion

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
)

type snapshotValue struct {
	Null     bool
	Bool     bool
	Int      int64
	Float    float64
	Text     string
	Bytes    []byte
	Children []snapshotValue
}

func normalizeSnapshotRow(fields []catalogdomain.Field, raw []any) ([]snapshotValue, error) {
	if len(raw) != len(fields) {
		return nil, fmt.Errorf("DuckDB returned %d columns for a %d-field snapshot schema", len(raw), len(fields))
	}
	result := make([]snapshotValue, len(fields))
	for index, field := range fields {
		value, err := normalizeSnapshotValue(field, raw[index])
		if err != nil {
			return nil, fmt.Errorf("normalize field %q: %w", field.Name, err)
		}
		result[index] = value
	}
	return result, nil
}

func normalizeSnapshotValue(field catalogdomain.Field, raw any) (snapshotValue, error) {
	raw = dereferenceSnapshotValue(raw)
	if strings.EqualFold(field.Mode, "REPEATED") {
		if raw == nil {
			return snapshotValue{Children: []snapshotValue{}}, nil
		}
		value := reflect.ValueOf(raw)
		if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
			return snapshotValue{}, fmt.Errorf("REPEATED value has Go type %T, want slice or array", raw)
		}
		elementField := field
		elementField.Mode = "REQUIRED"
		children := make([]snapshotValue, value.Len())
		for index := 0; index < value.Len(); index++ {
			child, err := normalizeSnapshotValue(elementField, value.Index(index).Interface())
			if err != nil {
				return snapshotValue{}, fmt.Errorf("array element %d: %w", index, err)
			}
			if child.Null {
				return snapshotValue{}, fmt.Errorf("BigQuery arrays cannot contain NULL elements")
			}
			children[index] = child
		}
		return snapshotValue{Children: children}, nil
	}
	if raw == nil {
		if strings.EqualFold(field.Mode, "REQUIRED") {
			return snapshotValue{}, fmt.Errorf("REQUIRED field is NULL")
		}
		return snapshotValue{Null: true}, nil
	}

	typeName := strings.ToUpper(field.Type)
	switch typeName {
	case "BOOL", "BOOLEAN":
		value, ok := raw.(bool)
		if !ok {
			return snapshotValue{}, typeMismatch(field, raw)
		}
		return snapshotValue{Bool: value}, nil
	case "INT64", "INTEGER":
		value, ok := snapshotInt64(raw)
		if !ok {
			return snapshotValue{}, typeMismatch(field, raw)
		}
		return snapshotValue{Int: value}, nil
	case "FLOAT64", "FLOAT":
		value, ok := snapshotFloat64(raw)
		if !ok {
			return snapshotValue{}, typeMismatch(field, raw)
		}
		return snapshotValue{Float: value}, nil
	case "NUMERIC", "BIGNUMERIC":
		value, err := snapshotDecimalString(raw)
		if err != nil {
			return snapshotValue{}, err
		}
		return snapshotValue{Text: value}, nil
	case "STRING":
		value, err := snapshotString(raw)
		if err != nil {
			return snapshotValue{}, err
		}
		return snapshotValue{Text: value}, nil
	case "JSON":
		value, err := snapshotJSONString(raw)
		if err != nil {
			return snapshotValue{}, err
		}
		return snapshotValue{Text: value}, nil
	case "BYTES":
		switch value := raw.(type) {
		case []byte:
			return snapshotValue{Bytes: bytes.Clone(value)}, nil
		case string:
			return snapshotValue{Bytes: []byte(value)}, nil
		default:
			return snapshotValue{}, typeMismatch(field, raw)
		}
	case "DATE":
		days, err := snapshotDate(raw)
		if err != nil {
			return snapshotValue{}, err
		}
		return snapshotValue{Int: days}, nil
	case "DATETIME":
		micros, literal, err := snapshotDateTime(raw)
		if err != nil {
			return snapshotValue{}, err
		}
		return snapshotValue{Int: micros, Text: literal}, nil
	case "TIME":
		micros, err := snapshotTimeMicros(raw)
		if err != nil {
			return snapshotValue{}, err
		}
		return snapshotValue{Int: micros}, nil
	case "TIMESTAMP":
		micros, err := snapshotTimestampMicros(raw)
		if err != nil {
			return snapshotValue{}, err
		}
		return snapshotValue{Int: micros}, nil
	case "RECORD", "STRUCT":
		children, err := snapshotStruct(field.Fields, raw)
		if err != nil {
			return snapshotValue{}, err
		}
		return snapshotValue{Children: children}, nil
	default:
		return snapshotValue{}, fmt.Errorf("unsupported BigQuery Storage Read type %q", field.Type)
	}
}

func snapshotStruct(fields []catalogdomain.Field, raw any) ([]snapshotValue, error) {
	values, err := snapshotStructMap(raw)
	if err != nil {
		return nil, err
	}
	children := make([]snapshotValue, len(fields))
	for index, field := range fields {
		rawChild, found := values[strings.ToLower(field.Name)]
		if !found {
			return nil, fmt.Errorf("STRUCT value is missing field %q", field.Name)
		}
		child, err := normalizeSnapshotValue(field, rawChild)
		if err != nil {
			return nil, fmt.Errorf("nested field %q: %w", field.Name, err)
		}
		children[index] = child
	}
	return children, nil
}

func snapshotStructMap(raw any) (map[string]any, error) {
	if ordered, ok := raw.(duckdbdriver.OrderedMap); ok {
		result := make(map[string]any, ordered.Len())
		for _, key := range ordered.Keys() {
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("STRUCT key has Go type %T, want string", key)
			}
			value, _ := ordered.Get(key)
			result[strings.ToLower(name)] = value
		}
		return result, nil
	}
	value := reflect.ValueOf(raw)
	if value.Kind() == reflect.Map {
		result := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			key, ok := iterator.Key().Interface().(string)
			if !ok {
				return nil, fmt.Errorf("STRUCT key has Go type %T, want string", iterator.Key().Interface())
			}
			result[strings.ToLower(key)] = iterator.Value().Interface()
		}
		return result, nil
	}
	if text, ok := raw.(string); ok {
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		var document map[string]any
		if err := decoder.Decode(&document); err == nil {
			result := make(map[string]any, len(document))
			for key, child := range document {
				result[strings.ToLower(key)] = child
			}
			return result, nil
		}
	}
	return nil, fmt.Errorf("STRUCT value has unsupported Go type %T", raw)
}

func dereferenceSnapshotValue(value any) any {
	if value == nil {
		return nil
	}
	reflected := reflect.ValueOf(value)
	for reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface {
		if reflected.IsNil() {
			return nil
		}
		reflected = reflected.Elem()
	}
	return reflected.Interface()
}

func snapshotInt64(raw any) (int64, bool) {
	switch value := raw.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) <= uint64(^uint64(0)>>1) {
			return int64(value), true
		}
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value <= uint64(^uint64(0)>>1) {
			return int64(value), true
		}
	case json.Number:
		parsed, err := value.Int64()
		return parsed, err == nil
	}
	return 0, false
}

func snapshotFloat64(raw any) (float64, bool) {
	switch value := raw.(type) {
	case float32:
		return float64(value), true
	case float64:
		return value, true
	case json.Number:
		parsed, err := value.Float64()
		return parsed, err == nil
	default:
		integer, ok := snapshotInt64(raw)
		return float64(integer), ok
	}
}

func snapshotDecimalString(raw any) (string, error) {
	switch value := raw.(type) {
	case duckdbdriver.Decimal:
		return value.String(), nil
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case fmt.Stringer:
		return value.String(), nil
	default:
		if integer, ok := snapshotInt64(raw); ok {
			return strconv.FormatInt(integer, 10), nil
		}
		if floating, ok := snapshotFloat64(raw); ok {
			return strconv.FormatFloat(floating, 'g', -1, 64), nil
		}
		return "", fmt.Errorf("decimal value has unsupported Go type %T", raw)
	}
}

func snapshotString(raw any) (string, error) {
	switch value := raw.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case json.RawMessage:
		return string(value), nil
	case fmt.Stringer:
		return value.String(), nil
	default:
		return "", fmt.Errorf("string value has unsupported Go type %T", raw)
	}
}

func snapshotJSONString(raw any) (string, error) {
	// duckdb-go may expose a JSON scalar as text and a JSON object/array as a
	// decoded Go composite. Storage Read maps JSON to an Arrow/Avro string, so
	// both driver shapes are normalized to valid JSON text here.
	// Source: https://cloud.google.com/bigquery/docs/reference/storage#schema_conversion
	switch value := raw.(type) {
	case string:
		if !json.Valid([]byte(value)) {
			return "", fmt.Errorf("DuckDB returned invalid JSON text")
		}
		return value, nil
	case []byte:
		if !json.Valid(value) {
			return "", fmt.Errorf("DuckDB returned invalid JSON bytes")
		}
		return string(value), nil
	case json.RawMessage:
		if !json.Valid(value) {
			return "", fmt.Errorf("DuckDB returned invalid JSON message")
		}
		return string(value), nil
	default:
		encoded, err := json.Marshal(raw)
		if err != nil {
			return "", fmt.Errorf("encode DuckDB JSON value of type %T: %w", raw, err)
		}
		if !json.Valid(encoded) {
			return "", fmt.Errorf("DuckDB JSON value of type %T encoded invalid JSON", raw)
		}
		return string(encoded), nil
	}
}

var snapshotDateLayouts = []string{"2006-01-02", time.RFC3339Nano}

func snapshotDate(raw any) (int64, error) {
	value, err := snapshotTimeValue(raw, snapshotDateLayouts)
	if err != nil {
		return 0, err
	}
	year, month, day := value.Date()
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return date.Unix() / 86400, nil
}

func snapshotDateTime(raw any) (int64, string, error) {
	value, err := snapshotTimeValue(raw, []string{
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", time.RFC3339Nano,
	})
	if err != nil {
		return 0, "", err
	}
	value = time.Date(value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), time.UTC)
	literal := value.Format("2006-01-02T15:04:05.999999")
	return value.UnixMicro(), literal, nil
}

func snapshotTimestampMicros(raw any) (int64, error) {
	value, err := snapshotTimeValue(raw, []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00"})
	if err != nil {
		return 0, err
	}
	return value.UTC().UnixMicro(), nil
}

func snapshotTimeMicros(raw any) (int64, error) {
	if value, ok := raw.(time.Time); ok {
		return int64(value.Hour())*int64(time.Hour/time.Microsecond) +
			int64(value.Minute())*int64(time.Minute/time.Microsecond) +
			int64(value.Second())*int64(time.Second/time.Microsecond) +
			int64(value.Nanosecond()/1000), nil
	}
	text, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("TIME value has unsupported Go type %T", raw)
	}
	for _, layout := range []string{"15:04:05.999999999", "15:04:05"} {
		if value, err := time.Parse(layout, text); err == nil {
			return int64(value.Hour())*int64(time.Hour/time.Microsecond) +
				int64(value.Minute())*int64(time.Minute/time.Microsecond) +
				int64(value.Second())*int64(time.Second/time.Microsecond) +
				int64(value.Nanosecond()/1000), nil
		}
	}
	return 0, fmt.Errorf("invalid TIME literal %q", text)
}

func snapshotTimeValue(raw any, layouts []string) (time.Time, error) {
	if value, ok := raw.(time.Time); ok {
		return value, nil
	}
	text, ok := raw.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("temporal value has unsupported Go type %T", raw)
	}
	for _, layout := range layouts {
		if value, err := time.Parse(layout, text); err == nil {
			return value, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid temporal literal %q", text)
}

func typeMismatch(field catalogdomain.Field, raw any) error {
	return fmt.Errorf("%s value has Go type %T", strings.ToUpper(field.Type), raw)
}

func encodeSnapshotRow(row []snapshotValue) ([]byte, error) {
	var output bytes.Buffer
	if err := gob.NewEncoder(&output).Encode(row); err != nil {
		return nil, fmt.Errorf("encode staged row: %w", err)
	}
	return output.Bytes(), nil
}

func decodeSnapshotRow(payload []byte) ([]snapshotValue, error) {
	var row []snapshotValue
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&row); err != nil {
		return nil, fmt.Errorf("decode staged row: %w", err)
	}
	return row, nil
}
