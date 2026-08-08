package sqltest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type Failure struct {
	Phase ErrorPhase
	Code  string
}

type Outcome struct {
	Result  *domain.QueryResult
	Failure *Failure
	Tables  map[string]TableOutcome
}

type TableOutcome struct {
	Exists bool
	Schema []domain.Field
	Rows   [][]any
}

func Compare(test Case, outcome Outcome) error {
	if err := comparePrimaryOutcome(test, outcome); err != nil {
		return err
	}
	return compareTableOutcomes(test, outcome.Tables)
}

func comparePrimaryOutcome(test Case, outcome Outcome) error {
	expected := test.Expected
	if expected.Kind == ExpectedError {
		if outcome.Failure == nil {
			return fmt.Errorf("case %s expected %s error %q, got success", test.ID, expected.Error.Phase, expected.Error.Code)
		}
		if outcome.Failure.Phase != expected.Error.Phase {
			return fmt.Errorf("case %s error phase = %q, want %q", test.ID, outcome.Failure.Phase, expected.Error.Phase)
		}
		if outcome.Failure.Code != expected.Error.Code {
			return fmt.Errorf("case %s error code = %q, want %q", test.ID, outcome.Failure.Code, expected.Error.Code)
		}
		return nil
	}
	if outcome.Failure != nil {
		return fmt.Errorf("case %s unexpected %s error %q", test.ID, outcome.Failure.Phase, outcome.Failure.Code)
	}
	if outcome.Result == nil {
		return fmt.Errorf("case %s produced no result", test.ID)
	}
	if expected.Kind == ExpectedAffected {
		if outcome.Result.AffectedRows != *expected.AffectedRows {
			return fmt.Errorf("case %s affectedRows = %d, want %d", test.ID, outcome.Result.AffectedRows, *expected.AffectedRows)
		}
		return nil
	}
	if expected.Kind != ExpectedRows {
		return fmt.Errorf("case %s has unsupported expected kind %q", test.ID, expected.Kind)
	}

	expectedSchema := fixtureFieldsToDomain(expected.Schema)
	if err := compareFields(expectedSchema, outcome.Result.Columns, "schema"); err != nil {
		return fmt.Errorf("case %s %w", test.ID, err)
	}
	expectedRows, err := canonicalRows(expectedSchema, expected.Rows)
	if err != nil {
		return fmt.Errorf("case %s expected rows: %w", test.ID, err)
	}
	actualRows, err := canonicalRows(expectedSchema, outcome.Result.Rows)
	if err != nil {
		return fmt.Errorf("case %s actual rows: %w", test.ID, err)
	}
	if test.RowOrder == RowOrderUnordered {
		slices.Sort(expectedRows)
		slices.Sort(actualRows)
	}
	if len(actualRows) != len(expectedRows) {
		return fmt.Errorf("case %s row count = %d, want %d", test.ID, len(actualRows), len(expectedRows))
	}
	for index := range expectedRows {
		if actualRows[index] != expectedRows[index] {
			return fmt.Errorf("case %s row[%d] = %s, want %s", test.ID, index, actualRows[index], expectedRows[index])
		}
	}
	return nil
}

func compareTableOutcomes(test Case, observed map[string]TableOutcome) error {
	if len(test.Expected.Tables) == 0 {
		return nil
	}
	for _, expected := range test.Expected.Tables {
		key := tableOutcomeKey(expected.ProjectID, expected.DatasetID, expected.TableID)
		actual, found := observed[key]
		if !found {
			return fmt.Errorf("case %s table %s was not observed", test.ID, key)
		}
		if actual.Exists != expected.Exists {
			return fmt.Errorf("case %s table %s exists = %t, want %t", test.ID, key, actual.Exists, expected.Exists)
		}
		if !expected.Exists {
			continue
		}
		expectedSchema := fixtureFieldsToDomain(expected.Schema)
		if err := compareFields(expectedSchema, actual.Schema, "table "+key+" schema"); err != nil {
			return fmt.Errorf("case %s %w", test.ID, err)
		}
		expectedRows, err := canonicalRows(expectedSchema, expected.Rows)
		if err != nil {
			return fmt.Errorf("case %s table %s expected rows: %w", test.ID, key, err)
		}
		actualRows, err := canonicalRows(expectedSchema, actual.Rows)
		if err != nil {
			return fmt.Errorf("case %s table %s actual rows: %w", test.ID, key, err)
		}
		if expected.RowOrder == RowOrderUnordered {
			slices.Sort(expectedRows)
			slices.Sort(actualRows)
		}
		if len(actualRows) != len(expectedRows) {
			return fmt.Errorf("case %s table %s row count = %d, want %d", test.ID, key, len(actualRows), len(expectedRows))
		}
		for index := range expectedRows {
			if actualRows[index] != expectedRows[index] {
				return fmt.Errorf("case %s table %s row[%d] = %s, want %s", test.ID, key, index, actualRows[index], expectedRows[index])
			}
		}
	}
	return nil
}

func tableOutcomeKey(projectID, datasetID, tableID string) string {
	return projectID + "." + datasetID + "." + tableID
}

func fixtureFieldsToDomain(fields []Field) []domain.Field {
	result := make([]domain.Field, len(fields))
	for index, field := range fields {
		result[index] = domain.Field{
			Name: field.Name, Type: field.Type, Mode: field.Mode,
			Precision: cloneInt64(field.Precision), Scale: cloneInt64(field.Scale),
			Fields: fixtureFieldsToDomain(field.Fields),
		}
		if field.RoundingMode != nil {
			result[index].RoundingMode = domain.RoundingMode(*field.RoundingMode)
		}
	}
	return result
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func compareFields(expected, actual []domain.Field, path string) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("%s length = %d, want %d", path, len(actual), len(expected))
	}
	for index := range expected {
		fieldPath := fmt.Sprintf("%s[%d]", path, index)
		if actual[index].Name != expected[index].Name {
			return fmt.Errorf("%s.name = %q, want %q", fieldPath, actual[index].Name, expected[index].Name)
		}
		if canonicalType(actual[index].Type) != canonicalType(expected[index].Type) {
			return fmt.Errorf("%s.type = %q, want %q", fieldPath, actual[index].Type, expected[index].Type)
		}
		if canonicalMode(actual[index].Mode) != canonicalMode(expected[index].Mode) {
			return fmt.Errorf("%s.mode = %q, want %q", fieldPath, actual[index].Mode, expected[index].Mode)
		}
		if err := compareOptionalInt64(expected[index].Precision, actual[index].Precision, fieldPath+".precision"); err != nil {
			return err
		}
		if err := compareOptionalInt64(expected[index].Scale, actual[index].Scale, fieldPath+".scale"); err != nil {
			return err
		}
		if actual[index].RoundingMode != expected[index].RoundingMode {
			return fmt.Errorf("%s.roundingMode = %q, want %q", fieldPath, actual[index].RoundingMode, expected[index].RoundingMode)
		}
		if err := compareFields(expected[index].Fields, actual[index].Fields, fieldPath+".fields"); err != nil {
			return err
		}
	}
	return nil
}

func compareOptionalInt64(expected, actual *int64, path string) error {
	if (expected == nil) != (actual == nil) {
		return fmt.Errorf("%s presence = %t, want %t", path, actual != nil, expected != nil)
	}
	if expected != nil && *actual != *expected {
		return fmt.Errorf("%s = %d, want %d", path, *actual, *expected)
	}
	return nil
}

func canonicalType(value string) string {
	switch strings.ToUpper(value) {
	case "BOOLEAN":
		return "BOOL"
	case "INTEGER":
		return "INT64"
	case "FLOAT":
		return "FLOAT64"
	case "STRUCT":
		return "RECORD"
	default:
		return strings.ToUpper(value)
	}
}

func canonicalMode(value string) string {
	if value == "" {
		return "NULLABLE"
	}
	return strings.ToUpper(value)
}

func canonicalRows(fields []domain.Field, rows [][]any) ([]string, error) {
	result := make([]string, len(rows))
	for rowIndex, row := range rows {
		if len(row) != len(fields) {
			return nil, fmt.Errorf("row[%d] has %d values, want %d", rowIndex, len(row), len(fields))
		}
		values := make([]string, len(row))
		for columnIndex, field := range fields {
			value, err := canonicalValue(field, row[columnIndex], fmt.Sprintf("row[%d].%s", rowIndex, field.Name))
			if err != nil {
				return nil, err
			}
			values[columnIndex] = frame(value)
		}
		result[rowIndex] = "row:" + strings.Join(values, "")
	}
	return result, nil
}

func canonicalValue(field domain.Field, value any, valuePath string) (string, error) {
	if value == nil {
		return "null", nil
	}
	if canonicalMode(field.Mode) == "REPEATED" {
		items, err := sequenceValues(value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", valuePath, err)
		}
		element := field
		element.Mode = "REQUIRED"
		encoded := make([]string, len(items))
		for index, item := range items {
			canonical, err := canonicalValue(element, item, fmt.Sprintf("%s[%d]", valuePath, index))
			if err != nil {
				return "", err
			}
			encoded[index] = frame(canonical)
		}
		return "array:" + strings.Join(encoded, ""), nil
	}

	switch canonicalType(field.Type) {
	case "BOOL":
		boolean, ok := value.(bool)
		if !ok {
			return "", typeError(valuePath, value, "bool")
		}
		return "bool:" + strconv.FormatBool(boolean), nil
	case "INT64":
		integer, err := canonicalInteger(value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", valuePath, err)
		}
		return "int:" + integer, nil
	case "FLOAT64":
		floating, err := canonicalFloat(value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", valuePath, err)
		}
		return "float:" + floating, nil
	case "NUMERIC", "BIGNUMERIC":
		text, err := scalarText(value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", valuePath, err)
		}
		normalized, err := field.NormalizeDecimalValue(text)
		if err != nil {
			return "", fmt.Errorf("%s: %w", valuePath, err)
		}
		return strings.ToLower(canonicalType(field.Type)) + ":" + normalized, nil
	case "STRING", "DATETIME":
		text, ok := value.(string)
		if !ok {
			return "", typeError(valuePath, value, "string")
		}
		return strings.ToLower(canonicalType(field.Type)) + ":" + text, nil
	case "JSON":
		canonical, err := canonicalJSON(value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", valuePath, err)
		}
		return "json:" + canonical, nil
	case "BYTES":
		encoded, err := canonicalBytes(value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", valuePath, err)
		}
		return "bytes:" + encoded, nil
	case "DATE":
		text, ok := value.(string)
		if !ok {
			return "", typeError(valuePath, value, "YYYY-MM-DD string")
		}
		parsed, err := time.Parse("2006-01-02", text)
		if err != nil {
			return "", fmt.Errorf("%s: invalid DATE %q", valuePath, text)
		}
		return "date:" + parsed.Format("2006-01-02"), nil
	case "TIME":
		text, ok := value.(string)
		if !ok {
			return "", typeError(valuePath, value, "TIME string")
		}
		parsed, err := time.Parse("15:04:05.999999999", text)
		if err != nil {
			return "", fmt.Errorf("%s: invalid TIME %q", valuePath, text)
		}
		return "time:" + parsed.Format("15:04:05.999999999"), nil
	case "TIMESTAMP":
		micros, err := timestampMicros(value)
		if err != nil {
			return "", fmt.Errorf("%s: %w", valuePath, err)
		}
		return "timestamp-micros:" + strconv.FormatInt(micros, 10), nil
	case "RECORD":
		return canonicalStruct(field.Fields, value, valuePath)
	default:
		return "", fmt.Errorf("%s: unsupported canonical type %q", valuePath, field.Type)
	}
}

func canonicalStruct(fields []domain.Field, value any, valuePath string) (string, error) {
	values, ok := value.(map[string]any)
	if !ok {
		return "", typeError(valuePath, value, "object")
	}
	lookup := make(map[string]any, len(values))
	for name, child := range values {
		lower := strings.ToLower(name)
		if _, exists := lookup[lower]; exists {
			return "", fmt.Errorf("%s contains duplicate field %q", valuePath, name)
		}
		lookup[lower] = child
	}
	encoded := make([]string, len(fields))
	for index, field := range fields {
		child, exists := lookup[strings.ToLower(field.Name)]
		if !exists {
			return "", fmt.Errorf("%s is missing field %q", valuePath, field.Name)
		}
		canonical, err := canonicalValue(field, child, valuePath+"."+field.Name)
		if err != nil {
			return "", err
		}
		encoded[index] = frame(field.Name) + frame(canonical)
	}
	if len(lookup) != len(fields) {
		return "", fmt.Errorf("%s has %d fields, want %d", valuePath, len(lookup), len(fields))
	}
	return "struct:" + strings.Join(encoded, ""), nil
}

func sequenceValues(value any) ([]any, error) {
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return nil, fmt.Errorf("value has type %T, want array", value)
	}
	result := make([]any, reflected.Len())
	for index := range result {
		result[index] = reflected.Index(index).Interface()
	}
	return result, nil
}

func canonicalInteger(value any) (string, error) {
	switch typed := value.(type) {
	case json.Number:
		integer, err := typed.Int64()
		if err != nil {
			return "", fmt.Errorf("invalid INT64 %q", typed)
		}
		return strconv.FormatInt(integer, 10), nil
	case int:
		return strconv.FormatInt(int64(typed), 10), nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case uint:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint64:
		if typed > math.MaxInt64 {
			return "", fmt.Errorf("INT64 overflows: %d", typed)
		}
		return strconv.FormatUint(typed, 10), nil
	default:
		return "", fmt.Errorf("value has type %T, want INT64", value)
	}
}

func canonicalFloat(value any) (string, error) {
	var floating float64
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return "", fmt.Errorf("invalid FLOAT64 %q", typed)
		}
		floating = parsed
	case float32:
		floating = float64(typed)
	case float64:
		floating = typed
	case string:
		switch typed {
		case "NaN":
			floating = math.NaN()
		case "Infinity":
			floating = math.Inf(1)
		case "-Infinity":
			floating = math.Inf(-1)
		default:
			return "", fmt.Errorf("invalid FLOAT64 %q", typed)
		}
	default:
		return "", fmt.Errorf("value has type %T, want FLOAT64", value)
	}
	if math.IsNaN(floating) {
		return "NaN", nil
	}
	if math.IsInf(floating, 1) {
		return "Infinity", nil
	}
	if math.IsInf(floating, -1) {
		return "-Infinity", nil
	}
	return strconv.FormatFloat(floating, 'g', -1, 64), nil
}

func scalarText(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case json.Number:
		return typed.String(), nil
	default:
		return "", fmt.Errorf("value has type %T, want decimal text", value)
	}
}

func canonicalJSON(value any) (string, error) {
	if text, ok := value.(string); ok {
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return "", fmt.Errorf("invalid JSON: %w", err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return "", fmt.Errorf("invalid JSON: trailing value")
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode canonical JSON: %w", err)
	}
	return string(encoded), nil
}

func canonicalBytes(value any) (string, error) {
	switch typed := value.(type) {
	case []byte:
		return base64.StdEncoding.EncodeToString(bytes.Clone(typed)), nil
	case string:
		decoded, err := base64.StdEncoding.DecodeString(typed)
		if err != nil {
			return "", fmt.Errorf("invalid base64 BYTES: %w", err)
		}
		return base64.StdEncoding.EncodeToString(decoded), nil
	default:
		return "", fmt.Errorf("value has type %T, want base64 string or bytes", value)
	}
}

func timestampMicros(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case json.Number:
		return typed.Int64()
	case time.Time:
		return typed.UTC().UnixMicro(), nil
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00"} {
			parsed, err := time.Parse(layout, typed)
			if err == nil {
				return parsed.UTC().UnixMicro(), nil
			}
		}
		return 0, fmt.Errorf("invalid TIMESTAMP %q", typed)
	default:
		return 0, fmt.Errorf("value has type %T, want epoch microseconds or timestamp", value)
	}
}

func typeError(path string, value any, expected string) error {
	return fmt.Errorf("%s has type %T, want %s", path, value, expected)
}

func frame(value string) string { return strconv.Itoa(len(value)) + ":" + value }
