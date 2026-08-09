package application

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

// TableDataInsertError identifies one rejected insertAll row without exposing
// request values. REST translates it to the official insertErrors shape.
type TableDataInsertError struct {
	Index    int
	Reason   string
	Location string
	Err      error
}

func (e *TableDataInsertError) Error() string {
	return fmt.Sprintf("%s at row %d field %s", e.Reason, e.Index, e.Location)
}
func (e *TableDataInsertError) Unwrap() error { return e.Err }

// InsertTableData resolves the live canonical schema, validates every input
// row before opening a physical transaction, then delegates one atomic batch.
// This intentionally implements insertAll's all-or-nothing local profile;
// callers receive row-indexed errors for malformed payloads.
func (s *CatalogService) InsertTableData(ctx context.Context, projectID, datasetID, tableID string, rows []ports.TableDataJSONRow) error {
	if s.tableDataWriter == nil {
		return fmt.Errorf("table data writer is not configured")
	}
	if len(rows) == 0 {
		return nil
	}
	operationCtx, cancel := context.WithTimeout(ctx, s.tableDataOperationTimeout)
	defer cancel()
	if err := s.resourceMutationMu.LockContext(operationCtx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()
	table, err := s.getTableLocked(operationCtx, projectID, datasetID, tableID)
	if err != nil {
		return err
	}
	reference := domain.TableReference{ProjectID: projectID, DatasetID: datasetID, TableID: tableID}
	insertIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.InsertID != "" {
			insertIDs = append(insertIDs, row.InsertID)
		}
	}
	existing := map[string]bool{}
	if s.tableDataInsertIDLedger != nil && len(insertIDs) > 0 {
		existing, err = s.tableDataInsertIDLedger.ExistingTableDataInsertIDs(operationCtx, reference, insertIDs)
		if err != nil {
			return err
		}
	}
	converted := make([][]any, 0, len(rows))
	acceptedIDs := make([]string, 0, len(insertIDs))
	seen := make(map[string]bool, len(insertIDs))
	for index, row := range rows {
		if row.InsertID != "" && (existing[row.InsertID] || seen[row.InsertID]) {
			continue
		}
		values, convertErr := tableDataInsertValues(table.Schema, row.JSON)
		if convertErr != nil {
			return &TableDataInsertError{Index: index, Reason: "invalid", Location: tableDataInsertLocation(convertErr), Err: fmt.Errorf("%w: %v", domain.ErrInvalid, convertErr)}
		}
		converted = append(converted, values)
		if row.InsertID != "" {
			seen[row.InsertID] = true
			acceptedIDs = append(acceptedIDs, row.InsertID)
		}
	}
	if err := s.tableDataWriter.InsertTableData(operationCtx, ports.TableDataWriteRequest{
		Reference: reference,
		Schema:    copyFields(table.Schema), Rows: converted,
	}); err != nil {
		return err
	}
	if s.tableDataInsertIDLedger != nil && len(acceptedIDs) > 0 {
		if err := s.tableDataInsertIDLedger.RecordTableDataInsertIDs(operationCtx, reference, acceptedIDs); err != nil {
			return err
		}
	}
	return nil
}

func tableDataInsertLocation(err error) string {
	// Conversion errors always start with a canonical field path. Do not emit
	// value text into protocol errors or logs.
	if index := strings.IndexByte(err.Error(), ':'); index > 0 {
		return err.Error()[:index]
	}
	return "json"
}

func tableDataInsertValues(schema []domain.Field, source map[string]json.RawMessage) ([]any, error) {
	byName := make(map[string]json.RawMessage, len(source))
	for name, value := range source {
		byName[strings.ToLower(name)] = value
	}
	values := make([]any, len(schema))
	for index, field := range schema {
		raw, found := byName[strings.ToLower(field.Name)]
		if !found {
			if strings.EqualFold(field.Mode, "REQUIRED") {
				return nil, fmt.Errorf("%s: required field is missing", field.Name)
			}
			values[index] = nil
			continue
		}
		value, err := tableDataInsertValue(field, raw, field.Name)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	for name := range byName {
		known := false
		for _, field := range schema {
			if strings.EqualFold(name, field.Name) {
				known = true
				break
			}
		}
		if !known {
			return nil, fmt.Errorf("%s: field is not in the table schema", name)
		}
	}
	return values, nil
}

func tableDataInsertValue(field domain.Field, raw json.RawMessage, path string) (any, error) {
	if string(raw) == "null" {
		if strings.EqualFold(field.Mode, "REQUIRED") {
			return nil, fmt.Errorf("%s: required field is NULL", path)
		}
		if strings.EqualFold(field.Mode, "REPEATED") {
			return "[]", nil
		}
		return nil, nil
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, fmt.Errorf("%s: repeated field must be an array", path)
		}
		element := field
		element.Mode = "REQUIRED"
		for index, item := range items {
			if _, err := tableDataInsertValue(element, item, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return nil, err
			}
		}
		return string(raw), nil
	}
	typeName := strings.ToUpper(field.Type)
	switch typeName {
	case "RECORD", "STRUCT":
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, fmt.Errorf("%s: STRUCT must be an object", path)
		}
		// Recursively validate every nested field. The original JSON is retained
		// because DuckDB's JSON-to-STRUCT cast supplies the typed binding.
		if _, err := tableDataInsertValues(field.Fields, object); err != nil {
			return nil, fmt.Errorf("%s.%v", path, err)
		}
		return string(raw), nil
	case "STRING", "GEOGRAPHY":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("%s: STRING must be a JSON string", path)
		}
		return value, nil
	case "BYTES":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("%s: BYTES must be base64", path)
		}
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid base64", path)
		}
		return decoded, nil
	case "BOOL", "BOOLEAN":
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("%s: BOOL must be boolean", path)
		}
		return value, nil
	case "INT64", "INTEGER":
		literal, err := tableDataInsertNumber(raw, path)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseInt(literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid INT64", path)
		}
		return value, nil
	case "FLOAT64", "FLOAT":
		literal, err := tableDataInsertNumber(raw, path)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseFloat(literal, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("%s: invalid FLOAT64", path)
		}
		return value, nil
	case "NUMERIC", "BIGNUMERIC":
		literal, err := tableDataInsertStringOrNumber(raw, path)
		if err != nil {
			return nil, err
		}
		if _, err := strconv.ParseFloat(literal, 64); err != nil {
			return nil, fmt.Errorf("%s: invalid decimal", path)
		}
		return literal, nil
	case "JSON":
		var ignored any
		if err := json.Unmarshal(raw, &ignored); err != nil {
			return nil, fmt.Errorf("%s: invalid JSON", path)
		}
		return string(raw), nil
	case "DATE":
		literal, err := tableDataInsertString(raw, path)
		if err != nil {
			return nil, err
		}
		if _, err := time.Parse("2006-01-02", literal); err != nil {
			return nil, fmt.Errorf("%s: invalid DATE", path)
		}
		return literal, nil
	case "TIME":
		literal, err := tableDataInsertString(raw, path)
		if err != nil {
			return nil, err
		}
		if _, err := time.Parse("15:04:05.999999", literal); err != nil {
			return nil, fmt.Errorf("%s: invalid TIME", path)
		}
		return literal, nil
	case "DATETIME":
		literal, err := tableDataInsertString(raw, path)
		if err != nil {
			return nil, err
		}
		if _, err := time.Parse("2006-01-02 15:04:05.999999", literal); err != nil {
			return nil, fmt.Errorf("%s: invalid DATETIME", path)
		}
		return literal, nil
	case "TIMESTAMP":
		literal, err := tableDataInsertString(raw, path)
		if err != nil {
			return nil, err
		}
		if _, err := time.Parse(time.RFC3339Nano, literal); err != nil {
			return nil, fmt.Errorf("%s: invalid TIMESTAMP", path)
		}
		return literal, nil
	default:
		return nil, fmt.Errorf("%s: unsupported field type %s", path, field.Type)
	}
}

func tableDataInsertString(raw json.RawMessage, path string) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s: value must be a JSON string", path)
	}
	return value, nil
}
func tableDataInsertNumber(raw json.RawMessage, path string) (string, error) {
	return tableDataInsertStringOrNumber(raw, path)
}
func tableDataInsertStringOrNumber(raw json.RawMessage, path string) (string, error) {
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err == nil {
		return number.String(), nil
	}
	return tableDataInsertString(raw, path)
}
