package rest

// BigQuery's tabledata.list response uses nested f/v objects rather than plain
// JSON objects. The encoder is driven by canonical catalog schema so STRUCT and
// REPEATED values keep field order across replaceable warehouse adapters.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list#response-body

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	tabledatabudget "github.com/leeyh0216/go-bemu/internal/tabledata"
)

const (
	defaultTableDataResponseBytes int64 = 10_000_000
	defaultTableDataRowBytes      int64 = 100_000_000
)

type tableDataListResponse struct {
	Kind      string     `json:"kind"`
	ETag      string     `json:"etag,omitempty"`
	TotalRows string     `json:"totalRows"`
	PageToken string     `json:"pageToken,omitempty"`
	Rows      []tableRow `json:"rows,omitempty"`
}

type encodedTableDataList struct {
	metadata []byte
	rows     [][]byte
	size     int64
	etag     string
	rowCount int
}

func (response encodedTableDataList) WriteTo(writer io.Writer) (int64, error) {
	if len(response.rows) == 0 {
		return writeTableDataFragment(writer, response.metadata)
	}
	var written int64
	fragments := [][]byte{response.metadata[:len(response.metadata)-1], []byte(`,"rows":[`)}
	for _, fragment := range fragments {
		count, err := writeTableDataFragment(writer, fragment)
		written += count
		if err != nil {
			return written, err
		}
	}
	for index, row := range response.rows {
		if index > 0 {
			count, err := writeTableDataFragment(writer, []byte{','})
			written += count
			if err != nil {
				return written, err
			}
		}
		count, err := writeTableDataFragment(writer, row)
		written += count
		if err != nil {
			return written, err
		}
	}
	count, err := writeTableDataFragment(writer, []byte{']', '}'})
	return written + count, err
}

func writeTableDataFragment(writer io.Writer, fragment []byte) (int64, error) {
	var written int64
	for len(fragment) > 0 {
		count, err := writer.Write(fragment)
		written += int64(count)
		fragment = fragment[count:]
		if err != nil {
			return written, err
		}
		if count == 0 {
			return written, io.ErrNoProgress
		}
	}
	return written, nil
}

// encodeTableDataListResponse incrementally converts one row at a time and caps
// the exact f/v JSON payload. BigQuery normally trims tabledata.list near 10 MB;
// one row may cross that boundary only up to the configured hard row ceiling.
// https://cloud.google.com/bigquery/docs/paging-results#api-limits
func encodeTableDataListResponse(page ports.TableDataPage, format tableDataFormatOptions, scope string, offset int64) (encodedTableDataList, error) {
	maximumResponse := page.MaxResponseBytes
	if maximumResponse <= 0 {
		maximumResponse = defaultTableDataResponseBytes
	}
	maximumRow := page.MaxRowBytes
	if maximumRow <= 0 {
		maximumRow = defaultTableDataRowBytes
	}

	digest := newTableDataETagDigest(scope, offset, page.TotalRows)
	etag := tableDataETag(digest)
	var rowPayloads [][]byte
	var rowPayloadBytes int64
	var metadata []byte
	for rowIndex, row := range page.Rows {
		wireRow, err := encodeTableDataRow(row, page.Schema, format)
		if err != nil {
			return encodedTableDataList{}, fmt.Errorf("encode table data row %d: %w", rowIndex, err)
		}
		encodedRow, err := json.Marshal(wireRow)
		if err != nil {
			return encodedTableDataList{}, fmt.Errorf("marshal table data row %d: %w", rowIndex, err)
		}
		if int64(len(encodedRow)) > maximumRow {
			return encodedTableDataList{}, fmt.Errorf("%w: wire_bytes=%d limit_bytes=%d", tabledatabudget.ErrRowTooLarge, len(encodedRow), maximumRow)
		}
		writeTableDataETagRow(digest, encodedRow)
		candidateETag := tableDataETag(digest)
		candidateCount := len(rowPayloads) + 1
		candidateToken := tableDataContinuation(scope, offset, candidateCount, page.TotalRows)
		candidateMetadata, err := tableDataMetadataJSON(candidateETag, page.TotalRows, candidateToken)
		if err != nil {
			return encodedTableDataList{}, err
		}
		candidateBytes := tableDataPayloadSize(candidateMetadata, rowPayloadBytes+int64(len(encodedRow)), candidateCount)
		singleRowException := candidateCount == 1 && candidateBytes > maximumResponse && candidateBytes <= maximumRow
		if candidateBytes > maximumResponse && !singleRowException {
			if candidateCount == 1 {
				return encodedTableDataList{}, fmt.Errorf("%w: wire_bytes=%d limit_bytes=%d", tabledatabudget.ErrResponseTooLarge, candidateBytes, maximumResponse)
			}
			break
		}
		rowPayloads = append(rowPayloads, encodedRow)
		rowPayloadBytes += int64(len(encodedRow))
		etag = candidateETag
		metadata = candidateMetadata
		if singleRowException {
			break
		}
	}
	if metadata == nil {
		var err error
		metadata, err = tableDataMetadataJSON(etag, page.TotalRows, tableDataContinuation(scope, offset, 0, page.TotalRows))
		if err != nil {
			return encodedTableDataList{}, err
		}
	}
	payloadSize := tableDataPayloadSize(metadata, rowPayloadBytes, len(rowPayloads))
	allowedPayload := maximumResponse
	if len(rowPayloads) == 1 && payloadSize > maximumResponse {
		allowedPayload = maximumRow
	}
	if payloadSize > allowedPayload {
		return encodedTableDataList{}, fmt.Errorf("%w: wire_bytes=%d limit_bytes=%d", tabledatabudget.ErrResponseTooLarge, payloadSize, allowedPayload)
	}
	return encodedTableDataList{
		metadata: metadata, rows: rowPayloads, size: payloadSize,
		etag: etag, rowCount: len(rowPayloads),
	}, nil
}

func encodeTableDataRow(row []any, schema []domain.Field, format tableDataFormatOptions) (tableRow, error) {
	if len(row) != len(schema) {
		return tableRow{}, fmt.Errorf("row has %d values for %d schema fields", len(row), len(schema))
	}
	fields := make([]tableCell, len(schema))
	for fieldIndex, field := range schema {
		value, err := tableDataValue(field, row[fieldIndex], format)
		if err != nil {
			return tableRow{}, fmt.Errorf("field %d: %w", fieldIndex, err)
		}
		fields[fieldIndex] = tableCell{Value: value}
	}
	return tableRow{Fields: fields}, nil
}

func newTableDataETagDigest(scope string, offset, totalRows int64) hash.Hash {
	digest := sha256.New()
	_, _ = digest.Write([]byte("bqemu-tabledata-wire-page-v1\x00"))
	writeTableDataETagBytes(digest, []byte(scope))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(offset))
	_, _ = digest.Write(number[:])
	binary.BigEndian.PutUint64(number[:], uint64(totalRows))
	_, _ = digest.Write(number[:])
	return digest
}

func writeTableDataETagRow(digest hash.Hash, row []byte) {
	writeTableDataETagBytes(digest, row)
}

func writeTableDataETagBytes(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func tableDataETag(digest hash.Hash) string {
	return base64.RawURLEncoding.EncodeToString(digest.Sum(nil))
}

func tableDataContinuation(scope string, offset int64, count int, totalRows int64) string {
	next := offset + int64(count)
	if count == 0 || next >= totalRows {
		return ""
	}
	return encodeQueryPageToken("tabledata-list", scope, strconv.FormatInt(next, 10))
}

func tableDataMetadataJSON(etag string, totalRows int64, pageToken string) ([]byte, error) {
	metadata, err := json.Marshal(tableDataListResponse{
		Kind: "bigquery#tableDataList", ETag: etag,
		TotalRows: strconv.FormatInt(totalRows, 10), PageToken: pageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal table data response metadata: %w", err)
	}
	return metadata, nil
}

func tableDataPayloadSize(metadata []byte, rowBytes int64, rowCount int) int64 {
	size := int64(len(metadata))
	if rowCount > 0 {
		// Replace the metadata object's closing brace with ,"rows":[...]}.
		size += int64(len(`,"rows":[`)) + rowBytes + int64(rowCount-1) + 1
	}
	return size
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
