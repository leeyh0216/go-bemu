package tabledata

// tabledata.list pages are bounded twice: this package measures the canonical
// adapter representation without materializing another whole-page string, and
// the REST transport separately bounds the exact encoded JSON response.
// BigQuery normally paginates around 10 MB and documents a larger single-row
// exception; BQEMU intentionally exposes hard local limits for deterministic
// memory use.
// https://cloud.google.com/bigquery/docs/paging-results#api-limits

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"reflect"
	"sort"
	"time"
)

const canonicalChunkBytes = 32 << 10

var (
	ErrRowTooLarge      = errors.New("table data row exceeds configured byte limit")
	ErrResponseTooLarge = errors.New("table data response exceeds configured byte limit")
)

// Metrics are safe to log: the digest is derived from type-tagged values and
// the byte count is the canonical logical representation, never a raw row.
type Metrics struct {
	Rows   int
	Bytes  int64
	Digest string
}

// Accumulator incrementally frames each row as (canonical byte count, digest).
// This makes the page digest deterministic without retaining or formatting a
// second copy of the complete page.
type Accumulator struct {
	maximum int64
	rows    int
	bytes   int64
	digest  hash.Hash
}

func NewAccumulator(maximum int64) *Accumulator {
	accumulator := &Accumulator{maximum: maximum, digest: sha256.New()}
	_, _ = accumulator.digest.Write([]byte("bqemu-tabledata-canonical-page-v1\x00"))
	return accumulator
}

// Add returns false without an error when another valid row would cross the
// page budget after at least one row. Callers can then emit a continuation page.
// An oversized first row is an error because returning an empty continuation
// would never advance the cursor.
func (accumulator *Accumulator) Add(row []any, maximumRowBytes int64) (bool, error) {
	rowBytes, rowDigest, err := measureRow(row, maximumRowBytes)
	if err != nil {
		return false, err
	}
	charge := rowBytes + 8
	if charge < rowBytes || (accumulator.maximum > 0 && charge > accumulator.maximum-accumulator.bytes) {
		// BigQuery documents a 10 MB normal page target and a larger one-row
		// exception. maximumRowBytes remains the hard ceiling for that exception.
		if accumulator.rows == 0 && (maximumRowBytes <= 0 || rowBytes <= maximumRowBytes) {
			var frame [8]byte
			binary.BigEndian.PutUint64(frame[:], uint64(rowBytes))
			_, _ = accumulator.digest.Write(frame[:])
			_, _ = accumulator.digest.Write(rowDigest[:])
			accumulator.rows++
			accumulator.bytes += charge
			return true, nil
		}
		if accumulator.rows == 0 {
			return false, fmt.Errorf("%w: canonical_bytes=%d limit_bytes=%d", ErrResponseTooLarge, charge, accumulator.maximum)
		}
		return false, nil
	}
	var frame [8]byte
	binary.BigEndian.PutUint64(frame[:], uint64(rowBytes))
	_, _ = accumulator.digest.Write(frame[:])
	_, _ = accumulator.digest.Write(rowDigest[:])
	accumulator.rows++
	accumulator.bytes += charge
	return true, nil
}

func (accumulator *Accumulator) Metrics() Metrics {
	return Metrics{
		Rows: accumulator.rows, Bytes: accumulator.bytes,
		Digest: "sha256:" + hex.EncodeToString(accumulator.digest.Sum(nil)),
	}
}

func measureRow(row []any, maximum int64) (int64, [sha256.Size]byte, error) {
	writer := &canonicalWriter{maximum: maximum, digest: sha256.New()}
	if err := writer.writeValue(reflect.ValueOf(row)); err != nil {
		if errors.Is(err, errCanonicalLimit) {
			return 0, [sha256.Size]byte{}, fmt.Errorf("%w: limit_bytes=%d", ErrRowTooLarge, maximum)
		}
		return 0, [sha256.Size]byte{}, fmt.Errorf("measure canonical table data row: %w", err)
	}
	var result [sha256.Size]byte
	copy(result[:], writer.digest.Sum(nil))
	return writer.bytes, result, nil
}

var errCanonicalLimit = errors.New("canonical byte limit exceeded")

type canonicalWriter struct {
	maximum int64
	bytes   int64
	digest  hash.Hash
}

func (writer *canonicalWriter) writeValue(value reflect.Value) error {
	for value.IsValid() && (value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer) {
		if value.IsNil() {
			return writer.writeTag('n')
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return writer.writeTag('n')
	}
	if value.Type() == reflect.TypeOf(time.Time{}) {
		instant := value.Interface().(time.Time).UTC()
		if err := writer.writeTag('t'); err != nil {
			return err
		}
		return writer.writeString(instant.Format(time.RFC3339Nano))
	}
	switch value.Kind() {
	case reflect.Bool:
		if value.Bool() {
			return writer.write([]byte{'b', 1})
		}
		return writer.write([]byte{'b', 0})
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return writer.writeNumber('i', uint64(value.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return writer.writeNumber('u', value.Uint())
	case reflect.Float32, reflect.Float64:
		return writer.writeNumber('f', math.Float64bits(value.Convert(reflect.TypeOf(float64(0))).Float()))
	case reflect.String:
		if err := writer.writeTag('s'); err != nil {
			return err
		}
		return writer.writeString(value.String())
	case reflect.Slice:
		if value.IsNil() {
			return writer.writeTag('n')
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			if err := writer.writeTag('x'); err != nil {
				return err
			}
			return writer.writeBytes(value.Bytes())
		}
		return writer.writeSequence('a', value)
	case reflect.Array:
		return writer.writeSequence('a', value)
	case reflect.Map:
		return writer.writeMap(value)
	default:
		return fmt.Errorf("unsupported canonical value type %s", value.Type())
	}
}

func (writer *canonicalWriter) writeSequence(tag byte, value reflect.Value) error {
	if err := writer.writeNumber(tag, uint64(value.Len())); err != nil {
		return err
	}
	for index := 0; index < value.Len(); index++ {
		if err := writer.writeValue(value.Index(index)); err != nil {
			return err
		}
	}
	return nil
}

func (writer *canonicalWriter) writeMap(value reflect.Value) error {
	if value.Type().Key().Kind() != reflect.String {
		return fmt.Errorf("unsupported canonical map key type %s", value.Type().Key())
	}
	if value.IsNil() {
		return writer.writeTag('n')
	}
	if err := writer.writeNumber('m', uint64(value.Len())); err != nil {
		return err
	}
	keys := make([]string, 0, value.Len())
	iterator := value.MapRange()
	for iterator.Next() {
		keys = append(keys, iterator.Key().String())
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := writer.writeString(key); err != nil {
			return err
		}
		if err := writer.writeValue(value.MapIndex(reflect.ValueOf(key).Convert(value.Type().Key()))); err != nil {
			return err
		}
	}
	return nil
}

func (writer *canonicalWriter) writeTag(tag byte) error {
	return writer.write([]byte{tag})
}

func (writer *canonicalWriter) writeNumber(tag byte, number uint64) error {
	var encoded [9]byte
	encoded[0] = tag
	binary.BigEndian.PutUint64(encoded[1:], number)
	return writer.write(encoded[:])
}

func (writer *canonicalWriter) writeString(value string) error {
	if err := writer.writeNumber('l', uint64(len(value))); err != nil {
		return err
	}
	for len(value) > 0 {
		chunk := len(value)
		if chunk > canonicalChunkBytes {
			chunk = canonicalChunkBytes
		}
		if err := writer.write([]byte(value[:chunk])); err != nil {
			return err
		}
		value = value[chunk:]
	}
	return nil
}

func (writer *canonicalWriter) writeBytes(value []byte) error {
	if err := writer.writeNumber('l', uint64(len(value))); err != nil {
		return err
	}
	for len(value) > 0 {
		chunk := len(value)
		if chunk > canonicalChunkBytes {
			chunk = canonicalChunkBytes
		}
		if err := writer.write(value[:chunk]); err != nil {
			return err
		}
		value = value[chunk:]
	}
	return nil
}

func (writer *canonicalWriter) write(value []byte) error {
	length := int64(len(value))
	if writer.maximum > 0 && length > writer.maximum-writer.bytes {
		return errCanonicalLimit
	}
	_, _ = writer.digest.Write(value)
	writer.bytes += length
	return nil
}
