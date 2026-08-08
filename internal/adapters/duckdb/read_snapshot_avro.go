package duckdb

// Avro responses contain concatenated binary datums only. The object container
// magic/header, sync markers, and block framing are not part of AvroRows.
//
// Protocol sources:
//   - BigQuery to Avro mappings and annotations: https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details
//   - Avro binary encoding: https://avro.apache.org/docs/1.11.4/specification/#binary-encoding

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
)

type avroRecordSchema struct {
	Type      string            `json:"type"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace,omitempty"`
	Fields    []avroFieldSchema `json:"fields"`
}

type avroFieldSchema struct {
	Name string `json:"name"`
	Type any    `json:"type"`
}

func buildAvroReferenceSchema(fields []catalogdomain.Field) ([]byte, error) {
	root := avroRecordSchema{Type: "record", Name: "root", Namespace: "bqemu.storage", Fields: make([]avroFieldSchema, len(fields))}
	for index, field := range fields {
		converted, err := avroFieldType(field, field.Name)
		if err != nil {
			return nil, err
		}
		root.Fields[index] = avroFieldSchema{Name: field.Name, Type: converted}
	}
	serialized, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("serialize Avro reference schema: %w", err)
	}
	return serialized, nil
}

func avroFieldType(field catalogdomain.Field, path string) (any, error) {
	if err := field.Validate(); err != nil {
		return nil, fmt.Errorf("map BigQuery field at %s: %w", path, err)
	}
	base, err := avroBaseType(field, path)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		return map[string]any{"type": "array", "items": base}, nil
	}
	if !strings.EqualFold(field.Mode, "REQUIRED") {
		return []any{"null", base}, nil
	}
	return base, nil
}

func avroBaseType(field catalogdomain.Field, path string) (any, error) {
	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		return "boolean", nil
	case "INT64", "INTEGER":
		return "long", nil
	case "FLOAT64", "FLOAT":
		return "double", nil
	case "BYTES":
		return "bytes", nil
	case "STRING":
		return "string", nil
	case "DATE":
		return map[string]any{"type": "int", "logicalType": "date"}, nil
	case "DATETIME":
		return map[string]any{"type": "string", "logicalType": "datetime"}, nil
	case "TIMESTAMP":
		return map[string]any{"type": "long", "logicalType": "timestamp-micros"}, nil
	case "TIME":
		return map[string]any{"type": "long", "logicalType": "time-micros"}, nil
	case "NUMERIC":
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			return nil, fmt.Errorf("map BigQuery field at %s: %w", path, err)
		}
		return map[string]any{"type": "bytes", "logicalType": "decimal", "precision": parameters.Precision, "scale": parameters.Scale}, nil
	case "BIGNUMERIC":
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			return nil, fmt.Errorf("map BigQuery field at %s: %w", path, err)
		}
		return map[string]any{"type": "bytes", "logicalType": "decimal", "precision": parameters.Precision, "scale": parameters.Scale}, nil
	case "JSON":
		return map[string]any{"type": "string", "sqlType": "JSON"}, nil
	case "RECORD", "STRUCT":
		record := avroRecordSchema{
			Type: "record", Name: avroRecordName(path),
			Fields: make([]avroFieldSchema, len(field.Fields)),
		}
		for index, child := range field.Fields {
			converted, err := avroFieldType(child, path+"."+child.Name)
			if err != nil {
				return nil, err
			}
			record.Fields[index] = avroFieldSchema{Name: child.Name, Type: converted}
		}
		return record, nil
	default:
		return nil, fmt.Errorf("map BigQuery field %q at %s to Avro: unsupported type", field.Type, path)
	}
}

func avroRecordName(path string) string {
	digest := sha256.Sum256([]byte(path))
	return "record_" + hex.EncodeToString(digest[:6])
}

func encodeAvroRows(fields []catalogdomain.Field, rows [][]snapshotValue) ([]byte, error) {
	var output bytes.Buffer
	for rowIndex, row := range rows {
		if len(row) != len(fields) {
			return nil, fmt.Errorf("Avro row %d has %d values, want %d", rowIndex, len(row), len(fields))
		}
		for fieldIndex, field := range fields {
			if err := appendAvroValue(&output, field, row[fieldIndex]); err != nil {
				return nil, fmt.Errorf("encode Avro row %d field %q: %w", rowIndex, field.Name, err)
			}
		}
	}
	return output.Bytes(), nil
}

func appendAvroValue(output *bytes.Buffer, field catalogdomain.Field, value snapshotValue) error {
	if strings.EqualFold(field.Mode, "REPEATED") {
		if value.Null {
			return fmt.Errorf("REPEATED field cannot be NULL")
		}
		// An Avro array is a sequence of blocks terminated by zero. The empty
		// array is therefore one zero block count, not two zero values.
		// Source: https://avro.apache.org/docs/1.11.4/specification/#arrays
		if len(value.Children) == 0 {
			appendAvroLong(output, 0)
			return nil
		}
		appendAvroLong(output, int64(len(value.Children)))
		element := field
		element.Mode = "REQUIRED"
		for _, child := range value.Children {
			if err := appendAvroValue(output, element, child); err != nil {
				return err
			}
		}
		appendAvroLong(output, 0)
		return nil
	}
	if !strings.EqualFold(field.Mode, "REQUIRED") {
		if value.Null {
			appendAvroLong(output, 0)
			return nil
		}
		appendAvroLong(output, 1)
	} else if value.Null {
		return fmt.Errorf("REQUIRED field cannot be NULL")
	}

	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		if value.Bool {
			output.WriteByte(1)
		} else {
			output.WriteByte(0)
		}
	case "INT64", "INTEGER", "TIMESTAMP", "TIME":
		appendAvroLong(output, value.Int)
	case "FLOAT64", "FLOAT":
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value.Float))
		output.Write(encoded[:])
	case "STRING", "JSON":
		appendAvroBytes(output, []byte(value.Text))
	case "BYTES":
		appendAvroBytes(output, value.Bytes)
	case "DATE":
		appendAvroLong(output, value.Int)
	case "DATETIME":
		appendAvroBytes(output, []byte(value.Text))
	case "NUMERIC":
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			return err
		}
		decimal, err := avroDecimalBytes(value.Text, parameters.Precision, parameters.Scale)
		if err != nil {
			return err
		}
		appendAvroBytes(output, decimal)
	case "BIGNUMERIC":
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			return err
		}
		decimal, err := avroDecimalBytes(value.Text, parameters.Precision, parameters.Scale)
		if err != nil {
			return err
		}
		appendAvroBytes(output, decimal)
	case "RECORD", "STRUCT":
		if len(value.Children) != len(field.Fields) {
			return fmt.Errorf("STRUCT has %d children, want %d", len(value.Children), len(field.Fields))
		}
		for index, child := range value.Children {
			if err := appendAvroValue(output, field.Fields[index], child); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported Avro field type %q", field.Type)
	}
	return nil
}

func appendAvroLong(output *bytes.Buffer, value int64) {
	zigzag := uint64(value<<1) ^ uint64(value>>63)
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], zigzag)
	output.Write(encoded[:length])
}

func appendAvroBytes(output *bytes.Buffer, value []byte) {
	appendAvroLong(output, int64(len(value)))
	output.Write(value)
}

func avroDecimalBytes(input string, precision, scale int64) ([]byte, error) {
	rational, ok := new(big.Rat).SetString(input)
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", input)
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil)
	rational.Mul(rational, new(big.Rat).SetInt(factor))
	if rational.Denom().Cmp(big.NewInt(1)) != 0 {
		return nil, fmt.Errorf("decimal %q exceeds scale %d", input, scale)
	}
	digits := len(new(big.Int).Abs(rational.Num()).String())
	if rational.Num().Sign() == 0 {
		digits = 1
	}
	if int64(digits) > precision {
		return nil, fmt.Errorf("decimal %q exceeds precision %d", input, precision)
	}
	return signedBigEndian(rational.Num()), nil
}

func signedBigEndian(value *big.Int) []byte {
	if value.Sign() == 0 {
		return []byte{0}
	}
	if value.Sign() > 0 {
		result := value.Bytes()
		if result[0]&0x80 != 0 {
			result = append([]byte{0}, result...)
		}
		return result
	}
	bits := value.BitLen() + 1
	length := (bits + 7) / 8
	modulus := new(big.Int).Lsh(big.NewInt(1), uint(length*8))
	twosComplement := new(big.Int).Add(modulus, value)
	result := twosComplement.FillBytes(make([]byte, length))
	for len(result) > 1 && result[0] == 0xff && result[1]&0x80 != 0 {
		result = result[1:]
	}
	if result[0]&0x80 == 0 {
		result = append([]byte{0xff}, result...)
	}
	return result
}
