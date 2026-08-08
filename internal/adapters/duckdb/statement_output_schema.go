package duckdb

import (
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

const (
	duckDBStatementOutputInvalidV1     = "query.googlesql.duckdb-output.invalid-v1"
	duckDBStatementOutputUnsupportedV1 = "query.googlesql.duckdb-output.unsupported-v1"
)

// duckDBStatementOutput owns the canonical client-visible schema attested by
// GoogleSQL analysis. DuckDB's result types are observations to validate
// against these fields, never the source of NUMERIC/BIGNUMERIC identity.
type duckDBStatementOutput struct {
	fields []domain.Field
}

func newDuckDBStatementOutput(statement semantic.Statement, producesRows bool) (duckDBStatementOutput, error) {
	columns := statement.OutputColumns()
	if (producesRows && len(columns) == 0) || (!producesRows && len(columns) != 0) {
		return duckDBStatementOutput{}, invalidDuckDBStatementOutput()
	}
	fields := make([]domain.Field, len(columns))
	for index, column := range columns {
		if column.Name() == "" || strings.IndexByte(column.Name(), 0) >= 0 {
			return duckDBStatementOutput{}, invalidDuckDBStatementOutput()
		}
		field, err := domainFieldFromSemanticType(column.Name(), column.Type())
		if err != nil {
			return duckDBStatementOutput{}, err
		}
		fields[index] = field
	}
	return duckDBStatementOutput{fields: domain.CloneFields(fields)}, nil
}

func (output duckDBStatementOutput) schemaHints() []domain.Field {
	return domain.CloneFields(output.fields)
}

func domainFieldFromSemanticType(name string, typ semantic.Type) (domain.Field, error) {
	field := domain.Field{Name: name, Mode: "NULLABLE"}
	switch typ.Kind() {
	case semantic.TypeBool:
		field.Type = "BOOLEAN"
	case semantic.TypeInt64:
		field.Type = "INTEGER"
	case semantic.TypeFloat64:
		field.Type = "FLOAT"
	case semantic.TypeNumeric, semantic.TypeBigNumeric:
		field.Type = string(typ.Kind())
		if precision, present := typ.Precision(); present {
			field.Precision = domain.CloneOptionalInt64(&precision)
		}
		if scale, present := typ.Scale(); present {
			field.Scale = domain.CloneOptionalInt64(&scale)
		}
		field.RoundingMode = typ.RoundingMode()
	case semantic.TypeString:
		field.Type = "STRING"
	case semantic.TypeBytes:
		field.Type = "BYTES"
	case semantic.TypeDate:
		field.Type = "DATE"
	case semantic.TypeDatetime:
		field.Type = "DATETIME"
	case semantic.TypeTime:
		field.Type = "TIME"
	case semantic.TypeTimestamp:
		field.Type = "TIMESTAMP"
	case semantic.TypeJSON:
		field.Type = "JSON"
	case semantic.TypeArray:
		element, ok := typ.Element()
		if !ok || element.Kind() == semantic.TypeArray {
			return domain.Field{}, unsupportedDuckDBStatementOutput()
		}
		var err error
		field, err = domainFieldFromSemanticType(name, element)
		if err != nil {
			return domain.Field{}, err
		}
		field.Mode = "REPEATED"
	case semantic.TypeStruct:
		semanticFields := typ.Fields()
		if len(semanticFields) == 0 {
			return domain.Field{}, invalidDuckDBStatementOutput()
		}
		field.Type = "RECORD"
		field.Fields = make([]domain.Field, len(semanticFields))
		for index, semanticField := range semanticFields {
			if semanticField.Name() == "" || strings.IndexByte(semanticField.Name(), 0) >= 0 {
				return domain.Field{}, invalidDuckDBStatementOutput()
			}
			child, err := domainFieldFromSemanticType(semanticField.Name(), semanticField.Type())
			if err != nil {
				return domain.Field{}, err
			}
			field.Fields[index] = child
		}
	default:
		return domain.Field{}, unsupportedDuckDBStatementOutput()
	}
	return field, nil
}

func canonicalizeDuckDBStatementOutput(
	observed []domain.Field,
	output duckDBStatementOutput,
) ([]domain.Field, error) {
	expected := output.schemaHints()
	if len(observed) != len(expected) {
		return nil, fmt.Errorf("%w: observed=%v expected=%v", invalidDuckDBStatementOutput(), observed, expected)
	}
	for index := range expected {
		if !duckDBStatementOutputNameMatches(index, observed[index].Name, expected[index].Name) ||
			!duckDBStatementOutputTypeMatches(observed[index], expected[index]) {
			return nil, fmt.Errorf("%w: column=%d observed=%v expected=%v", invalidDuckDBStatementOutput(), index, observed[index], expected[index])
		}
	}
	return expected, nil
}

func duckDBStatementOutputNameMatches(index int, observed, expected string) bool {
	if strings.EqualFold(observed, expected) {
		return true
	}
	// Official analysis canonicalizes anonymous output names to fN_. DuckDB
	// exposes expression text for the same column, which is intentionally not
	// a portable BigQuery field name. Only this closed ordinal form may differ.
	return expected == fmt.Sprintf("f%d_", index) && !portableQueryFieldName(observed)
}

func duckDBStatementOutputTypeMatches(observed, expected domain.Field) bool {
	if !strings.EqualFold(observed.Mode, expected.Mode) ||
		canonicalQueryDestinationType(observed.Type) != canonicalQueryDestinationType(expected.Type) ||
		len(observed.Fields) != len(expected.Fields) {
		return false
	}
	observedType := canonicalQueryDestinationType(observed.Type)
	if observedType == "NUMERIC" || observedType == "BIGNUMERIC" {
		observedParameters, observedErr := observed.EffectiveDecimalParameters()
		expectedParameters, expectedErr := expected.EffectiveDecimalParameters()
		if observedErr != nil || expectedErr != nil || observedParameters != expectedParameters {
			return false
		}
	}
	for index := range expected.Fields {
		if !strings.EqualFold(observed.Fields[index].Name, expected.Fields[index].Name) ||
			!duckDBStatementOutputTypeMatches(observed.Fields[index], expected.Fields[index]) {
			return false
		}
	}
	return true
}

func invalidDuckDBStatementOutput() error {
	return fmt.Errorf("%w: code=%s analyzed output schema does not match the DuckDB result", domain.ErrPrecondition, duckDBStatementOutputInvalidV1)
}

func unsupportedDuckDBStatementOutput() error {
	return fmt.Errorf("%w: code=%s analyzed output schema is not supported", domain.ErrUnsupported, duckDBStatementOutputUnsupportedV1)
}
