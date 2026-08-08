package duckdb

import (
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

func (renderer *duckDBStatementRenderer) renderType(typ queryast.Type) (string, error) {
	if typ == nil {
		return "", fmt.Errorf("%w: GoogleSQL type is missing", domain.ErrPrecondition)
	}
	visitor := &duckDBTypeVisitor{renderer: renderer}
	if err := typ.Accept(visitor); err != nil {
		return "", err
	}
	if visitor.result == "" {
		return "", fmt.Errorf("%w: type renderer produced no output", domain.ErrPrecondition)
	}
	return visitor.result, nil
}

type duckDBTypeVisitor struct {
	renderer *duckDBStatementRenderer
	result   string
}

func (visitor *duckDBTypeVisitor) VisitScalarType(typ *queryast.ScalarType) error {
	if typ.Kind() != queryast.TypeNumeric && typ.Kind() != queryast.TypeBigNumeric &&
		(typ.Precision() != nil || typ.Scale() != nil) {
		return fmt.Errorf("%w: non-decimal type has decimal parameters", domain.ErrInvalidQuery)
	}
	switch typ.Kind() {
	case queryast.TypeBool:
		visitor.result = "BOOLEAN"
	case queryast.TypeInt64:
		visitor.result = "BIGINT"
	case queryast.TypeFloat64:
		visitor.result = "DOUBLE"
	case queryast.TypeNumeric, queryast.TypeBigNumeric:
		field := domain.Field{
			Name:      "semantic_decimal",
			Type:      string(typ.Kind()),
			Precision: typ.Precision(),
			Scale:     typ.Scale(),
		}
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			return err
		}
		visitor.result = fmt.Sprintf("DECIMAL(%d,%d)", parameters.Precision, parameters.Scale)
	case queryast.TypeString:
		visitor.result = "VARCHAR"
	case queryast.TypeBytes:
		visitor.result = "BLOB"
	case queryast.TypeDate:
		visitor.result = "DATE"
	case queryast.TypeDatetime:
		visitor.result = "TIMESTAMP"
	case queryast.TypeTime:
		visitor.result = "TIME"
	case queryast.TypeTimestamp:
		visitor.result = "TIMESTAMPTZ"
	case queryast.TypeJSON:
		visitor.result = "JSON"
	case queryast.TypeGeography:
		return unsupportedDuckDBLowering("type", typ.Kind())
	default:
		return unsupportedDuckDBLowering("type", typ.Kind())
	}
	return nil
}

func (visitor *duckDBTypeVisitor) VisitArrayType(typ *queryast.ArrayType) error {
	element, err := visitor.renderer.renderType(typ.Element())
	if err != nil {
		return err
	}
	visitor.result = element + "[]"
	return nil
}

func (visitor *duckDBTypeVisitor) VisitStructType(typ *queryast.StructType) error {
	fields := typ.Fields()
	if len(fields) == 0 {
		return fmt.Errorf("%w: empty STRUCT type cannot be lowered", domain.ErrUnsupported)
	}
	rendered := make([]string, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for index, field := range fields {
		name := field.Name()
		if name == nil {
			return fmt.Errorf("%w: anonymous STRUCT field type cannot be lowered", domain.ErrUnsupported)
		}
		key := strings.ToLower(name.Value())
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: STRUCT type field names are duplicated", domain.ErrInvalidQuery)
		}
		seen[key] = struct{}{}
		fieldType, err := visitor.renderer.renderType(field.Type())
		if err != nil {
			return err
		}
		rendered[index] = quoteIdentifier(name.Value()) + " " + fieldType
	}
	visitor.result = "STRUCT(" + strings.Join(rendered, ", ") + ")"
	return nil
}
