package duckdb

// Storage Read row_restriction reaches this adapter only as the immutable AST
// produced by the official GoogleSQL parser boundary. This visitor binds every
// identifier to canonical table metadata and lowers the supported predicate
// subset to parameterized DuckDB SQL.

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

type rowRestrictionValueKind uint8

const (
	rowRestrictionPredicate rowRestrictionValueKind = iota + 1
	rowRestrictionColumn
	rowRestrictionLiteral
	rowRestrictionNull
)

type rowRestrictionLiteralKind uint8

const (
	rowRestrictionBoolean rowRestrictionLiteralKind = iota + 1
	rowRestrictionInteger
	rowRestrictionFloat
	rowRestrictionDecimal
	rowRestrictionString
	rowRestrictionTemporal
)

type compiledRowRestrictionValue struct {
	kind    rowRestrictionValueKind
	sql     string
	literal rowRestrictionLiteralValue
}

type rowRestrictionLiteralValue struct {
	kind        rowRestrictionLiteralKind
	boolean     bool
	canonical   string
	float       float64
	decimalType queryast.TypeKind
	temporal    queryast.TypeKind
	text        string
}

type rowRestrictionCompiler struct {
	schema    []catalogdomain.Field
	args      []any
	preflight bool
}

func validateRowRestrictionExpression(expression queryast.Expression) error {
	if expression == nil {
		return nil
	}
	compiler := &rowRestrictionCompiler{preflight: true}
	result, err := compiler.render(expression)
	if err != nil {
		return err
	}
	if result.kind != rowRestrictionPredicate || result.sql == "" {
		return invalidRowRestriction()
	}
	return nil
}

func compileRowRestriction(expression queryast.Expression, schema []catalogdomain.Field) (string, []any, error) {
	if expression == nil {
		return "", nil, nil
	}
	compiler := &rowRestrictionCompiler{schema: schema}
	result, err := compiler.render(expression)
	if err != nil {
		return "", nil, err
	}
	if result.kind != rowRestrictionPredicate || result.sql == "" {
		return "", nil, invalidRowRestriction()
	}
	return result.sql, compiler.args, nil
}

func (compiler *rowRestrictionCompiler) render(expression queryast.Expression) (compiledRowRestrictionValue, error) {
	if expression == nil {
		return compiledRowRestrictionValue{}, invalidRowRestriction()
	}
	visitor := &duckDBRowRestrictionVisitor{compiler: compiler}
	if err := expression.Accept(visitor); err != nil {
		return compiledRowRestrictionValue{}, err
	}
	if visitor.result.kind == 0 {
		return compiledRowRestrictionValue{}, invalidRowRestriction()
	}
	return visitor.result, nil
}

func (compiler *rowRestrictionCompiler) bindIdentifier(path queryast.IdentifierPath) (compiledRowRestrictionValue, error) {
	segments := path.Segments()
	if len(segments) == 0 {
		return compiledRowRestrictionValue{}, invalidRowRestriction()
	}
	if compiler.preflight {
		return compiledRowRestrictionValue{
			kind: rowRestrictionColumn, sql: quoteIdentifier("preflight"),
		}, nil
	}
	fields := compiler.schema
	canonical := make([]string, 0, len(segments))
	for index, segment := range segments {
		found := false
		for _, field := range fields {
			if !strings.EqualFold(field.Name, segment) {
				continue
			}
			if strings.EqualFold(field.Mode, "REPEATED") {
				return compiledRowRestrictionValue{}, invalidRowRestriction()
			}
			canonical = append(canonical, field.Name)
			if index != len(segments)-1 {
				if !strings.EqualFold(field.Type, "RECORD") && !strings.EqualFold(field.Type, "STRUCT") {
					return compiledRowRestrictionValue{}, invalidRowRestriction()
				}
				fields = field.Fields
			}
			found = true
			break
		}
		if !found {
			return compiledRowRestrictionValue{}, invalidRowRestriction()
		}
	}
	quoted := make([]string, len(canonical))
	for index, segment := range canonical {
		quoted[index] = quoteIdentifier(segment)
	}
	return compiledRowRestrictionValue{kind: rowRestrictionColumn, sql: strings.Join(quoted, ".")}, nil
}

func (compiler *rowRestrictionCompiler) bindLiteral(literal rowRestrictionLiteralValue) (string, error) {
	switch literal.kind {
	case rowRestrictionBoolean:
		if !compiler.preflight {
			compiler.args = append(compiler.args, literal.boolean)
		}
		return "?", nil
	case rowRestrictionInteger:
		value, err := strconv.ParseInt(literal.canonical, 10, 64)
		if err != nil {
			return "", invalidRowRestriction()
		}
		if !compiler.preflight {
			compiler.args = append(compiler.args, value)
		}
		return "?", nil
	case rowRestrictionFloat:
		if math.IsNaN(literal.float) || math.IsInf(literal.float, 0) {
			return "", invalidRowRestriction()
		}
		if !compiler.preflight {
			compiler.args = append(compiler.args, literal.float)
		}
		return "?", nil
	case rowRestrictionDecimal:
		field := catalogdomain.Field{Type: string(literal.decimalType)}
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			return "", unsupportedRowRestriction()
		}
		if !compiler.preflight {
			compiler.args = append(compiler.args, literal.canonical)
		}
		return fmt.Sprintf("CAST(? AS DECIMAL(%d,%d))", parameters.Precision, parameters.Scale), nil
	case rowRestrictionString:
		if !compiler.preflight {
			compiler.args = append(compiler.args, literal.text)
		}
		return "?", nil
	case rowRestrictionTemporal:
		physicalType := ""
		switch literal.temporal {
		case queryast.TypeDate:
			physicalType = "DATE"
		case queryast.TypeDatetime:
			physicalType = "TIMESTAMP"
		case queryast.TypeTime:
			physicalType = "TIME"
		case queryast.TypeTimestamp:
			physicalType = "TIMESTAMPTZ"
		default:
			return "", unsupportedRowRestriction()
		}
		if !compiler.preflight {
			compiler.args = append(compiler.args, literal.text)
		}
		return "CAST(? AS " + physicalType + ")", nil
	default:
		return "", invalidRowRestriction()
	}
}

type duckDBRowRestrictionVisitor struct {
	compiler *rowRestrictionCompiler
	result   compiledRowRestrictionValue
}

var _ queryast.ExpressionVisitor = (*duckDBRowRestrictionVisitor)(nil)

func (visitor *duckDBRowRestrictionVisitor) VisitIdentifierExpression(expression *queryast.IdentifierExpression) error {
	result, err := visitor.compiler.bindIdentifier(expression.Path())
	visitor.result = result
	return err
}

func (visitor *duckDBRowRestrictionVisitor) VisitStarExpression(*queryast.StarExpression) error {
	return unsupportedRowRestriction()
}

func (visitor *duckDBRowRestrictionVisitor) VisitNullLiteral(*queryast.NullLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionNull}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitBooleanLiteral(literal *queryast.BooleanLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionBoolean, boolean: literal.Value(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitIntegerLiteral(literal *queryast.IntegerLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionInteger, canonical: literal.CanonicalValue(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitFloatLiteral(literal *queryast.FloatLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionFloat, float: literal.Value(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitDecimalLiteral(literal *queryast.DecimalLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionDecimal, canonical: literal.CanonicalValue(), decimalType: literal.Type(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitStringLiteral(literal *queryast.StringLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionString, text: literal.Value(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitTemporalLiteral(literal *queryast.TemporalLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionTemporal, temporal: literal.Type(), text: literal.Value(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitArrayLiteral(*queryast.ArrayLiteral) error {
	return unsupportedRowRestriction()
}

func (visitor *duckDBRowRestrictionVisitor) VisitStructLiteral(*queryast.StructLiteral) error {
	return unsupportedRowRestriction()
}

func (visitor *duckDBRowRestrictionVisitor) VisitFunctionCall(*queryast.FunctionCall) error {
	return unsupportedRowRestriction()
}

func (visitor *duckDBRowRestrictionVisitor) VisitUnaryExpression(expression *queryast.UnaryExpression) error {
	value, err := visitor.compiler.render(expression.Value())
	if err != nil {
		return err
	}
	switch strings.ToUpper(strings.TrimSpace(string(expression.Operator()))) {
	case "NOT":
		if value.kind != rowRestrictionPredicate {
			return invalidRowRestriction()
		}
		visitor.result = compiledRowRestrictionValue{kind: rowRestrictionPredicate, sql: "(NOT " + value.sql + ")"}
		return nil
	case "+", "-":
		if value.kind != rowRestrictionLiteral {
			return invalidRowRestriction()
		}
		if err := applyRowRestrictionSign(&value.literal, string(expression.Operator())); err != nil {
			return err
		}
		visitor.result = value
		return nil
	default:
		return unsupportedRowRestriction()
	}
}

func applyRowRestrictionSign(literal *rowRestrictionLiteralValue, operator string) error {
	negative := strings.TrimSpace(operator) == "-"
	switch literal.kind {
	case rowRestrictionInteger:
		if !negative {
			return nil
		}
		value, ok := new(big.Int).SetString(literal.canonical, 10)
		if !ok {
			return invalidRowRestriction()
		}
		value.Neg(value)
		literal.canonical = value.String()
		return nil
	case rowRestrictionDecimal:
		if !negative || literal.canonical == "0" {
			return nil
		}
		if strings.HasPrefix(literal.canonical, "-") {
			literal.canonical = strings.TrimPrefix(literal.canonical, "-")
		} else {
			literal.canonical = "-" + literal.canonical
		}
		return nil
	case rowRestrictionFloat:
		if negative {
			literal.float = -literal.float
		}
		return nil
	default:
		return invalidRowRestriction()
	}
}

func (visitor *duckDBRowRestrictionVisitor) VisitBinaryExpression(expression *queryast.BinaryExpression) error {
	operator := strings.ToUpper(strings.TrimSpace(string(expression.Operator())))
	left, err := visitor.compiler.render(expression.Left())
	if err != nil {
		return err
	}
	right, err := visitor.compiler.render(expression.Right())
	if err != nil {
		return err
	}
	switch operator {
	case "AND", "OR":
		if left.kind != rowRestrictionPredicate || right.kind != rowRestrictionPredicate {
			return invalidRowRestriction()
		}
		visitor.result = compiledRowRestrictionValue{
			kind: rowRestrictionPredicate, sql: "(" + left.sql + " " + operator + " " + right.sql + ")",
		}
		return nil
	case "IS", "IS NOT":
		if left.kind != rowRestrictionColumn || right.kind != rowRestrictionNull {
			return invalidRowRestriction()
		}
		visitor.result = compiledRowRestrictionValue{
			kind: rowRestrictionPredicate, sql: left.sql + " " + operator + " NULL",
		}
		return nil
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		if left.kind != rowRestrictionColumn || right.kind != rowRestrictionLiteral {
			return invalidRowRestriction()
		}
		placeholder, err := visitor.compiler.bindLiteral(right.literal)
		if err != nil {
			return err
		}
		visitor.result = compiledRowRestrictionValue{
			kind: rowRestrictionPredicate, sql: left.sql + " " + operator + " " + placeholder,
		}
		return nil
	default:
		return unsupportedRowRestriction()
	}
}

func (visitor *duckDBRowRestrictionVisitor) VisitCastExpression(*queryast.CastExpression) error {
	return unsupportedRowRestriction()
}

func (visitor *duckDBRowRestrictionVisitor) VisitInExpression(*queryast.InExpression) error {
	return unsupportedRowRestriction()
}

func (visitor *duckDBRowRestrictionVisitor) VisitParenthesizedExpression(expression *queryast.ParenthesizedExpression) error {
	result, err := visitor.compiler.render(expression.Inner())
	if err != nil {
		return err
	}
	if result.sql != "" {
		result.sql = "(" + result.sql + ")"
	}
	visitor.result = result
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitSubqueryExpression(*queryast.SubqueryExpression) error {
	return unsupportedRowRestriction()
}

func invalidRowRestriction() error {
	return fmt.Errorf("%w: row restriction is invalid", catalogdomain.ErrInvalid)
}

func unsupportedRowRestriction() error {
	return fmt.Errorf("%w: row restriction expression is not supported", catalogdomain.ErrUnsupported)
}
