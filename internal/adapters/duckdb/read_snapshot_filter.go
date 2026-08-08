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
	kind        rowRestrictionValueKind
	sql         string
	logicalType queryast.TypeKind
	literal     rowRestrictionLiteralValue
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
	var resolved catalogdomain.Field
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
			resolved = field
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
	logicalType, err := rowRestrictionFieldType(resolved)
	if err != nil {
		return compiledRowRestrictionValue{}, err
	}
	return compiledRowRestrictionValue{
		kind: rowRestrictionColumn, sql: strings.Join(quoted, "."), logicalType: logicalType,
	}, nil
}

func rowRestrictionFieldType(field catalogdomain.Field) (queryast.TypeKind, error) {
	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		return queryast.TypeBool, nil
	case "INT64", "INTEGER":
		return queryast.TypeInt64, nil
	case "FLOAT64", "FLOAT":
		return queryast.TypeFloat64, nil
	case "NUMERIC":
		return queryast.TypeNumeric, nil
	case "BIGNUMERIC":
		return queryast.TypeBigNumeric, nil
	case "STRING":
		return queryast.TypeString, nil
	case "BYTES":
		return queryast.TypeBytes, nil
	case "DATE":
		return queryast.TypeDate, nil
	case "DATETIME":
		return queryast.TypeDatetime, nil
	case "TIME":
		return queryast.TypeTime, nil
	case "TIMESTAMP":
		return queryast.TypeTimestamp, nil
	case "JSON":
		return queryast.TypeJSON, nil
	case "RECORD", "STRUCT":
		return queryast.TypeStruct, nil
	default:
		return "", unsupportedRowRestriction()
	}
}

func compatibleRowRestrictionTypes(left, right queryast.TypeKind) bool {
	if left == "" || right == "" {
		return true
	}
	if left == right {
		return left != queryast.TypeJSON && left != queryast.TypeStruct && left != queryast.TypeArray && left != queryast.TypeGeography
	}
	return isRowRestrictionNumeric(left) && isRowRestrictionNumeric(right)
}

func isRowRestrictionNumeric(kind queryast.TypeKind) bool {
	switch kind {
	case queryast.TypeInt64, queryast.TypeFloat64, queryast.TypeNumeric, queryast.TypeBigNumeric:
		return true
	default:
		return false
	}
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

func (compiler *rowRestrictionCompiler) renderOperand(value compiledRowRestrictionValue) (string, error) {
	if value.sql != "" {
		return value.sql, nil
	}
	switch value.kind {
	case rowRestrictionLiteral:
		return compiler.bindLiteral(value.literal)
	case rowRestrictionNull:
		return "NULL", nil
	default:
		return "", invalidRowRestriction()
	}
}

func renderRowRestrictionCastType(typ queryast.Type) (string, error) {
	scalar, ok := typ.(*queryast.ScalarType)
	if !ok {
		return "", unsupportedRowRestriction()
	}
	switch scalar.Kind() {
	case queryast.TypeBool:
		return "BOOLEAN", nil
	case queryast.TypeInt64:
		return "BIGINT", nil
	case queryast.TypeFloat64:
		return "DOUBLE", nil
	case queryast.TypeNumeric, queryast.TypeBigNumeric:
		field := catalogdomain.Field{
			Type: string(scalar.Kind()), Precision: scalar.Precision(), Scale: scalar.Scale(),
		}
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			return "", unsupportedRowRestriction()
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", parameters.Precision, parameters.Scale), nil
	case queryast.TypeString:
		return "VARCHAR", nil
	case queryast.TypeDate:
		return "DATE", nil
	case queryast.TypeDatetime:
		return "TIMESTAMP", nil
	case queryast.TypeTime:
		return "TIME", nil
	case queryast.TypeTimestamp:
		return "TIMESTAMPTZ", nil
	default:
		return "", unsupportedRowRestriction()
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
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, logicalType: queryast.TypeBool, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionBoolean, boolean: literal.Value(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitIntegerLiteral(literal *queryast.IntegerLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, logicalType: queryast.TypeInt64, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionInteger, canonical: literal.CanonicalValue(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitFloatLiteral(literal *queryast.FloatLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, logicalType: queryast.TypeFloat64, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionFloat, float: literal.Value(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitDecimalLiteral(literal *queryast.DecimalLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, logicalType: literal.Type(), literal: rowRestrictionLiteralValue{
		kind: rowRestrictionDecimal, canonical: literal.CanonicalValue(), decimalType: literal.Type(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitStringLiteral(literal *queryast.StringLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, logicalType: queryast.TypeString, literal: rowRestrictionLiteralValue{
		kind: rowRestrictionString, text: literal.Value(),
	}}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitTemporalLiteral(literal *queryast.TemporalLiteral) error {
	visitor.result = compiledRowRestrictionValue{kind: rowRestrictionLiteral, logicalType: literal.Type(), literal: rowRestrictionLiteralValue{
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
		if left.kind == rowRestrictionPredicate || right.kind != rowRestrictionNull {
			return invalidRowRestriction()
		}
		leftSQL, err := visitor.compiler.renderOperand(left)
		if err != nil {
			return err
		}
		visitor.result = compiledRowRestrictionValue{
			kind: rowRestrictionPredicate, sql: leftSQL + " " + operator + " NULL",
		}
		return nil
	case "=", "!=", "<>", "<", "<=", ">", ">=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM":
		if left.kind == rowRestrictionPredicate || right.kind == rowRestrictionPredicate ||
			(left.kind != rowRestrictionColumn && right.kind != rowRestrictionColumn) ||
			left.kind == rowRestrictionNull || right.kind == rowRestrictionNull {
			return invalidRowRestriction()
		}
		if !compatibleRowRestrictionTypes(left.logicalType, right.logicalType) {
			return invalidRowRestriction()
		}
		leftSQL, err := visitor.compiler.renderOperand(left)
		if err != nil {
			return err
		}
		rightSQL, err := visitor.compiler.renderOperand(right)
		if err != nil {
			return err
		}
		visitor.result = compiledRowRestrictionValue{
			kind: rowRestrictionPredicate, sql: leftSQL + " " + operator + " " + rightSQL,
		}
		return nil
	case "LIKE", "NOT LIKE":
		if left.kind != rowRestrictionColumn || right.kind != rowRestrictionLiteral ||
			right.logicalType != queryast.TypeString ||
			!compatibleRowRestrictionTypes(left.logicalType, queryast.TypeString) {
			return invalidRowRestriction()
		}
		rightSQL, err := visitor.compiler.renderOperand(right)
		if err != nil {
			return err
		}
		visitor.result = compiledRowRestrictionValue{
			kind: rowRestrictionPredicate, sql: left.sql + " " + operator + " " + rightSQL,
		}
		return nil
	default:
		return unsupportedRowRestriction()
	}
}

func (visitor *duckDBRowRestrictionVisitor) VisitBetweenExpression(expression *queryast.BetweenExpression) error {
	value, err := visitor.compiler.render(expression.Value())
	if err != nil {
		return err
	}
	low, err := visitor.compiler.render(expression.Low())
	if err != nil {
		return err
	}
	high, err := visitor.compiler.render(expression.High())
	if err != nil {
		return err
	}
	if value.kind != rowRestrictionColumn || low.kind != rowRestrictionLiteral || high.kind != rowRestrictionLiteral ||
		!compatibleRowRestrictionTypes(value.logicalType, low.logicalType) ||
		!compatibleRowRestrictionTypes(value.logicalType, high.logicalType) {
		return invalidRowRestriction()
	}
	lowPlaceholder, err := visitor.compiler.renderOperand(low)
	if err != nil {
		return err
	}
	highPlaceholder, err := visitor.compiler.renderOperand(high)
	if err != nil {
		return err
	}
	operator := " BETWEEN "
	if expression.Not() {
		operator = " NOT BETWEEN "
	}
	visitor.result = compiledRowRestrictionValue{
		kind: rowRestrictionPredicate,
		sql:  "(" + value.sql + operator + lowPlaceholder + " AND " + highPlaceholder + ")",
	}
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitCastExpression(expression *queryast.CastExpression) error {
	value, err := visitor.compiler.render(expression.Value())
	if err != nil {
		return err
	}
	if value.kind != rowRestrictionColumn && value.kind != rowRestrictionLiteral && value.kind != rowRestrictionNull {
		return invalidRowRestriction()
	}
	operand, err := visitor.compiler.renderOperand(value)
	if err != nil {
		return err
	}
	physicalType, err := renderRowRestrictionCastType(expression.Type())
	if err != nil {
		return err
	}
	cast := "CAST"
	if expression.Safe() {
		cast = "TRY_CAST"
	}
	value.sql = cast + "(" + operand + " AS " + physicalType + ")"
	value.logicalType = expression.Type().Kind()
	visitor.result = value
	return nil
}

func (visitor *duckDBRowRestrictionVisitor) VisitInExpression(expression *queryast.InExpression) error {
	if expression.Subquery() != nil || expression.Unnest() != nil {
		return unsupportedRowRestriction()
	}
	value, err := visitor.compiler.render(expression.Value())
	if err != nil {
		return err
	}
	if value.kind != rowRestrictionColumn {
		return invalidRowRestriction()
	}
	options := expression.Options()
	if len(options) == 0 {
		return invalidRowRestriction()
	}
	placeholders := make([]string, len(options))
	for index, option := range options {
		compiled, renderErr := visitor.compiler.render(option)
		if renderErr != nil {
			return renderErr
		}
		if compiled.kind != rowRestrictionLiteral && compiled.kind != rowRestrictionNull {
			return invalidRowRestriction()
		}
		if compiled.kind == rowRestrictionLiteral &&
			!compatibleRowRestrictionTypes(value.logicalType, compiled.logicalType) {
			return invalidRowRestriction()
		}
		placeholders[index], renderErr = visitor.compiler.renderOperand(compiled)
		if renderErr != nil {
			return renderErr
		}
	}
	operator := " IN "
	if expression.Not() {
		operator = " NOT IN "
	}
	visitor.result = compiledRowRestrictionValue{
		kind: rowRestrictionPredicate,
		sql:  value.sql + operator + "(" + strings.Join(placeholders, ", ") + ")",
	}
	return nil
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
