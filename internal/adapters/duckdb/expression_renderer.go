package duckdb

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

func (renderer *duckDBStatementRenderer) renderExpression(expression queryast.Expression) (string, error) {
	if expression == nil {
		return "", fmt.Errorf("%w: expression is missing", domain.ErrPrecondition)
	}
	visitor := &duckDBExpressionVisitor{renderer: renderer}
	if err := expression.Accept(visitor); err != nil {
		return "", err
	}
	if visitor.result == "" {
		return "", fmt.Errorf("%w: expression renderer produced no output", domain.ErrPrecondition)
	}
	return visitor.result, nil
}

func (renderer *duckDBStatementRenderer) renderExpressionList(expressions []queryast.Expression) (string, error) {
	rendered := make([]string, len(expressions))
	for index, expression := range expressions {
		value, err := renderer.renderExpression(expression)
		if err != nil {
			return "", err
		}
		rendered[index] = value
	}
	return strings.Join(rendered, ", "), nil
}

func (renderer *duckDBStatementRenderer) bindArgument(value any) string {
	renderer.arguments = append(renderer.arguments, value)
	return "?"
}

type duckDBExpressionVisitor struct {
	renderer *duckDBStatementRenderer
	result   string
}

func (visitor *duckDBExpressionVisitor) VisitIdentifierExpression(expression *queryast.IdentifierExpression) error {
	if table, found := visitor.renderer.scriptVariables[expression.NodeKey()]; found {
		visitor.result = "(SELECT " + quoteIdentifier("value") + " FROM " + quoteIdentifier(table) + ")"
		return nil
	}
	visitor.result = renderIdentifierPath(expression.Path())
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitStarExpression(expression *queryast.StarExpression) error {
	if qualifier := expression.Qualifier(); qualifier != nil {
		visitor.result = renderIdentifierPath(*qualifier) + ".*"
	} else {
		visitor.result = "*"
	}
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitNullLiteral(*queryast.NullLiteral) error {
	visitor.result = "NULL"
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitBooleanLiteral(literal *queryast.BooleanLiteral) error {
	visitor.result = visitor.renderer.bindArgument(literal.Value())
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitIntegerLiteral(literal *queryast.IntegerLiteral) error {
	value, err := strconv.ParseInt(literal.CanonicalValue(), 10, 64)
	if err != nil {
		return fmt.Errorf("%w: INT64 literal is outside the supported range", domain.ErrInvalidQuery)
	}
	visitor.result = visitor.renderer.bindArgument(value)
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitFloatLiteral(literal *queryast.FloatLiteral) error {
	if math.IsNaN(literal.Value()) || math.IsInf(literal.Value(), 0) {
		return fmt.Errorf("%w: non-finite FLOAT64 literal is invalid", domain.ErrInvalidQuery)
	}
	visitor.result = visitor.renderer.bindArgument(literal.Value())
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitDecimalLiteral(literal *queryast.DecimalLiteral) error {
	field := domain.Field{Name: "decimal_literal", Type: string(literal.Type())}
	parameters, err := field.EffectiveDecimalParameters()
	if err != nil {
		return unsupportedDuckDBLowering("decimal literal", literal.Type())
	}
	physicalType := fmt.Sprintf("DECIMAL(%d,%d)", parameters.Precision, parameters.Scale)
	visitor.result = "CAST(" + visitor.renderer.bindArgument(literal.CanonicalValue()) + " AS " + physicalType + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitStringLiteral(literal *queryast.StringLiteral) error {
	visitor.result = visitor.renderer.bindArgument(literal.Value())
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitTemporalLiteral(literal *queryast.TemporalLiteral) error {
	var physicalType string
	switch literal.Type() {
	case queryast.TypeDate:
		physicalType = "DATE"
	case queryast.TypeDatetime:
		physicalType = "TIMESTAMP"
	case queryast.TypeTime:
		physicalType = "TIME"
	case queryast.TypeTimestamp:
		physicalType = "TIMESTAMPTZ"
	default:
		return unsupportedDuckDBLowering("temporal literal", literal.Type())
	}
	visitor.result = "CAST(" + visitor.renderer.bindArgument(literal.Value()) + " AS " + physicalType + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitArrayLiteral(literal *queryast.ArrayLiteral) error {
	elements := literal.Elements()
	if len(elements) == 0 && literal.ElementType() == nil {
		return fmt.Errorf("%w: untyped empty ARRAY cannot be lowered", domain.ErrUnsupported)
	}
	rendered, err := visitor.renderer.renderExpressionList(elements)
	if err != nil {
		return err
	}
	result := "[" + rendered + "]"
	if literal.ElementType() != nil {
		elementType, err := visitor.renderer.renderType(literal.ElementType())
		if err != nil {
			return err
		}
		result = "CAST(" + result + " AS " + elementType + "[])"
	}
	visitor.result = result
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitStructLiteral(literal *queryast.StructLiteral) error {
	fields := literal.Fields()
	if len(fields) == 0 {
		return fmt.Errorf("%w: empty STRUCT literal cannot be lowered", domain.ErrUnsupported)
	}
	names := make([]string, len(fields))
	allNamed := true
	anyNamed := false
	seen := make(map[string]struct{}, len(fields))
	for index, field := range fields {
		if field.Name() == nil {
			allNamed = false
			continue
		}
		name := field.Name().Value()
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: STRUCT literal field names are duplicated", domain.ErrInvalidQuery)
		}
		seen[key] = struct{}{}
		names[index], anyNamed = name, true
	}
	if anyNamed && !allNamed {
		return fmt.Errorf("%w: partially named STRUCT literal cannot be lowered", domain.ErrUnsupported)
	}
	values := make([]string, len(fields))
	for index, field := range fields {
		rendered, err := visitor.renderer.renderExpression(field.Value())
		if err != nil {
			return err
		}
		values[index] = rendered
	}
	var result string
	if allNamed {
		arguments := make([]string, len(fields))
		for index := range fields {
			arguments[index] = quoteIdentifier(names[index]) + " := " + values[index]
		}
		result = "struct_pack(" + strings.Join(arguments, ", ") + ")"
	} else {
		result = "row(" + strings.Join(values, ", ") + ")"
	}
	if literal.Type() != nil {
		if literal.Type().Kind() != queryast.TypeStruct {
			return fmt.Errorf("%w: STRUCT literal carries a non-STRUCT type", domain.ErrInvalidQuery)
		}
		typeName, err := visitor.renderer.renderType(literal.Type())
		if err != nil {
			return err
		}
		result = "CAST(" + result + " AS " + typeName + ")"
	}
	visitor.result = result
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitFunctionCall(call *queryast.FunctionCall) error {
	segments := call.Name().Segments()
	if len(segments) != 1 {
		return unsupportedDuckDBLowering("function", "qualified")
	}
	name := strings.ToUpper(segments[0])
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return visitor.renderAggregate(call, strings.ToLower(name))
	case "ARRAY_AGG":
		return visitor.renderArrayAgg(call)
	case "COALESCE":
		return visitor.renderPlainFunction(call, "coalesce", 1, -1)
	case "IFNULL":
		return visitor.renderPlainFunction(call, "ifnull", 2, 2)
	case "NULLIF":
		return visitor.renderPlainFunction(call, "nullif", 2, 2)
	case "IF":
		return visitor.renderPlainFunction(call, "if", 3, 3)
	case "ABS", "CEIL", "CEILING", "FLOOR", "LOWER", "UPPER", "LENGTH":
		return visitor.renderPlainFunction(call, strings.ToLower(name), 1, 1)
	case "ROUND":
		return visitor.renderRound(call)
	case "ARRAY_LENGTH":
		return visitor.renderPlainFunction(call, "array_length", 1, 1)
	case "TO_JSON":
		return visitor.renderPlainFunction(call, "to_json", 1, 1)
	case "DATE_TRUNC", "TIMESTAMP_TRUNC":
		return visitor.renderTemporalTrunc(call)
	case "GENERATE_ARRAY":
		return visitor.renderGenerateArray(call)
	case "RANGE_BUCKET":
		return visitor.renderRangeBucket(call)
	default:
		return unsupportedDuckDBLowering("function", name)
	}
}

func (visitor *duckDBExpressionVisitor) renderRound(call *queryast.FunctionCall) error {
	arguments := call.Arguments()
	if call.Distinct() || call.NullHandling() != queryast.FunctionNullHandlingDefault ||
		(len(arguments) != 1 && len(arguments) != 2) {
		return unsupportedDuckDBLowering("function modifier", "ROUND")
	}
	value, err := visitor.renderer.renderExpression(arguments[0])
	if err != nil {
		return err
	}
	if len(arguments) == 1 {
		visitor.result = "round(" + value + ")"
		return nil
	}
	digits, err := visitor.renderer.renderExpression(arguments[1])
	if err != nil {
		return err
	}
	// GoogleSQL analyzes the digits argument as INT64. DuckDB's two-argument
	// ROUND overload requires INTEGER, so the adapter performs the dialect-only
	// narrowing explicitly after semantic analysis.
	result := "round(" + value + ", CAST(" + digits + " AS INTEGER))"
	typ, found := visitor.renderer.analysis.ExpressionType(call.NodeKey())
	if !found {
		return fmt.Errorf("%w: ROUND expression type binding is missing", domain.ErrPrecondition)
	}
	if typ.Kind() == semantic.TypeNumeric || typ.Kind() == semantic.TypeBigNumeric {
		parameters, ok := typ.EffectiveDecimalParameters()
		if !ok {
			return fmt.Errorf("%w: ROUND decimal type binding is invalid", domain.ErrPrecondition)
		}
		result = fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", result, parameters.Precision, parameters.Scale)
	}
	visitor.result = result
	return nil
}

func (visitor *duckDBExpressionVisitor) renderAggregate(call *queryast.FunctionCall, name string) error {
	if call.NullHandling() != queryast.FunctionNullHandlingDefault || len(call.Arguments()) != 1 {
		return unsupportedDuckDBLowering("aggregate modifier", strings.ToUpper(name))
	}
	argument, err := visitor.renderer.renderExpression(call.Arguments()[0])
	if err != nil {
		return err
	}
	modifier := ""
	if call.Distinct() {
		modifier = "DISTINCT "
	}
	visitor.result = name + "(" + modifier + argument + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) renderArrayAgg(call *queryast.FunctionCall) error {
	if len(call.Arguments()) != 1 {
		return unsupportedDuckDBLowering("ARRAY_AGG arity", len(call.Arguments()))
	}
	argument, err := visitor.renderer.renderExpression(call.Arguments()[0])
	if err != nil {
		return err
	}
	modifier := ""
	if call.Distinct() {
		modifier = "DISTINCT "
	}
	result := "array_agg(" + modifier + argument + ")"
	switch call.NullHandling() {
	case queryast.FunctionNullHandlingDefault, queryast.FunctionRespectNulls:
	case queryast.FunctionIgnoreNulls:
		filterArgument, err := visitor.renderer.renderExpression(call.Arguments()[0])
		if err != nil {
			return err
		}
		result += " FILTER (WHERE " + filterArgument + " IS NOT NULL)"
	default:
		return unsupportedDuckDBLowering("ARRAY_AGG null handling", call.NullHandling())
	}
	visitor.result = result
	return nil
}

func (visitor *duckDBExpressionVisitor) renderPlainFunction(call *queryast.FunctionCall, name string, minimum, maximum int) error {
	arguments := call.Arguments()
	if call.Distinct() || call.NullHandling() != queryast.FunctionNullHandlingDefault ||
		len(arguments) < minimum || maximum >= 0 && len(arguments) > maximum {
		return unsupportedDuckDBLowering("function modifier", strings.ToUpper(name))
	}
	rendered, err := visitor.renderer.renderExpressionList(arguments)
	if err != nil {
		return err
	}
	visitor.result = name + "(" + rendered + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) renderTemporalTrunc(call *queryast.FunctionCall) error {
	if call.Distinct() || call.NullHandling() != queryast.FunctionNullHandlingDefault || len(call.Arguments()) != 2 {
		return unsupportedDuckDBLowering("temporal truncation", call.Name().Segments()[0])
	}
	part, ok := call.Arguments()[1].(*queryast.IdentifierExpression)
	if !ok || part.Path().Len() != 1 {
		return fmt.Errorf("%w: temporal truncation part is invalid", domain.ErrInvalidQuery)
	}
	partName := strings.ToUpper(part.Path().Segments()[0])
	switch partName {
	case "MICROSECOND", "MILLISECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
	default:
		return unsupportedDuckDBLowering("temporal truncation part", partName)
	}
	value, err := visitor.renderer.renderExpression(call.Arguments()[0])
	if err != nil {
		return err
	}
	visitor.result = "date_trunc(" + visitor.renderer.bindArgument(strings.ToLower(partName)) + ", " + value + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) renderGenerateArray(call *queryast.FunctionCall) error {
	if call.Distinct() || call.NullHandling() != queryast.FunctionNullHandlingDefault ||
		(len(call.Arguments()) != 2 && len(call.Arguments()) != 3) {
		return unsupportedDuckDBLowering("GENERATE_ARRAY signature", len(call.Arguments()))
	}
	arguments, err := visitor.renderer.renderExpressionList(call.Arguments())
	if err != nil {
		return err
	}
	visitor.result = "generate_series(" + arguments + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) renderRangeBucket(call *queryast.FunctionCall) error {
	if call.Distinct() || call.NullHandling() != queryast.FunctionNullHandlingDefault || len(call.Arguments()) != 2 {
		return unsupportedDuckDBLowering("RANGE_BUCKET signature", len(call.Arguments()))
	}
	pointForNull, err := visitor.renderer.renderExpression(call.Arguments()[0])
	if err != nil {
		return err
	}
	boundaries, err := visitor.renderer.renderExpression(call.Arguments()[1])
	if err != nil {
		return err
	}
	pointForComparison, err := visitor.renderer.renderExpression(call.Arguments()[0])
	if err != nil {
		return err
	}
	boundaryTable := quoteIdentifier("__bqemu_range_bucket")
	boundaryColumn := quoteIdentifier("__bqemu_boundary")
	visitor.result = "(CASE WHEN " + pointForNull + " IS NULL THEN NULL ELSE (SELECT count(*) FROM UNNEST(" +
		boundaries + ") AS " + boundaryTable + "(" + boundaryColumn + ") WHERE " + boundaryColumn + " <= " +
		pointForComparison + ") END)"
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitUnaryExpression(expression *queryast.UnaryExpression) error {
	operator := strings.ToUpper(strings.TrimSpace(string(expression.Operator())))
	switch operator {
	case "NOT", "+", "-", "~":
	default:
		return unsupportedDuckDBLowering("unary operator", operator)
	}
	value, err := visitor.renderer.renderExpression(expression.Value())
	if err != nil {
		return err
	}
	if operator == "NOT" {
		visitor.result = "(NOT " + value + ")"
	} else {
		visitor.result = "(" + operator + value + ")"
	}
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitBinaryExpression(expression *queryast.BinaryExpression) error {
	operator := strings.ToUpper(strings.TrimSpace(string(expression.Operator())))
	physicalOperator := operator
	switch operator {
	case "=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "||",
		"AND", "OR", "LIKE", "IS", "IS NOT", "IS DISTINCT FROM", "IS NOT DISTINCT FROM":
	case "DIV":
		physicalOperator = "//"
	default:
		return unsupportedDuckDBLowering("binary operator", operator)
	}
	left, err := visitor.renderer.renderExpression(expression.Left())
	if err != nil {
		return err
	}
	right, err := visitor.renderer.renderExpression(expression.Right())
	if err != nil {
		return err
	}
	visitor.result = "(" + left + " " + physicalOperator + " " + right + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitBetweenExpression(expression *queryast.BetweenExpression) error {
	value, err := visitor.renderer.renderExpression(expression.Value())
	if err != nil {
		return err
	}
	low, err := visitor.renderer.renderExpression(expression.Low())
	if err != nil {
		return err
	}
	high, err := visitor.renderer.renderExpression(expression.High())
	if err != nil {
		return err
	}
	operator := " BETWEEN "
	if expression.Not() {
		operator = " NOT BETWEEN "
	}
	visitor.result = "(" + value + operator + low + " AND " + high + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitCastExpression(expression *queryast.CastExpression) error {
	value, err := visitor.renderer.renderExpression(expression.Value())
	if err != nil {
		return err
	}
	typeName, err := visitor.renderer.renderType(expression.Type())
	if err != nil {
		return err
	}
	function := "CAST"
	if expression.Safe() {
		function = "TRY_CAST"
	}
	visitor.result = function + "(" + value + " AS " + typeName + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitInExpression(expression *queryast.InExpression) error {
	value, err := visitor.renderer.renderExpression(expression.Value())
	if err != nil {
		return err
	}
	operator := " IN "
	if expression.Not() {
		operator = " NOT IN "
	}
	switch {
	case len(expression.Options()) != 0:
		options, err := visitor.renderer.renderExpressionList(expression.Options())
		if err != nil {
			return err
		}
		visitor.result = "(" + value + operator + "(" + options + "))"
	case expression.Subquery() != nil:
		query, err := visitor.renderer.renderQuery(*expression.Subquery())
		if err != nil {
			return err
		}
		visitor.result = "(" + value + operator + "(" + query + "))"
	case expression.Unnest() != nil:
		array, err := visitor.renderer.renderExpression(expression.Unnest())
		if err != nil {
			return err
		}
		result := "list_contains(" + array + ", " + value + ")"
		if expression.Not() {
			result = "(NOT " + result + ")"
		}
		visitor.result = result
	default:
		return fmt.Errorf("%w: IN expression has no input", domain.ErrPrecondition)
	}
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitParenthesizedExpression(expression *queryast.ParenthesizedExpression) error {
	inner, err := visitor.renderer.renderExpression(expression.Inner())
	if err != nil {
		return err
	}
	visitor.result = "(" + inner + ")"
	return nil
}

func (visitor *duckDBExpressionVisitor) VisitSubqueryExpression(expression *queryast.SubqueryExpression) error {
	query, err := visitor.renderer.renderQuery(expression.Query())
	if err != nil {
		return err
	}
	visitor.result = "(" + query + ")"
	return nil
}
