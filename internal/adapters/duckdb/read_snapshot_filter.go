package duckdb

// Storage Read row_restriction enters through the official GoogleSQL AST. Every
// literal becomes a database parameter, while identifiers are resolved against
// the canonical table schema before a statement is materialized.

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	gsql "github.com/goccy/go-googlesql"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

func compileRowRestriction(input string, schema []catalogdomain.Field) (string, []any, error) {
	expression, args, _, err := compileRowRestrictionWithShape(input, schema)
	return expression, args, err
}

func compileRowRestrictionWithShape(input string, schema []catalogdomain.Field) (string, []any, readdomain.FilterShape, error) {
	if strings.TrimSpace(input) == "" {
		return "", nil, readdomain.FilterShape{}, nil
	}
	parser, err := googlesqladapter.NewParser()
	if err != nil {
		return "", nil, readdomain.FilterShape{}, fmt.Errorf("initialize GoogleSQL row restriction parser: %w", err)
	}
	expression, err := parser.ParseStorageReadPredicate(context.Background(), input)
	if err != nil {
		return "", nil, readdomain.FilterShape{}, err
	}
	lowerer := restrictionLowerer{schema: schema}
	sql, err := lowerer.expression(expression, nil)
	if err != nil {
		return "", nil, readdomain.FilterShape{}, err
	}
	return sql, lowerer.args, readdomain.FilterShape{
		PredicateCount: lowerer.predicateCount, LogicalOperatorCount: lowerer.logicalOperatorCount,
	}, nil
}

type restrictionLowerer struct {
	schema               []catalogdomain.Field
	args                 []any
	predicateCount       int
	logicalOperatorCount int
}

func (l *restrictionLowerer) expression(node gsql.ASTExpressionNode, expected *catalogdomain.Field) (string, error) {
	if node == nil {
		return "", fmt.Errorf("row restriction has an empty expression")
	}
	switch value := node.(type) {
	case *gsql.ASTAndExpr:
		return l.logical("AND", value, value.Conjuncts)
	case *gsql.ASTOrExpr:
		return l.logical("OR", value, value.Disjuncts)
	case *gsql.ASTUnaryExpression:
		operator, err := value.GetSQLForOperator()
		if err != nil {
			return "", fmt.Errorf("read GoogleSQL unary operator: %w", err)
		}
		operator = strings.ToUpper(strings.TrimSpace(operator))
		if operator != "NOT" && operator != "+" && operator != "-" {
			return "", fmt.Errorf("unsupported GoogleSQL unary expression")
		}
		operand, err := value.Operand()
		if err != nil {
			return "", fmt.Errorf("read unary expression: %w", err)
		}
		sql, err := l.expression(operand, nil)
		if err != nil {
			return "", err
		}
		if operator == "NOT" {
			l.logicalOperatorCount++
			return "(NOT " + sql + ")", nil
		}
		return "(" + operator + sql + ")", nil
	case *gsql.ASTBinaryExpression:
		return l.binary(value)
	case *gsql.ASTBetweenExpression:
		return l.between(value)
	case *gsql.ASTInExpression:
		return l.in(value)
	case *gsql.ASTCastExpression:
		return l.cast(value)
	case *gsql.ASTFunctionCall:
		return l.function(value)
	case *gsql.ASTPathExpression:
		path, field, err := l.path(value)
		if err != nil {
			return "", err
		}
		_ = field
		return path, nil
	case *gsql.ASTStringLiteral:
		literal, err := value.StringValue()
		if err != nil {
			return "", err
		}
		l.args = append(l.args, literal)
		return "?", nil
	case *gsql.ASTBooleanLiteral:
		literal, err := value.Value()
		if err != nil {
			return "", err
		}
		l.args = append(l.args, literal)
		return "?", nil
	case *gsql.ASTIntLiteral:
		return l.number(value.ASTPrintableLeaf, expected)
	case *gsql.ASTFloatLiteral:
		return l.number(value.ASTPrintableLeaf, expected)
	case *gsql.ASTNullLiteral:
		return "NULL", nil
	case *gsql.ASTDateOrTimeLiteral:
		return l.dateOrTime(value)
	default:
		return "", fmt.Errorf("unsupported GoogleSQL row restriction expression %T", node)
	}
}

func (l *restrictionLowerer) cast(node *gsql.ASTCastExpression) (string, error) {
	expression, err := node.Expr()
	if err != nil {
		return "", err
	}
	sql, err := l.expression(expression, nil)
	if err != nil {
		return "", err
	}
	typeNode, err := node.Type()
	if err != nil {
		return "", err
	}
	simple, ok := typeNode.(*gsql.ASTSimpleType)
	if !ok {
		return "", fmt.Errorf("unsupported GoogleSQL CAST target %T", typeNode)
	}
	path, err := simple.TypeName()
	if err != nil || path == nil {
		return "", fmt.Errorf("read GoogleSQL CAST target: %w", err)
	}
	names, err := path.ToIdentifierVector()
	if err != nil || len(names) != 1 {
		return "", fmt.Errorf("unsupported GoogleSQL CAST target")
	}
	target, ok := duckDBCastTarget(names[0])
	if !ok {
		return "", fmt.Errorf("unsupported GoogleSQL CAST target %q", names[0])
	}
	return "CAST(" + sql + " AS " + target + ")", nil
}

func duckDBCastTarget(name string) (string, bool) {
	switch strings.ToUpper(name) {
	case "STRING":
		return "VARCHAR", true
	case "BOOL", "BOOLEAN":
		return "BOOLEAN", true
	case "INT64", "INTEGER":
		return "BIGINT", true
	case "FLOAT64", "FLOAT":
		return "DOUBLE", true
	case "DATE":
		return "DATE", true
	case "TIMESTAMP":
		return "TIMESTAMPTZ", true
	default:
		return "", false
	}
}

func (l *restrictionLowerer) logical(operator string, node gsql.ASTExpressionNode, child func(int32) (gsql.ASTExpressionNode, error)) (string, error) {
	childCount, err := node.NumChildren()
	if err != nil || childCount < 2 {
		return "", fmt.Errorf("GoogleSQL %s requires at least two expressions", operator)
	}
	parts := make([]string, 0, int(childCount))
	for index := int32(0); index < childCount; index++ {
		node, err := child(index)
		if err != nil {
			return "", fmt.Errorf("read %s expression: %w", operator, err)
		}
		part, err := l.expression(node, nil)
		if err != nil {
			return "", err
		}
		parts = append(parts, part)
	}
	l.logicalOperatorCount += len(parts) - 1
	return "(" + strings.Join(parts, " "+operator+" ") + ")", nil
}

func (l *restrictionLowerer) binary(node *gsql.ASTBinaryExpression) (string, error) {
	lhs, err := node.Lhs()
	if err != nil {
		return "", err
	}
	path, field, fieldBound, err := l.comparisonLeft(lhs)
	if err != nil {
		return "", err
	}
	operator, err := node.GetSQLForOperator()
	if err != nil {
		return "", err
	}
	operator = strings.ToUpper(strings.TrimSpace(operator))
	rhs, err := node.Rhs()
	if err != nil {
		return "", err
	}
	if operator == "IS" || operator == "IS NOT" {
		if _, ok := rhs.(*gsql.ASTNullLiteral); !ok {
			return "", fmt.Errorf("row restriction %s only supports NULL", operator)
		}
		l.predicateCount++
		return path + " " + operator + " NULL", nil
	}
	switch operator {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
	default:
		return "", fmt.Errorf("unsupported GoogleSQL comparison operator %q", operator)
	}
	var expected *catalogdomain.Field
	if fieldBound {
		expected = &field
	}
	right, err := l.expression(rhs, expected)
	if err != nil {
		return "", err
	}
	l.predicateCount++
	return path + " " + operator + " " + right, nil
}

func (l *restrictionLowerer) comparisonLeft(node gsql.ASTExpressionNode) (string, catalogdomain.Field, bool, error) {
	path, field, err := l.identifier(node)
	if err == nil {
		return path, field, true, nil
	}
	sql, lowerErr := l.expression(node, nil)
	if lowerErr != nil {
		return "", catalogdomain.Field{}, false, lowerErr
	}
	return sql, catalogdomain.Field{}, false, nil
}

// function maps the small Storage Read function subset from the official AST
// to DuckDB. Names are never copied from user input; a function outside this
// allowlist fails before the materialization statement is constructed.
func (l *restrictionLowerer) function(node *gsql.ASTFunctionCall) (string, error) {
	if hasModifiers, err := node.HasModifiers(false); err != nil || hasModifiers {
		return "", fmt.Errorf("unsupported GoogleSQL row restriction function modifiers")
	}
	function, err := node.Function()
	if err != nil || function == nil {
		return "", fmt.Errorf("read GoogleSQL row restriction function")
	}
	name, err := function.ToIdentifierVector()
	if err != nil || len(name) != 1 {
		return "", fmt.Errorf("unsupported GoogleSQL row restriction function")
	}
	childCount, err := node.NumChildren()
	if err != nil || childCount < 1 {
		return "", fmt.Errorf("read GoogleSQL row restriction function arguments")
	}
	// With modifiers rejected above, the first child is the function path and
	// every remaining child is an argument. Do not probe Arguments past this
	// count: the pinned upstream WASM binding faults on an out-of-range index.
	argumentCount := childCount - 1
	arguments := make([]string, 0, int(argumentCount))
	for index := int32(0); index < argumentCount; index++ {
		argument, err := node.Arguments(index)
		if err != nil {
			return "", fmt.Errorf("read GoogleSQL row restriction function argument: %w", err)
		}
		sql, err := l.expression(argument, nil)
		if err != nil {
			return "", err
		}
		arguments = append(arguments, sql)
	}
	switch strings.ToUpper(name[0]) {
	case "LOWER":
		if len(arguments) != 1 {
			return "", fmt.Errorf("GoogleSQL LOWER requires one argument")
		}
		return "LOWER(" + arguments[0] + ")", nil
	case "STARTS_WITH":
		if len(arguments) != 2 {
			return "", fmt.Errorf("GoogleSQL STARTS_WITH requires two arguments")
		}
		return "STARTS_WITH(" + strings.Join(arguments, ", ") + ")", nil
	default:
		return "", fmt.Errorf("unsupported GoogleSQL row restriction function %q", name[0])
	}
}

func (l *restrictionLowerer) between(node *gsql.ASTBetweenExpression) (string, error) {
	lhs, err := node.Lhs()
	if err != nil {
		return "", err
	}
	path, field, err := l.identifier(lhs)
	if err != nil {
		return "", err
	}
	low, err := node.Low()
	if err != nil {
		return "", err
	}
	high, err := node.High()
	if err != nil {
		return "", err
	}
	lowSQL, err := l.expression(low, &field)
	if err != nil {
		return "", err
	}
	highSQL, err := l.expression(high, &field)
	if err != nil {
		return "", err
	}
	not, err := node.IsNot()
	if err != nil {
		return "", err
	}
	l.predicateCount++
	if not {
		return path + " NOT BETWEEN " + lowSQL + " AND " + highSQL, nil
	}
	return path + " BETWEEN " + lowSQL + " AND " + highSQL, nil
}

func (l *restrictionLowerer) in(node *gsql.ASTInExpression) (string, error) {
	lhs, err := node.Lhs()
	if err != nil {
		return "", err
	}
	path, field, err := l.identifier(lhs)
	if err != nil {
		return "", err
	}
	list, err := node.InList()
	if err != nil || list == nil {
		return "", fmt.Errorf("row restriction IN supports only a literal list")
	}
	itemCount, err := list.NumChildren()
	if err != nil || itemCount == 0 {
		return "", fmt.Errorf("row restriction IN requires at least one literal")
	}
	items := make([]string, 0, int(itemCount))
	for index := int32(0); index < itemCount; index++ {
		item, err := list.List(index)
		if err != nil {
			return "", err
		}
		sql, err := l.expression(item, &field)
		if err != nil {
			return "", err
		}
		items = append(items, sql)
	}
	not, err := node.IsNot()
	if err != nil {
		return "", err
	}
	l.predicateCount++
	if not {
		return path + " NOT IN (" + strings.Join(items, ", ") + ")", nil
	}
	return path + " IN (" + strings.Join(items, ", ") + ")", nil
}

func (l *restrictionLowerer) identifier(node gsql.ASTExpressionNode) (string, catalogdomain.Field, error) {
	pathNode, ok := node.(*gsql.ASTPathExpression)
	if !ok {
		return "", catalogdomain.Field{}, fmt.Errorf("row restriction comparison requires a table field")
	}
	return l.path(pathNode)
}

func (l *restrictionLowerer) path(node *gsql.ASTPathExpression) (string, catalogdomain.Field, error) {
	components, err := node.ToIdentifierVector()
	if err != nil {
		return "", catalogdomain.Field{}, err
	}
	field, found := findFieldPath(l.schema, components)
	if !found {
		return "", catalogdomain.Field{}, fmt.Errorf("row restriction references unknown field %q", strings.Join(components, "."))
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		return "", catalogdomain.Field{}, fmt.Errorf("row restriction on repeated field %q is not supported", strings.Join(components, "."))
	}
	quoted := make([]string, len(components))
	for i, component := range components {
		quoted[i] = quoteIdentifier(component)
	}
	return strings.Join(quoted, "."), field, nil
}

func (l *restrictionLowerer) number(node *gsql.ASTPrintableLeaf, expected *catalogdomain.Field) (string, error) {
	image, err := node.Image()
	if err != nil {
		return "", err
	}
	if expected != nil && (strings.EqualFold(expected.Type, "INT64") || strings.EqualFold(expected.Type, "INTEGER")) {
		value, err := strconv.ParseInt(image, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid INT64 literal %q", image)
		}
		l.args = append(l.args, value)
		return "?", nil
	}
	value, err := strconv.ParseFloat(image, 64)
	if err != nil {
		return "", fmt.Errorf("invalid numeric literal %q", image)
	}
	l.args = append(l.args, value)
	return "?", nil
}

func (l *restrictionLowerer) dateOrTime(node *gsql.ASTDateOrTimeLiteral) (string, error) {
	literal, err := node.StringLiteral()
	if err != nil || literal == nil {
		return "", fmt.Errorf("read date/time literal: %w", err)
	}
	value, err := literal.StringValue()
	if err != nil {
		return "", err
	}
	kind, err := node.TypeKind()
	if err != nil {
		return "", err
	}
	l.args = append(l.args, value)
	switch kind {
	case gsql.TypeKindTypeDate:
		return "CAST(? AS DATE)", nil
	case gsql.TypeKindTypeTimestamp:
		return "CAST(? AS TIMESTAMPTZ)", nil
	default:
		return "", fmt.Errorf("unsupported GoogleSQL temporal literal %s", kind)
	}
}

// findFieldPath resolves case-insensitively through the canonical catalog
// schema. It is shared by row restriction binding and selected-field
// validation; callers decide whether a missing path is invalid or unsupported.
func findFieldPath(schema []catalogdomain.Field, path []string) (catalogdomain.Field, bool) {
	fields := schema
	for pathIndex, component := range path {
		found := false
		for _, field := range fields {
			if !strings.EqualFold(field.Name, component) {
				continue
			}
			if pathIndex == len(path)-1 {
				return field, true
			}
			fields = field.Fields
			found = true
			break
		}
		if !found {
			return catalogdomain.Field{}, false
		}
	}
	return catalogdomain.Field{}, false
}
