package googlesql

import (
	"math/big"
	"strconv"
	"strings"

	gsql "github.com/goccy/go-googlesql"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

func (mapper *statementMapper) mapExpression(statementKind queryast.StatementKind, node gsql.ASTExpressionNode) (queryast.Expression, error) {
	if gsqlNodeIsNil(node) {
		return nil, parserFailure()
	}

	expression, err := mapper.mapExpressionNode(statementKind, node)
	if err != nil {
		return nil, err
	}
	parenthesized, err := node.Parenthesized()
	if err != nil {
		return nil, parserFailure()
	}
	if !parenthesized {
		return expression, nil
	}
	key, err := mapper.key(node, "parenthesized-expression")
	if err != nil {
		return nil, err
	}
	wrapped, err := queryast.NewParenthesizedExpression(key, expression)
	if err != nil {
		return nil, parserFailure()
	}
	return wrapped, nil
}

func (mapper *statementMapper) mapExpressionNode(statementKind queryast.StatementKind, node gsql.ASTExpressionNode) (queryast.Expression, error) {
	switch expression := node.(type) {
	case *gsql.ASTPathExpression:
		path, err := mapPath(expression)
		if err != nil {
			return nil, err
		}
		key, err := mapper.key(expression, "identifier-expression")
		if err != nil {
			return nil, err
		}
		return queryast.NewIdentifierExpression(key, path)
	case *gsql.ASTStar:
		key, err := mapper.key(expression, "star-expression")
		if err != nil {
			return nil, err
		}
		return queryast.NewStarExpression(key)
	case *gsql.ASTDotStar:
		return mapper.mapQualifiedStar(statementKind, expression)
	case *gsql.ASTStarWithModifiers, *gsql.ASTDotStarWithModifiers:
		return nil, unsupportedNode(statementKind, "star-modifiers", node)
	case *gsql.ASTNullLiteral:
		key, err := mapper.key(expression, "null-literal")
		if err != nil {
			return nil, err
		}
		return queryast.NewNullLiteral(key)
	case *gsql.ASTBooleanLiteral:
		value, err := expression.Value()
		if err != nil {
			return nil, parserFailure()
		}
		key, err := mapper.key(expression, "boolean-literal")
		if err != nil {
			return nil, err
		}
		return queryast.NewBooleanLiteral(key, value)
	case *gsql.ASTIntLiteral:
		return mapper.mapIntegerLiteral(expression)
	case *gsql.ASTFloatLiteral:
		return mapper.mapFloatLiteral(expression)
	case *gsql.ASTStringLiteral:
		value, err := expression.StringValue()
		if err != nil {
			return nil, parserFailure()
		}
		key, err := mapper.key(expression, "string-literal")
		if err != nil {
			return nil, err
		}
		return queryast.NewStringLiteral(key, value)
	case *gsql.ASTDateOrTimeLiteral:
		return mapper.mapTemporalLiteral(statementKind, expression)
	case *gsql.ASTArrayConstructor:
		return mapper.mapArrayLiteral(statementKind, expression)
	case *gsql.ASTStructConstructorWithKeyword:
		return mapper.mapStructLiteral(statementKind, expression)
	case *gsql.ASTFunctionCall:
		return mapper.mapFunctionCall(statementKind, expression)
	case *gsql.ASTUnaryExpression:
		return mapper.mapUnaryExpression(statementKind, expression)
	case *gsql.ASTBinaryExpression:
		return mapper.mapBinaryExpression(statementKind, expression)
	case *gsql.ASTAndExpr:
		return mapper.mapBooleanChain(statementKind, expression, "AND")
	case *gsql.ASTOrExpr:
		return mapper.mapBooleanChain(statementKind, expression, "OR")
	case *gsql.ASTCastExpression:
		return mapper.mapCastExpression(statementKind, expression)
	case *gsql.ASTInExpression:
		return mapper.mapInExpression(statementKind, expression)
	case *gsql.ASTExpressionSubquery:
		return mapper.mapSubqueryExpression(statementKind, expression)
	default:
		return nil, unsupportedNode(statementKind, "expression", node)
	}
}

func (mapper *statementMapper) mapQualifiedStar(statementKind queryast.StatementKind, node *gsql.ASTDotStar) (queryast.Expression, error) {
	qualifierNode, err := node.Expr()
	if err != nil || qualifierNode == nil {
		return nil, parserFailure()
	}
	pathNode, ok := qualifierNode.(*gsql.ASTPathExpression)
	if !ok {
		return nil, unsupportedNode(statementKind, "qualified-star-expression", qualifierNode)
	}
	path, err := mapPath(pathNode)
	if err != nil {
		return nil, err
	}
	key, err := mapper.key(node, "star-expression")
	if err != nil {
		return nil, err
	}
	return queryast.NewQualifiedStarExpression(key, path)
}

func (mapper *statementMapper) mapIntegerLiteral(node *gsql.ASTIntLiteral) (queryast.Expression, error) {
	image, err := node.Image()
	if err != nil {
		return nil, parserFailure()
	}
	base := 10
	if hexadecimal, inspectErr := node.IsHex(); inspectErr != nil {
		return nil, parserFailure()
	} else if hexadecimal {
		base = 0
	}
	value, ok := new(big.Int).SetString(image, base)
	if !ok {
		return nil, parserFailure()
	}
	key, err := mapper.key(node, "integer-literal")
	if err != nil {
		return nil, err
	}
	return queryast.NewIntegerLiteral(key, value.String())
}

func (mapper *statementMapper) mapFloatLiteral(node *gsql.ASTFloatLiteral) (queryast.Expression, error) {
	image, err := node.Image()
	if err != nil {
		return nil, parserFailure()
	}
	value, err := strconv.ParseFloat(image, 64)
	if err != nil {
		return nil, parserFailure()
	}
	key, err := mapper.key(node, "float-literal")
	if err != nil {
		return nil, err
	}
	return queryast.NewFloatLiteral(key, value)
}

func (mapper *statementMapper) mapTemporalLiteral(statementKind queryast.StatementKind, node *gsql.ASTDateOrTimeLiteral) (queryast.Expression, error) {
	typeKind, err := node.TypeKind()
	if err != nil {
		return nil, parserFailure()
	}
	var typ queryast.TypeKind
	switch typeKind {
	case gsql.TypeKindTypeDate:
		typ = queryast.TypeDate
	case gsql.TypeKindTypeDatetime:
		typ = queryast.TypeDatetime
	case gsql.TypeKindTypeTime:
		typ = queryast.TypeTime
	case gsql.TypeKindTypeTimestamp:
		typ = queryast.TypeTimestamp
	default:
		return nil, unsupportedNode(statementKind, "temporal-literal-type", node)
	}
	literal, err := node.StringLiteral()
	if err != nil || literal == nil {
		return nil, parserFailure()
	}
	value, err := literal.StringValue()
	if err != nil {
		return nil, parserFailure()
	}
	key, err := mapper.key(node, "temporal-literal")
	if err != nil {
		return nil, err
	}
	return queryast.NewTemporalLiteral(key, typ, value)
}

func (mapper *statementMapper) mapArrayLiteral(statementKind queryast.StatementKind, node *gsql.ASTArrayConstructor) (queryast.Expression, error) {
	var elementType queryast.Type
	externalType, err := node.Type()
	if err != nil {
		return nil, parserFailure()
	}
	if externalType != nil {
		if collate, inspectErr := externalType.Collate(); inspectErr != nil {
			return nil, parserFailure()
		} else if collate != nil {
			return nil, unsupportedNode(statementKind, "array-type-collation", collate)
		}
		if parameters, inspectErr := externalType.TypeParameters(); inspectErr != nil {
			return nil, parserFailure()
		} else if parameters != nil {
			return nil, unsupportedNode(statementKind, "array-type-parameters", parameters)
		}
		elementNode, inspectErr := externalType.ElementType()
		if inspectErr != nil || elementNode == nil {
			return nil, parserFailure()
		}
		elementType, err = mapper.mapType(statementKind, elementNode)
		if err != nil {
			return nil, err
		}
	}
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	elements := make([]queryast.Expression, 0, len(children))
	for _, child := range children {
		expressionNode, ok := child.(gsql.ASTExpressionNode)
		if !ok {
			if externalType != nil {
				if _, typeChild := child.(*gsql.ASTArrayType); typeChild {
					continue
				}
			}
			return nil, unsupportedNode(statementKind, "array-literal-child", child)
		}
		element, mapErr := mapper.mapExpression(statementKind, expressionNode)
		if mapErr != nil {
			return nil, mapErr
		}
		elements = append(elements, element)
	}
	key, err := mapper.key(node, "array-literal")
	if err != nil {
		return nil, err
	}
	return queryast.NewArrayLiteral(key, elementType, elements)
}

func (mapper *statementMapper) mapStructLiteral(statementKind queryast.StatementKind, node *gsql.ASTStructConstructorWithKeyword) (queryast.Expression, error) {
	var typ queryast.Type
	externalType, err := node.StructType()
	if err != nil {
		return nil, parserFailure()
	}
	if externalType != nil {
		typ, err = mapper.mapType(statementKind, externalType)
		if err != nil {
			return nil, err
		}
	}
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	fields := make([]queryast.StructLiteralField, 0, len(children))
	for _, child := range children {
		argument, ok := child.(*gsql.ASTStructConstructorArg)
		if !ok {
			if externalType != nil {
				if _, typeChild := child.(*gsql.ASTStructType); typeChild {
					continue
				}
			}
			return nil, unsupportedNode(statementKind, "struct-literal-child", child)
		}
		expressionNode, inspectErr := argument.Expression()
		if inspectErr != nil || expressionNode == nil {
			return nil, parserFailure()
		}
		value, mapErr := mapper.mapExpression(statementKind, expressionNode)
		if mapErr != nil {
			return nil, mapErr
		}
		aliasNode, inspectErr := argument.Alias()
		if inspectErr != nil {
			return nil, parserFailure()
		}
		name, mapErr := mapAlias(aliasNode)
		if mapErr != nil {
			return nil, mapErr
		}
		field, mapErr := queryast.NewStructLiteralField(name, value)
		if mapErr != nil {
			return nil, parserFailure()
		}
		fields = append(fields, field)
	}
	key, err := mapper.key(node, "struct-literal")
	if err != nil {
		return nil, err
	}
	return queryast.NewStructLiteral(key, typ, fields)
}

func (mapper *statementMapper) mapFunctionCall(statementKind queryast.StatementKind, node *gsql.ASTFunctionCall) (queryast.Expression, error) {
	for _, optional := range []struct {
		kind string
		get  func() (gsql.ASTNode, error)
	}{
		{kind: "function-clamped-between", get: func() (gsql.ASTNode, error) { return node.ClampedBetweenModifier() }},
		{kind: "function-group-by", get: func() (gsql.ASTNode, error) { return node.GroupBy() }},
		{kind: "function-having", get: func() (gsql.ASTNode, error) { return node.HavingExpr() }},
		{kind: "function-having-modifier", get: func() (gsql.ASTNode, error) { return node.HavingModifier() }},
		{kind: "function-hint", get: func() (gsql.ASTNode, error) { return node.Hint() }},
		{kind: "function-limit", get: func() (gsql.ASTNode, error) { return node.LimitOffset() }},
		{kind: "function-order-by", get: func() (gsql.ASTNode, error) { return node.OrderBy() }},
		{kind: "function-where", get: func() (gsql.ASTNode, error) { return node.WhereExpr() }},
		{kind: "function-report", get: func() (gsql.ASTNode, error) { return node.WithReportModifier() }},
	} {
		value, err := optional.get()
		if err != nil {
			return nil, parserFailure()
		}
		if !gsqlNodeIsNil(value) {
			return nil, unsupportedNode(statementKind, optional.kind, value)
		}
	}
	if chained, err := node.IsChainedCall(); err != nil {
		return nil, parserFailure()
	} else if chained {
		return nil, unsupportedNode(statementKind, "chained-function-call", node)
	}
	nameNode, err := node.Function()
	if err != nil || nameNode == nil {
		return nil, parserFailure()
	}
	name, err := mapPath(nameNode)
	if err != nil {
		return nil, err
	}
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	if len(children) == 0 {
		return nil, parserFailure()
	}
	if _, ok := children[0].(*gsql.ASTPathExpression); !ok {
		return nil, unsupportedNode(statementKind, "function-name", children[0])
	}
	arguments := make([]queryast.Expression, 0, len(children)-1)
	for _, child := range children[1:] {
		argumentNode, ok := child.(gsql.ASTExpressionNode)
		if !ok {
			return nil, unsupportedNode(statementKind, "function-child", child)
		}
		argument, mapErr := mapper.mapExpression(statementKind, argumentNode)
		if mapErr != nil {
			return nil, mapErr
		}
		arguments = append(arguments, argument)
	}
	distinct, err := node.Distinct()
	if err != nil {
		return nil, parserFailure()
	}
	nullModifier, err := node.NullHandlingModifier()
	if err != nil {
		return nil, parserFailure()
	}
	nullHandling := queryast.FunctionNullHandlingDefault
	switch nullModifier {
	case gsql.ASTFunctionCallEnums_NullHandlingModifierDefaultNullHandling:
	case gsql.ASTFunctionCallEnums_NullHandlingModifierIgnoreNulls:
		nullHandling = queryast.FunctionIgnoreNulls
	case gsql.ASTFunctionCallEnums_NullHandlingModifierRespectNulls:
		nullHandling = queryast.FunctionRespectNulls
	default:
		return nil, unsupportedNode(statementKind, "function-null-handling", node)
	}
	key, err := mapper.key(node, "function-call")
	if err != nil {
		return nil, err
	}
	return queryast.NewFunctionCall(key, name, arguments, distinct, nullHandling)
}

func (mapper *statementMapper) mapUnaryExpression(statementKind queryast.StatementKind, node *gsql.ASTUnaryExpression) (queryast.Expression, error) {
	operator, err := node.GetSQLForOperator()
	if err != nil {
		return nil, parserFailure()
	}
	operator = strings.ToUpper(strings.TrimSpace(operator))
	switch operator {
	case "NOT", "~", "-", "+":
	default:
		return nil, unsupportedNode(statementKind, "unary-operator", node)
	}
	operandNode, err := node.Operand()
	if err != nil || operandNode == nil {
		return nil, parserFailure()
	}
	operand, err := mapper.mapExpression(statementKind, operandNode)
	if err != nil {
		return nil, err
	}
	key, err := mapper.key(node, "unary-expression")
	if err != nil {
		return nil, err
	}
	return queryast.NewUnaryExpression(key, queryast.UnaryOperator(operator), operand)
}

func (mapper *statementMapper) mapBinaryExpression(statementKind queryast.StatementKind, node *gsql.ASTBinaryExpression) (queryast.Expression, error) {
	operator, err := node.GetSQLForOperator()
	if err != nil {
		return nil, parserFailure()
	}
	operator = strings.ToUpper(strings.TrimSpace(operator))
	switch operator {
	case "=", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "NOT LIKE", "IS", "IS NOT",
		"IS DISTINCT FROM", "IS NOT DISTINCT FROM", "|", "^", "&", "+", "-", "*", "/", "||":
	default:
		return nil, unsupportedNode(statementKind, "binary-operator", node)
	}
	leftNode, err := node.Lhs()
	if err != nil || leftNode == nil {
		return nil, parserFailure()
	}
	rightNode, err := node.Rhs()
	if err != nil || rightNode == nil {
		return nil, parserFailure()
	}
	left, err := mapper.mapExpression(statementKind, leftNode)
	if err != nil {
		return nil, err
	}
	right, err := mapper.mapExpression(statementKind, rightNode)
	if err != nil {
		return nil, err
	}
	key, err := mapper.key(node, "binary-expression")
	if err != nil {
		return nil, err
	}
	return queryast.NewBinaryExpression(key, queryast.BinaryOperator(operator), left, right)
}

func (mapper *statementMapper) mapBooleanChain(statementKind queryast.StatementKind, node gsql.ASTExpressionNode, operator string) (queryast.Expression, error) {
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	operands := make([]queryast.Expression, 0, len(children))
	for _, child := range children {
		expressionNode, ok := child.(gsql.ASTExpressionNode)
		if !ok {
			return nil, unsupportedNode(statementKind, "boolean-operand", child)
		}
		operand, mapErr := mapper.mapExpression(statementKind, expressionNode)
		if mapErr != nil {
			return nil, mapErr
		}
		operands = append(operands, operand)
	}
	if len(operands) < 2 {
		return nil, parserFailure()
	}
	result := operands[0]
	for _, operand := range operands[1:] {
		key, keyErr := mapper.key(node, "boolean-expression")
		if keyErr != nil {
			return nil, keyErr
		}
		result, err = queryast.NewBinaryExpression(key, queryast.BinaryOperator(operator), result, operand)
		if err != nil {
			return nil, parserFailure()
		}
	}
	return result, nil
}

func (mapper *statementMapper) mapCastExpression(statementKind queryast.StatementKind, node *gsql.ASTCastExpression) (queryast.Expression, error) {
	if format, err := node.Format(); err != nil {
		return nil, parserFailure()
	} else if format != nil {
		return nil, unsupportedNode(statementKind, "cast-format", format)
	}
	valueNode, err := node.Expr()
	if err != nil || valueNode == nil {
		return nil, parserFailure()
	}
	typeNode, err := node.Type()
	if err != nil || typeNode == nil {
		return nil, parserFailure()
	}
	value, err := mapper.mapExpression(statementKind, valueNode)
	if err != nil {
		return nil, err
	}
	typ, err := mapper.mapType(statementKind, typeNode)
	if err != nil {
		return nil, err
	}
	safe, err := node.IsSafeCast()
	if err != nil {
		return nil, parserFailure()
	}
	key, err := mapper.key(node, "cast-expression")
	if err != nil {
		return nil, err
	}
	return queryast.NewCastExpression(key, value, typ, safe)
}

func (mapper *statementMapper) mapInExpression(statementKind queryast.StatementKind, node *gsql.ASTInExpression) (queryast.Expression, error) {
	if hint, err := node.Hint(); err != nil {
		return nil, parserFailure()
	} else if hint != nil {
		return nil, unsupportedNode(statementKind, "in-hint", hint)
	}
	valueNode, err := node.Lhs()
	if err != nil || valueNode == nil {
		return nil, parserFailure()
	}
	value, err := mapper.mapExpression(statementKind, valueNode)
	if err != nil {
		return nil, err
	}
	not, err := node.IsNot()
	if err != nil {
		return nil, parserFailure()
	}
	key, err := mapper.key(node, "in-expression")
	if err != nil {
		return nil, err
	}
	list, err := node.InList()
	if err != nil {
		return nil, parserFailure()
	}
	queryNode, err := node.Query()
	if err != nil {
		return nil, parserFailure()
	}
	unnestNode, err := node.UnnestExpr()
	if err != nil {
		return nil, parserFailure()
	}
	sources := 0
	if list != nil {
		sources++
	}
	if queryNode != nil {
		sources++
	}
	if unnestNode != nil {
		sources++
	}
	if sources != 1 {
		return nil, parserFailure()
	}
	if list != nil {
		children, childErr := astChildren(list)
		if childErr != nil {
			return nil, childErr
		}
		options := make([]queryast.Expression, 0, len(children))
		for _, child := range children {
			expressionNode, ok := child.(gsql.ASTExpressionNode)
			if !ok {
				return nil, unsupportedNode(statementKind, "in-list-option", child)
			}
			option, mapErr := mapper.mapExpression(statementKind, expressionNode)
			if mapErr != nil {
				return nil, mapErr
			}
			options = append(options, option)
		}
		return queryast.NewInListExpression(key, value, not, options)
	}
	if queryNode != nil {
		query, mapErr := mapper.mapQuery(statementKind, queryNode)
		if mapErr != nil {
			return nil, mapErr
		}
		return queryast.NewInSubqueryExpression(key, value, not, query)
	}
	if zipMode, inspectErr := unnestNode.ArrayZipMode(); inspectErr != nil {
		return nil, parserFailure()
	} else if zipMode != nil {
		return nil, unsupportedNode(statementKind, "unnest-array-zip-mode", zipMode)
	}
	unnestExpressionNode, err := unnestNode.Expression()
	if err != nil || unnestExpressionNode == nil {
		return nil, parserFailure()
	}
	unnest, err := mapper.mapExpression(statementKind, unnestExpressionNode)
	if err != nil {
		return nil, err
	}
	return queryast.NewInUnnestExpression(key, value, not, unnest)
}

func (mapper *statementMapper) mapSubqueryExpression(statementKind queryast.StatementKind, node *gsql.ASTExpressionSubquery) (queryast.Expression, error) {
	if hint, err := node.Hint(); err != nil {
		return nil, parserFailure()
	} else if hint != nil {
		return nil, unsupportedNode(statementKind, "subquery-hint", hint)
	}
	modifier, err := node.Modifier()
	if err != nil {
		return nil, parserFailure()
	}
	if modifier != gsql.ASTExpressionSubqueryEnums_ModifierNone {
		return nil, unsupportedNode(statementKind, "subquery-modifier", node)
	}
	queryNode, err := node.Query()
	if err != nil || queryNode == nil {
		return nil, parserFailure()
	}
	query, err := mapper.mapQuery(statementKind, queryNode)
	if err != nil {
		return nil, err
	}
	key, err := mapper.key(node, "subquery-expression")
	if err != nil {
		return nil, err
	}
	return queryast.NewSubqueryExpression(key, query)
}
