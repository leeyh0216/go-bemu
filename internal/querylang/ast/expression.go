package ast

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

type ExpressionKind string

const (
	ExpressionIdentifier    ExpressionKind = "IDENTIFIER"
	ExpressionStar          ExpressionKind = "STAR"
	ExpressionNull          ExpressionKind = "NULL_LITERAL"
	ExpressionBoolean       ExpressionKind = "BOOLEAN_LITERAL"
	ExpressionInteger       ExpressionKind = "INTEGER_LITERAL"
	ExpressionFloat         ExpressionKind = "FLOAT_LITERAL"
	ExpressionDecimal       ExpressionKind = "DECIMAL_LITERAL"
	ExpressionString        ExpressionKind = "STRING_LITERAL"
	ExpressionTemporal      ExpressionKind = "TEMPORAL_LITERAL"
	ExpressionArray         ExpressionKind = "ARRAY_LITERAL"
	ExpressionStruct        ExpressionKind = "STRUCT_LITERAL"
	ExpressionFunction      ExpressionKind = "FUNCTION_CALL"
	ExpressionUnary         ExpressionKind = "UNARY"
	ExpressionBinary        ExpressionKind = "BINARY"
	ExpressionCast          ExpressionKind = "CAST"
	ExpressionIn            ExpressionKind = "IN"
	ExpressionBetween       ExpressionKind = "BETWEEN"
	ExpressionParenthesized ExpressionKind = "PARENTHESIZED"
	ExpressionSubquery      ExpressionKind = "SUBQUERY"
)

type Expression interface {
	Kind() ExpressionKind
	NodeKey() NodeKey
	Span() Span
	Accept(ExpressionVisitor) error
	expressionNode()
	semanticWriter
}

type ExpressionVisitor interface {
	VisitIdentifierExpression(*IdentifierExpression) error
	VisitStarExpression(*StarExpression) error
	VisitNullLiteral(*NullLiteral) error
	VisitBooleanLiteral(*BooleanLiteral) error
	VisitIntegerLiteral(*IntegerLiteral) error
	VisitFloatLiteral(*FloatLiteral) error
	VisitDecimalLiteral(*DecimalLiteral) error
	VisitStringLiteral(*StringLiteral) error
	VisitTemporalLiteral(*TemporalLiteral) error
	VisitArrayLiteral(*ArrayLiteral) error
	VisitStructLiteral(*StructLiteral) error
	VisitFunctionCall(*FunctionCall) error
	VisitUnaryExpression(*UnaryExpression) error
	VisitBinaryExpression(*BinaryExpression) error
	VisitCastExpression(*CastExpression) error
	VisitInExpression(*InExpression) error
	VisitBetweenExpression(*BetweenExpression) error
	VisitParenthesizedExpression(*ParenthesizedExpression) error
	VisitSubqueryExpression(*SubqueryExpression) error
}

type expressionBase struct{ key NodeKey }

func (base expressionBase) NodeKey() NodeKey { return base.key }
func (base expressionBase) Span() Span       { return base.key.span }

type IdentifierExpression struct {
	expressionBase
	path IdentifierPath
}

func NewIdentifierExpression(key NodeKey, path IdentifierPath) (*IdentifierExpression, error) {
	if !validNodeKey(key) || path.Len() == 0 {
		return nil, fmt.Errorf("identifier expression path is empty")
	}
	return &IdentifierExpression{expressionBase: expressionBase{key: key}, path: path}, nil
}

func (*IdentifierExpression) expressionNode()                 {}
func (*IdentifierExpression) Kind() ExpressionKind            { return ExpressionIdentifier }
func (expression *IdentifierExpression) Path() IdentifierPath { return clonePath(expression.path) }
func (expression *IdentifierExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitIdentifierExpression(expression)
}
func (expression *IdentifierExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("identifier")
	writePath(builder, expression.path)
}

// qualifier is nil for * and set for path.*.
type StarExpression struct {
	expressionBase
	qualifier *IdentifierPath
}

func NewStarExpression(key NodeKey) (*StarExpression, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid star expression node key")
	}
	return &StarExpression{expressionBase: expressionBase{key: key}}, nil
}

func NewQualifiedStarExpression(key NodeKey, qualifier IdentifierPath) (*StarExpression, error) {
	if !validNodeKey(key) || qualifier.Len() == 0 {
		return nil, fmt.Errorf("invalid qualified star expression")
	}
	cloned := clonePath(qualifier)
	return &StarExpression{expressionBase: expressionBase{key: key}, qualifier: &cloned}, nil
}

func (*StarExpression) expressionNode()      {}
func (*StarExpression) Kind() ExpressionKind { return ExpressionStar }
func (expression *StarExpression) Qualifier() *IdentifierPath {
	if expression.qualifier == nil {
		return nil
	}
	cloned := clonePath(*expression.qualifier)
	return &cloned
}
func (expression *StarExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitStarExpression(expression)
}
func (expression *StarExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("star")
	if expression.qualifier == nil {
		builder.token("")
	} else {
		writePath(builder, *expression.qualifier)
	}
}

type NullLiteral struct{ expressionBase }

func NewNullLiteral(key NodeKey) (*NullLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid NULL literal node key")
	}
	return &NullLiteral{expressionBase: expressionBase{key: key}}, nil
}
func (*NullLiteral) expressionNode()      {}
func (*NullLiteral) Kind() ExpressionKind { return ExpressionNull }
func (literal *NullLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitNullLiteral(literal)
}
func (*NullLiteral) writeSemantic(builder *fingerprintBuilder) { builder.token("null") }

type BooleanLiteral struct {
	expressionBase
	value bool
}

func NewBooleanLiteral(key NodeKey, value bool) (*BooleanLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid boolean literal node key")
	}
	return &BooleanLiteral{expressionBase: expressionBase{key: key}, value: value}, nil
}
func (*BooleanLiteral) expressionNode()      {}
func (*BooleanLiteral) Kind() ExpressionKind { return ExpressionBoolean }
func (literal *BooleanLiteral) Value() bool  { return literal.value }
func (literal *BooleanLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitBooleanLiteral(literal)
}
func (literal *BooleanLiteral) writeSemantic(builder *fingerprintBuilder) {
	builder.token("bool")
	builder.boolean(literal.value)
}

// IntegerLiteral holds the canonical base-10 integer value. This avoids loss
// of precision and never retains the submitted lexeme.
type IntegerLiteral struct {
	expressionBase
	canonical string
}

func NewIntegerLiteral(key NodeKey, canonical string) (*IntegerLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid integer literal node key")
	}
	value, ok := new(big.Int).SetString(canonical, 10)
	if !ok {
		return nil, fmt.Errorf("invalid integer literal")
	}
	return &IntegerLiteral{expressionBase: expressionBase{key: key}, canonical: value.String()}, nil
}
func (*IntegerLiteral) expressionNode()                {}
func (*IntegerLiteral) Kind() ExpressionKind           { return ExpressionInteger }
func (literal *IntegerLiteral) CanonicalValue() string { return literal.canonical }
func (literal *IntegerLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitIntegerLiteral(literal)
}
func (literal *IntegerLiteral) writeSemantic(builder *fingerprintBuilder) {
	builder.token("integer")
	builder.token(literal.canonical)
}

type FloatLiteral struct {
	expressionBase
	value float64
}

func NewFloatLiteral(key NodeKey, value float64) (*FloatLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid float literal node key")
	}
	return &FloatLiteral{expressionBase: expressionBase{key: key}, value: value}, nil
}
func (*FloatLiteral) expressionNode()        {}
func (*FloatLiteral) Kind() ExpressionKind   { return ExpressionFloat }
func (literal *FloatLiteral) Value() float64 { return literal.value }
func (literal *FloatLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitFloatLiteral(literal)
}
func (literal *FloatLiteral) writeSemantic(builder *fingerprintBuilder) {
	builder.token("float")
	builder.token(strconv.FormatFloat(literal.value, 'g', -1, 64))
}

// DecimalLiteral holds an exact canonical decimal value. The submitted
// string lexeme and its quoting are intentionally not retained.
type DecimalLiteral struct {
	expressionBase
	type_     TypeKind
	canonical string
}

func NewDecimalLiteral(key NodeKey, typ TypeKind, value string) (*DecimalLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid decimal literal node key")
	}
	if typ != TypeNumeric && typ != TypeBigNumeric {
		return nil, fmt.Errorf("invalid decimal literal type")
	}
	canonical, err := canonicalDecimal(value)
	if err != nil {
		return nil, err
	}
	return &DecimalLiteral{expressionBase: expressionBase{key: key}, type_: typ, canonical: canonical}, nil
}
func (*DecimalLiteral) expressionNode()                {}
func (*DecimalLiteral) Kind() ExpressionKind           { return ExpressionDecimal }
func (literal *DecimalLiteral) Type() TypeKind         { return literal.type_ }
func (literal *DecimalLiteral) CanonicalValue() string { return literal.canonical }
func (literal *DecimalLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitDecimalLiteral(literal)
}
func (literal *DecimalLiteral) writeSemantic(builder *fingerprintBuilder) {
	builder.token("decimal")
	builder.token(string(literal.type_))
	builder.token(literal.canonical)
}

func canonicalDecimal(input string) (string, error) {
	value := strings.TrimSpace(input)
	if value == "" {
		return "", fmt.Errorf("invalid decimal literal")
	}
	negative := false
	if value[0] == '+' || value[0] == '-' {
		negative = value[0] == '-'
		value = value[1:]
	}
	if value == "" {
		return "", fmt.Errorf("invalid decimal literal")
	}

	exponent := int64(0)
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		if strings.IndexAny(value[index+1:], "eE") >= 0 {
			return "", fmt.Errorf("invalid decimal literal")
		}
		parsed, err := strconv.ParseInt(value[index+1:], 10, 32)
		if err != nil || parsed < -1000 || parsed > 1000 {
			return "", fmt.Errorf("invalid decimal literal exponent")
		}
		exponent = parsed
		value = value[:index]
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || len(parts) == 0 || parts[0] == "" && (len(parts) == 1 || parts[1] == "") {
		return "", fmt.Errorf("invalid decimal literal")
	}
	integer := parts[0]
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if integer == "" {
		integer = "0"
	}
	digits := integer + fraction
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return "", fmt.Errorf("invalid decimal literal")
		}
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "0", nil
	}
	scale := int64(len(fraction)) - exponent
	if scale <= 0 {
		if -scale > 1000 {
			return "", fmt.Errorf("invalid decimal literal exponent")
		}
		canonical := digits + strings.Repeat("0", int(-scale))
		if negative {
			canonical = "-" + canonical
		}
		return canonical, nil
	}
	if scale > 1000 {
		return "", fmt.Errorf("invalid decimal literal exponent")
	}
	if int64(len(digits)) <= scale {
		digits = strings.Repeat("0", int(scale)-len(digits)+1) + digits
	}
	split := len(digits) - int(scale)
	whole := strings.TrimLeft(digits[:split], "0")
	if whole == "" {
		whole = "0"
	}
	fraction = strings.TrimRight(digits[split:], "0")
	canonical := whole
	if fraction != "" {
		canonical += "." + fraction
	}
	if negative {
		canonical = "-" + canonical
	}
	return canonical, nil
}

type StringLiteral struct {
	expressionBase
	value string
}

func NewStringLiteral(key NodeKey, value string) (*StringLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid string literal node key")
	}
	return &StringLiteral{expressionBase: expressionBase{key: key}, value: value}, nil
}
func (*StringLiteral) expressionNode()       {}
func (*StringLiteral) Kind() ExpressionKind  { return ExpressionString }
func (literal *StringLiteral) Value() string { return literal.value }
func (literal *StringLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitStringLiteral(literal)
}
func (literal *StringLiteral) writeSemantic(builder *fingerprintBuilder) {
	builder.token("string")
	builder.token(literal.value)
}

type TemporalLiteral struct {
	expressionBase
	type_ TypeKind
	value string
}

func NewTemporalLiteral(key NodeKey, typ TypeKind, value string) (*TemporalLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid temporal literal node key")
	}
	switch typ {
	case TypeDate, TypeDatetime, TypeTime, TypeTimestamp:
	default:
		return nil, fmt.Errorf("unsupported temporal literal type")
	}
	if value == "" {
		return nil, fmt.Errorf("temporal literal value is empty")
	}
	return &TemporalLiteral{expressionBase: expressionBase{key: key}, type_: typ, value: value}, nil
}
func (*TemporalLiteral) expressionNode()        {}
func (*TemporalLiteral) Kind() ExpressionKind   { return ExpressionTemporal }
func (literal *TemporalLiteral) Type() TypeKind { return literal.type_ }
func (literal *TemporalLiteral) Value() string  { return literal.value }
func (literal *TemporalLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitTemporalLiteral(literal)
}
func (literal *TemporalLiteral) writeSemantic(builder *fingerprintBuilder) {
	builder.token("temporal")
	builder.token(string(literal.type_))
	builder.token(literal.value)
}

type ArrayLiteral struct {
	expressionBase
	elementType Type
	elements    []Expression
}

func NewArrayLiteral(key NodeKey, elementType Type, elements []Expression) (*ArrayLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid array literal node key")
	}
	cloned := append([]Expression(nil), elements...)
	for _, element := range cloned {
		if expressionIsNil(element) {
			return nil, fmt.Errorf("array literal element is nil")
		}
	}
	return &ArrayLiteral{expressionBase: expressionBase{key: key}, elementType: elementType, elements: cloned}, nil
}
func (*ArrayLiteral) expressionNode()           {}
func (*ArrayLiteral) Kind() ExpressionKind      { return ExpressionArray }
func (literal *ArrayLiteral) ElementType() Type { return literal.elementType }
func (literal *ArrayLiteral) Elements() []Expression {
	return append([]Expression(nil), literal.elements...)
}
func (literal *ArrayLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitArrayLiteral(literal)
}
func (literal *ArrayLiteral) writeSemantic(builder *fingerprintBuilder) {
	builder.token("array")
	if literal.elementType == nil {
		builder.token("")
	} else {
		literal.elementType.writeSemantic(builder)
	}
	builder.token(strconv.Itoa(len(literal.elements)))
	for _, element := range literal.elements {
		element.writeSemantic(builder)
	}
}

type StructLiteralField struct {
	name  *Identifier
	value Expression
}

func NewStructLiteralField(name *Identifier, value Expression) (StructLiteralField, error) {
	if expressionIsNil(value) {
		return StructLiteralField{}, fmt.Errorf("struct literal field value is required")
	}
	return StructLiteralField{name: cloneIdentifier(name), value: value}, nil
}
func (field StructLiteralField) Name() *Identifier { return cloneIdentifier(field.name) }
func (field StructLiteralField) Value() Expression { return field.value }

type StructLiteral struct {
	expressionBase
	type_  Type
	fields []StructLiteralField
}

func NewStructLiteral(key NodeKey, typ Type, fields []StructLiteralField) (*StructLiteral, error) {
	if !validNodeKey(key) {
		return nil, fmt.Errorf("invalid struct literal node key")
	}
	cloned := append([]StructLiteralField(nil), fields...)
	for _, field := range cloned {
		if expressionIsNil(field.value) {
			return nil, fmt.Errorf("struct literal field value is required")
		}
	}
	return &StructLiteral{expressionBase: expressionBase{key: key}, type_: typ, fields: cloned}, nil
}
func (*StructLiteral) expressionNode()      {}
func (*StructLiteral) Kind() ExpressionKind { return ExpressionStruct }
func (literal *StructLiteral) Type() Type   { return literal.type_ }
func (literal *StructLiteral) Fields() []StructLiteralField {
	return append([]StructLiteralField(nil), literal.fields...)
}
func (literal *StructLiteral) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitStructLiteral(literal)
}
func (literal *StructLiteral) writeSemantic(builder *fingerprintBuilder) {
	builder.token("struct")
	if literal.type_ == nil {
		builder.token("")
	} else {
		literal.type_.writeSemantic(builder)
	}
	builder.token(strconv.Itoa(len(literal.fields)))
	for _, field := range literal.fields {
		if field.name == nil {
			builder.token("")
		} else {
			writeIdentifier(builder, *field.name)
		}
		field.value.writeSemantic(builder)
	}
}

type FunctionNullHandling string

const (
	FunctionNullHandlingDefault FunctionNullHandling = "DEFAULT"
	FunctionIgnoreNulls         FunctionNullHandling = "IGNORE_NULLS"
	FunctionRespectNulls        FunctionNullHandling = "RESPECT_NULLS"
)

type FunctionCall struct {
	expressionBase
	name         IdentifierPath
	arguments    []Expression
	distinct     bool
	nullHandling FunctionNullHandling
}

func NewFunctionCall(key NodeKey, name IdentifierPath, arguments []Expression, distinct bool, nullHandling FunctionNullHandling) (*FunctionCall, error) {
	if !validNodeKey(key) || name.Len() == 0 {
		return nil, fmt.Errorf("function name is empty")
	}
	switch nullHandling {
	case FunctionNullHandlingDefault, FunctionIgnoreNulls, FunctionRespectNulls:
	default:
		return nil, fmt.Errorf("invalid function null handling")
	}
	cloned := append([]Expression(nil), arguments...)
	for _, argument := range cloned {
		if expressionIsNil(argument) {
			return nil, fmt.Errorf("function argument is nil")
		}
	}
	return &FunctionCall{expressionBase: expressionBase{key: key}, name: name, arguments: cloned, distinct: distinct, nullHandling: nullHandling}, nil
}
func (*FunctionCall) expressionNode()           {}
func (*FunctionCall) Kind() ExpressionKind      { return ExpressionFunction }
func (call *FunctionCall) Name() IdentifierPath { return clonePath(call.name) }
func (call *FunctionCall) Arguments() []Expression {
	return append([]Expression(nil), call.arguments...)
}
func (call *FunctionCall) Distinct() bool                     { return call.distinct }
func (call *FunctionCall) NullHandling() FunctionNullHandling { return call.nullHandling }
func (call *FunctionCall) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitFunctionCall(call)
}
func (call *FunctionCall) writeSemantic(builder *fingerprintBuilder) {
	builder.token("function")
	writePath(builder, call.name)
	builder.boolean(call.distinct)
	builder.token(string(call.nullHandling))
	builder.token(strconv.Itoa(len(call.arguments)))
	for _, argument := range call.arguments {
		argument.writeSemantic(builder)
	}
}

type UnaryOperator string
type BinaryOperator string

type UnaryExpression struct {
	expressionBase
	operator UnaryOperator
	value    Expression
}

func NewUnaryExpression(key NodeKey, operator UnaryOperator, value Expression) (*UnaryExpression, error) {
	if !validNodeKey(key) || strings.TrimSpace(string(operator)) == "" || expressionIsNil(value) {
		return nil, fmt.Errorf("invalid unary expression")
	}
	return &UnaryExpression{expressionBase: expressionBase{key: key}, operator: operator, value: value}, nil
}
func (*UnaryExpression) expressionNode()                    {}
func (*UnaryExpression) Kind() ExpressionKind               { return ExpressionUnary }
func (expression *UnaryExpression) Operator() UnaryOperator { return expression.operator }
func (expression *UnaryExpression) Value() Expression       { return expression.value }
func (expression *UnaryExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitUnaryExpression(expression)
}
func (expression *UnaryExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("unary")
	builder.token(string(expression.operator))
	expression.value.writeSemantic(builder)
}

type BinaryExpression struct {
	expressionBase
	operator BinaryOperator
	left     Expression
	right    Expression
}

func NewBinaryExpression(key NodeKey, operator BinaryOperator, left, right Expression) (*BinaryExpression, error) {
	if !validNodeKey(key) || strings.TrimSpace(string(operator)) == "" || expressionIsNil(left) || expressionIsNil(right) {
		return nil, fmt.Errorf("invalid binary expression")
	}
	return &BinaryExpression{expressionBase: expressionBase{key: key}, operator: operator, left: left, right: right}, nil
}
func (*BinaryExpression) expressionNode()                     {}
func (*BinaryExpression) Kind() ExpressionKind                { return ExpressionBinary }
func (expression *BinaryExpression) Operator() BinaryOperator { return expression.operator }
func (expression *BinaryExpression) Left() Expression         { return expression.left }
func (expression *BinaryExpression) Right() Expression        { return expression.right }
func (expression *BinaryExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitBinaryExpression(expression)
}
func (expression *BinaryExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("binary")
	builder.token(string(expression.operator))
	expression.left.writeSemantic(builder)
	expression.right.writeSemantic(builder)
}

type CastExpression struct {
	expressionBase
	value Expression
	type_ Type
	safe  bool
}

func NewCastExpression(key NodeKey, value Expression, typ Type, safe bool) (*CastExpression, error) {
	if !validNodeKey(key) || expressionIsNil(value) || typeIsNil(typ) {
		return nil, fmt.Errorf("invalid cast expression")
	}
	return &CastExpression{expressionBase: expressionBase{key: key}, value: value, type_: typ, safe: safe}, nil
}
func (*CastExpression) expressionNode()              {}
func (*CastExpression) Kind() ExpressionKind         { return ExpressionCast }
func (expression *CastExpression) Value() Expression { return expression.value }
func (expression *CastExpression) Type() Type        { return expression.type_ }
func (expression *CastExpression) Safe() bool        { return expression.safe }
func (expression *CastExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitCastExpression(expression)
}
func (expression *CastExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("cast")
	builder.boolean(expression.safe)
	expression.value.writeSemantic(builder)
	expression.type_.writeSemantic(builder)
}

type InExpression struct {
	expressionBase
	value    Expression
	not      bool
	options  []Expression
	subquery *Query
	unnest   Expression
}

// BetweenExpression is the immutable canonical form of GoogleSQL BETWEEN.
// It deliberately retains only the parsed operands and negation state, never
// the submitted SQL text.
type BetweenExpression struct {
	expressionBase
	value Expression
	low   Expression
	high  Expression
	not   bool
}

func NewBetweenExpression(key NodeKey, value, low, high Expression, not bool) (*BetweenExpression, error) {
	if !validNodeKey(key) || expressionIsNil(value) || expressionIsNil(low) || expressionIsNil(high) {
		return nil, fmt.Errorf("invalid BETWEEN expression")
	}
	return &BetweenExpression{expressionBase: expressionBase{key: key}, value: value, low: low, high: high, not: not}, nil
}
func (*BetweenExpression) expressionNode()              {}
func (*BetweenExpression) Kind() ExpressionKind         { return ExpressionBetween }
func (expression *BetweenExpression) Value() Expression { return expression.value }
func (expression *BetweenExpression) Low() Expression   { return expression.low }
func (expression *BetweenExpression) High() Expression  { return expression.high }
func (expression *BetweenExpression) Not() bool         { return expression.not }
func (expression *BetweenExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitBetweenExpression(expression)
}
func (expression *BetweenExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("between")
	builder.boolean(expression.not)
	expression.value.writeSemantic(builder)
	expression.low.writeSemantic(builder)
	expression.high.writeSemantic(builder)
}

func NewInListExpression(key NodeKey, value Expression, not bool, options []Expression) (*InExpression, error) {
	if !validNodeKey(key) || expressionIsNil(value) || len(options) == 0 {
		return nil, fmt.Errorf("invalid IN-list expression")
	}
	cloned := append([]Expression(nil), options...)
	for _, option := range cloned {
		if expressionIsNil(option) {
			return nil, fmt.Errorf("IN-list option is nil")
		}
	}
	return &InExpression{expressionBase: expressionBase{key: key}, value: value, not: not, options: cloned}, nil
}

func NewInSubqueryExpression(key NodeKey, value Expression, not bool, query Query) (*InExpression, error) {
	if !validNodeKey(key) || expressionIsNil(value) || query.body == nil {
		return nil, fmt.Errorf("invalid IN-subquery expression")
	}
	cloned := query.clone()
	return &InExpression{expressionBase: expressionBase{key: key}, value: value, not: not, subquery: &cloned}, nil
}

func NewInUnnestExpression(key NodeKey, value Expression, not bool, unnest Expression) (*InExpression, error) {
	if !validNodeKey(key) || expressionIsNil(value) || expressionIsNil(unnest) {
		return nil, fmt.Errorf("invalid IN-UNNEST expression")
	}
	return &InExpression{expressionBase: expressionBase{key: key}, value: value, not: not, unnest: unnest}, nil
}
func (*InExpression) expressionNode()              {}
func (*InExpression) Kind() ExpressionKind         { return ExpressionIn }
func (expression *InExpression) Value() Expression { return expression.value }
func (expression *InExpression) Not() bool         { return expression.not }
func (expression *InExpression) Options() []Expression {
	return append([]Expression(nil), expression.options...)
}
func (expression *InExpression) Subquery() *Query {
	if expression.subquery == nil {
		return nil
	}
	cloned := expression.subquery.clone()
	return &cloned
}
func (expression *InExpression) Unnest() Expression { return expression.unnest }
func (expression *InExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitInExpression(expression)
}
func (expression *InExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("in")
	builder.boolean(expression.not)
	expression.value.writeSemantic(builder)
	if expression.subquery != nil {
		builder.token("subquery")
		expression.subquery.writeSemantic(builder)
		return
	}
	if expression.unnest != nil {
		builder.token("unnest")
		expression.unnest.writeSemantic(builder)
		return
	}
	builder.token("list")
	builder.token(strconv.Itoa(len(expression.options)))
	for _, option := range expression.options {
		option.writeSemantic(builder)
	}
}

type ParenthesizedExpression struct {
	expressionBase
	inner Expression
}

func NewParenthesizedExpression(key NodeKey, inner Expression) (*ParenthesizedExpression, error) {
	if !validNodeKey(key) || expressionIsNil(inner) {
		return nil, fmt.Errorf("invalid parenthesized expression")
	}
	return &ParenthesizedExpression{expressionBase: expressionBase{key: key}, inner: inner}, nil
}
func (*ParenthesizedExpression) expressionNode()              {}
func (*ParenthesizedExpression) Kind() ExpressionKind         { return ExpressionParenthesized }
func (expression *ParenthesizedExpression) Inner() Expression { return expression.inner }
func (expression *ParenthesizedExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitParenthesizedExpression(expression)
}
func (expression *ParenthesizedExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("parenthesized")
	expression.inner.writeSemantic(builder)
}

type SubqueryExpression struct {
	expressionBase
	query Query
}

func NewSubqueryExpression(key NodeKey, query Query) (*SubqueryExpression, error) {
	if !validNodeKey(key) || query.body == nil {
		return nil, fmt.Errorf("invalid subquery expression")
	}
	return &SubqueryExpression{expressionBase: expressionBase{key: key}, query: query.clone()}, nil
}
func (*SubqueryExpression) expressionNode()         {}
func (*SubqueryExpression) Kind() ExpressionKind    { return ExpressionSubquery }
func (expression *SubqueryExpression) Query() Query { return expression.query.clone() }
func (expression *SubqueryExpression) Accept(visitor ExpressionVisitor) error {
	return visitor.VisitSubqueryExpression(expression)
}
func (expression *SubqueryExpression) writeSemantic(builder *fingerprintBuilder) {
	builder.token("subquery-expression")
	expression.query.writeSemantic(builder)
}

func expressionIsNil(expression Expression) bool { return expression == nil }

func clonePath(path IdentifierPath) IdentifierPath {
	return IdentifierPath{parts: append([]Identifier(nil), path.parts...)}
}
