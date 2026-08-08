package ast

import (
	"fmt"
	"strconv"
)

type RelationKind string

const (
	RelationTable    RelationKind = "TABLE"
	RelationSubquery RelationKind = "SUBQUERY"
	RelationJoin     RelationKind = "JOIN"
	RelationUnnest   RelationKind = "UNNEST"
)

type Relation interface {
	Kind() RelationKind
	NodeKey() NodeKey
	Span() Span
	Accept(RelationVisitor) error
	relationNode()
	semanticWriter
}

type RelationVisitor interface {
	VisitTableRelation(*TableRelation) error
	VisitSubqueryRelation(*SubqueryRelation) error
	VisitJoinRelation(*JoinRelation) error
	VisitUnnestRelation(*UnnestRelation) error
}

type relationBase struct{ key NodeKey }

func (base relationBase) NodeKey() NodeKey { return base.key }
func (base relationBase) Span() Span       { return base.key.span }

// TableRelation contains an unresolved syntax path. A later analyzer must
// decide whether it names a CTE or resolve it to a canonical table reference.
type TableRelation struct {
	relationBase
	path  IdentifierPath
	alias *Identifier
}

func NewTableRelation(key NodeKey, path IdentifierPath, alias *Identifier) (*TableRelation, error) {
	if !validNodeKey(key) || path.Len() == 0 {
		return nil, fmt.Errorf("table relation path is empty")
	}
	return &TableRelation{relationBase: relationBase{key: key}, path: clonePath(path), alias: cloneIdentifier(alias)}, nil
}
func (*TableRelation) relationNode()                 {}
func (*TableRelation) Kind() RelationKind            { return RelationTable }
func (relation *TableRelation) Path() IdentifierPath { return clonePath(relation.path) }
func (relation *TableRelation) Alias() *Identifier   { return cloneIdentifier(relation.alias) }
func (relation *TableRelation) Accept(visitor RelationVisitor) error {
	return visitor.VisitTableRelation(relation)
}
func (relation *TableRelation) writeSemantic(builder *fingerprintBuilder) {
	builder.token("table-relation")
	writePath(builder, relation.path)
	writeOptionalIdentifier(builder, relation.alias)
}

type SubqueryRelation struct {
	relationBase
	query Query
	alias *Identifier
}

func NewSubqueryRelation(key NodeKey, query Query, alias *Identifier) (*SubqueryRelation, error) {
	if !validNodeKey(key) || query.body == nil {
		return nil, fmt.Errorf("subquery relation query is empty")
	}
	return &SubqueryRelation{relationBase: relationBase{key: key}, query: query.clone(), alias: cloneIdentifier(alias)}, nil
}
func (*SubqueryRelation) relationNode()               {}
func (*SubqueryRelation) Kind() RelationKind          { return RelationSubquery }
func (relation *SubqueryRelation) Query() Query       { return relation.query.clone() }
func (relation *SubqueryRelation) Alias() *Identifier { return cloneIdentifier(relation.alias) }
func (relation *SubqueryRelation) Accept(visitor RelationVisitor) error {
	return visitor.VisitSubqueryRelation(relation)
}
func (relation *SubqueryRelation) writeSemantic(builder *fingerprintBuilder) {
	builder.token("subquery-relation")
	relation.query.writeSemantic(builder)
	writeOptionalIdentifier(builder, relation.alias)
}

type JoinType string

const (
	JoinComma JoinType = "COMMA"
	JoinCross JoinType = "CROSS"
	JoinInner JoinType = "INNER"
	JoinLeft  JoinType = "LEFT"
	JoinRight JoinType = "RIGHT"
	JoinFull  JoinType = "FULL"
)

type JoinConditionKind string

const (
	JoinConditionNone  JoinConditionKind = "NONE"
	JoinConditionOn    JoinConditionKind = "ON"
	JoinConditionUsing JoinConditionKind = "USING"
)

type JoinCondition struct {
	kind    JoinConditionKind
	on      Expression
	columns []Identifier
}

func NewJoinOn(expression Expression) (JoinCondition, error) {
	if expressionIsNil(expression) {
		return JoinCondition{}, fmt.Errorf("join ON expression is required")
	}
	return JoinCondition{kind: JoinConditionOn, on: expression}, nil
}

func NewJoinUsing(columns []Identifier) (JoinCondition, error) {
	if len(columns) == 0 {
		return JoinCondition{}, fmt.Errorf("join USING columns are empty")
	}
	return JoinCondition{kind: JoinConditionUsing, columns: append([]Identifier(nil), columns...)}, nil
}

func NewJoinWithoutCondition() JoinCondition            { return JoinCondition{kind: JoinConditionNone} }
func (condition JoinCondition) Kind() JoinConditionKind { return condition.kind }
func (condition JoinCondition) On() Expression          { return condition.on }
func (condition JoinCondition) Columns() []Identifier {
	return append([]Identifier(nil), condition.columns...)
}

type JoinRelation struct {
	relationBase
	type_     JoinType
	left      Relation
	right     Relation
	condition JoinCondition
}

func NewJoinRelation(key NodeKey, typ JoinType, left, right Relation, condition JoinCondition) (*JoinRelation, error) {
	if !validNodeKey(key) || left == nil || right == nil {
		return nil, fmt.Errorf("join operands are required")
	}
	switch typ {
	case JoinComma, JoinCross:
		if condition.kind != JoinConditionNone {
			return nil, fmt.Errorf("comma and cross joins cannot have a condition")
		}
	case JoinInner, JoinLeft, JoinRight, JoinFull:
		if condition.kind != JoinConditionOn && condition.kind != JoinConditionUsing {
			return nil, fmt.Errorf("qualified join condition is required")
		}
	default:
		return nil, fmt.Errorf("invalid join type")
	}
	return &JoinRelation{relationBase: relationBase{key: key}, type_: typ, left: left, right: right, condition: condition}, nil
}
func (*JoinRelation) relationNode()            {}
func (*JoinRelation) Kind() RelationKind       { return RelationJoin }
func (relation *JoinRelation) Type() JoinType  { return relation.type_ }
func (relation *JoinRelation) Left() Relation  { return relation.left }
func (relation *JoinRelation) Right() Relation { return relation.right }
func (relation *JoinRelation) Condition() JoinCondition {
	condition := relation.condition
	condition.columns = append([]Identifier(nil), condition.columns...)
	return condition
}
func (relation *JoinRelation) Accept(visitor RelationVisitor) error {
	return visitor.VisitJoinRelation(relation)
}
func (relation *JoinRelation) writeSemantic(builder *fingerprintBuilder) {
	builder.token("join-relation")
	builder.token(string(relation.type_))
	relation.left.writeSemantic(builder)
	relation.right.writeSemantic(builder)
	builder.token(string(relation.condition.kind))
	if relation.condition.on != nil {
		relation.condition.on.writeSemantic(builder)
	}
	builder.token(strconv.Itoa(len(relation.condition.columns)))
	for _, column := range relation.condition.columns {
		writeIdentifier(builder, column)
	}
}

type UnnestRelation struct {
	relationBase
	expression Expression
	alias      *Identifier
}

func NewUnnestRelation(key NodeKey, expression Expression, alias *Identifier) (*UnnestRelation, error) {
	if !validNodeKey(key) || expressionIsNil(expression) {
		return nil, fmt.Errorf("UNNEST expression is required")
	}
	return &UnnestRelation{relationBase: relationBase{key: key}, expression: expression, alias: cloneIdentifier(alias)}, nil
}
func (*UnnestRelation) relationNode()                   {}
func (*UnnestRelation) Kind() RelationKind              { return RelationUnnest }
func (relation *UnnestRelation) Expression() Expression { return relation.expression }
func (relation *UnnestRelation) Alias() *Identifier     { return cloneIdentifier(relation.alias) }
func (relation *UnnestRelation) Accept(visitor RelationVisitor) error {
	return visitor.VisitUnnestRelation(relation)
}
func (relation *UnnestRelation) writeSemantic(builder *fingerprintBuilder) {
	builder.token("unnest-relation")
	relation.expression.writeSemantic(builder)
	writeOptionalIdentifier(builder, relation.alias)
}

type QueryBodyKind string

const (
	QueryBodySelect QueryBodyKind = "SELECT"
	QueryBodySet    QueryBodyKind = "SET_OPERATION"
)

type QueryBody interface {
	Kind() QueryBodyKind
	Accept(QueryBodyVisitor) error
	queryBodyNode()
	semanticWriter
}

type QueryBodyVisitor interface {
	VisitSelectQuery(*SelectQuery) error
	VisitSetOperationQuery(*SetOperationQuery) error
}

type SelectItem struct {
	expression Expression
	alias      *Identifier
}

func NewSelectItem(expression Expression, alias *Identifier) (SelectItem, error) {
	if expressionIsNil(expression) {
		return SelectItem{}, fmt.Errorf("select item expression is required")
	}
	return SelectItem{expression: expression, alias: cloneIdentifier(alias)}, nil
}
func (item SelectItem) Expression() Expression { return item.expression }
func (item SelectItem) Alias() *Identifier     { return cloneIdentifier(item.alias) }

type SelectQuery struct {
	distinct bool
	items    []SelectItem
	from     Relation
	where    Expression
	groupBy  []Expression
	having   Expression
	qualify  Expression
}

func NewSelectQuery(distinct bool, items []SelectItem, from Relation, where Expression, groupBy []Expression, having, qualify Expression) (*SelectQuery, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("select list is empty")
	}
	clonedItems := append([]SelectItem(nil), items...)
	clonedGroup := append([]Expression(nil), groupBy...)
	for _, item := range clonedItems {
		if expressionIsNil(item.expression) {
			return nil, fmt.Errorf("select item expression is required")
		}
	}
	for _, expression := range clonedGroup {
		if expressionIsNil(expression) {
			return nil, fmt.Errorf("group by expression is nil")
		}
	}
	return &SelectQuery{distinct: distinct, items: clonedItems, from: from, where: where, groupBy: clonedGroup, having: having, qualify: qualify}, nil
}
func (*SelectQuery) queryBodyNode()              {}
func (*SelectQuery) Kind() QueryBodyKind         { return QueryBodySelect }
func (query *SelectQuery) Distinct() bool        { return query.distinct }
func (query *SelectQuery) Items() []SelectItem   { return append([]SelectItem(nil), query.items...) }
func (query *SelectQuery) From() Relation        { return query.from }
func (query *SelectQuery) Where() Expression     { return query.where }
func (query *SelectQuery) GroupBy() []Expression { return append([]Expression(nil), query.groupBy...) }
func (query *SelectQuery) Having() Expression    { return query.having }
func (query *SelectQuery) Qualify() Expression   { return query.qualify }
func (query *SelectQuery) Accept(visitor QueryBodyVisitor) error {
	return visitor.VisitSelectQuery(query)
}
func (query *SelectQuery) writeSemantic(builder *fingerprintBuilder) {
	builder.token("select-query")
	builder.boolean(query.distinct)
	builder.token(strconv.Itoa(len(query.items)))
	for _, item := range query.items {
		item.expression.writeSemantic(builder)
		writeOptionalIdentifier(builder, item.alias)
	}
	writeOptionalSemantic(builder, query.from)
	writeOptionalSemantic(builder, query.where)
	builder.token(strconv.Itoa(len(query.groupBy)))
	for _, expression := range query.groupBy {
		expression.writeSemantic(builder)
	}
	writeOptionalSemantic(builder, query.having)
	writeOptionalSemantic(builder, query.qualify)
}

type SetOperation string

const (
	SetUnion     SetOperation = "UNION"
	SetIntersect SetOperation = "INTERSECT"
	SetExcept    SetOperation = "EXCEPT"
)

type SetOperationQuery struct {
	operator SetOperation
	all      bool
	left     QueryBody
	right    QueryBody
}

func NewSetOperationQuery(operator SetOperation, all bool, left, right QueryBody) (*SetOperationQuery, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("set operation operands are required")
	}
	switch operator {
	case SetUnion, SetIntersect, SetExcept:
	default:
		return nil, fmt.Errorf("invalid set operator")
	}
	return &SetOperationQuery{operator: operator, all: all, left: left, right: right}, nil
}
func (*SetOperationQuery) queryBodyNode()               {}
func (*SetOperationQuery) Kind() QueryBodyKind          { return QueryBodySet }
func (query *SetOperationQuery) Operator() SetOperation { return query.operator }
func (query *SetOperationQuery) All() bool              { return query.all }
func (query *SetOperationQuery) Left() QueryBody        { return query.left }
func (query *SetOperationQuery) Right() QueryBody       { return query.right }
func (query *SetOperationQuery) Accept(visitor QueryBodyVisitor) error {
	return visitor.VisitSetOperationQuery(query)
}
func (query *SetOperationQuery) writeSemantic(builder *fingerprintBuilder) {
	builder.token("set-query")
	builder.token(string(query.operator))
	builder.boolean(query.all)
	query.left.writeSemantic(builder)
	query.right.writeSemantic(builder)
}

type CommonTableExpression struct {
	name    Identifier
	columns []Identifier
	query   Query
}

func NewCommonTableExpression(name Identifier, columns []Identifier, query Query) (CommonTableExpression, error) {
	if name.value == "" || query.body == nil {
		return CommonTableExpression{}, fmt.Errorf("invalid common table expression")
	}
	return CommonTableExpression{name: name, columns: append([]Identifier(nil), columns...), query: query.clone()}, nil
}
func (expression CommonTableExpression) Name() Identifier { return expression.name }
func (expression CommonTableExpression) Columns() []Identifier {
	return append([]Identifier(nil), expression.columns...)
}
func (expression CommonTableExpression) Query() Query { return expression.query.clone() }

type SortDirection string
type NullOrdering string

const (
	SortDefault         SortDirection = "DEFAULT"
	SortAscending       SortDirection = "ASC"
	SortDescending      SortDirection = "DESC"
	NullOrderingDefault NullOrdering  = "DEFAULT"
	NullsFirst          NullOrdering  = "FIRST"
	NullsLast           NullOrdering  = "LAST"
)

type OrderItem struct {
	expression Expression
	direction  SortDirection
	nulls      NullOrdering
}

func NewOrderItem(expression Expression, direction SortDirection, nulls NullOrdering) (OrderItem, error) {
	if expressionIsNil(expression) {
		return OrderItem{}, fmt.Errorf("order expression is required")
	}
	return OrderItem{expression: expression, direction: direction, nulls: nulls}, nil
}
func (item OrderItem) Expression() Expression     { return item.expression }
func (item OrderItem) Direction() SortDirection   { return item.direction }
func (item OrderItem) NullOrdering() NullOrdering { return item.nulls }

type Query struct {
	with      []CommonTableExpression
	recursive bool
	body      QueryBody
	orderBy   []OrderItem
	limit     *int64
	offset    *int64
}

func NewQuery(with []CommonTableExpression, recursive bool, body QueryBody, orderBy []OrderItem, limit, offset *int64) (Query, error) {
	if body == nil {
		return Query{}, fmt.Errorf("query body is required")
	}
	if limit != nil && *limit < 0 || offset != nil && *offset < 0 {
		return Query{}, fmt.Errorf("query limit and offset must be non-negative")
	}
	return Query{with: append([]CommonTableExpression(nil), with...), recursive: recursive, body: body,
		orderBy: append([]OrderItem(nil), orderBy...), limit: cloneInt64(limit), offset: cloneInt64(offset)}, nil
}
func (query Query) With() []CommonTableExpression {
	return append([]CommonTableExpression(nil), query.with...)
}
func (query Query) Recursive() bool      { return query.recursive }
func (query Query) Body() QueryBody      { return query.body }
func (query Query) OrderBy() []OrderItem { return append([]OrderItem(nil), query.orderBy...) }
func (query Query) Limit() *int64        { return cloneInt64(query.limit) }
func (query Query) Offset() *int64       { return cloneInt64(query.offset) }
func (query Query) clone() Query {
	return Query{with: append([]CommonTableExpression(nil), query.with...), recursive: query.recursive, body: query.body,
		orderBy: append([]OrderItem(nil), query.orderBy...), limit: cloneInt64(query.limit), offset: cloneInt64(query.offset)}
}
func (query Query) writeSemantic(builder *fingerprintBuilder) {
	builder.token("query")
	builder.boolean(query.recursive)
	builder.token(strconv.Itoa(len(query.with)))
	for _, expression := range query.with {
		writeIdentifier(builder, expression.name)
		builder.token(strconv.Itoa(len(expression.columns)))
		for _, column := range expression.columns {
			writeIdentifier(builder, column)
		}
		expression.query.writeSemantic(builder)
	}
	query.body.writeSemantic(builder)
	builder.token(strconv.Itoa(len(query.orderBy)))
	for _, item := range query.orderBy {
		item.expression.writeSemantic(builder)
		builder.token(string(item.direction))
		builder.token(string(item.nulls))
	}
	writeOptionalInt64(builder, query.limit)
	writeOptionalInt64(builder, query.offset)
}

func writeOptionalIdentifier(builder *fingerprintBuilder, identifier *Identifier) {
	if identifier == nil {
		builder.token("")
		return
	}
	writeIdentifier(builder, *identifier)
}

func writeOptionalSemantic(builder *fingerprintBuilder, value semanticWriter) {
	if value == nil {
		builder.token("")
		return
	}
	value.writeSemantic(builder)
}

func writeOptionalInt64(builder *fingerprintBuilder, value *int64) {
	if value == nil {
		builder.token("")
		return
	}
	builder.token(strconv.FormatInt(*value, 10))
}
