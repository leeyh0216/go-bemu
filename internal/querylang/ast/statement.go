package ast

import (
	"fmt"
	"strconv"
)

type StatementKind string

const (
	StatementScript        StatementKind = "SCRIPT"
	StatementDeclare       StatementKind = "DECLARE"
	StatementSet           StatementKind = "SET"
	StatementSelect        StatementKind = "SELECT"
	StatementInsert        StatementKind = "INSERT"
	StatementUpdate        StatementKind = "UPDATE"
	StatementDelete        StatementKind = "DELETE"
	StatementMerge         StatementKind = "MERGE"
	StatementCreateTable   StatementKind = "CREATE_TABLE"
	StatementDropTable     StatementKind = "DROP_TABLE"
	StatementCreateView    StatementKind = "CREATE_VIEW"
	StatementDropView      StatementKind = "DROP_VIEW"
	StatementAlterTable    StatementKind = "ALTER_TABLE"
	StatementTruncateTable StatementKind = "TRUNCATE_TABLE"
)

type Statement interface {
	Kind() StatementKind
	Source() Source
	SemanticFingerprint() string
	Accept(StatementVisitor) error
	statementNode()
	semanticWriter
}

type StatementVisitor interface {
	VisitScript(*ScriptStatement) error
	VisitDeclare(*DeclareStatement) error
	VisitSet(*SetStatement) error
	VisitSelect(*SelectStatement) error
	VisitInsert(*InsertStatement) error
	VisitUpdate(*UpdateStatement) error
	VisitDelete(*DeleteStatement) error
	VisitMerge(*MergeStatement) error
	VisitCreateTable(*CreateTableStatement) error
	VisitDropTable(*DropTableStatement) error
	VisitCreateView(*CreateViewStatement) error
	VisitDropView(*DropViewStatement) error
	VisitAlterTable(*AlterTableStatement) error
	VisitTruncateTable(*TruncateTableStatement) error
}

type statementBase struct {
	source      Source
	fingerprint string
}

func (base statementBase) Source() Source              { return base.source }
func (base statementBase) SemanticFingerprint() string { return base.fingerprint }

func validSource(source Source) bool {
	return digestPattern.MatchString(source.digest) && source.span.end >= source.span.start
}

type ScriptStatement struct {
	statementBase
	statements []Statement
}

func NewScriptStatement(source Source, statements []Statement) (*ScriptStatement, error) {
	if !validSource(source) || len(statements) == 0 {
		return nil, fmt.Errorf("invalid script statement")
	}
	cloned := append([]Statement(nil), statements...)
	for _, statement := range cloned {
		if statement == nil || statement.Kind() == StatementScript {
			return nil, fmt.Errorf("script child statement is invalid")
		}
	}
	value := &ScriptStatement{statementBase: statementBase{source: source}, statements: cloned}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*ScriptStatement) statementNode()      {}
func (*ScriptStatement) Kind() StatementKind { return StatementScript }
func (statement *ScriptStatement) Statements() []Statement {
	return append([]Statement(nil), statement.statements...)
}
func (statement *ScriptStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitScript(statement)
}
func (statement *ScriptStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("script")
	builder.token(strconv.Itoa(len(statement.statements)))
	for _, child := range statement.statements {
		child.writeSemantic(builder)
	}
}

type DeclareStatement struct {
	statementBase
	variables    []Identifier
	type_        Type
	defaultValue Expression
}

func NewDeclareStatement(source Source, variables []Identifier, typ Type, defaultValue Expression) (*DeclareStatement, error) {
	if !validSource(source) || len(variables) == 0 || typ == nil && defaultValue == nil {
		return nil, fmt.Errorf("invalid DECLARE statement")
	}
	value := &DeclareStatement{statementBase: statementBase{source: source}, variables: append([]Identifier(nil), variables...), type_: typ, defaultValue: defaultValue}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*DeclareStatement) statementNode()      {}
func (*DeclareStatement) Kind() StatementKind { return StatementDeclare }
func (statement *DeclareStatement) Variables() []Identifier {
	return append([]Identifier(nil), statement.variables...)
}
func (statement *DeclareStatement) Type() Type               { return statement.type_ }
func (statement *DeclareStatement) DefaultValue() Expression { return statement.defaultValue }
func (statement *DeclareStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitDeclare(statement)
}
func (statement *DeclareStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("declare-statement")
	builder.token(strconv.Itoa(len(statement.variables)))
	for _, variable := range statement.variables {
		writeIdentifier(builder, variable)
	}
	writeOptionalSemantic(builder, statement.type_)
	writeOptionalSemantic(builder, statement.defaultValue)
}

type SetStatement struct {
	statementBase
	target IdentifierPath
	value  Expression
}

func NewSetStatement(source Source, target IdentifierPath, value Expression) (*SetStatement, error) {
	if !validSource(source) || target.Len() == 0 || expressionIsNil(value) {
		return nil, fmt.Errorf("invalid SET statement")
	}
	statement := &SetStatement{statementBase: statementBase{source: source}, target: clonePath(target), value: value}
	statement.fingerprint = semanticFingerprint(statement)
	return statement, nil
}
func (*SetStatement) statementNode()                   {}
func (*SetStatement) Kind() StatementKind              { return StatementSet }
func (statement *SetStatement) Target() IdentifierPath { return clonePath(statement.target) }
func (statement *SetStatement) Value() Expression      { return statement.value }
func (statement *SetStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitSet(statement)
}
func (statement *SetStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("set-statement")
	writePath(builder, statement.target)
	statement.value.writeSemantic(builder)
}

type SelectStatement struct {
	statementBase
	query Query
}

func NewSelectStatement(source Source, query Query) (*SelectStatement, error) {
	if !validSource(source) || query.body == nil {
		return nil, fmt.Errorf("invalid SELECT statement")
	}
	value := &SelectStatement{statementBase: statementBase{source: source}, query: query.clone()}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*SelectStatement) statementNode()         {}
func (*SelectStatement) Kind() StatementKind    { return StatementSelect }
func (statement *SelectStatement) Query() Query { return statement.query.clone() }
func (statement *SelectStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitSelect(statement)
}
func (statement *SelectStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("select-statement")
	statement.query.writeSemantic(builder)
}

type InsertSourceKind string

const (
	InsertValues InsertSourceKind = "VALUES"
	InsertQuery  InsertSourceKind = "QUERY"
)

type InsertStatement struct {
	statementBase
	target  *TableRelation
	columns []Identifier
	rows    [][]Expression
	query   *Query
}

func NewInsertValuesStatement(source Source, target *TableRelation, columns []Identifier, rows [][]Expression) (*InsertStatement, error) {
	if !validSource(source) || target == nil || len(rows) == 0 {
		return nil, fmt.Errorf("invalid INSERT VALUES statement")
	}
	clonedRows := cloneExpressionRows(rows)
	for _, row := range clonedRows {
		if len(row) == 0 {
			return nil, fmt.Errorf("INSERT VALUES row is empty")
		}
		for _, expression := range row {
			if expressionIsNil(expression) {
				return nil, fmt.Errorf("INSERT VALUES expression is nil")
			}
		}
	}
	value := &InsertStatement{statementBase: statementBase{source: source}, target: target,
		columns: append([]Identifier(nil), columns...), rows: clonedRows}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}

func NewInsertQueryStatement(source Source, target *TableRelation, columns []Identifier, query Query) (*InsertStatement, error) {
	if !validSource(source) || target == nil || query.body == nil {
		return nil, fmt.Errorf("invalid INSERT query statement")
	}
	clonedQuery := query.clone()
	value := &InsertStatement{statementBase: statementBase{source: source}, target: target,
		columns: append([]Identifier(nil), columns...), query: &clonedQuery}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*InsertStatement) statementNode()                   {}
func (*InsertStatement) Kind() StatementKind              { return StatementInsert }
func (statement *InsertStatement) Target() *TableRelation { return statement.target }
func (statement *InsertStatement) Columns() []Identifier {
	return append([]Identifier(nil), statement.columns...)
}
func (statement *InsertStatement) SourceKind() InsertSourceKind {
	if statement.query != nil {
		return InsertQuery
	}
	return InsertValues
}
func (statement *InsertStatement) Rows() [][]Expression { return cloneExpressionRows(statement.rows) }
func (statement *InsertStatement) Query() *Query {
	if statement.query == nil {
		return nil
	}
	cloned := statement.query.clone()
	return &cloned
}
func (statement *InsertStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitInsert(statement)
}
func (statement *InsertStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("insert-statement")
	statement.target.writeSemantic(builder)
	builder.token(strconv.Itoa(len(statement.columns)))
	for _, column := range statement.columns {
		writeIdentifier(builder, column)
	}
	if statement.query != nil {
		builder.token("query")
		statement.query.writeSemantic(builder)
		return
	}
	builder.token("values")
	builder.token(strconv.Itoa(len(statement.rows)))
	for _, row := range statement.rows {
		builder.token(strconv.Itoa(len(row)))
		for _, expression := range row {
			expression.writeSemantic(builder)
		}
	}
}

type Assignment struct {
	target IdentifierPath
	value  Expression
}

func NewAssignment(target IdentifierPath, value Expression) (Assignment, error) {
	if target.Len() == 0 || expressionIsNil(value) {
		return Assignment{}, fmt.Errorf("invalid assignment")
	}
	return Assignment{target: clonePath(target), value: value}, nil
}
func (assignment Assignment) Target() IdentifierPath { return clonePath(assignment.target) }
func (assignment Assignment) Value() Expression      { return assignment.value }

type UpdateStatement struct {
	statementBase
	target      *TableRelation
	assignments []Assignment
	from        Relation
	where       Expression
}

func NewUpdateStatement(source Source, target *TableRelation, assignments []Assignment, from Relation, where Expression) (*UpdateStatement, error) {
	if !validSource(source) || target == nil || len(assignments) == 0 {
		return nil, fmt.Errorf("invalid UPDATE statement")
	}
	value := &UpdateStatement{statementBase: statementBase{source: source}, target: target,
		assignments: append([]Assignment(nil), assignments...), from: from, where: where}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*UpdateStatement) statementNode()                   {}
func (*UpdateStatement) Kind() StatementKind              { return StatementUpdate }
func (statement *UpdateStatement) Target() *TableRelation { return statement.target }
func (statement *UpdateStatement) Assignments() []Assignment {
	return append([]Assignment(nil), statement.assignments...)
}
func (statement *UpdateStatement) From() Relation    { return statement.from }
func (statement *UpdateStatement) Where() Expression { return statement.where }
func (statement *UpdateStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitUpdate(statement)
}
func (statement *UpdateStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("update-statement")
	statement.target.writeSemantic(builder)
	builder.token(strconv.Itoa(len(statement.assignments)))
	for _, assignment := range statement.assignments {
		writePath(builder, assignment.target)
		assignment.value.writeSemantic(builder)
	}
	writeOptionalSemantic(builder, statement.from)
	writeOptionalSemantic(builder, statement.where)
}

type DeleteStatement struct {
	statementBase
	target *TableRelation
	where  Expression
}

func NewDeleteStatement(source Source, target *TableRelation, where Expression) (*DeleteStatement, error) {
	if !validSource(source) || target == nil {
		return nil, fmt.Errorf("invalid DELETE statement")
	}
	value := &DeleteStatement{statementBase: statementBase{source: source}, target: target, where: where}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*DeleteStatement) statementNode()                   {}
func (*DeleteStatement) Kind() StatementKind              { return StatementDelete }
func (statement *DeleteStatement) Target() *TableRelation { return statement.target }
func (statement *DeleteStatement) Where() Expression      { return statement.where }
func (statement *DeleteStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitDelete(statement)
}
func (statement *DeleteStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("delete-statement")
	statement.target.writeSemantic(builder)
	writeOptionalSemantic(builder, statement.where)
}

type MergeMatchKind string
type MergeActionKind string

const (
	MergeMatched            MergeMatchKind  = "MATCHED"
	MergeNotMatchedByTarget MergeMatchKind  = "NOT_MATCHED_BY_TARGET"
	MergeNotMatchedBySource MergeMatchKind  = "NOT_MATCHED_BY_SOURCE"
	MergeActionInsert       MergeActionKind = "INSERT"
	MergeActionInsertRow    MergeActionKind = "INSERT_ROW"
	MergeActionUpdate       MergeActionKind = "UPDATE"
	MergeActionDelete       MergeActionKind = "DELETE"
)

type MergeAction struct {
	kind        MergeActionKind
	columns     []Identifier
	values      []Expression
	assignments []Assignment
}

func NewMergeInsertAction(columns []Identifier, values []Expression) (MergeAction, error) {
	if len(values) == 0 || len(columns) != 0 && len(columns) != len(values) {
		return MergeAction{}, fmt.Errorf("invalid MERGE INSERT action")
	}
	for _, value := range values {
		if expressionIsNil(value) {
			return MergeAction{}, fmt.Errorf("MERGE INSERT value is nil")
		}
	}
	return MergeAction{kind: MergeActionInsert, columns: append([]Identifier(nil), columns...), values: append([]Expression(nil), values...)}, nil
}

func NewMergeInsertRowAction() MergeAction { return MergeAction{kind: MergeActionInsertRow} }

func NewMergeUpdateAction(assignments []Assignment) (MergeAction, error) {
	if len(assignments) == 0 {
		return MergeAction{}, fmt.Errorf("MERGE UPDATE assignments are empty")
	}
	return MergeAction{kind: MergeActionUpdate, assignments: append([]Assignment(nil), assignments...)}, nil
}

func NewMergeDeleteAction() MergeAction { return MergeAction{kind: MergeActionDelete} }

func (action MergeAction) Kind() MergeActionKind { return action.kind }
func (action MergeAction) Columns() []Identifier { return append([]Identifier(nil), action.columns...) }
func (action MergeAction) Values() []Expression  { return append([]Expression(nil), action.values...) }
func (action MergeAction) Assignments() []Assignment {
	return append([]Assignment(nil), action.assignments...)
}

type MergeWhen struct {
	match     MergeMatchKind
	condition Expression
	action    MergeAction
}

func NewMergeWhen(match MergeMatchKind, condition Expression, action MergeAction) (MergeWhen, error) {
	switch match {
	case MergeMatched, MergeNotMatchedByTarget, MergeNotMatchedBySource:
	default:
		return MergeWhen{}, fmt.Errorf("invalid MERGE match kind")
	}
	if action.kind == "" {
		return MergeWhen{}, fmt.Errorf("MERGE action is required")
	}
	return MergeWhen{match: match, condition: condition, action: action}, nil
}

func (when MergeWhen) Match() MergeMatchKind { return when.match }
func (when MergeWhen) Condition() Expression { return when.condition }
func (when MergeWhen) Action() MergeAction   { return when.action }

type MergeStatement struct {
	statementBase
	target    *TableRelation
	source    Relation
	condition Expression
	when      []MergeWhen
}

func NewMergeStatement(source Source, target *TableRelation, relation Relation, condition Expression, when []MergeWhen) (*MergeStatement, error) {
	if !validSource(source) || target == nil || relation == nil || expressionIsNil(condition) || len(when) == 0 {
		return nil, fmt.Errorf("invalid MERGE statement")
	}
	value := &MergeStatement{statementBase: statementBase{source: source}, target: target,
		source: relation, condition: condition, when: append([]MergeWhen(nil), when...)}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*MergeStatement) statementNode()                   {}
func (*MergeStatement) Kind() StatementKind              { return StatementMerge }
func (statement *MergeStatement) Target() *TableRelation { return statement.target }
func (statement *MergeStatement) MergeSource() Relation  { return statement.source }
func (statement *MergeStatement) Condition() Expression  { return statement.condition }
func (statement *MergeStatement) When() []MergeWhen {
	return append([]MergeWhen(nil), statement.when...)
}
func (statement *MergeStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitMerge(statement)
}
func (statement *MergeStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("merge-statement")
	statement.target.writeSemantic(builder)
	statement.source.writeSemantic(builder)
	statement.condition.writeSemantic(builder)
	builder.token(strconv.Itoa(len(statement.when)))
	for _, when := range statement.when {
		builder.token(string(when.match))
		writeOptionalSemantic(builder, when.condition)
		builder.token(string(when.action.kind))
		builder.token(strconv.Itoa(len(when.action.columns)))
		for _, column := range when.action.columns {
			writeIdentifier(builder, column)
		}
		builder.token(strconv.Itoa(len(when.action.values)))
		for _, value := range when.action.values {
			value.writeSemantic(builder)
		}
		builder.token(strconv.Itoa(len(when.action.assignments)))
		for _, assignment := range when.action.assignments {
			writePath(builder, assignment.target)
			assignment.value.writeSemantic(builder)
		}
	}
}

type ColumnDefinition struct {
	name    Identifier
	type_   Type
	notNull bool
}

func NewColumnDefinition(name Identifier, typ Type, notNull bool) (ColumnDefinition, error) {
	if name.value == "" || typeIsNil(typ) {
		return ColumnDefinition{}, fmt.Errorf("invalid column definition")
	}
	return ColumnDefinition{name: name, type_: typ, notNull: notNull}, nil
}
func (column ColumnDefinition) Name() Identifier { return column.name }
func (column ColumnDefinition) Type() Type       { return column.type_ }
func (column ColumnDefinition) NotNull() bool    { return column.notNull }

type CreateTableStatement struct {
	statementBase
	target  *TableRelation
	columns []ColumnDefinition
}

func NewCreateTableStatement(source Source, target *TableRelation, columns []ColumnDefinition) (*CreateTableStatement, error) {
	if !validSource(source) || target == nil || len(columns) == 0 {
		return nil, fmt.Errorf("invalid CREATE TABLE statement")
	}
	value := &CreateTableStatement{statementBase: statementBase{source: source}, target: target, columns: append([]ColumnDefinition(nil), columns...)}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*CreateTableStatement) statementNode()                   {}
func (*CreateTableStatement) Kind() StatementKind              { return StatementCreateTable }
func (statement *CreateTableStatement) Target() *TableRelation { return statement.target }
func (statement *CreateTableStatement) Columns() []ColumnDefinition {
	return append([]ColumnDefinition(nil), statement.columns...)
}
func (statement *CreateTableStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitCreateTable(statement)
}
func (statement *CreateTableStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("create-table")
	statement.target.writeSemantic(builder)
	builder.token(strconv.Itoa(len(statement.columns)))
	for _, column := range statement.columns {
		writeIdentifier(builder, column.name)
		column.type_.writeSemantic(builder)
		builder.boolean(column.notNull)
	}
}

type DropTableStatement struct {
	statementBase
	target *TableRelation
}

// CreateViewStatement keeps the view body as a typed GoogleSQL query AST so
// consumers never need to parse the definition text themselves.
type CreateViewStatement struct {
	statementBase
	target      *TableRelation
	query       Query
	querySource Source
	orReplace   bool
}

func NewCreateViewStatement(source Source, target *TableRelation, query Query, querySource Source, orReplace bool) (*CreateViewStatement, error) {
	if !validSource(source) || !validSource(querySource) || querySource.digest != source.digest || target == nil || query.body == nil {
		return nil, fmt.Errorf("invalid CREATE VIEW statement")
	}
	value := &CreateViewStatement{statementBase: statementBase{source: source}, target: target, query: query.clone(), querySource: querySource, orReplace: orReplace}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*CreateViewStatement) statementNode()                   {}
func (*CreateViewStatement) Kind() StatementKind              { return StatementCreateView }
func (statement *CreateViewStatement) Target() *TableRelation { return statement.target }
func (statement *CreateViewStatement) Query() Query           { return statement.query.clone() }
func (statement *CreateViewStatement) QuerySource() Source    { return statement.querySource }
func (statement *CreateViewStatement) OrReplace() bool        { return statement.orReplace }
func (statement *CreateViewStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitCreateView(statement)
}
func (statement *CreateViewStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("create-view")
	statement.target.writeSemantic(builder)
	statement.query.writeSemantic(builder)
	builder.boolean(statement.orReplace)
}

type DropViewStatement struct {
	statementBase
	target *TableRelation
}

func NewDropViewStatement(source Source, target *TableRelation) (*DropViewStatement, error) {
	if !validSource(source) || target == nil {
		return nil, fmt.Errorf("invalid DROP VIEW statement")
	}
	value := &DropViewStatement{statementBase: statementBase{source: source}, target: target}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*DropViewStatement) statementNode()                   {}
func (*DropViewStatement) Kind() StatementKind              { return StatementDropView }
func (statement *DropViewStatement) Target() *TableRelation { return statement.target }
func (statement *DropViewStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitDropView(statement)
}
func (statement *DropViewStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("drop-view")
	statement.target.writeSemantic(builder)
}

func NewDropTableStatement(source Source, target *TableRelation) (*DropTableStatement, error) {
	if !validSource(source) || target == nil {
		return nil, fmt.Errorf("invalid DROP TABLE statement")
	}
	value := &DropTableStatement{statementBase: statementBase{source: source}, target: target}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*DropTableStatement) statementNode()                   {}
func (*DropTableStatement) Kind() StatementKind              { return StatementDropTable }
func (statement *DropTableStatement) Target() *TableRelation { return statement.target }
func (statement *DropTableStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitDropTable(statement)
}
func (statement *DropTableStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("drop-table")
	statement.target.writeSemantic(builder)
}

type AlterActionKind string

const (
	AlterAddColumn    AlterActionKind = "ADD_COLUMN"
	AlterDropColumn   AlterActionKind = "DROP_COLUMN"
	AlterRenameColumn AlterActionKind = "RENAME_COLUMN"
	AlterColumnType   AlterActionKind = "ALTER_COLUMN_TYPE"
)

type AlterAction struct {
	kind    AlterActionKind
	column  *ColumnDefinition
	name    Identifier
	newName Identifier
}

func NewAlterAddColumnAction(column ColumnDefinition) (AlterAction, error) {
	if column.name.value == "" || typeIsNil(column.type_) {
		return AlterAction{}, fmt.Errorf("invalid ADD COLUMN action")
	}
	copy := column
	return AlterAction{kind: AlterAddColumn, column: &copy}, nil
}

func NewAlterDropColumnAction(name Identifier) (AlterAction, error) {
	if name.value == "" {
		return AlterAction{}, fmt.Errorf("invalid DROP COLUMN action")
	}
	return AlterAction{kind: AlterDropColumn, name: name}, nil
}

func NewAlterRenameColumnAction(name, newName Identifier) (AlterAction, error) {
	if name.value == "" || newName.value == "" {
		return AlterAction{}, fmt.Errorf("invalid RENAME COLUMN action")
	}
	return AlterAction{kind: AlterRenameColumn, name: name, newName: newName}, nil
}

func NewAlterColumnTypeAction(name Identifier, typ Type) (AlterAction, error) {
	if name.value == "" || typeIsNil(typ) {
		return AlterAction{}, fmt.Errorf("invalid ALTER COLUMN TYPE action")
	}
	column, err := NewColumnDefinition(name, typ, false)
	if err != nil {
		return AlterAction{}, err
	}
	return AlterAction{kind: AlterColumnType, name: name, column: &column}, nil
}
func (action AlterAction) Kind() AlterActionKind { return action.kind }
func (action AlterAction) Column() *ColumnDefinition {
	if action.column == nil {
		return nil
	}
	copy := *action.column
	return &copy
}
func (action AlterAction) Name() Identifier    { return action.name }
func (action AlterAction) NewName() Identifier { return action.newName }

type AlterTableStatement struct {
	statementBase
	target *TableRelation
	action AlterAction
}

func NewAlterTableStatement(source Source, target *TableRelation, action AlterAction) (*AlterTableStatement, error) {
	if !validSource(source) || target == nil || action.kind == "" {
		return nil, fmt.Errorf("invalid ALTER TABLE statement")
	}
	value := &AlterTableStatement{statementBase: statementBase{source: source}, target: target, action: action}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*AlterTableStatement) statementNode()                   {}
func (*AlterTableStatement) Kind() StatementKind              { return StatementAlterTable }
func (statement *AlterTableStatement) Target() *TableRelation { return statement.target }
func (statement *AlterTableStatement) Action() AlterAction    { return statement.action }
func (statement *AlterTableStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitAlterTable(statement)
}
func (statement *AlterTableStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("alter-table")
	statement.target.writeSemantic(builder)
	builder.token(string(statement.action.kind))
	if statement.action.column != nil {
		writeIdentifier(builder, statement.action.column.name)
		statement.action.column.type_.writeSemantic(builder)
		builder.boolean(statement.action.column.notNull)
	}
	writeIdentifier(builder, statement.action.name)
	writeIdentifier(builder, statement.action.newName)
}

type TruncateTableStatement struct {
	statementBase
	target *TableRelation
}

func NewTruncateTableStatement(source Source, target *TableRelation) (*TruncateTableStatement, error) {
	if !validSource(source) || target == nil {
		return nil, fmt.Errorf("invalid TRUNCATE TABLE statement")
	}
	value := &TruncateTableStatement{statementBase: statementBase{source: source}, target: target}
	value.fingerprint = semanticFingerprint(value)
	return value, nil
}
func (*TruncateTableStatement) statementNode()                   {}
func (*TruncateTableStatement) Kind() StatementKind              { return StatementTruncateTable }
func (statement *TruncateTableStatement) Target() *TableRelation { return statement.target }
func (statement *TruncateTableStatement) Accept(visitor StatementVisitor) error {
	return visitor.VisitTruncateTable(statement)
}
func (statement *TruncateTableStatement) writeSemantic(builder *fingerprintBuilder) {
	builder.token("truncate-table")
	statement.target.writeSemantic(builder)
}

func cloneExpressionRows(rows [][]Expression) [][]Expression {
	cloned := make([][]Expression, len(rows))
	for index, row := range rows {
		cloned[index] = append([]Expression(nil), row...)
	}
	return cloned
}
