package duckdb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

type duckDBTableBindingKind uint8

const (
	duckDBTableBindingPhysical duckDBTableBindingKind = iota + 1
	duckDBTableBindingLocal
)

// duckDBTableBinding is adapter-private. The semantic boundary supplies one
// for every TableRelation NodeKey, so the renderer never interprets an
// unresolved GoogleSQL path or applies default-dataset rules.
type duckDBTableBinding struct {
	kind             duckDBTableBindingKind
	reference        domain.TableReference
	localName        string
	schema           []domain.Field
	timePartitioning *domain.TimePartitioning
}

func newDuckDBPhysicalTableBinding(reference domain.TableReference) (duckDBTableBinding, error) {
	return newDuckDBPhysicalTableBindingWithMetadata(reference, nil, nil)
}

func newDuckDBPhysicalTableBindingWithMetadata(
	reference domain.TableReference,
	schema []domain.Field,
	timePartitioning *domain.TimePartitioning,
) (duckDBTableBinding, error) {
	if _, err := renderPhysicalTable(reference); err != nil {
		return duckDBTableBinding{}, err
	}
	binding := duckDBTableBinding{
		kind: duckDBTableBindingPhysical, reference: reference, schema: domain.CloneFields(schema),
	}
	if timePartitioning != nil {
		clone := *timePartitioning
		binding.timePartitioning = &clone
	}
	return binding, nil
}

func newDuckDBLocalTableBinding(name string) (duckDBTableBinding, error) {
	if name == "" || strings.IndexByte(name, 0) >= 0 {
		return duckDBTableBinding{}, fmt.Errorf("%w: local relation binding is invalid", domain.ErrPrecondition)
	}
	return duckDBTableBinding{kind: duckDBTableBindingLocal, localName: name}, nil
}

type duckDBTableBindingResolver func(queryast.NodeKey) (duckDBTableBinding, bool, error)

type duckDBStatementRenderer struct {
	analysis          semantic.Statement
	bindings          map[queryast.NodeKey]duckDBTableBinding
	scriptVariables   map[queryast.NodeKey]string
	arguments         []any
	mergeInsertSchema []domain.Field
}

func lowerDuckDBStatement(
	statement semantic.Statement,
) (duckDBStatementPlan, error) {
	if statement.Syntax() == nil || !statement.RelationsComplete() {
		return duckDBStatementPlan{}, fmt.Errorf("%w: semantic statement is missing", domain.ErrPrecondition)
	}
	return lowerDuckDBSyntax(statement, statement.Syntax(), nil)
}

func lowerDuckDBSyntax(
	statement semantic.Statement,
	syntax queryast.Statement,
	variables map[string]string,
) (duckDBStatementPlan, error) {
	renderer, err := newDuckDBStatementRenderer(statement, syntax, variables)
	if err != nil {
		return duckDBStatementPlan{}, err
	}
	visitor := &duckDBTopLevelVisitor{renderer: renderer}
	if err := syntax.Accept(visitor); err != nil {
		return duckDBStatementPlan{}, err
	}
	plan, err := newDuckDBStatementPlan(
		visitor.statement, renderer.arguments, visitor.producesRows, statement.AnalysisFingerprint(),
	)
	if err != nil {
		return duckDBStatementPlan{}, err
	}
	return plan.withPreconditions(visitor.preconditions), nil
}

func newDuckDBStatementRenderer(
	statement semantic.Statement,
	syntax queryast.Statement,
	variables map[string]string,
) (*duckDBStatementRenderer, error) {
	if syntax == nil || syntax.Source().Digest() != statement.Syntax().Source().Digest() {
		return nil, fmt.Errorf("%w: semantic statement syntax is invalid", domain.ErrPrecondition)
	}
	resolve := func(key queryast.NodeKey) (duckDBTableBinding, bool, error) {
		binding, err := statement.RequireRelationBinding(key)
		if err != nil {
			return duckDBTableBinding{}, false, err
		}
		switch binding.Kind() {
		case semantic.RelationPhysical:
			reference, ok := binding.Reference()
			if !ok {
				return duckDBTableBinding{}, false, fmt.Errorf("%w: physical relation binding has no reference", domain.ErrPrecondition)
			}
			converted, err := newDuckDBPhysicalTableBindingWithMetadata(
				reference, binding.Schema(), binding.TimePartitioning(),
			)
			return converted, err == nil, err
		case semantic.RelationLocal:
			name, ok := binding.LocalName()
			if !ok {
				return duckDBTableBinding{}, false, fmt.Errorf("%w: local relation binding has no name", domain.ErrPrecondition)
			}
			converted, err := newDuckDBLocalTableBinding(name)
			return converted, err == nil, err
		default:
			return duckDBTableBinding{}, false, fmt.Errorf("%w: semantic relation binding kind is unsupported", domain.ErrPrecondition)
		}
	}
	bindings, err := collectDuckDBTableBindings(syntax, resolve)
	if err != nil {
		return nil, err
	}
	scriptVariables, err := collectDuckDBScriptVariables(statement, syntax, variables)
	if err != nil {
		return nil, err
	}
	return &duckDBStatementRenderer{analysis: statement, bindings: bindings, scriptVariables: scriptVariables}, nil
}

func collectDuckDBScriptVariables(
	statement semantic.Statement,
	syntax queryast.Statement,
	variables map[string]string,
) (map[queryast.NodeKey]string, error) {
	expressions, err := queryast.Expressions(syntax)
	if err != nil {
		return nil, fmt.Errorf("%w: semantic expression traversal failed", domain.ErrPrecondition)
	}
	bindings := make(map[queryast.NodeKey]string)
	for _, expression := range expressions {
		binding, found := statement.SymbolBinding(expression.NodeKey())
		if !found || binding.Kind() != semantic.SymbolScriptVariable {
			continue
		}
		table, exists := variables[strings.ToLower(binding.Name())]
		if !exists || table == "" || strings.IndexByte(table, 0) >= 0 {
			return nil, fmt.Errorf("%w: script variable runtime binding is missing", domain.ErrPrecondition)
		}
		bindings[expression.NodeKey()] = table
	}
	return bindings, nil
}

func collectDuckDBTableBindings(
	statement queryast.Statement,
	resolve duckDBTableBindingResolver,
) (map[queryast.NodeKey]duckDBTableBinding, error) {
	if resolve == nil {
		return nil, fmt.Errorf("%w: semantic relation resolver is missing", domain.ErrPrecondition)
	}
	relations, err := queryast.Relations(statement)
	if err != nil {
		return nil, fmt.Errorf("%w: semantic relation traversal failed", domain.ErrPrecondition)
	}
	bindings := make(map[queryast.NodeKey]duckDBTableBinding)
	for _, relation := range relations {
		table, ok := relation.(*queryast.TableRelation)
		if !ok {
			continue
		}
		key := table.NodeKey()
		if _, duplicate := bindings[key]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate semantic table binding node_key=%s",
				domain.ErrPrecondition, key.Fingerprint(),
			)
		}
		binding, found, err := resolve(key)
		if err != nil {
			return nil, fmt.Errorf("resolve relation %v: %w", table.Path().Segments(), err)
		}
		if !found {
			return nil, fmt.Errorf(
				"%w: semantic table binding is missing node_key=%s",
				domain.ErrPrecondition, key.Fingerprint(),
			)
		}
		if err := validateDuckDBTableBinding(binding); err != nil {
			return nil, fmt.Errorf(
				"%w: semantic table binding is invalid node_key=%s",
				domain.ErrPrecondition, key.Fingerprint(),
			)
		}
		bindings[key] = binding
	}
	return bindings, nil
}

func validateDuckDBTableBinding(binding duckDBTableBinding) error {
	switch binding.kind {
	case duckDBTableBindingPhysical:
		_, err := renderPhysicalTable(binding.reference)
		return err
	case duckDBTableBindingLocal:
		if binding.localName == "" || strings.IndexByte(binding.localName, 0) >= 0 {
			return fmt.Errorf("local binding is empty")
		}
		return nil
	default:
		return fmt.Errorf("unknown table binding kind")
	}
}

type duckDBTopLevelVisitor struct {
	renderer      *duckDBStatementRenderer
	statement     string
	producesRows  bool
	preconditions []duckDBStatementPrecondition
}

func (visitor *duckDBTopLevelVisitor) VisitScript(*queryast.ScriptStatement) error {
	return unsupportedDuckDBLowering("statement", queryast.StatementScript)
}

func (visitor *duckDBTopLevelVisitor) VisitDeclare(*queryast.DeclareStatement) error {
	return unsupportedDuckDBLowering("statement", queryast.StatementDeclare)
}

func (visitor *duckDBTopLevelVisitor) VisitSet(*queryast.SetStatement) error {
	return unsupportedDuckDBLowering("statement", queryast.StatementSet)
}

func (visitor *duckDBTopLevelVisitor) VisitSelect(statement *queryast.SelectStatement) error {
	rendered, err := visitor.renderer.renderQuery(statement.Query())
	if err != nil {
		return err
	}
	visitor.statement, visitor.producesRows = rendered, true
	return nil
}

func (visitor *duckDBTopLevelVisitor) VisitInsert(statement *queryast.InsertStatement) error {
	target, err := visitor.renderer.renderMutationTarget(statement.Target())
	if err != nil {
		return err
	}
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(target)
	columns := statement.Columns()
	if len(columns) == 0 {
		columns, err = visitor.renderer.ingestionMutationColumns(statement.Target())
		if err != nil {
			return err
		}
	}
	if len(columns) != 0 {
		builder.WriteString(" (")
		builder.WriteString(renderIdentifierList(columns))
		builder.WriteByte(')')
	}
	switch statement.SourceKind() {
	case queryast.InsertValues:
		rows := statement.Rows()
		if len(rows) == 0 {
			return fmt.Errorf("%w: INSERT VALUES has no rows", domain.ErrInvalidQuery)
		}
		columnCount := len(rows[0])
		if len(columns) != 0 && len(columns) != columnCount {
			return fmt.Errorf("%w: INSERT column and value counts differ", domain.ErrInvalidQuery)
		}
		builder.WriteString(" VALUES ")
		for rowIndex, row := range rows {
			if len(row) != columnCount {
				return fmt.Errorf("%w: INSERT rows have different value counts", domain.ErrInvalidQuery)
			}
			if rowIndex != 0 {
				builder.WriteString(", ")
			}
			values, err := visitor.renderer.renderExpressionList(row)
			if err != nil {
				return err
			}
			builder.WriteByte('(')
			builder.WriteString(values)
			builder.WriteByte(')')
		}
	case queryast.InsertQuery:
		query := statement.Query()
		if query == nil {
			return fmt.Errorf("%w: INSERT query is missing", domain.ErrPrecondition)
		}
		rendered, err := visitor.renderer.renderQuery(*query)
		if err != nil {
			return err
		}
		builder.WriteByte(' ')
		builder.WriteString(rendered)
	default:
		return unsupportedDuckDBLowering("INSERT source", statement.SourceKind())
	}
	visitor.statement = builder.String()
	return nil
}

func (visitor *duckDBTopLevelVisitor) VisitUpdate(statement *queryast.UpdateStatement) error {
	target, err := visitor.renderer.renderMutationTarget(statement.Target())
	if err != nil {
		return err
	}
	assignments, err := visitor.renderer.renderAssignments(statement.Assignments())
	if err != nil {
		return err
	}
	var builder strings.Builder
	builder.WriteString("UPDATE ")
	builder.WriteString(target)
	builder.WriteString(" SET ")
	builder.WriteString(assignments)
	if statement.From() != nil {
		from, err := visitor.renderer.renderRelation(statement.From())
		if err != nil {
			return err
		}
		builder.WriteString(" FROM ")
		builder.WriteString(from)
	}
	if statement.Where() == nil {
		return fmt.Errorf("%w: GoogleSQL UPDATE requires WHERE", domain.ErrInvalidQuery)
	}
	where, err := visitor.renderer.renderExpression(statement.Where())
	if err != nil {
		return err
	}
	builder.WriteString(" WHERE ")
	builder.WriteString(where)
	visitor.statement = builder.String()
	return nil
}

func (visitor *duckDBTopLevelVisitor) VisitDelete(statement *queryast.DeleteStatement) error {
	target, err := visitor.renderer.renderMutationTarget(statement.Target())
	if err != nil {
		return err
	}
	if statement.Where() == nil {
		return fmt.Errorf("%w: GoogleSQL DELETE requires WHERE", domain.ErrInvalidQuery)
	}
	where, err := visitor.renderer.renderExpression(statement.Where())
	if err != nil {
		return err
	}
	visitor.statement = "DELETE FROM " + target + " WHERE " + where
	return nil
}

func (visitor *duckDBTopLevelVisitor) VisitMerge(statement *queryast.MergeStatement) error {
	preconditionArgumentStart := len(visitor.renderer.arguments)
	target, err := visitor.renderer.renderMutationTarget(statement.Target())
	if err != nil {
		return err
	}
	visitor.renderer.mergeInsertSchema = visitor.renderer.ingestionMutationFields(statement.Target())
	source, err := visitor.renderer.renderRelation(statement.MergeSource())
	if err != nil {
		return err
	}
	condition, err := visitor.renderer.renderExpression(statement.Condition())
	if err != nil {
		return err
	}
	preconditionArguments := cloneStatementArguments(
		visitor.renderer.arguments[preconditionArgumentStart:],
	)
	if mergeRequiresSourceCardinalityCheck(statement) {
		precondition, err := newDuckDBStatementPrecondition(
			"SELECT 1 FROM "+target+" WHERE (SELECT count(*) FROM "+
				source+" WHERE "+condition+") > 1 LIMIT 1",
			preconditionArguments,
			duckDBMergeSourceCardinalityV1,
		)
		if err != nil {
			return err
		}
		visitor.preconditions = append(visitor.preconditions, precondition)
	}
	var builder strings.Builder
	builder.WriteString("MERGE INTO ")
	builder.WriteString(target)
	builder.WriteString(" USING ")
	builder.WriteString(source)
	builder.WriteString(" ON ")
	builder.WriteString(condition)
	for _, when := range statement.When() {
		rendered, err := visitor.renderer.renderMergeWhen(when)
		if err != nil {
			return err
		}
		builder.WriteByte(' ')
		builder.WriteString(rendered)
	}
	visitor.statement = builder.String()
	return nil
}

func mergeRequiresSourceCardinalityCheck(statement *queryast.MergeStatement) bool {
	for _, when := range statement.When() {
		if when.Match() != queryast.MergeMatched {
			continue
		}
		switch when.Action().Kind() {
		case queryast.MergeActionUpdate, queryast.MergeActionDelete:
			return true
		}
	}
	return false
}

func (visitor *duckDBTopLevelVisitor) VisitCreateTable(*queryast.CreateTableStatement) error {
	return unsupportedDuckDBLowering("statement", queryast.StatementCreateTable)
}

func (visitor *duckDBTopLevelVisitor) VisitDropTable(*queryast.DropTableStatement) error {
	return unsupportedDuckDBLowering("statement", queryast.StatementDropTable)
}

func (visitor *duckDBTopLevelVisitor) VisitAlterTable(*queryast.AlterTableStatement) error {
	return unsupportedDuckDBLowering("statement", queryast.StatementAlterTable)
}

func (visitor *duckDBTopLevelVisitor) VisitTruncateTable(*queryast.TruncateTableStatement) error {
	return unsupportedDuckDBLowering("statement", queryast.StatementTruncateTable)
}

func (renderer *duckDBStatementRenderer) renderQuery(query queryast.Query) (string, error) {
	var builder strings.Builder
	if expressions := query.With(); len(expressions) != 0 {
		builder.WriteString("WITH ")
		if query.Recursive() {
			builder.WriteString("RECURSIVE ")
		}
		for index, expression := range expressions {
			if index != 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(quoteIdentifier(expression.Name().Value()))
			if columns := expression.Columns(); len(columns) != 0 {
				builder.WriteString(" (")
				builder.WriteString(renderIdentifierList(columns))
				builder.WriteByte(')')
			}
			rendered, err := renderer.renderQuery(expression.Query())
			if err != nil {
				return "", err
			}
			builder.WriteString(" AS (")
			builder.WriteString(rendered)
			builder.WriteByte(')')
		}
		builder.WriteByte(' ')
	}
	body, err := renderer.renderQueryBody(query.Body())
	if err != nil {
		return "", err
	}
	builder.WriteString(body)
	if order := query.OrderBy(); len(order) != 0 {
		builder.WriteString(" ORDER BY ")
		for index, item := range order {
			if index != 0 {
				builder.WriteString(", ")
			}
			rendered, err := renderer.renderExpression(item.Expression())
			if err != nil {
				return "", err
			}
			builder.WriteString(rendered)
			switch item.Direction() {
			case queryast.SortDefault:
			case queryast.SortAscending:
				builder.WriteString(" ASC")
			case queryast.SortDescending:
				builder.WriteString(" DESC")
			default:
				return "", unsupportedDuckDBLowering("sort direction", item.Direction())
			}
			switch item.NullOrdering() {
			case queryast.NullOrderingDefault:
			case queryast.NullsFirst:
				builder.WriteString(" NULLS FIRST")
			case queryast.NullsLast:
				builder.WriteString(" NULLS LAST")
			default:
				return "", unsupportedDuckDBLowering("null ordering", item.NullOrdering())
			}
		}
	}
	if limit := query.Limit(); limit != nil {
		builder.WriteString(" LIMIT ")
		builder.WriteString(strconv.FormatInt(*limit, 10))
	}
	if offset := query.Offset(); offset != nil {
		builder.WriteString(" OFFSET ")
		builder.WriteString(strconv.FormatInt(*offset, 10))
	}
	return builder.String(), nil
}

func (renderer *duckDBStatementRenderer) renderQueryBody(body queryast.QueryBody) (string, error) {
	if body == nil {
		return "", fmt.Errorf("%w: query body is missing", domain.ErrPrecondition)
	}
	visitor := &duckDBQueryBodyVisitor{renderer: renderer}
	if err := body.Accept(visitor); err != nil {
		return "", err
	}
	return visitor.result, nil
}

type duckDBQueryBodyVisitor struct {
	renderer *duckDBStatementRenderer
	result   string
}

func (visitor *duckDBQueryBodyVisitor) VisitSelectQuery(query *queryast.SelectQuery) error {
	var builder strings.Builder
	builder.WriteString("SELECT ")
	if query.Distinct() {
		builder.WriteString("DISTINCT ")
	}
	for index, item := range query.Items() {
		if index != 0 {
			builder.WriteString(", ")
		}
		var rendered string
		var err error
		if star, ok := item.Expression().(*queryast.StarExpression); ok {
			rendered, err = visitor.renderer.renderSelectStar(star, query.From())
		} else {
			rendered, err = visitor.renderer.renderExpression(item.Expression())
		}
		if err != nil {
			return err
		}
		builder.WriteString(rendered)
		if alias := item.Alias(); alias != nil {
			builder.WriteString(" AS ")
			builder.WriteString(quoteIdentifier(alias.Value()))
		}
	}
	if query.From() != nil {
		from, err := visitor.renderer.renderRelation(query.From())
		if err != nil {
			return err
		}
		builder.WriteString(" FROM ")
		builder.WriteString(from)
	}
	if query.Where() != nil {
		where, err := visitor.renderer.renderExpression(query.Where())
		if err != nil {
			return err
		}
		builder.WriteString(" WHERE ")
		builder.WriteString(where)
	}
	if group := query.GroupBy(); len(group) != 0 {
		rendered, err := visitor.renderer.renderExpressionList(group)
		if err != nil {
			return err
		}
		builder.WriteString(" GROUP BY ")
		builder.WriteString(rendered)
	}
	if query.Having() != nil {
		having, err := visitor.renderer.renderExpression(query.Having())
		if err != nil {
			return err
		}
		builder.WriteString(" HAVING ")
		builder.WriteString(having)
	}
	if query.Qualify() != nil {
		qualify, err := visitor.renderer.renderExpression(query.Qualify())
		if err != nil {
			return err
		}
		builder.WriteString(" QUALIFY ")
		builder.WriteString(qualify)
	}
	visitor.result = builder.String()
	return nil
}

func (visitor *duckDBQueryBodyVisitor) VisitSetOperationQuery(query *queryast.SetOperationQuery) error {
	left, err := visitor.renderer.renderQueryBody(query.Left())
	if err != nil {
		return err
	}
	right, err := visitor.renderer.renderQueryBody(query.Right())
	if err != nil {
		return err
	}
	var operator string
	switch query.Operator() {
	case queryast.SetUnion:
		operator = "UNION"
	case queryast.SetIntersect:
		operator = "INTERSECT"
	case queryast.SetExcept:
		operator = "EXCEPT"
	default:
		return unsupportedDuckDBLowering("set operation", query.Operator())
	}
	if query.All() {
		operator += " ALL"
	}
	visitor.result = "(" + left + ") " + operator + " (" + right + ")"
	return nil
}

func (renderer *duckDBStatementRenderer) renderRelation(relation queryast.Relation) (string, error) {
	if relation == nil {
		return "", fmt.Errorf("%w: relation is missing", domain.ErrPrecondition)
	}
	visitor := &duckDBRelationVisitor{renderer: renderer}
	if err := relation.Accept(visitor); err != nil {
		return "", err
	}
	return visitor.result, nil
}

type duckDBRelationVisitor struct {
	renderer *duckDBStatementRenderer
	result   string
}

func (visitor *duckDBRelationVisitor) VisitTableRelation(relation *queryast.TableRelation) error {
	binding, ok := visitor.renderer.bindings[relation.NodeKey()]
	if !ok {
		return fmt.Errorf(
			"%w: semantic table binding is missing node_key=%s",
			domain.ErrPrecondition, relation.NodeKey().Fingerprint(),
		)
	}
	var rendered string
	var err error
	switch binding.kind {
	case duckDBTableBindingPhysical:
		rendered, err = renderPhysicalTable(binding.reference)
	case duckDBTableBindingLocal:
		rendered = quoteIdentifier(binding.localName)
	default:
		err = fmt.Errorf("%w: semantic table binding kind is invalid", domain.ErrPrecondition)
	}
	if err != nil {
		return err
	}
	visitor.result = appendRelationAlias(rendered, relation.Alias())
	return nil
}

func (visitor *duckDBRelationVisitor) VisitSubqueryRelation(relation *queryast.SubqueryRelation) error {
	query, err := visitor.renderer.renderQuery(relation.Query())
	if err != nil {
		return err
	}
	visitor.result = appendRelationAlias("("+query+")", relation.Alias())
	return nil
}

func (visitor *duckDBRelationVisitor) VisitJoinRelation(relation *queryast.JoinRelation) error {
	left, err := visitor.renderer.renderRelation(relation.Left())
	if err != nil {
		return err
	}
	right, err := visitor.renderer.renderRelation(relation.Right())
	if err != nil {
		return err
	}
	condition := relation.Condition()
	var builder strings.Builder
	builder.WriteString(left)
	switch relation.Type() {
	case queryast.JoinComma:
		builder.WriteString(", ")
	case queryast.JoinCross:
		builder.WriteString(" CROSS JOIN ")
	case queryast.JoinInner:
		builder.WriteString(" INNER JOIN ")
	case queryast.JoinLeft:
		builder.WriteString(" LEFT JOIN ")
	case queryast.JoinRight:
		builder.WriteString(" RIGHT JOIN ")
	case queryast.JoinFull:
		builder.WriteString(" FULL JOIN ")
	default:
		return unsupportedDuckDBLowering("join", relation.Type())
	}
	builder.WriteString(right)
	switch condition.Kind() {
	case queryast.JoinConditionNone:
	case queryast.JoinConditionOn:
		rendered, err := visitor.renderer.renderExpression(condition.On())
		if err != nil {
			return err
		}
		builder.WriteString(" ON ")
		builder.WriteString(rendered)
	case queryast.JoinConditionUsing:
		builder.WriteString(" USING (")
		builder.WriteString(renderIdentifierList(condition.Columns()))
		builder.WriteByte(')')
	default:
		return unsupportedDuckDBLowering("join condition", condition.Kind())
	}
	visitor.result = builder.String()
	return nil
}

func (visitor *duckDBRelationVisitor) VisitUnnestRelation(*queryast.UnnestRelation) error {
	return unsupportedDuckDBLowering("relation", queryast.RelationUnnest)
}

func (renderer *duckDBStatementRenderer) renderMutationTarget(relation *queryast.TableRelation) (string, error) {
	if relation == nil {
		return "", fmt.Errorf("%w: mutation target is missing", domain.ErrPrecondition)
	}
	binding, ok := renderer.bindings[relation.NodeKey()]
	if !ok || binding.kind != duckDBTableBindingPhysical {
		return "", fmt.Errorf(
			"%w: mutation target is not bound to a physical table node_key=%s",
			domain.ErrPrecondition, relation.NodeKey().Fingerprint(),
		)
	}
	physical, err := renderPhysicalTable(binding.reference)
	if err != nil {
		return "", err
	}
	return appendRelationAlias(physical, relation.Alias()), nil
}

func (renderer *duckDBStatementRenderer) renderAssignments(assignments []queryast.Assignment) (string, error) {
	if len(assignments) == 0 {
		return "", fmt.Errorf("%w: mutation assignments are empty", domain.ErrInvalidQuery)
	}
	parts := make([]string, len(assignments))
	for index, assignment := range assignments {
		value, err := renderer.renderExpression(assignment.Value())
		if err != nil {
			return "", err
		}
		parts[index] = renderIdentifierPath(assignment.Target()) + " = " + value
	}
	return strings.Join(parts, ", "), nil
}

func (renderer *duckDBStatementRenderer) renderMergeWhen(when queryast.MergeWhen) (string, error) {
	var builder strings.Builder
	builder.WriteString("WHEN ")
	switch when.Match() {
	case queryast.MergeMatched:
		builder.WriteString("MATCHED")
	case queryast.MergeNotMatchedByTarget:
		builder.WriteString("NOT MATCHED")
	case queryast.MergeNotMatchedBySource:
		builder.WriteString("NOT MATCHED BY SOURCE")
	default:
		return "", unsupportedDuckDBLowering("MERGE match", when.Match())
	}
	if when.Condition() != nil {
		condition, err := renderer.renderExpression(when.Condition())
		if err != nil {
			return "", err
		}
		builder.WriteString(" AND ")
		builder.WriteString(condition)
	}
	builder.WriteString(" THEN ")
	action := when.Action()
	switch action.Kind() {
	case queryast.MergeActionInsertRow:
		builder.WriteString("INSERT BY NAME")
	case queryast.MergeActionInsert:
		values := action.Values()
		if len(values) == 0 {
			return "", fmt.Errorf("%w: MERGE INSERT values are empty", domain.ErrInvalidQuery)
		}
		columns := action.Columns()
		if len(columns) == 0 && len(renderer.mergeInsertSchema) != 0 {
			var err error
			columns, err = fieldsAsIdentifiers(renderer.mergeInsertSchema)
			if err != nil {
				return "", err
			}
		}
		if len(columns) != 0 {
			if len(columns) != len(values) {
				return "", fmt.Errorf("%w: MERGE INSERT column and value counts differ", domain.ErrInvalidQuery)
			}
			builder.WriteString("INSERT (")
			builder.WriteString(renderIdentifierList(columns))
			builder.WriteString(") ")
		} else {
			builder.WriteString("INSERT ")
		}
		rendered, err := renderer.renderExpressionList(values)
		if err != nil {
			return "", err
		}
		builder.WriteString("VALUES (")
		builder.WriteString(rendered)
		builder.WriteByte(')')
	case queryast.MergeActionUpdate:
		assignments, err := renderer.renderAssignments(action.Assignments())
		if err != nil {
			return "", err
		}
		builder.WriteString("UPDATE SET ")
		builder.WriteString(assignments)
	case queryast.MergeActionDelete:
		builder.WriteString("DELETE")
	default:
		return "", unsupportedDuckDBLowering("MERGE action", action.Kind())
	}
	return builder.String(), nil
}

func (renderer *duckDBStatementRenderer) ingestionMutationFields(
	relation *queryast.TableRelation,
) []domain.Field {
	if relation == nil {
		return nil
	}
	binding, found := renderer.bindings[relation.NodeKey()]
	if !found || !binding.isIngestionTimePartitioned() {
		return nil
	}
	return domain.CloneFields(binding.schema)
}

func (renderer *duckDBStatementRenderer) ingestionMutationColumns(
	relation *queryast.TableRelation,
) ([]queryast.Identifier, error) {
	return fieldsAsIdentifiers(renderer.ingestionMutationFields(relation))
}

func fieldsAsIdentifiers(fields []domain.Field) ([]queryast.Identifier, error) {
	identifiers := make([]queryast.Identifier, len(fields))
	for index, field := range fields {
		identifier, err := queryast.NewIdentifier(field.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: canonical mutation field is invalid", domain.ErrPrecondition)
		}
		identifiers[index] = identifier
	}
	return identifiers, nil
}

func (binding duckDBTableBinding) isIngestionTimePartitioned() bool {
	if binding.kind != duckDBTableBindingPhysical || binding.timePartitioning == nil ||
		strings.TrimSpace(binding.timePartitioning.Field) != "" {
		return false
	}
	switch strings.ToUpper(binding.timePartitioning.Type) {
	case "DAY", "HOUR", "MONTH", "YEAR":
		return true
	default:
		return false
	}
}

func (renderer *duckDBStatementRenderer) renderSelectStar(
	star *queryast.StarExpression,
	from queryast.Relation,
) (string, error) {
	rendered, err := renderer.renderExpression(star)
	if err != nil {
		return "", err
	}
	if !renderer.relationContainsIngestionPartition(from, star.Qualifier()) {
		return rendered, nil
	}
	return rendered + " EXCLUDE (" + quoteIdentifier(domain.PartitionTimePseudoColumn) + ", " +
		quoteIdentifier(domain.PartitionDatePseudoColumn) + ")", nil
}

func (renderer *duckDBStatementRenderer) relationContainsIngestionPartition(
	relation queryast.Relation,
	qualifier *queryast.IdentifierPath,
) bool {
	switch value := relation.(type) {
	case *queryast.TableRelation:
		binding, found := renderer.bindings[value.NodeKey()]
		return found && binding.isIngestionTimePartitioned() && relationMatchesQualifier(value, qualifier)
	case *queryast.JoinRelation:
		return renderer.relationContainsIngestionPartition(value.Left(), qualifier) ||
			renderer.relationContainsIngestionPartition(value.Right(), qualifier)
	default:
		return false
	}
}

func relationMatchesQualifier(relation *queryast.TableRelation, qualifier *queryast.IdentifierPath) bool {
	if qualifier == nil {
		return true
	}
	segments := qualifier.Segments()
	if len(segments) == 0 {
		return false
	}
	if alias := relation.Alias(); alias != nil {
		return len(segments) == 1 && strings.EqualFold(segments[0], alias.Value())
	}
	path := relation.Path().Segments()
	if len(path) == 0 || len(segments) > len(path) {
		return false
	}
	offset := len(path) - len(segments)
	for index := range segments {
		if !strings.EqualFold(segments[index], path[offset+index]) {
			return false
		}
	}
	return true
}

func appendRelationAlias(relation string, alias *queryast.Identifier) string {
	if alias == nil {
		return relation
	}
	return relation + " AS " + quoteIdentifier(alias.Value())
}

func renderIdentifier(identifier queryast.Identifier) string {
	return quoteIdentifier(identifier.Value())
}

func renderIdentifierList(identifiers []queryast.Identifier) string {
	rendered := make([]string, len(identifiers))
	for index, identifier := range identifiers {
		rendered[index] = renderIdentifier(identifier)
	}
	return strings.Join(rendered, ", ")
}

func renderIdentifierPath(path queryast.IdentifierPath) string {
	parts := path.Parts()
	rendered := make([]string, len(parts))
	for index, part := range parts {
		rendered[index] = renderIdentifier(part)
	}
	return strings.Join(rendered, ".")
}

const duckDBGoogleSQLLoweringUnsupportedV1 = "query.googlesql.duckdb-lowering.unsupported-v1"

func unsupportedDuckDBLowering(category string, value any) error {
	return fmt.Errorf(
		"%w: code=%s semantic node is not supported by the DuckDB adapter: category=%s value=%v",
		domain.ErrUnsupported, duckDBGoogleSQLLoweringUnsupportedV1, category, value,
	)
}
