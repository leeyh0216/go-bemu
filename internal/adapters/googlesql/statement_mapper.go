package googlesql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	gsql "github.com/goccy/go-googlesql"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

func (mapper *statementMapper) mapStatement(node gsql.ASTStatementNode) (queryast.Statement, error) {
	switch statement := node.(type) {
	case *gsql.ASTQueryStatement:
		return mapper.mapQueryStatement(statement)
	case *gsql.ASTInsertStatement:
		return mapper.mapInsertStatement(statement)
	case *gsql.ASTUpdateStatement:
		return mapper.mapUpdateStatement(statement)
	case *gsql.ASTDeleteStatement:
		return mapper.mapDeleteStatement(statement)
	case *gsql.ASTMergeStatement:
		return mapper.mapMergeStatement(statement)
	case *gsql.ASTCreateTableStatement:
		return mapper.mapCreateTableStatement(statement)
	case *gsql.ASTDropStatement:
		return mapper.mapDropStatement(statement)
	case *gsql.ASTAlterTableStatement:
		return mapper.mapAlterTableStatement(statement)
	case *gsql.ASTTruncateStatement:
		return mapper.mapTruncateStatement(statement)
	case *gsql.ASTVariableDeclaration:
		return mapper.mapDeclareStatement(statement)
	case *gsql.ASTSingleAssignment:
		return mapper.mapSetStatement(statement)
	default:
		return nil, unsupportedNode("UNKNOWN", "statement", node)
	}
}

func (mapper *statementMapper) mapQueryStatement(node *gsql.ASTQueryStatement) (queryast.Statement, error) {
	queryNode, err := node.Query()
	if err != nil || queryNode == nil {
		return nil, parserFailure()
	}
	query, err := mapper.mapQuery(queryast.StatementSelect, queryNode)
	if err != nil {
		return nil, err
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewSelectStatement(source, query)
}

func (mapper *statementMapper) mapInsertStatement(node *gsql.ASTInsertStatement) (queryast.Statement, error) {
	mode, err := node.InsertMode()
	if err != nil {
		return nil, parserFailure()
	}
	if mode != gsql.ASTInsertStatementEnums_InsertModeDefaultMode {
		return nil, unsupportedNode(queryast.StatementInsert, "insert-mode", node)
	}
	for _, optional := range []struct {
		kind string
		get  func() (gsql.ASTNode, error)
	}{
		{kind: "insert-assert-rows-modified", get: func() (gsql.ASTNode, error) { return node.AssertRowsModified() }},
		{kind: "insert-hint", get: func() (gsql.ASTNode, error) { return node.Hint() }},
		{kind: "insert-on-conflict", get: func() (gsql.ASTNode, error) { return node.OnConflict() }},
		{kind: "insert-returning", get: func() (gsql.ASTNode, error) { return node.Returning() }},
	} {
		value, err := optional.get()
		if err != nil {
			return nil, parserFailure()
		}
		if !gsqlNodeIsNil(value) {
			return nil, unsupportedNode(queryast.StatementInsert, optional.kind, value)
		}
	}
	targetNode, err := node.GetTargetPathForNonNested()
	if err != nil || targetNode == nil {
		return nil, parserFailure()
	}
	target, err := mapper.mapTargetRelation(targetNode, nil)
	if err != nil {
		return nil, err
	}
	columns, err := mapColumnList(node)
	if err != nil {
		return nil, err
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	rowsNode, err := node.Rows()
	if err != nil {
		return nil, parserFailure()
	}
	queryNode, err := node.Query()
	if err != nil {
		return nil, parserFailure()
	}
	if rowsNode != nil && queryNode != nil {
		return nil, parserFailure()
	}
	if rowsNode != nil {
		rowChildren, err := astChildren(rowsNode)
		if err != nil {
			return nil, err
		}
		rows := make([][]queryast.Expression, 0, len(rowChildren))
		for _, child := range rowChildren {
			rowNode, ok := child.(*gsql.ASTInsertValuesRow)
			if !ok {
				return nil, unsupportedNode(queryast.StatementInsert, "insert-values-row", child)
			}
			valueChildren, err := astChildren(rowNode)
			if err != nil {
				return nil, err
			}
			row := make([]queryast.Expression, 0, len(valueChildren))
			for _, valueChild := range valueChildren {
				expressionNode, ok := valueChild.(gsql.ASTExpressionNode)
				if !ok {
					return nil, unsupportedNode(queryast.StatementInsert, "insert-value", valueChild)
				}
				expression, err := mapper.mapExpression(queryast.StatementInsert, expressionNode)
				if err != nil {
					return nil, err
				}
				row = append(row, expression)
			}
			rows = append(rows, row)
		}
		return queryast.NewInsertValuesStatement(source, target, columns, rows)
	}
	if queryNode != nil {
		query, err := mapper.mapQuery(queryast.StatementInsert, queryNode)
		if err != nil {
			return nil, err
		}
		return queryast.NewInsertQueryStatement(source, target, columns, query)
	}
	return nil, unsupportedNode(queryast.StatementInsert, "insert-source", node)
}

func mapColumnList(node *gsql.ASTInsertStatement) ([]queryast.Identifier, error) {
	list, err := node.ColumnList()
	if err != nil {
		return nil, parserFailure()
	}
	if list == nil {
		return nil, nil
	}
	children, err := astChildren(list)
	if err != nil {
		return nil, err
	}
	columns := make([]queryast.Identifier, 0, len(children))
	for _, child := range children {
		identifierNode, ok := child.(*gsql.ASTIdentifier)
		if !ok {
			return nil, unsupportedNode(queryast.StatementInsert, "insert-column", child)
		}
		identifier, err := mapIdentifier(identifierNode)
		if err != nil {
			return nil, err
		}
		columns = append(columns, identifier)
	}
	return columns, nil
}

func (mapper *statementMapper) mapTargetRelation(node *gsql.ASTPathExpression, alias *queryast.Identifier) (*queryast.TableRelation, error) {
	path, err := mapTablePath(node)
	if err != nil {
		return nil, err
	}
	key, err := mapper.key(node, "table-relation")
	if err != nil {
		return nil, err
	}
	return queryast.NewTableRelation(key, path, alias)
}

func (mapper *statementMapper) mapQuery(statementKind queryast.StatementKind, node *gsql.ASTQuery) (queryast.Query, error) {
	withNode, err := node.WithClause()
	if err != nil {
		return queryast.Query{}, parserFailure()
	}
	with, recursive, err := mapper.mapWithClause(statementKind, withNode)
	if err != nil {
		return queryast.Query{}, err
	}
	if lockMode, err := node.LockMode(); err != nil {
		return queryast.Query{}, parserFailure()
	} else if lockMode != nil {
		return queryast.Query{}, unsupportedNode(statementKind, "lock-mode", lockMode)
	}
	if pipe, err := node.PipeOperatorList(0); err == nil && pipe != nil {
		return queryast.Query{}, unsupportedNode(statementKind, "pipe-operator", pipe)
	}

	expression, err := node.QueryExpr()
	if err != nil || expression == nil {
		return queryast.Query{}, parserFailure()
	}
	body, err := mapper.mapQueryBody(statementKind, expression)
	if err != nil {
		return queryast.Query{}, err
	}

	orderByNode, err := node.OrderBy()
	if err != nil {
		return queryast.Query{}, parserFailure()
	}
	orderBy, err := mapper.mapOrderBy(statementKind, orderByNode)
	if err != nil {
		return queryast.Query{}, err
	}
	limitNode, err := node.LimitOffset()
	if err != nil {
		return queryast.Query{}, parserFailure()
	}
	limit, offset, err := mapper.mapLimitOffset(statementKind, limitNode)
	if err != nil {
		return queryast.Query{}, err
	}
	return queryast.NewQuery(with, recursive, body, orderBy, limit, offset)
}

func (mapper *statementMapper) mapWithClause(
	statementKind queryast.StatementKind,
	node *gsql.ASTWithClause,
) ([]queryast.CommonTableExpression, bool, error) {
	if node == nil {
		return nil, false, nil
	}
	recursive, err := node.Recursive()
	if err != nil {
		return nil, false, parserFailure()
	}
	children, err := astChildren(node)
	if err != nil {
		return nil, false, err
	}
	expressions := make([]queryast.CommonTableExpression, 0, len(children))
	for _, child := range children {
		entry, ok := child.(*gsql.ASTWithClauseEntry)
		if !ok {
			return nil, false, unsupportedNode(statementKind, "with-entry", child)
		}
		if groupRows, err := entry.AliasedGroupRows(); err != nil {
			return nil, false, parserFailure()
		} else if groupRows != nil {
			return nil, false, unsupportedNode(statementKind, "with-group-rows", groupRows)
		}
		aliased, err := entry.AliasedQuery()
		if err != nil || aliased == nil {
			return nil, false, parserFailure()
		}
		if modifiers, err := aliased.Modifiers(); err != nil {
			return nil, false, parserFailure()
		} else if modifiers != nil {
			return nil, false, unsupportedNode(statementKind, "with-modifiers", modifiers)
		}
		aliasNode, err := aliased.Alias()
		if err != nil || aliasNode == nil {
			return nil, false, parserFailure()
		}
		alias, err := mapIdentifier(aliasNode)
		if err != nil {
			return nil, false, err
		}
		queryNode, err := aliased.Query()
		if err != nil || queryNode == nil {
			return nil, false, parserFailure()
		}
		query, err := mapper.mapQuery(statementKind, queryNode)
		if err != nil {
			return nil, false, err
		}
		expression, err := queryast.NewCommonTableExpression(alias, nil, query)
		if err != nil {
			return nil, false, parserFailure()
		}
		expressions = append(expressions, expression)
	}
	if len(expressions) == 0 {
		return nil, false, parserFailure()
	}
	return expressions, recursive, nil
}

func (mapper *statementMapper) mapQueryBody(statementKind queryast.StatementKind, node gsql.ASTQueryExpressionNode) (queryast.QueryBody, error) {
	switch expression := node.(type) {
	case *gsql.ASTSelect:
		return mapper.mapSelectQuery(statementKind, expression)
	case *gsql.ASTSetOperation:
		return mapper.mapSetOperation(statementKind, expression)
	default:
		return nil, unsupportedNode(statementKind, "query-expression", node)
	}
}

func (mapper *statementMapper) mapSelectQuery(statementKind queryast.StatementKind, selectNode *gsql.ASTSelect) (queryast.QueryBody, error) {
	if selectAs, err := selectNode.SelectAs(); err != nil {
		return nil, parserFailure()
	} else if selectAs != nil {
		return nil, unsupportedNode(statementKind, "select-as", selectAs)
	}
	for _, optional := range []struct {
		kind string
		get  func() (gsql.ASTNode, error)
	}{
		{kind: "qualify", get: func() (gsql.ASTNode, error) { return selectNode.Qualify() }},
		{kind: "window", get: func() (gsql.ASTNode, error) { return selectNode.WindowClause() }},
		{kind: "select-modifier", get: func() (gsql.ASTNode, error) { return selectNode.WithModifier() }},
	} {
		value, err := optional.get()
		if err != nil {
			return nil, parserFailure()
		}
		if !gsqlNodeIsNil(value) {
			return nil, unsupportedNode(statementKind, optional.kind, value)
		}
	}

	list, err := selectNode.SelectList()
	if err != nil || list == nil {
		return nil, parserFailure()
	}
	children, err := astChildren(list)
	if err != nil {
		return nil, err
	}
	items := make([]queryast.SelectItem, 0, len(children))
	for _, child := range children {
		column, ok := child.(*gsql.ASTSelectColumn)
		if !ok {
			return nil, unsupportedNode(statementKind, "select-list-item", child)
		}
		expressionNode, err := column.Expression()
		if err != nil || expressionNode == nil {
			return nil, parserFailure()
		}
		expression, err := mapper.mapExpression(statementKind, expressionNode)
		if err != nil {
			return nil, err
		}
		aliasNode, err := column.Alias()
		if err != nil {
			return nil, parserFailure()
		}
		alias, err := mapAlias(aliasNode)
		if err != nil {
			return nil, err
		}
		item, err := queryast.NewSelectItem(expression, alias)
		if err != nil {
			return nil, parserFailure()
		}
		items = append(items, item)
	}

	fromNode, err := selectNode.FromClause()
	if err != nil {
		return nil, parserFailure()
	}
	var from queryast.Relation
	if fromNode != nil {
		tableExpression, err := fromNode.TableExpression()
		if err != nil || tableExpression == nil {
			return nil, parserFailure()
		}
		from, err = mapper.mapRelation(statementKind, tableExpression)
		if err != nil {
			return nil, err
		}
	}
	whereNode, err := selectNode.WhereClause()
	if err != nil {
		return nil, parserFailure()
	}
	var where queryast.Expression
	if whereNode != nil {
		expressionNode, err := whereNode.Expression()
		if err != nil || expressionNode == nil {
			return nil, parserFailure()
		}
		where, err = mapper.mapExpression(statementKind, expressionNode)
		if err != nil {
			return nil, err
		}
	}
	distinct, err := selectNode.Distinct()
	if err != nil {
		return nil, parserFailure()
	}
	groupByNode, err := selectNode.GroupBy()
	if err != nil {
		return nil, parserFailure()
	}
	groupBy, err := mapper.mapGroupBy(statementKind, groupByNode)
	if err != nil {
		return nil, err
	}
	havingNode, err := selectNode.Having()
	if err != nil {
		return nil, parserFailure()
	}
	having, err := mapper.mapHaving(statementKind, havingNode)
	if err != nil {
		return nil, err
	}
	return queryast.NewSelectQuery(distinct, items, from, where, groupBy, having, nil)
}

func (mapper *statementMapper) mapGroupBy(
	statementKind queryast.StatementKind,
	node *gsql.ASTGroupBy,
) ([]queryast.Expression, error) {
	if node == nil {
		return nil, nil
	}
	if all, err := node.All(); err != nil {
		return nil, parserFailure()
	} else if all != nil {
		return nil, unsupportedNode(statementKind, "group-by-all", all)
	}
	if andOrderBy, err := node.AndOrderBy(); err != nil {
		return nil, parserFailure()
	} else if andOrderBy {
		return nil, unsupportedNode(statementKind, "group-by-and-order-by", node)
	}
	if hint, err := node.Hint(); err != nil {
		return nil, parserFailure()
	} else if hint != nil {
		return nil, unsupportedNode(statementKind, "group-by-hint", hint)
	}
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	expressions := make([]queryast.Expression, 0, len(children))
	for _, child := range children {
		item, ok := child.(*gsql.ASTGroupingItem)
		if !ok {
			return nil, unsupportedNode(statementKind, "grouping-item", child)
		}
		for _, optional := range []struct {
			kind string
			get  func() (gsql.ASTNode, error)
		}{
			{kind: "grouping-alias", get: func() (gsql.ASTNode, error) { return item.Alias() }},
			{kind: "grouping-cube", get: func() (gsql.ASTNode, error) { return item.Cube() }},
			{kind: "grouping-order", get: func() (gsql.ASTNode, error) { return item.GroupingItemOrder() }},
			{kind: "grouping-set", get: func() (gsql.ASTNode, error) { return item.GroupingSetList() }},
			{kind: "grouping-rollup", get: func() (gsql.ASTNode, error) { return item.Rollup() }},
		} {
			value, inspectErr := optional.get()
			if inspectErr != nil {
				return nil, parserFailure()
			}
			if !gsqlNodeIsNil(value) {
				return nil, unsupportedNode(statementKind, optional.kind, value)
			}
		}
		expressionNode, err := item.Expression()
		if err != nil || expressionNode == nil {
			return nil, parserFailure()
		}
		expression, err := mapper.mapExpression(statementKind, expressionNode)
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, expression)
	}
	if len(expressions) == 0 {
		return nil, parserFailure()
	}
	return expressions, nil
}

func (mapper *statementMapper) mapHaving(
	statementKind queryast.StatementKind,
	node *gsql.ASTHaving,
) (queryast.Expression, error) {
	if node == nil {
		return nil, nil
	}
	expressionNode, err := node.Expression()
	if err != nil || expressionNode == nil {
		return nil, parserFailure()
	}
	return mapper.mapExpression(statementKind, expressionNode)
}

func (mapper *statementMapper) mapSetOperation(statementKind queryast.StatementKind, node *gsql.ASTSetOperation) (queryast.QueryBody, error) {
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	inputs := make([]gsql.ASTQueryExpressionNode, 0, len(children))
	for _, child := range children {
		input, ok := child.(gsql.ASTQueryExpressionNode)
		if ok {
			inputs = append(inputs, input)
			continue
		}
		if _, ok := child.(*gsql.ASTSetOperationMetadataList); !ok {
			return nil, unsupportedNode(statementKind, "set-operation-child", child)
		}
	}
	if len(inputs) < 2 {
		return nil, parserFailure()
	}
	metadataList, err := node.Metadata()
	if err != nil || metadataList == nil {
		return nil, parserFailure()
	}
	metadataChildren, err := astChildren(metadataList)
	if err != nil {
		return nil, err
	}
	metadata := make([]*gsql.ASTSetOperationMetadata, 0, len(metadataChildren))
	for _, child := range metadataChildren {
		item, ok := child.(*gsql.ASTSetOperationMetadata)
		if !ok {
			return nil, unsupportedNode(statementKind, "set-operation-metadata", child)
		}
		metadata = append(metadata, item)
	}
	if len(metadata) != len(inputs)-1 {
		return nil, parserFailure()
	}
	left, err := mapper.mapQueryBody(statementKind, inputs[0])
	if err != nil {
		return nil, err
	}
	for index, item := range metadata {
		operator, all, inspectErr := inspectSetOperation(item)
		if inspectErr != nil {
			return nil, inspectErr
		}
		if operator != queryast.SetUnion || !all {
			return nil, unsupportedNode(statementKind, "set-operation", item)
		}
		right, mapErr := mapper.mapQueryBody(statementKind, inputs[index+1])
		if mapErr != nil {
			return nil, mapErr
		}
		left, mapErr = queryast.NewSetOperationQuery(operator, all, left, right)
		if mapErr != nil {
			return nil, parserFailure()
		}
	}
	return left, nil
}

func inspectSetOperation(node *gsql.ASTSetOperationMetadata) (queryast.SetOperation, bool, error) {
	if hint, err := node.Hint(); err != nil {
		return "", false, parserFailure()
	} else if hint != nil {
		return "", false, unsupportedNode(queryast.StatementSelect, "set-operation-hint", hint)
	}
	if columns, err := node.CorrespondingByColumnList(); err != nil {
		return "", false, parserFailure()
	} else if columns != nil {
		return "", false, unsupportedNode(queryast.StatementSelect, "set-operation-columns", columns)
	}
	opNode, err := node.OpType()
	if err != nil || opNode == nil {
		return "", false, parserFailure()
	}
	op, err := opNode.Value()
	if err != nil {
		return "", false, parserFailure()
	}
	var operator queryast.SetOperation
	switch op {
	case gsql.ASTSetOperationEnums_OperationTypeUnion:
		operator = queryast.SetUnion
	case gsql.ASTSetOperationEnums_OperationTypeExcept:
		operator = queryast.SetExcept
	case gsql.ASTSetOperationEnums_OperationTypeIntersect:
		operator = queryast.SetIntersect
	default:
		return "", false, parserFailure()
	}
	allNode, err := node.AllOrDistinct()
	if err != nil || allNode == nil {
		return "", false, parserFailure()
	}
	allOrDistinct, err := allNode.Value()
	if err != nil {
		return "", false, parserFailure()
	}
	all := allOrDistinct == gsql.ASTSetOperationEnums_AllOrDistinctAll
	if !all && allOrDistinct != gsql.ASTSetOperationEnums_AllOrDistinctDistinct {
		return "", false, parserFailure()
	}
	matchNode, err := node.ColumnMatchMode()
	if err != nil {
		return "", false, parserFailure()
	}
	if matchNode != nil {
		match, inspectErr := matchNode.Value()
		if inspectErr != nil {
			return "", false, parserFailure()
		}
		if match != gsql.ASTSetOperationEnums_ColumnMatchModeByPosition {
			return "", false, unsupportedNode(queryast.StatementSelect, "set-operation-column-match", matchNode)
		}
	}
	propagationNode, err := node.ColumnPropagationMode()
	if err != nil {
		return "", false, parserFailure()
	}
	if propagationNode != nil {
		propagation, inspectErr := propagationNode.Value()
		if inspectErr != nil {
			return "", false, parserFailure()
		}
		if propagation != gsql.ASTSetOperationEnums_ColumnPropagationModeStrict {
			return "", false, unsupportedNode(queryast.StatementSelect, "set-operation-column-propagation", propagationNode)
		}
	}
	return operator, all, nil
}

func (mapper *statementMapper) mapRelation(statementKind queryast.StatementKind, node gsql.ASTTableExpressionNode) (queryast.Relation, error) {
	switch relation := node.(type) {
	case *gsql.ASTTablePathExpression:
		for _, optional := range []struct {
			kind string
			get  func() (gsql.ASTNode, error)
		}{
			{kind: "for-system-time", get: func() (gsql.ASTNode, error) { return relation.ForSystemTime() }},
			{kind: "table-hint", get: func() (gsql.ASTNode, error) { return relation.Hint() }},
			{kind: "with-offset", get: func() (gsql.ASTNode, error) { return relation.WithOffset() }},
			{kind: "unnest-expression", get: func() (gsql.ASTNode, error) { return relation.UnnestExpr() }},
		} {
			value, err := optional.get()
			if err != nil {
				return nil, parserFailure()
			}
			if !gsqlNodeIsNil(value) {
				return nil, unsupportedNode(statementKind, optional.kind, value)
			}
		}
		pathNode, err := relation.PathExpr()
		if err != nil || pathNode == nil {
			return nil, parserFailure()
		}
		path, err := mapTablePath(pathNode)
		if err != nil {
			return nil, err
		}
		aliasNode, err := relation.Alias()
		if err != nil {
			return nil, parserFailure()
		}
		alias, err := mapAlias(aliasNode)
		if err != nil {
			return nil, err
		}
		key, err := mapper.key(relation, "table-relation")
		if err != nil {
			return nil, err
		}
		return queryast.NewTableRelation(key, path, alias)
	case *gsql.ASTJoin:
		return mapper.mapJoinRelation(statementKind, relation)
	case *gsql.ASTTableSubquery:
		lateral, err := relation.IsLateral()
		if err != nil {
			return nil, parserFailure()
		}
		if lateral {
			return nil, unsupportedNode(statementKind, "lateral-subquery", relation)
		}
		queryNode, err := relation.Subquery()
		if err != nil || queryNode == nil {
			return nil, parserFailure()
		}
		query, err := mapper.mapQuery(statementKind, queryNode)
		if err != nil {
			return nil, err
		}
		aliasNode, err := relation.Alias()
		if err != nil {
			return nil, parserFailure()
		}
		alias, err := mapAlias(aliasNode)
		if err != nil {
			return nil, err
		}
		key, err := mapper.key(relation, "subquery-relation")
		if err != nil {
			return nil, err
		}
		return queryast.NewSubqueryRelation(key, query, alias)
	default:
		return nil, unsupportedNode(statementKind, "table-expression", node)
	}
}

func (mapper *statementMapper) mapJoinRelation(
	statementKind queryast.StatementKind,
	node *gsql.ASTJoin,
) (queryast.Relation, error) {
	if node == nil {
		return nil, parserFailure()
	}
	if natural, err := node.Natural(); err != nil {
		return nil, parserFailure()
	} else if natural {
		return nil, unsupportedNode(statementKind, "natural-join", node)
	}
	if hint, err := node.Hint(); err != nil {
		return nil, parserFailure()
	} else if hint != nil {
		return nil, unsupportedNode(statementKind, "join-hint", hint)
	}
	if hint, err := node.JoinHint(); err != nil {
		return nil, parserFailure()
	} else if hint != gsql.ASTJoinEnums_JoinHintNoJoinHint {
		return nil, unsupportedNode(statementKind, "join-method", node)
	}
	if transformed, err := node.TransformationNeeded(); err != nil {
		return nil, parserFailure()
	} else if transformed {
		return nil, parserFailure()
	}
	if unmatched, err := node.UnmatchedJoinCount(); err != nil {
		return nil, parserFailure()
	} else if unmatched != 0 {
		return nil, parserFailure()
	}

	leftNode, err := node.Lhs()
	if err != nil || leftNode == nil {
		return nil, parserFailure()
	}
	left, err := mapper.mapRelation(statementKind, leftNode)
	if err != nil {
		return nil, err
	}
	rightNode, err := node.Rhs()
	if err != nil || rightNode == nil {
		return nil, parserFailure()
	}
	right, err := mapper.mapRelation(statementKind, rightNode)
	if err != nil {
		return nil, err
	}

	typeValue, err := node.JoinType()
	if err != nil {
		return nil, parserFailure()
	}
	var joinType queryast.JoinType
	switch typeValue {
	case gsql.ASTJoinEnums_JoinTypeDefaultJoinType, gsql.ASTJoinEnums_JoinTypeInner:
		joinType = queryast.JoinInner
	case gsql.ASTJoinEnums_JoinTypeComma:
		joinType = queryast.JoinComma
	case gsql.ASTJoinEnums_JoinTypeCross:
		joinType = queryast.JoinCross
	case gsql.ASTJoinEnums_JoinTypeLeft:
		joinType = queryast.JoinLeft
	case gsql.ASTJoinEnums_JoinTypeRight:
		joinType = queryast.JoinRight
	case gsql.ASTJoinEnums_JoinTypeFull:
		joinType = queryast.JoinFull
	default:
		return nil, unsupportedNode(statementKind, "join-type", node)
	}
	condition, err := mapper.mapJoinCondition(statementKind, node, joinType)
	if err != nil {
		return nil, err
	}
	key, err := mapper.key(node, "join-relation")
	if err != nil {
		return nil, err
	}
	joined, err := queryast.NewJoinRelation(key, joinType, left, right, condition)
	if err != nil {
		return nil, parserFailure()
	}
	return joined, nil
}

func (mapper *statementMapper) mapJoinCondition(
	statementKind queryast.StatementKind,
	node *gsql.ASTJoin,
	joinType queryast.JoinType,
) (queryast.JoinCondition, error) {
	on, err := node.OnClause()
	if err != nil {
		return queryast.JoinCondition{}, parserFailure()
	}
	using, err := node.UsingClause()
	if err != nil {
		return queryast.JoinCondition{}, parserFailure()
	}
	if on != nil && using != nil {
		return queryast.JoinCondition{}, parserFailure()
	}
	if on != nil {
		expressionNode, err := on.Expression()
		if err != nil || expressionNode == nil {
			return queryast.JoinCondition{}, parserFailure()
		}
		expression, err := mapper.mapExpression(statementKind, expressionNode)
		if err != nil {
			return queryast.JoinCondition{}, err
		}
		return queryast.NewJoinOn(expression)
	}
	if using != nil {
		children, err := astChildren(using)
		if err != nil {
			return queryast.JoinCondition{}, err
		}
		columns := make([]queryast.Identifier, 0, len(children))
		for _, child := range children {
			identifierNode, ok := child.(*gsql.ASTIdentifier)
			if !ok {
				return queryast.JoinCondition{}, unsupportedNode(statementKind, "join-using-key", child)
			}
			identifier, err := mapIdentifier(identifierNode)
			if err != nil {
				return queryast.JoinCondition{}, err
			}
			columns = append(columns, identifier)
		}
		condition, err := queryast.NewJoinUsing(columns)
		if err != nil {
			return queryast.JoinCondition{}, parserFailure()
		}
		return condition, nil
	}
	if joinType != queryast.JoinComma && joinType != queryast.JoinCross {
		return queryast.JoinCondition{}, parserFailure()
	}
	return queryast.NewJoinWithoutCondition(), nil
}

func (mapper *statementMapper) mapOrderBy(statementKind queryast.StatementKind, node *gsql.ASTOrderBy) ([]queryast.OrderItem, error) {
	if node == nil {
		return nil, nil
	}
	if hint, err := node.Hint(); err != nil {
		return nil, parserFailure()
	} else if hint != nil {
		return nil, unsupportedNode(statementKind, "order-by-hint", hint)
	}
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	items := make([]queryast.OrderItem, 0, len(children))
	for _, child := range children {
		ordering, ok := child.(*gsql.ASTOrderingExpression)
		if !ok {
			return nil, unsupportedNode(statementKind, "ordering-expression", child)
		}
		if collate, err := ordering.Collate(); err != nil {
			return nil, parserFailure()
		} else if collate != nil {
			return nil, unsupportedNode(statementKind, "order-by-collation", collate)
		}
		if options, err := ordering.OptionList(); err != nil {
			return nil, parserFailure()
		} else if options != nil {
			return nil, unsupportedNode(statementKind, "order-by-options", options)
		}
		expressionNode, err := ordering.Expression()
		if err != nil || expressionNode == nil {
			return nil, parserFailure()
		}
		expression, err := mapper.mapExpression(statementKind, expressionNode)
		if err != nil {
			return nil, err
		}
		descending, err := ordering.Descending()
		if err != nil {
			return nil, parserFailure()
		}
		direction := queryast.SortAscending
		if descending {
			direction = queryast.SortDescending
		}
		nullOrdering := queryast.NullOrderingDefault
		if nullNode, err := ordering.NullOrder(); err != nil {
			return nil, parserFailure()
		} else if nullNode != nil {
			first, err := nullNode.NullsFirst()
			if err != nil {
				return nil, parserFailure()
			}
			if first {
				nullOrdering = queryast.NullsFirst
			} else {
				nullOrdering = queryast.NullsLast
			}
		}
		item, err := queryast.NewOrderItem(expression, direction, nullOrdering)
		if err != nil {
			return nil, parserFailure()
		}
		items = append(items, item)
	}
	return items, nil
}

func (mapper *statementMapper) mapLimitOffset(statementKind queryast.StatementKind, node *gsql.ASTLimitOffset) (*int64, *int64, error) {
	if node == nil {
		return nil, nil, nil
	}
	all, err := node.HasLimitAll()
	if err != nil {
		return nil, nil, parserFailure()
	}
	if all {
		return nil, nil, unsupportedNode(statementKind, "limit-all", node)
	}
	limitNode, err := node.LimitExpression()
	if err != nil || limitNode == nil {
		return nil, nil, parserFailure()
	}
	limit, err := integerExpressionValue(limitNode)
	if err != nil {
		return nil, nil, unsupportedNode(statementKind, "limit-expression", limitNode)
	}
	var offset *int64
	offsetNode, err := node.Offset()
	if err != nil {
		return nil, nil, parserFailure()
	}
	if offsetNode != nil {
		value, err := integerExpressionValue(offsetNode)
		if err != nil {
			return nil, nil, unsupportedNode(statementKind, "offset-expression", offsetNode)
		}
		offset = &value
	}
	return &limit, offset, nil
}

func integerExpressionValue(node gsql.ASTExpressionNode) (int64, error) {
	literal, ok := node.(*gsql.ASTIntLiteral)
	if !ok {
		return 0, fmt.Errorf("not an integer literal")
	}
	image, err := literal.Image()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(image, 10, 64)
}

func mapAlias(node *gsql.ASTAlias) (*queryast.Identifier, error) {
	if node == nil {
		return nil, nil
	}
	value, err := node.GetAsString()
	if err != nil {
		return nil, parserFailure()
	}
	identifier, err := queryast.NewIdentifier(value)
	if err != nil {
		return nil, invalid("invalid GoogleSQL identifier")
	}
	return &identifier, nil
}

func mapPath(node *gsql.ASTPathExpression) (queryast.IdentifierPath, error) {
	values, err := node.ToIdentifierVector()
	if err != nil || len(values) == 0 {
		return queryast.IdentifierPath{}, parserFailure()
	}
	parts := make([]queryast.Identifier, len(values))
	for index, value := range values {
		identifier, err := queryast.NewIdentifier(value)
		if err != nil {
			return queryast.IdentifierPath{}, invalid("invalid GoogleSQL identifier")
		}
		parts[index] = identifier
	}
	path, err := queryast.NewIdentifierPath(parts)
	if err != nil {
		return queryast.IdentifierPath{}, invalid("invalid GoogleSQL identifier path")
	}
	return path, nil
}

func mapTablePath(node *gsql.ASTPathExpression) (queryast.IdentifierPath, error) {
	values, err := node.ToIdentifierVector()
	if err != nil || len(values) == 0 {
		return queryast.IdentifierPath{}, parserFailure()
	}
	if len(values) == 1 {
		segments := strings.Split(values[0], ".")
		if len(segments) >= 2 && len(segments) <= 3 {
			values = segments
		}
	}
	parts := make([]queryast.Identifier, len(values))
	for index, value := range values {
		identifier, err := queryast.NewIdentifier(value)
		if err != nil {
			return queryast.IdentifierPath{}, invalid("invalid GoogleSQL table path")
		}
		parts[index] = identifier
	}
	path, err := queryast.NewIdentifierPath(parts)
	if err != nil {
		return queryast.IdentifierPath{}, invalid("invalid GoogleSQL table path")
	}
	return path, nil
}

func gsqlNodeIsNil(node gsql.ASTNode) bool {
	if node == nil {
		return true
	}
	value := reflect.ValueOf(node)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func mapIdentifier(node *gsql.ASTIdentifier) (queryast.Identifier, error) {
	if node == nil {
		return queryast.Identifier{}, parserFailure()
	}
	value, err := node.GetAsString()
	if err != nil {
		return queryast.Identifier{}, parserFailure()
	}
	identifier, err := queryast.NewIdentifier(value)
	if err != nil {
		return queryast.Identifier{}, invalid("invalid GoogleSQL identifier")
	}
	return identifier, nil
}
