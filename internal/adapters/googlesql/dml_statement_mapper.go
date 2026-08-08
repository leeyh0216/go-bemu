package googlesql

import (
	gsql "github.com/goccy/go-googlesql"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

func (mapper *statementMapper) mapUpdateStatement(node *gsql.ASTUpdateStatement) (queryast.Statement, error) {
	if err := rejectUpdateOptionalClauses(node); err != nil {
		return nil, err
	}
	targetNode, err := node.GetTargetPathForNonNested()
	if err != nil || targetNode == nil {
		return nil, parserFailure()
	}
	aliasNode, err := node.Alias()
	if err != nil {
		return nil, parserFailure()
	}
	alias, err := mapAlias(aliasNode)
	if err != nil {
		return nil, err
	}
	target, err := mapper.mapTargetRelation(targetNode, alias)
	if err != nil {
		return nil, err
	}
	itemsNode, err := node.UpdateItemList()
	if err != nil || itemsNode == nil {
		return nil, parserFailure()
	}
	assignments, err := mapper.mapUpdateAssignments(queryast.StatementUpdate, itemsNode)
	if err != nil {
		return nil, err
	}
	var from queryast.Relation
	fromNode, err := node.FromClause()
	if err != nil {
		return nil, parserFailure()
	}
	if fromNode != nil {
		tableExpression, inspectErr := fromNode.TableExpression()
		if inspectErr != nil || tableExpression == nil {
			return nil, parserFailure()
		}
		from, err = mapper.mapRelation(queryast.StatementUpdate, tableExpression)
		if err != nil {
			return nil, err
		}
	}
	var where queryast.Expression
	whereNode, err := node.Where()
	if err != nil {
		return nil, parserFailure()
	}
	if whereNode != nil {
		where, err = mapper.mapExpression(queryast.StatementUpdate, whereNode)
		if err != nil {
			return nil, err
		}
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewUpdateStatement(source, target, assignments, from, where)
}

func rejectUpdateOptionalClauses(node *gsql.ASTUpdateStatement) error {
	for _, optional := range []struct {
		kind string
		get  func() (gsql.ASTNode, error)
	}{
		{kind: "update-assert-rows-modified", get: func() (gsql.ASTNode, error) { return node.AssertRowsModified() }},
		{kind: "update-hint", get: func() (gsql.ASTNode, error) { return node.Hint() }},
		{kind: "update-offset", get: func() (gsql.ASTNode, error) { return node.Offset() }},
		{kind: "update-returning", get: func() (gsql.ASTNode, error) { return node.Returning() }},
	} {
		value, err := optional.get()
		if err != nil {
			return parserFailure()
		}
		if !gsqlNodeIsNil(value) {
			return unsupportedNode(queryast.StatementUpdate, optional.kind, value)
		}
	}
	return nil
}

func (mapper *statementMapper) mapDeleteStatement(node *gsql.ASTDeleteStatement) (queryast.Statement, error) {
	for _, optional := range []struct {
		kind string
		get  func() (gsql.ASTNode, error)
	}{
		{kind: "delete-assert-rows-modified", get: func() (gsql.ASTNode, error) { return node.AssertRowsModified() }},
		{kind: "delete-hint", get: func() (gsql.ASTNode, error) { return node.Hint() }},
		{kind: "delete-offset", get: func() (gsql.ASTNode, error) { return node.Offset() }},
		{kind: "delete-returning", get: func() (gsql.ASTNode, error) { return node.Returning() }},
	} {
		value, err := optional.get()
		if err != nil {
			return nil, parserFailure()
		}
		if !gsqlNodeIsNil(value) {
			return nil, unsupportedNode(queryast.StatementDelete, optional.kind, value)
		}
	}
	targetNode, err := node.GetTargetPathForNonNested()
	if err != nil || targetNode == nil {
		return nil, parserFailure()
	}
	aliasNode, err := node.Alias()
	if err != nil {
		return nil, parserFailure()
	}
	alias, err := mapAlias(aliasNode)
	if err != nil {
		return nil, err
	}
	target, err := mapper.mapTargetRelation(targetNode, alias)
	if err != nil {
		return nil, err
	}
	var where queryast.Expression
	whereNode, err := node.Where()
	if err != nil {
		return nil, parserFailure()
	}
	if whereNode != nil {
		where, err = mapper.mapExpression(queryast.StatementDelete, whereNode)
		if err != nil {
			return nil, err
		}
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewDeleteStatement(source, target, where)
}

func (mapper *statementMapper) mapMergeStatement(node *gsql.ASTMergeStatement) (queryast.Statement, error) {
	targetNode, err := node.TargetPath()
	if err != nil || targetNode == nil {
		return nil, parserFailure()
	}
	aliasNode, err := node.Alias()
	if err != nil {
		return nil, parserFailure()
	}
	alias, err := mapAlias(aliasNode)
	if err != nil {
		return nil, err
	}
	target, err := mapper.mapTargetRelation(targetNode, alias)
	if err != nil {
		return nil, err
	}
	sourceNode, err := node.TableExpression()
	if err != nil || sourceNode == nil {
		return nil, parserFailure()
	}
	relation, err := mapper.mapRelation(queryast.StatementMerge, sourceNode)
	if err != nil {
		return nil, err
	}
	conditionNode, err := node.MergeCondition()
	if err != nil || conditionNode == nil {
		return nil, parserFailure()
	}
	condition, err := mapper.mapExpression(queryast.StatementMerge, conditionNode)
	if err != nil {
		return nil, err
	}
	whenList, err := node.WhenClauses()
	if err != nil || whenList == nil {
		return nil, parserFailure()
	}
	children, err := astChildren(whenList)
	if err != nil {
		return nil, err
	}
	when := make([]queryast.MergeWhen, 0, len(children))
	for _, child := range children {
		clause, ok := child.(*gsql.ASTMergeWhenClause)
		if !ok {
			return nil, unsupportedNode(queryast.StatementMerge, "merge-when-clause", child)
		}
		mapped, mapErr := mapper.mapMergeWhen(clause)
		if mapErr != nil {
			return nil, mapErr
		}
		when = append(when, mapped)
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewMergeStatement(source, target, relation, condition, when)
}

func (mapper *statementMapper) mapMergeWhen(node *gsql.ASTMergeWhenClause) (queryast.MergeWhen, error) {
	externalMatch, err := node.MatchType()
	if err != nil {
		return queryast.MergeWhen{}, parserFailure()
	}
	var match queryast.MergeMatchKind
	switch externalMatch {
	case gsql.ASTMergeWhenClauseEnums_MatchTypeMatched:
		match = queryast.MergeMatched
	case gsql.ASTMergeWhenClauseEnums_MatchTypeNotMatchedBySource:
		match = queryast.MergeNotMatchedBySource
	case gsql.ASTMergeWhenClauseEnums_MatchTypeNotMatchedByTarget:
		match = queryast.MergeNotMatchedByTarget
	default:
		return queryast.MergeWhen{}, unsupportedNode(queryast.StatementMerge, "merge-match-type", node)
	}
	var condition queryast.Expression
	conditionNode, err := node.SearchCondition()
	if err != nil {
		return queryast.MergeWhen{}, parserFailure()
	}
	if conditionNode != nil {
		condition, err = mapper.mapExpression(queryast.StatementMerge, conditionNode)
		if err != nil {
			return queryast.MergeWhen{}, err
		}
	}
	actionNode, err := node.Action()
	if err != nil || actionNode == nil {
		return queryast.MergeWhen{}, parserFailure()
	}
	action, err := mapper.mapMergeAction(actionNode)
	if err != nil {
		return queryast.MergeWhen{}, err
	}
	return queryast.NewMergeWhen(match, condition, action)
}

func (mapper *statementMapper) mapMergeAction(node *gsql.ASTMergeAction) (queryast.MergeAction, error) {
	actionType, err := node.ActionType()
	if err != nil {
		return queryast.MergeAction{}, parserFailure()
	}
	switch actionType {
	case gsql.ASTMergeActionEnums_ActionTypeInsert:
		columnsNode, inspectErr := node.InsertColumnList()
		if inspectErr != nil {
			return queryast.MergeAction{}, parserFailure()
		}
		columns, mapErr := mapIdentifierList(columnsNode, queryast.StatementMerge, "merge-insert-column")
		if mapErr != nil {
			return queryast.MergeAction{}, mapErr
		}
		rowNode, inspectErr := node.InsertRow()
		if inspectErr != nil || rowNode == nil {
			return queryast.MergeAction{}, parserFailure()
		}
		values, mapErr := mapper.mapExpressionChildren(queryast.StatementMerge, rowNode, "merge-insert-value")
		if mapErr != nil {
			return queryast.MergeAction{}, mapErr
		}
		if len(columns) == 0 && len(values) == 0 {
			return queryast.NewMergeInsertRowAction(), nil
		}
		return queryast.NewMergeInsertAction(columns, values)
	case gsql.ASTMergeActionEnums_ActionTypeUpdate:
		items, inspectErr := node.UpdateItemList()
		if inspectErr != nil || items == nil {
			return queryast.MergeAction{}, parserFailure()
		}
		assignments, mapErr := mapper.mapUpdateAssignments(queryast.StatementMerge, items)
		if mapErr != nil {
			return queryast.MergeAction{}, mapErr
		}
		return queryast.NewMergeUpdateAction(assignments)
	case gsql.ASTMergeActionEnums_ActionTypeDelete:
		return queryast.NewMergeDeleteAction(), nil
	default:
		return queryast.MergeAction{}, unsupportedNode(queryast.StatementMerge, "merge-action", node)
	}
}

func (mapper *statementMapper) mapUpdateAssignments(statementKind queryast.StatementKind, list *gsql.ASTUpdateItemList) ([]queryast.Assignment, error) {
	children, err := astChildren(list)
	if err != nil {
		return nil, err
	}
	assignments := make([]queryast.Assignment, 0, len(children))
	for _, child := range children {
		item, ok := child.(*gsql.ASTUpdateItem)
		if !ok {
			return nil, unsupportedNode(statementKind, "update-item", child)
		}
		for _, nested := range []func() (gsql.ASTNode, error){
			func() (gsql.ASTNode, error) { return item.DeleteStatement() },
			func() (gsql.ASTNode, error) { return item.InsertStatement() },
			func() (gsql.ASTNode, error) { return item.UpdateStatement() },
		} {
			value, inspectErr := nested()
			if inspectErr != nil {
				return nil, parserFailure()
			}
			if !gsqlNodeIsNil(value) {
				return nil, unsupportedNode(statementKind, "nested-dml-update-item", value)
			}
		}
		set, inspectErr := item.SetValue()
		if inspectErr != nil || set == nil {
			return nil, parserFailure()
		}
		pathNode, inspectErr := set.Path()
		if inspectErr != nil || pathNode == nil {
			return nil, parserFailure()
		}
		path, mapErr := mapGeneralizedPath(pathNode)
		if mapErr != nil {
			return nil, mapErr
		}
		valueNode, inspectErr := set.Value()
		if inspectErr != nil || valueNode == nil {
			return nil, parserFailure()
		}
		value, mapErr := mapper.mapExpression(statementKind, valueNode)
		if mapErr != nil {
			return nil, mapErr
		}
		assignment, mapErr := queryast.NewAssignment(path, value)
		if mapErr != nil {
			return nil, parserFailure()
		}
		assignments = append(assignments, assignment)
	}
	return assignments, nil
}

func (mapper *statementMapper) mapDeclareStatement(node *gsql.ASTVariableDeclaration) (queryast.Statement, error) {
	variablesNode, err := node.VariableList()
	if err != nil || variablesNode == nil {
		return nil, parserFailure()
	}
	variables, err := mapIdentifierList(variablesNode, queryast.StatementDeclare, "declare-variable")
	if err != nil {
		return nil, err
	}
	var typ queryast.Type
	typeNode, err := node.Type()
	if err != nil {
		return nil, parserFailure()
	}
	if typeNode != nil {
		typ, err = mapper.mapType(queryast.StatementDeclare, typeNode)
		if err != nil {
			return nil, err
		}
	}
	var defaultValue queryast.Expression
	defaultNode, err := node.DefaultValue()
	if err != nil {
		return nil, parserFailure()
	}
	if defaultNode != nil {
		defaultValue, err = mapper.mapExpression(queryast.StatementDeclare, defaultNode)
		if err != nil {
			return nil, err
		}
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewDeclareStatement(source, variables, typ, defaultValue)
}

func (mapper *statementMapper) mapSetStatement(node *gsql.ASTSingleAssignment) (queryast.Statement, error) {
	variableNode, err := node.Variable()
	if err != nil || variableNode == nil {
		return nil, parserFailure()
	}
	variable, err := mapIdentifier(variableNode)
	if err != nil {
		return nil, err
	}
	target, err := queryast.NewIdentifierPath([]queryast.Identifier{variable})
	if err != nil {
		return nil, parserFailure()
	}
	valueNode, err := node.Expression()
	if err != nil || valueNode == nil {
		return nil, parserFailure()
	}
	value, err := mapper.mapExpression(queryast.StatementSet, valueNode)
	if err != nil {
		return nil, err
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewSetStatement(source, target, value)
}

func (mapper *statementMapper) mapExpressionChildren(statementKind queryast.StatementKind, node gsql.ASTNode, childKind string) ([]queryast.Expression, error) {
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	values := make([]queryast.Expression, 0, len(children))
	for _, child := range children {
		expressionNode, ok := child.(gsql.ASTExpressionNode)
		if !ok {
			return nil, unsupportedNode(statementKind, childKind, child)
		}
		value, mapErr := mapper.mapExpression(statementKind, expressionNode)
		if mapErr != nil {
			return nil, mapErr
		}
		values = append(values, value)
	}
	return values, nil
}

func mapIdentifierList(node gsql.ASTNode, statementKind queryast.StatementKind, childKind string) ([]queryast.Identifier, error) {
	if gsqlNodeIsNil(node) {
		return nil, nil
	}
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	values := make([]queryast.Identifier, 0, len(children))
	for _, child := range children {
		identifierNode, ok := child.(*gsql.ASTIdentifier)
		if !ok {
			return nil, unsupportedNode(statementKind, childKind, child)
		}
		identifier, mapErr := mapIdentifier(identifierNode)
		if mapErr != nil {
			return nil, mapErr
		}
		values = append(values, identifier)
	}
	return values, nil
}

func mapGeneralizedPath(node gsql.ASTGeneralizedPathExpressionNode) (queryast.IdentifierPath, error) {
	switch path := node.(type) {
	case *gsql.ASTPathExpression:
		return mapPath(path)
	case *gsql.ASTDotIdentifier:
		leftNode, err := path.Expr()
		if err != nil || leftNode == nil {
			return queryast.IdentifierPath{}, parserFailure()
		}
		leftPathNode, ok := leftNode.(gsql.ASTGeneralizedPathExpressionNode)
		if !ok {
			return queryast.IdentifierPath{}, unsupportedNode(queryast.StatementUpdate, "assignment-path", leftNode)
		}
		left, err := mapGeneralizedPath(leftPathNode)
		if err != nil {
			return queryast.IdentifierPath{}, err
		}
		nameNode, err := path.Name()
		if err != nil || nameNode == nil {
			return queryast.IdentifierPath{}, parserFailure()
		}
		name, err := mapIdentifier(nameNode)
		if err != nil {
			return queryast.IdentifierPath{}, err
		}
		return queryast.NewIdentifierPath(append(left.Parts(), name))
	default:
		return queryast.IdentifierPath{}, unsupportedNode(queryast.StatementUpdate, "assignment-path", node)
	}
}
