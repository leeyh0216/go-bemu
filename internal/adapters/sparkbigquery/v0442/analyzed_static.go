package v0442

import (
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

// matchAnalyzedStaticOverwrite recognizes the exact constant-false MERGE
// emitted by connector 0.44.2. It deliberately accepts only an AST already
// built by the official GoogleSQL gateway; source text never participates in
// syntax recognition here.
func matchAnalyzedStaticOverwrite(statement semantic.Statement, request ports.QueryRequest) (ports.QueryOperation, bool, error) {
	merge, ok := statement.Syntax().(*queryast.MergeStatement)
	if !ok {
		return ports.QueryOperation{}, false, nil
	}
	if !isStaticOverwriteSource(merge.MergeSource()) || !isFalseLiteral(merge.Condition()) {
		return ports.QueryOperation{}, false, nil
	}
	if !isStaticOverwriteActions(merge.When()) {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %s MERGE actions differ from the connector profile", domain.ErrUnsupported, StaticOverwriteProfile)
	}
	destination, err := statement.RequireRelationBinding(merge.Target().NodeKey())
	if err != nil {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %s target binding is missing", domain.ErrPrecondition, StaticOverwriteProfile)
	}
	sourceRelation := mergeSourceTableRelation(merge.MergeSource())
	if sourceRelation == nil {
		return ports.QueryOperation{}, false, nil
	}
	source, err := statement.RequireRelationBinding(sourceRelation.NodeKey())
	if err != nil {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %s source binding is missing", domain.ErrPrecondition, StaticOverwriteProfile)
	}
	destinationReference, destinationOK := destination.Reference()
	sourceReference, sourceOK := source.Reference()
	if !destinationOK || !sourceOK {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %s relation binding is not a canonical table", domain.ErrPrecondition, StaticOverwriteProfile)
	}
	operation, err := ports.NewQueryOperation(ports.QueryOperationDescriptor{
		Kind: ports.QueryOperationSparkStaticOverwrite, ProfileID: StaticOverwriteProfile,
		Destination: destinationReference, Source: sourceReference, Request: request,
	})
	if err != nil {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %v", domain.ErrInvalid, err)
	}
	return operation, true, nil
}

func isStaticOverwriteSource(relation queryast.Relation) bool {
	return mergeSourceTableRelation(relation) != nil
}

func mergeSourceTableRelation(relation queryast.Relation) *queryast.TableRelation {
	subquery, ok := relation.(*queryast.SubqueryRelation)
	if !ok || subquery.Alias() != nil {
		return nil
	}
	query := subquery.Query()
	if len(query.With()) != 0 || query.Recursive() || len(query.OrderBy()) != 0 || query.Limit() != nil || query.Offset() != nil {
		return nil
	}
	selectQuery, ok := query.Body().(*queryast.SelectQuery)
	if !ok || selectQuery.Distinct() || selectQuery.Where() != nil || len(selectQuery.GroupBy()) != 0 || selectQuery.Having() != nil || selectQuery.Qualify() != nil {
		return nil
	}
	items := selectQuery.Items()
	if len(items) != 1 || items[0].Alias() != nil {
		return nil
	}
	star, ok := items[0].Expression().(*queryast.StarExpression)
	if !ok || star.Qualifier() != nil {
		return nil
	}
	table, ok := selectQuery.From().(*queryast.TableRelation)
	if !ok || table.Alias() != nil {
		return nil
	}
	return table
}

func isFalseLiteral(expression queryast.Expression) bool {
	literal, ok := expression.(*queryast.BooleanLiteral)
	return ok && !literal.Value()
}

func isStaticOverwriteActions(when []queryast.MergeWhen) bool {
	if len(when) != 2 {
		return false
	}
	first, second := when[0], when[1]
	return first.Match() == queryast.MergeNotMatchedByTarget && first.Condition() == nil && first.Action().Kind() == queryast.MergeActionInsertRow &&
		second.Match() == queryast.MergeNotMatchedBySource && second.Condition() == nil && second.Action().Kind() == queryast.MergeActionDelete
}
