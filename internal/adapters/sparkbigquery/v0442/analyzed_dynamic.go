package v0442

import (
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

func matchAnalyzedDynamicTimeOverwrite(statement semantic.Statement, request ports.QueryRequest) (ports.QueryOperation, bool, error) {
	script, ok := statement.Syntax().(*queryast.ScriptStatement)
	if !ok {
		return ports.QueryOperation{}, false, nil
	}
	children := script.Statements()
	if len(children) != 2 {
		return ports.QueryOperation{}, false, nil
	}
	if _, ok := children[0].(*queryast.DeclareStatement); !ok {
		return ports.QueryOperation{}, false, nil
	}
	merge, ok := children[1].(*queryast.MergeStatement)
	if !ok || !isFalseLiteral(merge.Condition()) {
		return ports.QueryOperation{}, false, nil
	}
	if containsAnalyzedFunction(script, "RANGE_BUCKET") && containsAnalyzedFunction(script, "GENERATE_ARRAY") {
		return ports.QueryOperation{}, true, fmt.Errorf(
			"%w: connector range-partition overwrite remains an explicit gap; capability=%s model_version=%s",
			domain.ErrUnsupported, domain.GapSparkDynamicRangePartitionOverwriteV1, DynamicRangeOverwriteProfile,
		)
	}
	function, field, granularity, ok := analyzedPartitionFunction(script)
	if !ok || !isDynamicOverwriteActions(merge.When()) {
		return ports.QueryOperation{}, false, nil
	}
	sourceRelation, ok := merge.MergeSource().(*queryast.TableRelation)
	if !ok {
		return ports.QueryOperation{}, false, nil
	}
	if !isDynamicConnectorAliases(merge.Target(), sourceRelation) {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %s relation aliases differ from the connector profile", domain.ErrUnsupported, DynamicTimeOverwriteProfile)
	}
	destinationBinding, err := statement.RequireRelationBinding(merge.Target().NodeKey())
	if err != nil {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %s target binding is missing", domain.ErrPrecondition, DynamicTimeOverwriteProfile)
	}
	sourceBinding, err := statement.RequireRelationBinding(sourceRelation.NodeKey())
	if err != nil {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %s source binding is missing", domain.ErrPrecondition, DynamicTimeOverwriteProfile)
	}
	destination, destinationOK := destinationBinding.Reference()
	source, sourceOK := sourceBinding.Reference()
	if !destinationOK || !sourceOK {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %s relation binding is not a canonical table", domain.ErrPrecondition, DynamicTimeOverwriteProfile)
	}
	insert := merge.When()[1].Action()
	fields := make([]string, 0, len(insert.Columns()))
	for _, column := range insert.Columns() {
		fields = append(fields, column.Value())
	}
	operation, err := ports.NewQueryOperation(ports.QueryOperationDescriptor{
		Kind: ports.QueryOperationSparkDynamicTimeOverwrite, ProfileID: DynamicTimeOverwriteProfile,
		Destination: destination, Source: source, PartitionFunction: function,
		PartitionField: field, Granularity: granularity, InsertFields: fields, Request: request,
	})
	if err != nil {
		return ports.QueryOperation{}, true, fmt.Errorf("%w: %v", domain.ErrInvalid, err)
	}
	return operation, true, nil
}

func isDynamicConnectorAliases(target, source *queryast.TableRelation) bool {
	if target == nil || source == nil || target.Alias() == nil || source.Alias() == nil {
		return false
	}
	return target.Alias().Value() == "__target_0123456789abcdef0123456789abcdef" &&
		source.Alias().Value() == "__source_fedcba9876543210fedcba9876543210"
}

func isDynamicOverwriteActions(when []queryast.MergeWhen) bool {
	if len(when) != 2 {
		return false
	}
	delete, insert := when[0], when[1]
	if delete.Match() != queryast.MergeNotMatchedBySource || delete.Condition() == nil || delete.Action().Kind() != queryast.MergeActionDelete ||
		insert.Match() != queryast.MergeNotMatchedByTarget || insert.Condition() != nil || insert.Action().Kind() != queryast.MergeActionInsert {
		return false
	}
	return len(insert.Action().Columns()) != 0 && len(insert.Action().Columns()) == len(insert.Action().Values())
}

func containsAnalyzedFunction(statement queryast.Statement, expected string) bool {
	expressions, err := queryast.Expressions(statement)
	if err != nil {
		return false
	}
	for _, expression := range expressions {
		call, ok := expression.(*queryast.FunctionCall)
		if ok && strings.EqualFold(lastPathSegment(call.Name()), expected) {
			return true
		}
	}
	return false
}

func analyzedPartitionFunction(statement queryast.Statement) (function, field, granularity string, matched bool) {
	expressions, err := queryast.Expressions(statement)
	if err != nil {
		return "", "", "", false
	}
	for _, expression := range expressions {
		call, ok := expression.(*queryast.FunctionCall)
		if !ok {
			continue
		}
		name := strings.ToUpper(lastPathSegment(call.Name()))
		if name != "DATE_TRUNC" && name != "TIMESTAMP_TRUNC" {
			continue
		}
		arguments := call.Arguments()
		if len(arguments) != 2 {
			continue
		}
		fieldPath, fieldOK := identifierPath(arguments[0])
		granularityPath, granularityOK := identifierPath(arguments[1])
		if !fieldOK || !granularityOK {
			continue
		}
		return name, lastPathSegment(fieldPath), strings.ToUpper(lastPathSegment(granularityPath)), true
	}
	return "", "", "", false
}

func identifierPath(expression queryast.Expression) (queryast.IdentifierPath, bool) {
	identifier, ok := expression.(*queryast.IdentifierExpression)
	if !ok {
		return queryast.IdentifierPath{}, false
	}
	return identifier.Path(), true
}

func lastPathSegment(path queryast.IdentifierPath) string {
	segments := path.Segments()
	if len(segments) == 0 {
		return ""
	}
	return segments[len(segments)-1]
}
