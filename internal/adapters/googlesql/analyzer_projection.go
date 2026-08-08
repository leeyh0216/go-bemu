package googlesql

import (
	"context"
	"fmt"
	"strings"

	gsql "github.com/goccy/go-googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

type resolvedProjection struct {
	ctx             context.Context
	snapshot        *catalogSnapshot
	references      []domain.TableReference
	canonicalByID   map[int32]semantic.Type
	expressions     []gsql.ResolvedExprNode
	locationOffset  int
	scriptVariables map[string]string
}

func projectResolvedStatement(
	ctx context.Context,
	request ports.QueryRequest,
	syntax queryast.Statement,
	snapshot *catalogSnapshot,
	resolved gsql.ResolvedStatementNode,
) (semantic.Statement, error) {
	return projectResolvedStatementWithVariables(ctx, request, syntax, snapshot, resolved, nil)
}

func projectResolvedStatementWithVariables(
	ctx context.Context,
	request ports.QueryRequest,
	syntax queryast.Statement,
	snapshot *catalogSnapshot,
	resolved gsql.ResolvedStatementNode,
	variables map[string]string,
) (semantic.Statement, error) {
	kind, err := resolvedStatementKind(resolved)
	if err != nil {
		return semantic.Statement{}, err
	}
	projection := &resolvedProjection{
		ctx: ctx, snapshot: snapshot,
		canonicalByID: make(map[int32]semantic.Type), scriptVariables: variables,
	}
	if err := projection.walk(resolved); err != nil {
		return semantic.Statement{}, err
	}
	bindings, err := projection.relationBindings(request, syntax)
	if err != nil {
		return semantic.Statement{}, err
	}
	outputs, err := projection.outputColumns(resolved)
	if err != nil {
		return semantic.Statement{}, err
	}
	expressionTypes, expressionsComplete, err := projection.expressionBindings(syntax, outputs)
	if err != nil {
		return semantic.Statement{}, err
	}
	symbolBindings, err := projection.symbolBindings(syntax)
	if err != nil {
		return semantic.Statement{}, err
	}
	return semantic.NewStatement(semantic.StatementDescriptor{
		Syntax: syntax, ResolvedKind: kind, RelationBindings: bindings,
		ExpressionTypes: expressionTypes, ExpressionsComplete: expressionsComplete,
		SymbolBindings: symbolBindings, OutputColumns: outputs,
	})
}

func resolvedStatementKind(resolved gsql.ResolvedStatementNode) (queryast.StatementKind, error) {
	switch statement := resolved.(type) {
	case *gsql.ResolvedQueryStmt:
		return queryast.StatementSelect, nil
	case *gsql.ResolvedInsertStmt:
		return queryast.StatementInsert, nil
	case *gsql.ResolvedUpdateStmt:
		return queryast.StatementUpdate, nil
	case *gsql.ResolvedDeleteStmt:
		return queryast.StatementDelete, nil
	case *gsql.ResolvedMergeStmt:
		return queryast.StatementMerge, nil
	case *gsql.ResolvedCreateTableAsSelectStmt:
		return "", unsupportedResolvedStatement()
	case *gsql.ResolvedCreateTableStmt:
		return queryast.StatementCreateTable, nil
	case *gsql.ResolvedAlterTableStmt:
		return queryast.StatementAlterTable, nil
	case *gsql.ResolvedDropStmt:
		objectType, err := statement.ObjectType()
		if err != nil {
			return "", analyzerBoundaryFailure()
		}
		if !strings.EqualFold(objectType, "TABLE") {
			return "", unsupportedResolvedStatement()
		}
		return queryast.StatementDropTable, nil
	case *gsql.ResolvedTruncateStmt:
		return queryast.StatementTruncateTable, nil
	default:
		return "", unsupportedResolvedStatement()
	}
}

func unsupportedResolvedStatement() error {
	return fmt.Errorf(
		"%w: capability=%s resolved statement kind is outside the engine-neutral contract",
		domain.ErrUnsupported, CapabilityResolvedStatementV1,
	)
}

func (projection *resolvedProjection) walk(node gsql.ResolvedNode) error {
	if err := projection.ctx.Err(); err != nil {
		return err
	}
	if tableScan, ok := node.(*gsql.ResolvedTableScan); ok {
		if err := projection.collectTableScan(tableScan); err != nil {
			return err
		}
	}
	if expression, ok := node.(gsql.ResolvedExprNode); ok {
		projection.expressions = append(projection.expressions, expression)
	}
	children, err := node.GetChildNodes()
	if err != nil {
		return analyzerBoundaryFailureAt("resolved-children", err)
	}
	for _, child := range children {
		if child == nil {
			continue
		}
		if err := projection.walk(child); err != nil {
			return err
		}
	}
	return nil
}

func (projection *resolvedProjection) collectTableScan(scan *gsql.ResolvedTableScan) error {
	registered, err := projection.registeredTable(scan)
	if err != nil {
		return err
	}
	projection.references = append(projection.references, registered.reference)
	columns, err := scan.ColumnList()
	if err != nil {
		return analyzerBoundaryFailureAt("table-scan-columns", err)
	}
	indices, err := scan.ColumnIndexList()
	if err != nil {
		return analyzerBoundaryFailureAt("table-scan-indices", err)
	}
	if len(indices) == 0 && len(columns) <= len(registered.logical) {
		indices = make([]int32, len(columns))
		for index := range columns {
			indices[index] = int32(index)
		}
	}
	if len(columns) != len(indices) {
		return analyzerBoundaryFailureAt("table-scan-shape")
	}
	for index, column := range columns {
		fieldIndex := indices[index]
		if column == nil || fieldIndex < 0 || int(fieldIndex) >= len(registered.logical) {
			return analyzerBoundaryFailureAt("table-scan-field-index")
		}
		columnID, err := column.ColumnId()
		if err != nil {
			return analyzerBoundaryFailureAt("table-scan-column-id", err)
		}
		projection.canonicalByID[columnID] = registered.logical[fieldIndex]
	}
	return nil
}

func (projection *resolvedProjection) registeredTable(scan *gsql.ResolvedTableScan) (registeredTable, error) {
	tableNode, err := scan.Table()
	if err != nil || tableNode == nil {
		return registeredTable{}, analyzerBoundaryFailureAt("table-scan-table", err)
	}
	fullName, err := tableNode.FullName()
	if err != nil {
		return registeredTable{}, analyzerBoundaryFailureAt("table-scan-full-name", err)
	}
	registered, found := projection.snapshot.tables[strings.ToLower(fullName)]
	if !found {
		return registeredTable{}, analyzerBoundaryFailureAt("table-scan-canonical-lookup")
	}
	return registered, nil
}

func (projection *resolvedProjection) relationBindings(
	request ports.QueryRequest,
	syntax queryast.Statement,
) ([]semantic.RelationBindingDescriptor, error) {
	relations, err := queryast.Relations(syntax)
	if err != nil {
		return nil, analyzerBoundaryFailureAt("syntax-relations")
	}
	targetKeys, err := syntaxTargetKeys(syntax)
	if err != nil {
		return nil, unsupportedResolvedStatement()
	}
	targets := make(map[queryast.NodeKey]struct{}, len(targetKeys))
	for _, key := range targetKeys {
		targets[key] = struct{}{}
	}
	resolvedReferences := make(map[string]struct{}, len(projection.references))
	for _, reference := range projection.references {
		resolvedReferences[tableKey(reference)] = struct{}{}
	}

	bindings := make([]semantic.RelationBindingDescriptor, 0, len(relations))
	physicalReferences := make(map[string]struct{})
	for _, relation := range relations {
		if relation.Kind() != queryast.RelationTable {
			bindings = append(bindings, semantic.RelationBindingDescriptor{
				Key: relation.NodeKey(), Kind: semantic.RelationLocal,
			})
			continue
		}
		table, ok := relation.(*queryast.TableRelation)
		if !ok {
			return nil, analyzerBoundaryFailureAt("syntax-table-relation")
		}
		segments := table.Path().Segments()
		reference, resolveErr := resolveAnalyzedPath(request, segments)
		_, target := targets[table.NodeKey()]
		if resolveErr == nil {
			if registered, found := projection.snapshot.tables[tableKey(reference)]; found || target {
				descriptor := semantic.RelationBindingDescriptor{
					Key: table.NodeKey(), Kind: semantic.RelationPhysical, Reference: reference,
				}
				if found {
					descriptor.Schema = domain.CloneFields(registered.schema)
					if registered.timePartitioning != nil {
						clone := *registered.timePartitioning
						descriptor.TimePartitioning = &clone
					}
				}
				bindings = append(bindings, descriptor)
				physicalReferences[tableKey(reference)] = struct{}{}
				continue
			}
		}
		// A one-segment name accepted by the official analyzer but absent from
		// the canonical snapshot can only be a local query scope (for example,
		// a CTE). Multi-segment misses are never guessed into local bindings.
		if !target && len(segments) == 1 {
			bindings = append(bindings, semantic.RelationBindingDescriptor{
				Key: table.NodeKey(), Kind: semantic.RelationLocal, LocalName: segments[0],
			})
			continue
		}
		return nil, analyzerBoundaryFailureAt("canonical-relation-binding")
	}
	for key := range resolvedReferences {
		if _, bound := physicalReferences[key]; !bound {
			return nil, analyzerBoundaryFailureAt("resolved-relation-attestation")
		}
	}
	return bindings, nil
}

func syntaxTargetKeys(statement queryast.Statement) ([]queryast.NodeKey, error) {
	switch value := statement.(type) {
	case *queryast.ScriptStatement:
		var keys []queryast.NodeKey
		for _, child := range value.Statements() {
			childKeys, err := syntaxTargetKeys(child)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
		return keys, nil
	case *queryast.InsertStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.UpdateStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.DeleteStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.MergeStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.CreateTableStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.DropTableStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.AlterTableStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.TruncateTableStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.DeclareStatement, *queryast.SetStatement, *queryast.SelectStatement:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown syntax statement")
	}
}

type byteRange struct {
	start int
	end   int
}

func (projection *resolvedProjection) expressionBindings(
	syntax queryast.Statement,
	outputs []semantic.ColumnDescriptor,
) ([]semantic.ExpressionTypeDescriptor, bool, error) {
	expressions, err := queryast.Expressions(syntax)
	if err != nil {
		return nil, false, analyzerBoundaryFailureAt("syntax-expressions", err)
	}
	resolvedByRange := make(map[byteRange][]semantic.Type)
	for _, expression := range projection.expressions {
		range_, present, err := resolvedByteRange(expression)
		if err != nil {
			return nil, false, analyzerBoundaryFailureAt("resolved-expression-location", err)
		}
		if !present {
			continue
		}
		range_.start += projection.locationOffset
		range_.end += projection.locationOffset
		typ, err := projection.resolvedExpressionType(expression)
		if err != nil {
			return nil, false, err
		}
		resolvedByRange[range_] = append(resolvedByRange[range_], typ)
	}

	bound := make(map[queryast.NodeKey]semantic.Type, len(expressions))
	for _, expression := range expressions {
		span := expression.Span()
		candidates := resolvedByRange[byteRange{start: span.Start(), end: span.End()}]
		if len(candidates) == 0 || !sameSemanticTypes(candidates) {
			continue
		}
		bound[expression.NodeKey()] = candidates[0]
	}
	if roots := selectOutputExpressions(syntax); len(roots) == len(outputs) {
		for index, expression := range roots {
			bound[expression.NodeKey()] = outputs[index].Type
		}
	}
	bindings := make([]semantic.ExpressionTypeDescriptor, 0, len(bound))
	for _, expression := range expressions {
		typ, exists := bound[expression.NodeKey()]
		if !exists {
			continue
		}
		bindings = append(bindings, semantic.ExpressionTypeDescriptor{Key: expression.NodeKey(), Type: typ})
	}
	return bindings, len(bindings) == len(expressions), nil
}

func (projection *resolvedProjection) symbolBindings(
	syntax queryast.Statement,
) ([]semantic.SymbolBindingDescriptor, error) {
	if len(projection.scriptVariables) == 0 {
		return nil, nil
	}
	type symbolCandidate struct {
		kind semantic.SymbolBindingKind
		name string
	}
	resolvedByRange := make(map[byteRange][]symbolCandidate)
	unmatchedVariables := make(map[byteRange][]string)
	for _, expression := range projection.expressions {
		candidate := symbolCandidate{}
		registeredVariable := false
		switch resolved := expression.(type) {
		case *gsql.ResolvedColumnRef:
			candidate.kind = semantic.SymbolColumn
		case *gsql.ResolvedLiteral:
			candidate.kind = semantic.SymbolValue
		case *gsql.ResolvedConstant:
			constant, err := resolved.Constant()
			if err != nil || constant == nil {
				return nil, analyzerBoundaryFailureAt("resolved-script-variable")
			}
			path, err := constant.NamePath()
			if err != nil {
				return nil, analyzerBoundaryFailureAt("resolved-script-variable-name")
			}
			if len(path) != 1 {
				continue
			}
			name, registered := projection.scriptVariables[strings.ToLower(path[0])]
			if !registered {
				continue
			}
			candidate = symbolCandidate{kind: semantic.SymbolScriptVariable, name: name}
			registeredVariable = true
		default:
			continue
		}
		range_, present, err := resolvedByteRange(expression)
		if err != nil {
			return nil, analyzerBoundaryFailureAt("resolved-symbol-location")
		}
		if !present {
			if registeredVariable {
				return nil, analyzerBoundaryFailureAt("resolved-script-variable-location")
			}
			continue
		}
		range_.start += projection.locationOffset
		range_.end += projection.locationOffset
		duplicate := false
		for _, existing := range resolvedByRange[range_] {
			if existing.kind == candidate.kind && strings.EqualFold(existing.name, candidate.name) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			resolvedByRange[range_] = append(resolvedByRange[range_], candidate)
		}
		if registeredVariable {
			unmatchedVariables[range_] = append(unmatchedVariables[range_], candidate.name)
		}
	}

	expressions, err := queryast.Expressions(syntax)
	if err != nil {
		return nil, analyzerBoundaryFailureAt("script-variable-syntax")
	}
	bindings := make([]semantic.SymbolBindingDescriptor, 0)
	for _, expression := range expressions {
		identifier, ok := expression.(*queryast.IdentifierExpression)
		if !ok || identifier.Path().Len() != 1 {
			continue
		}
		segment := identifier.Path().Segments()[0]
		name, declared := projection.scriptVariables[strings.ToLower(segment)]
		if !declared {
			continue
		}
		span := identifier.Span()
		candidates := resolvedByRange[byteRange{start: span.Start(), end: span.End()}]
		if len(candidates) == 0 {
			// The analyzer may omit a parse location for references to an
			// output column, notably in ORDER BY. Every resolved script
			// constant is still accounted for by unmatchedVariables below, so
			// an unlocated identifier can safely be bound as a non-variable.
			bindings = append(bindings, semantic.SymbolBindingDescriptor{
				Key: identifier.NodeKey(), Kind: semantic.SymbolValue, Name: name,
			})
			continue
		}
		kind := semantic.SymbolValue
		variableName := ""
		for _, candidate := range candidates {
			switch candidate.kind {
			case semantic.SymbolScriptVariable:
				if kind == semantic.SymbolColumn {
					return nil, analyzerBoundaryFailureAt("resolved-script-symbol-ambiguity")
				}
				if variableName != "" && !strings.EqualFold(variableName, candidate.name) {
					return nil, analyzerBoundaryFailureAt("resolved-script-symbol-ambiguity")
				}
				kind, variableName = candidate.kind, candidate.name
			case semantic.SymbolColumn:
				if kind == semantic.SymbolScriptVariable {
					return nil, analyzerBoundaryFailureAt("resolved-script-symbol-ambiguity")
				}
				kind = candidate.kind
			}
		}
		if kind == semantic.SymbolScriptVariable && !strings.EqualFold(variableName, name) {
			return nil, analyzerBoundaryFailureAt("resolved-script-variable-identity")
		}
		bindings = append(bindings, semantic.SymbolBindingDescriptor{
			Key: identifier.NodeKey(), Kind: kind, Name: name,
		})
		if kind == semantic.SymbolScriptVariable {
			delete(unmatchedVariables, byteRange{start: span.Start(), end: span.End()})
		}
	}
	for _, names := range unmatchedVariables {
		if len(names) != 0 {
			return nil, analyzerBoundaryFailureAt("resolved-script-variable-attestation")
		}
	}
	return bindings, nil
}

func selectOutputExpressions(statement queryast.Statement) []queryast.Expression {
	selectStatement, ok := statement.(*queryast.SelectStatement)
	if !ok {
		return nil
	}
	selectQuery, ok := selectStatement.Query().Body().(*queryast.SelectQuery)
	if !ok {
		return nil
	}
	items := selectQuery.Items()
	expressions := make([]queryast.Expression, len(items))
	for index, item := range items {
		expressions[index] = item.Expression()
	}
	return expressions
}

func (projection *resolvedProjection) resolvedExpressionType(expression gsql.ResolvedExprNode) (semantic.Type, error) {
	if columnReference, ok := expression.(*gsql.ResolvedColumnRef); ok {
		column, err := columnReference.Column()
		if err != nil || column == nil {
			return semantic.Type{}, analyzerBoundaryFailureAt("resolved-column-reference", err)
		}
		columnID, err := column.ColumnId()
		if err != nil {
			return semantic.Type{}, analyzerBoundaryFailureAt("resolved-column-reference-id", err)
		}
		if canonical, found := projection.canonicalByID[columnID]; found {
			return canonical, nil
		}
	}
	typeNode, err := expression.Type()
	if err != nil || typeNode == nil {
		return semantic.Type{}, analyzerBoundaryFailureAt("resolved-expression-type", err)
	}
	return semanticTypeFromGoogleSQL(typeNode)
}

func resolvedByteRange(node gsql.ResolvedNode) (byteRange, bool, error) {
	location, err := node.GetParseLocationRangeOrNULL()
	if err != nil {
		return byteRange{}, false, err
	}
	if location == nil {
		return byteRange{}, false, nil
	}
	start, err := location.Start()
	if err != nil || start == nil {
		return byteRange{}, false, err
	}
	end, err := location.End()
	if err != nil || end == nil {
		return byteRange{}, false, err
	}
	startOffset, err := start.GetByteOffset()
	if err != nil {
		return byteRange{}, false, err
	}
	endOffset, err := end.GetByteOffset()
	if err != nil {
		return byteRange{}, false, err
	}
	return byteRange{start: int(startOffset), end: int(endOffset)}, true, nil
}

func sameSemanticTypes(types []semantic.Type) bool {
	for index := 1; index < len(types); index++ {
		if !sameSemanticType(types[0], types[index]) {
			return false
		}
	}
	return true
}

func sameSemanticType(left, right semantic.Type) bool {
	if left.Kind() != right.Kind() || left.RoundingMode() != right.RoundingMode() {
		return false
	}
	leftPrecision, leftHasPrecision := left.Precision()
	rightPrecision, rightHasPrecision := right.Precision()
	leftScale, leftHasScale := left.Scale()
	rightScale, rightHasScale := right.Scale()
	if leftHasPrecision != rightHasPrecision || leftPrecision != rightPrecision || leftHasScale != rightHasScale || leftScale != rightScale {
		return false
	}
	leftElement, leftHasElement := left.Element()
	rightElement, rightHasElement := right.Element()
	if leftHasElement != rightHasElement || leftHasElement && !sameSemanticType(leftElement, rightElement) {
		return false
	}
	leftFields, rightFields := left.Fields(), right.Fields()
	if len(leftFields) != len(rightFields) {
		return false
	}
	for index := range leftFields {
		if leftFields[index].Name() != rightFields[index].Name() || !sameSemanticType(leftFields[index].Type(), rightFields[index].Type()) {
			return false
		}
	}
	return true
}

func (projection *resolvedProjection) outputColumns(resolved gsql.ResolvedStatementNode) ([]semantic.ColumnDescriptor, error) {
	var (
		columns []*gsql.ResolvedOutputColumn
		err     error
	)
	switch statement := resolved.(type) {
	case *gsql.ResolvedQueryStmt:
		columns, err = statement.OutputColumnList()
	case *gsql.ResolvedCreateTableAsSelectStmt:
		columns, err = statement.OutputColumnList()
	default:
		return nil, nil
	}
	if err != nil {
		return nil, analyzerBoundaryFailure()
	}
	result := make([]semantic.ColumnDescriptor, 0, len(columns))
	for index, output := range columns {
		if output == nil {
			return nil, analyzerBoundaryFailure()
		}
		name, err := output.Name()
		if err != nil {
			return nil, analyzerBoundaryFailure()
		}
		if name == "" || strings.HasPrefix(name, "$") {
			name = fmt.Sprintf("f%d_", index)
		}
		column, err := output.Column()
		if err != nil || column == nil {
			return nil, analyzerBoundaryFailure()
		}
		columnID, err := column.ColumnId()
		if err != nil {
			return nil, analyzerBoundaryFailure()
		}
		logical, found := projection.canonicalByID[columnID]
		if !found {
			typeNode, err := column.Type()
			if err != nil || typeNode == nil {
				return nil, analyzerBoundaryFailure()
			}
			logical, err = semanticTypeFromGoogleSQL(typeNode)
			if err != nil {
				return nil, err
			}
		}
		result = append(result, semantic.ColumnDescriptor{Name: name, Type: logical})
	}
	return result, nil
}

func resolveAnalyzedPath(request ports.QueryRequest, path []string) (domain.TableReference, error) {
	projectID := request.DefaultProjectID
	if projectID == "" {
		projectID = request.ProjectID
	}
	var reference domain.TableReference
	switch len(path) {
	case 1:
		reference = domain.TableReference{ProjectID: projectID, DatasetID: request.DefaultDataset, TableID: path[0]}
	case 2:
		reference = domain.TableReference{ProjectID: projectID, DatasetID: path[0], TableID: path[1]}
	case 3:
		reference = domain.TableReference{ProjectID: path[0], DatasetID: path[1], TableID: path[2]}
	default:
		return domain.TableReference{}, fmt.Errorf("%w: analyzed table path is outside the canonical reference contract", domain.ErrInvalidQuery)
	}
	if reference.ProjectID == "" || reference.DatasetID == "" || reference.TableID == "" {
		return domain.TableReference{}, fmt.Errorf("%w: analyzed table path requires project and dataset context", domain.ErrInvalidQuery)
	}
	return reference, nil
}

func semanticTypeFromGoogleSQL(typeNode gsql.Googlesql_TypeNode) (semantic.Type, error) {
	kind, err := typeNode.Kind()
	if err != nil {
		return semantic.Type{}, analyzerBoundaryFailure()
	}
	descriptor := semantic.TypeDescriptor{}
	switch kind {
	case gsql.TypeKindTypeBool:
		descriptor.Kind = semantic.TypeBool
	case gsql.TypeKindTypeInt64:
		descriptor.Kind = semantic.TypeInt64
	case gsql.TypeKindTypeDouble:
		descriptor.Kind = semantic.TypeFloat64
	case gsql.TypeKindTypeNumeric:
		descriptor.Kind = semantic.TypeNumeric
	case gsql.TypeKindTypeBignumeric:
		descriptor.Kind = semantic.TypeBigNumeric
	case gsql.TypeKindTypeString:
		descriptor.Kind = semantic.TypeString
	case gsql.TypeKindTypeBytes:
		descriptor.Kind = semantic.TypeBytes
	case gsql.TypeKindTypeDate:
		descriptor.Kind = semantic.TypeDate
	case gsql.TypeKindTypeDatetime:
		descriptor.Kind = semantic.TypeDatetime
	case gsql.TypeKindTypeTime:
		descriptor.Kind = semantic.TypeTime
	case gsql.TypeKindTypeTimestamp:
		descriptor.Kind = semantic.TypeTimestamp
	case gsql.TypeKindTypeJson:
		descriptor.Kind = semantic.TypeJSON
	case gsql.TypeKindTypeArray:
		array, err := typeNode.AsArray()
		if err != nil || array == nil {
			return semantic.Type{}, analyzerBoundaryFailure()
		}
		element, err := array.ElementType()
		if err != nil || element == nil {
			return semantic.Type{}, analyzerBoundaryFailure()
		}
		elementType, err := semanticTypeFromGoogleSQL(element)
		if err != nil {
			return semantic.Type{}, err
		}
		elementDescriptor := descriptorFromSemanticType(elementType)
		descriptor = semantic.TypeDescriptor{Kind: semantic.TypeArray, Element: &elementDescriptor}
	case gsql.TypeKindTypeStruct:
		structure, err := typeNode.AsStruct()
		if err != nil || structure == nil {
			return semantic.Type{}, analyzerBoundaryFailure()
		}
		fields, err := structure.Fields()
		if err != nil {
			return semantic.Type{}, analyzerBoundaryFailure()
		}
		descriptor.Kind = semantic.TypeStruct
		descriptor.Fields = make([]semantic.FieldDescriptor, 0, len(fields))
		for _, field := range fields {
			if field == nil || field.Type_ == nil {
				return semantic.Type{}, analyzerBoundaryFailure()
			}
			fieldType, err := semanticTypeFromGoogleSQL(field.Type_)
			if err != nil {
				return semantic.Type{}, err
			}
			descriptor.Fields = append(descriptor.Fields, semantic.FieldDescriptor{
				Name: field.Name, Type: descriptorFromSemanticType(fieldType),
			})
		}
	default:
		return semantic.Type{}, fmt.Errorf(
			"%w: capability=%s resolved expression type is outside the engine-neutral contract",
			domain.ErrUnsupported, CapabilityResolvedStatementV1,
		)
	}
	logical, err := semantic.NewType(descriptor)
	if err != nil {
		return semantic.Type{}, canonicalSchemaFailure(err)
	}
	return logical, nil
}

func descriptorFromSemanticType(typ semantic.Type) semantic.TypeDescriptor {
	descriptor := semantic.TypeDescriptor{Kind: typ.Kind(), RoundingMode: typ.RoundingMode()}
	if precision, present := typ.Precision(); present {
		descriptor.Precision = &precision
	}
	if scale, present := typ.Scale(); present {
		descriptor.Scale = &scale
	}
	if element, present := typ.Element(); present {
		elementDescriptor := descriptorFromSemanticType(element)
		descriptor.Element = &elementDescriptor
	}
	for _, field := range typ.Fields() {
		descriptor.Fields = append(descriptor.Fields, semantic.FieldDescriptor{
			Name: field.Name(), Type: descriptorFromSemanticType(field.Type()),
		})
	}
	return descriptor
}
