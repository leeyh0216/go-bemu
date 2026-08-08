package googlesql

import (
	"context"
	"fmt"
	"runtime"
	"strings"

	gsql "github.com/goccy/go-googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

const CapabilityGoogleSQLScriptV1 = "query.googlesql.script-v1"

type scriptVariable struct {
	name    string
	logical semantic.Type
}

type scriptAnalyzer struct {
	ctx         context.Context
	request     ports.QueryRequest
	snapshot    *catalogSnapshot
	options     *gsql.AnalyzerOptions
	outputs     []*gsql.AnalyzerOutput
	values      []*gsql.Value
	constants   []*gsql.SimpleConstant
	variables   map[string]scriptVariable
	symbolNames map[string]string
}

// analyzeScript resolves each child in source order. go-googlesql v0.4.0
// exposes no resolved-script API:
// AnalyzeStatementFromParserAST rejects DECLARE and SET and does not carry
// variables into the next statement. Their expression spans are therefore
// analyzed with the official AnalyzeExpression entrypoint, and typed NULL
// constants are registered in the same request-scoped official catalog before
// analyzing subsequent original statement AST handles.
func (gateway *Gateway) analyzeScript(
	ctx context.Context,
	request ports.QueryRequest,
	document parsedDocument,
	snapshot *catalogSnapshot,
	options *gsql.AnalyzerOptions,
) (semantic.Statement, error) {
	mapper := statementMapper{sourceDigest: document.source.Digest()}
	children := make([]queryast.Statement, 0, len(document.statements))
	for _, external := range document.statements {
		if err := ctx.Err(); err != nil {
			return semantic.Statement{}, err
		}
		child, err := mapper.mapStatement(external)
		if err != nil {
			return semantic.Statement{}, err
		}
		children = append(children, child)
	}
	syntax, err := queryast.NewScriptStatement(document.source, children)
	if err != nil {
		return semantic.Statement{}, analyzerBoundaryFailureAt("script-syntax")
	}
	state := &scriptAnalyzer{
		ctx: ctx, request: request, snapshot: snapshot, options: options,
		variables: make(map[string]scriptVariable), symbolNames: declaredScriptVariableNames(children),
	}
	defer func() { runtime.KeepAlive(state) }()

	analyzedChildren := make([]semantic.Statement, 0, len(children))
	for index, child := range children {
		if err := ctx.Err(); err != nil {
			return semantic.Statement{}, err
		}
		var analyzed semantic.Statement
		switch statement := child.(type) {
		case *queryast.DeclareStatement:
			external, ok := document.statements[index].(*gsql.ASTVariableDeclaration)
			if !ok {
				return semantic.Statement{}, analyzerBoundaryFailureAt("script-declare-handle")
			}
			analyzed, err = state.analyzeDeclare(statement, external)
		case *queryast.SetStatement:
			external, ok := document.statements[index].(*gsql.ASTSingleAssignment)
			if !ok {
				return semantic.Statement{}, analyzerBoundaryFailureAt("script-set-handle")
			}
			analyzed, err = state.analyzeSet(statement, external)
		case *queryast.SelectStatement, *queryast.InsertStatement,
			*queryast.UpdateStatement, *queryast.DeleteStatement, *queryast.MergeStatement:
			analyzed, err = state.analyzeStatement(statement, document.statements[index])
		default:
			return semantic.Statement{}, unsupportedGoogleSQLScript()
		}
		if err != nil {
			return semantic.Statement{}, err
		}
		analyzedChildren = append(analyzedChildren, analyzed)
	}
	return mergeScriptAnalysis(syntax, analyzedChildren)
}

func declaredScriptVariableNames(statements []queryast.Statement) map[string]string {
	names := make(map[string]string)
	for _, statement := range statements {
		declaration, ok := statement.(*queryast.DeclareStatement)
		if !ok {
			continue
		}
		for _, variable := range declaration.Variables() {
			names[strings.ToLower(variable.Value())] = variable.Value()
		}
	}
	return names
}

func unsupportedGoogleSQLScript() error {
	return fmt.Errorf(
		"%w: capability=%s multi-statement input is outside the supported GoogleSQL script subset",
		domain.ErrUnsupported, CapabilityGoogleSQLScriptV1,
	)
}

func (state *scriptAnalyzer) analyzeDeclare(
	statement *queryast.DeclareStatement,
	external *gsql.ASTVariableDeclaration,
) (semantic.Statement, error) {
	var (
		analyzed     semantic.Statement
		inferred     semantic.Type
		externalType gsql.Googlesql_TypeNode
		err          error
	)
	if defaultValue := statement.DefaultValue(); defaultValue != nil {
		externalExpression, inspectErr := external.DefaultValue()
		if inspectErr != nil || externalExpression == nil {
			return semantic.Statement{}, analyzerBoundaryFailureAt("script-declare-default")
		}
		analyzed, inferred, externalType, err = state.analyzeExpression(statement, defaultValue, externalExpression)
		if err != nil {
			return semantic.Statement{}, err
		}
	} else {
		analyzed, err = emptyScriptChildAnalysis(statement)
		if err != nil {
			return semantic.Statement{}, err
		}
	}

	logical := inferred
	if declaredType := statement.Type(); declaredType != nil {
		logical, externalType, err = semanticTypeFromSyntax(state.snapshot.typeFactory, declaredType)
		if err != nil {
			return semantic.Statement{}, err
		}
		if statement.DefaultValue() != nil && !sameSemanticType(logical, inferred) {
			return semantic.Statement{}, fmt.Errorf(
				"%w: code=%s script declaration type does not match its analyzed default",
				domain.ErrInvalidQuery, ErrorAnalysisInvalidV1,
			)
		}
	}
	if externalType == nil {
		return semantic.Statement{}, analyzerBoundaryFailureAt("script-variable-type")
	}
	for _, variable := range statement.Variables() {
		name := strings.ToLower(variable.Value())
		if _, duplicate := state.variables[name]; duplicate {
			return semantic.Statement{}, fmt.Errorf(
				"%w: code=%s script variable declaration is duplicated",
				domain.ErrInvalidQuery, ErrorAnalysisInvalidV1,
			)
		}
		if err := state.registerVariable(variable.Value(), logical, externalType); err != nil {
			return semantic.Statement{}, err
		}
	}
	return analyzed, nil
}

func (state *scriptAnalyzer) analyzeSet(
	statement *queryast.SetStatement,
	external *gsql.ASTSingleAssignment,
) (semantic.Statement, error) {
	target := statement.Target().Segments()
	if len(target) != 1 {
		return semantic.Statement{}, unsupportedGoogleSQLScript()
	}
	variable, found := state.variables[strings.ToLower(target[0])]
	if !found {
		return semantic.Statement{}, fmt.Errorf(
			"%w: code=%s script assignment target was not declared",
			domain.ErrInvalidQuery, ErrorAnalysisInvalidV1,
		)
	}
	externalExpression, err := external.Expression()
	if err != nil || externalExpression == nil {
		return semantic.Statement{}, analyzerBoundaryFailureAt("script-set-expression")
	}
	analyzed, logical, _, err := state.analyzeExpression(statement, statement.Value(), externalExpression)
	if err != nil {
		return semantic.Statement{}, err
	}
	if !sameSemanticType(variable.logical, logical) {
		return semantic.Statement{}, fmt.Errorf(
			"%w: code=%s script assignment type does not match its declaration",
			domain.ErrInvalidQuery, ErrorAnalysisInvalidV1,
		)
	}
	return analyzed, nil
}

func (state *scriptAnalyzer) analyzeStatement(
	syntax queryast.Statement,
	external gsql.ASTStatementNode,
) (semantic.Statement, error) {
	output, err := gsql.AnalyzeStatementFromParserAST(
		external, state.options, state.request.SQL, state.snapshot.root, state.snapshot.typeFactory,
	)
	if err != nil {
		return semantic.Statement{}, classifyAnalysisError(err)
	}
	if output == nil {
		return semantic.Statement{}, analyzerBoundaryFailure()
	}
	state.outputs = append(state.outputs, output)
	resolved, err := output.ResolvedStatement()
	if err != nil || resolved == nil {
		return semantic.Statement{}, analyzerBoundaryFailure()
	}
	return projectResolvedStatementWithVariables(
		state.ctx, state.request, syntax, state.snapshot, resolved, state.variableNames(),
	)
}

func (state *scriptAnalyzer) analyzeExpression(
	syntax queryast.Statement,
	root queryast.Expression,
	external gsql.ASTExpressionNode,
) (semantic.Statement, semantic.Type, gsql.Googlesql_TypeNode, error) {
	span := root.Span()
	externalSpan, err := sourceSpan(external)
	if err != nil || externalSpan.Start() != span.Start() || externalSpan.End() != span.End() {
		return semantic.Statement{}, semantic.Type{}, nil, analyzerBoundaryFailureAt("script-expression-identity")
	}
	if span.Start() < 0 || span.End() < span.Start() || span.End() > len(state.request.SQL) {
		return semantic.Statement{}, semantic.Type{}, nil, analyzerBoundaryFailureAt("script-expression-span")
	}
	// The binding has no AnalyzeExpressionFromParserAST API. The fragment is
	// selected only by the official AST byte span; it is never tokenized,
	// retained, logged, included in an error, or exposed through a port.
	fragment := state.request.SQL[span.Start():span.End()]
	output, err := gsql.AnalyzeExpression(fragment, state.options, state.snapshot.root, state.snapshot.typeFactory)
	if err != nil {
		return semantic.Statement{}, semantic.Type{}, nil, classifyAnalysisError(err)
	}
	if output == nil {
		return semantic.Statement{}, semantic.Type{}, nil, analyzerBoundaryFailure()
	}
	state.outputs = append(state.outputs, output)
	resolved, err := output.ResolvedExpr()
	if err != nil || resolved == nil {
		return semantic.Statement{}, semantic.Type{}, nil, analyzerBoundaryFailureAt("script-resolved-expression")
	}
	externalType, err := resolved.Type()
	if err != nil || externalType == nil {
		return semantic.Statement{}, semantic.Type{}, nil, analyzerBoundaryFailureAt("script-resolved-expression-type")
	}
	logical, err := semanticTypeFromGoogleSQL(externalType)
	if err != nil {
		return semantic.Statement{}, semantic.Type{}, nil, err
	}
	analyzed, err := projectResolvedExpression(
		state.ctx, state.request, syntax, root, state.snapshot, resolved, span.Start(), logical,
		state.variableNames(),
	)
	if err != nil {
		return semantic.Statement{}, semantic.Type{}, nil, err
	}
	return analyzed, logical, externalType, nil
}

func (state *scriptAnalyzer) registerVariable(
	name string,
	logical semantic.Type,
	externalType gsql.Googlesql_TypeNode,
) error {
	value, err := gsql.NewValueNull(externalType)
	if err != nil || value == nil {
		return analyzerBoundaryFailureAt("script-variable-value")
	}
	constant, err := gsql.NewSimpleConstantCreate([]string{name}, value)
	if err != nil || constant == nil {
		return analyzerBoundaryFailureAt("script-variable-constant")
	}
	if err := state.snapshot.root.AddConstant(constant); err != nil {
		return fmt.Errorf(
			"%w: code=%s script variable cannot be registered",
			domain.ErrInvalidQuery, ErrorAnalysisInvalidV1,
		)
	}
	state.values = append(state.values, value)
	state.constants = append(state.constants, constant)
	state.variables[strings.ToLower(name)] = scriptVariable{name: name, logical: logical}
	return nil
}

func (state *scriptAnalyzer) variableNames() map[string]string {
	names := make(map[string]string, len(state.symbolNames))
	for normalized, name := range state.symbolNames {
		names[normalized] = name
	}
	return names
}

func projectResolvedExpression(
	ctx context.Context,
	request ports.QueryRequest,
	syntax queryast.Statement,
	root queryast.Expression,
	snapshot *catalogSnapshot,
	resolved gsql.ResolvedExprNode,
	locationOffset int,
	rootType semantic.Type,
	variables map[string]string,
) (semantic.Statement, error) {
	projection := &resolvedProjection{
		ctx: ctx, snapshot: snapshot, canonicalByID: make(map[int32]semantic.Type),
		locationOffset: locationOffset, scriptVariables: variables,
	}
	if err := projection.walk(resolved); err != nil {
		return semantic.Statement{}, err
	}
	relations, err := projection.relationBindings(request, syntax)
	if err != nil {
		return semantic.Statement{}, err
	}
	expressionTypes, _, err := projection.expressionBindings(syntax, nil)
	if err != nil {
		return semantic.Statement{}, err
	}
	expressionTypes = upsertExpressionType(expressionTypes, root.NodeKey(), rootType)
	symbolBindings, err := projection.symbolBindings(syntax)
	if err != nil {
		return semantic.Statement{}, err
	}
	expressions, err := queryast.Expressions(syntax)
	if err != nil {
		return semantic.Statement{}, analyzerBoundaryFailureAt("script-syntax-expressions")
	}
	return semantic.NewStatement(semantic.StatementDescriptor{
		Syntax: syntax, ResolvedKind: syntax.Kind(), RelationBindings: relations,
		ExpressionTypes: expressionTypes, ExpressionsComplete: len(expressionTypes) == len(expressions),
		SymbolBindings: symbolBindings,
	})
}

func upsertExpressionType(
	descriptors []semantic.ExpressionTypeDescriptor,
	key queryast.NodeKey,
	typ semantic.Type,
) []semantic.ExpressionTypeDescriptor {
	for index := range descriptors {
		if descriptors[index].Key == key {
			descriptors[index].Type = typ
			return descriptors
		}
	}
	return append(descriptors, semantic.ExpressionTypeDescriptor{Key: key, Type: typ})
}

func emptyScriptChildAnalysis(syntax queryast.Statement) (semantic.Statement, error) {
	return semantic.NewStatement(semantic.StatementDescriptor{
		Syntax: syntax, ResolvedKind: syntax.Kind(), ExpressionsComplete: true,
	})
}

func mergeScriptAnalysis(
	syntax *queryast.ScriptStatement,
	children []semantic.Statement,
) (semantic.Statement, error) {
	if len(children) == 0 {
		return semantic.Statement{}, analyzerBoundaryFailureAt("script-children")
	}
	relationBindings := make([]semantic.RelationBindingDescriptor, 0)
	expressionTypes := make([]semantic.ExpressionTypeDescriptor, 0)
	symbolBindings := make([]semantic.SymbolBindingDescriptor, 0)
	expressionsComplete := true
	for _, child := range children {
		relations, err := queryast.Relations(child.Syntax())
		if err != nil {
			return semantic.Statement{}, analyzerBoundaryFailureAt("script-child-relations")
		}
		for _, relation := range relations {
			binding, err := child.RequireRelationBinding(relation.NodeKey())
			if err != nil {
				return semantic.Statement{}, err
			}
			descriptor := semantic.RelationBindingDescriptor{Key: binding.Key(), Kind: binding.Kind()}
			if reference, physical := binding.Reference(); physical {
				descriptor.Reference = reference
			} else if name, local := binding.LocalName(); local {
				descriptor.LocalName = name
			}
			relationBindings = append(relationBindings, descriptor)
		}
		expressions, err := queryast.Expressions(child.Syntax())
		if err != nil {
			return semantic.Statement{}, analyzerBoundaryFailureAt("script-child-expressions")
		}
		for _, expression := range expressions {
			if typ, found := child.ExpressionType(expression.NodeKey()); found {
				expressionTypes = append(expressionTypes, semantic.ExpressionTypeDescriptor{
					Key: expression.NodeKey(), Type: typ,
				})
			}
			if binding, found := child.SymbolBinding(expression.NodeKey()); found {
				symbolBindings = append(symbolBindings, semantic.SymbolBindingDescriptor{
					Key: binding.Key(), Kind: binding.Kind(), Name: binding.Name(),
				})
			}
		}
		expressionsComplete = expressionsComplete && child.ExpressionsComplete()
	}
	lastColumns := children[len(children)-1].OutputColumns()
	outputColumns := make([]semantic.ColumnDescriptor, len(lastColumns))
	for index, column := range lastColumns {
		outputColumns[index] = semantic.ColumnDescriptor{Name: column.Name(), Type: column.Type()}
	}
	return semantic.NewStatement(semantic.StatementDescriptor{
		Syntax: syntax, ResolvedKind: queryast.StatementScript,
		RelationBindings: relationBindings, ExpressionTypes: expressionTypes,
		SymbolBindings: symbolBindings, ExpressionsComplete: expressionsComplete,
		OutputColumns: outputColumns,
	})
}

func semanticTypeFromSyntax(
	typeFactory *gsql.TypeFactory,
	typ queryast.Type,
) (semantic.Type, gsql.Googlesql_TypeNode, error) {
	descriptor, err := semanticDescriptorFromSyntax(typ)
	if err != nil {
		return semantic.Type{}, nil, err
	}
	logical, err := semantic.NewType(descriptor)
	if err != nil {
		return semantic.Type{}, nil, canonicalSchemaFailure(err)
	}
	external, err := googleSQLType(typeFactory, descriptor)
	if err != nil {
		return semantic.Type{}, nil, err
	}
	return logical, external, nil
}

func semanticDescriptorFromSyntax(typ queryast.Type) (semantic.TypeDescriptor, error) {
	switch value := typ.(type) {
	case *queryast.ScalarType:
		descriptor := semantic.TypeDescriptor{}
		switch value.Kind() {
		case queryast.TypeBool:
			descriptor.Kind = semantic.TypeBool
		case queryast.TypeInt64:
			descriptor.Kind = semantic.TypeInt64
		case queryast.TypeFloat64:
			descriptor.Kind = semantic.TypeFloat64
		case queryast.TypeNumeric:
			descriptor.Kind = semantic.TypeNumeric
		case queryast.TypeBigNumeric:
			descriptor.Kind = semantic.TypeBigNumeric
		case queryast.TypeString:
			descriptor.Kind = semantic.TypeString
		case queryast.TypeBytes:
			descriptor.Kind = semantic.TypeBytes
		case queryast.TypeDate:
			descriptor.Kind = semantic.TypeDate
		case queryast.TypeDatetime:
			descriptor.Kind = semantic.TypeDatetime
		case queryast.TypeTime:
			descriptor.Kind = semantic.TypeTime
		case queryast.TypeTimestamp:
			descriptor.Kind = semantic.TypeTimestamp
		case queryast.TypeJSON:
			descriptor.Kind = semantic.TypeJSON
		case queryast.TypeGeography:
			return semantic.TypeDescriptor{}, fmt.Errorf(
				"%w: capability=%s script variable type is unsupported",
				domain.ErrUnsupported, domain.GapGeographyUnsupportedV1,
			)
		default:
			return semantic.TypeDescriptor{}, unsupportedGoogleSQLScript()
		}
		descriptor.Precision = value.Precision()
		descriptor.Scale = value.Scale()
		return descriptor, nil
	case *queryast.ArrayType:
		element, err := semanticDescriptorFromSyntax(value.Element())
		if err != nil {
			return semantic.TypeDescriptor{}, err
		}
		return semantic.TypeDescriptor{Kind: semantic.TypeArray, Element: &element}, nil
	case *queryast.StructType:
		descriptor := semantic.TypeDescriptor{Kind: semantic.TypeStruct}
		for _, field := range value.Fields() {
			name := field.Name()
			if name == nil {
				return semantic.TypeDescriptor{}, unsupportedGoogleSQLScript()
			}
			fieldType, err := semanticDescriptorFromSyntax(field.Type())
			if err != nil {
				return semantic.TypeDescriptor{}, err
			}
			descriptor.Fields = append(descriptor.Fields, semantic.FieldDescriptor{
				Name: name.Value(), Type: fieldType,
			})
		}
		return descriptor, nil
	default:
		return semantic.TypeDescriptor{}, unsupportedGoogleSQLScript()
	}
}
