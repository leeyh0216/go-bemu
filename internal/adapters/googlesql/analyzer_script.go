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

const CapabilityConnectorScriptV1 = "query.googlesql.connector-script-v1"

type scriptVariable struct {
	logical semantic.Type
}

type connectorScriptAnalyzer struct {
	ctx       context.Context
	request   ports.QueryRequest
	snapshot  *catalogSnapshot
	options   *gsql.AnalyzerOptions
	outputs   []*gsql.AnalyzerOutput
	values    []*gsql.Value
	constants []*gsql.SimpleConstant
	variables map[string]scriptVariable
}

// analyzeConnectorScript supports the DECLARE/SET/MERGE shape emitted by the
// Spark BigQuery connector. go-googlesql v0.4.0 exposes no resolved-script API:
// AnalyzeStatementFromParserAST rejects DECLARE and SET and does not carry
// variables into the next statement. Their expression spans are therefore
// analyzed with the official AnalyzeExpression entrypoint, and typed NULL
// constants are registered in the same request-scoped official catalog before
// analyzing the original MERGE AST handle.
func (gateway *Gateway) analyzeConnectorScript(
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
	if !isConnectorScript(children) {
		return semantic.Statement{}, unsupportedConnectorScript()
	}
	syntax, err := queryast.NewScriptStatement(document.source, children)
	if err != nil {
		return semantic.Statement{}, analyzerBoundaryFailureAt("script-syntax")
	}
	state := &connectorScriptAnalyzer{
		ctx: ctx, request: request, snapshot: snapshot, options: options,
		variables: make(map[string]scriptVariable),
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
		case *queryast.MergeStatement:
			analyzed, err = state.analyzeStatement(statement, document.statements[index])
		default:
			return semantic.Statement{}, unsupportedConnectorScript()
		}
		if err != nil {
			return semantic.Statement{}, err
		}
		analyzedChildren = append(analyzedChildren, analyzed)
	}
	return mergeScriptAnalysis(syntax, analyzedChildren)
}

func isConnectorScript(statements []queryast.Statement) bool {
	if len(statements) < 2 || statements[len(statements)-1].Kind() != queryast.StatementMerge {
		return false
	}
	hasDeclaration := false
	for index, statement := range statements {
		if index == len(statements)-1 {
			return statement.Kind() == queryast.StatementMerge && hasDeclaration
		}
		switch statement.Kind() {
		case queryast.StatementDeclare:
			hasDeclaration = true
		case queryast.StatementSet:
		default:
			return false
		}
	}
	return false
}

func unsupportedConnectorScript() error {
	return fmt.Errorf(
		"%w: capability=%s multi-statement input is outside the connector script profile",
		domain.ErrUnsupported, CapabilityConnectorScriptV1,
	)
}

func (state *connectorScriptAnalyzer) analyzeDeclare(
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

func (state *connectorScriptAnalyzer) analyzeSet(
	statement *queryast.SetStatement,
	external *gsql.ASTSingleAssignment,
) (semantic.Statement, error) {
	target := statement.Target().Segments()
	if len(target) != 1 {
		return semantic.Statement{}, unsupportedConnectorScript()
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

func (state *connectorScriptAnalyzer) analyzeStatement(
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
	return projectResolvedStatement(state.ctx, state.request, syntax, state.snapshot, resolved)
}

func (state *connectorScriptAnalyzer) analyzeExpression(
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
	)
	if err != nil {
		return semantic.Statement{}, semantic.Type{}, nil, err
	}
	return analyzed, logical, externalType, nil
}

func (state *connectorScriptAnalyzer) registerVariable(
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
	state.variables[strings.ToLower(name)] = scriptVariable{logical: logical}
	return nil
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
) (semantic.Statement, error) {
	projection := &resolvedProjection{
		ctx: ctx, snapshot: snapshot, canonicalByID: make(map[int32]semantic.Type),
		locationOffset: locationOffset,
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
	expressions, err := queryast.Expressions(syntax)
	if err != nil {
		return semantic.Statement{}, analyzerBoundaryFailureAt("script-syntax-expressions")
	}
	return semantic.NewStatement(semantic.StatementDescriptor{
		Syntax: syntax, ResolvedKind: syntax.Kind(), RelationBindings: relations,
		ExpressionTypes: expressionTypes, ExpressionsComplete: len(expressionTypes) == len(expressions),
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
	relationBindings := make([]semantic.RelationBindingDescriptor, 0)
	expressionTypes := make([]semantic.ExpressionTypeDescriptor, 0)
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
		}
		expressionsComplete = expressionsComplete && child.ExpressionsComplete()
	}
	return semantic.NewStatement(semantic.StatementDescriptor{
		Syntax: syntax, ResolvedKind: queryast.StatementScript,
		RelationBindings: relationBindings, ExpressionTypes: expressionTypes,
		ExpressionsComplete: expressionsComplete,
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
			return semantic.TypeDescriptor{}, unsupportedConnectorScript()
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
				return semantic.TypeDescriptor{}, unsupportedConnectorScript()
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
		return semantic.TypeDescriptor{}, unsupportedConnectorScript()
	}
}
