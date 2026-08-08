// Package semantictest builds analyzed statements for unit tests that exercise
// application behavior independently from the production GoogleSQL adapter.
package semantictest

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

const sourceDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type Analysis struct {
	ReferencedTables        []domain.TableReference
	MutationTargets         []domain.TableReference
	ProducesRows            bool
	RequiresCatalogMutation bool
}

type Gateway struct {
	statement semantic.Statement
}

func NewGateway(analysis Analysis) (*Gateway, error) {
	statement, err := NewStatement(analysis)
	if err != nil {
		return nil, err
	}
	return &Gateway{statement: statement}, nil
}

func (gateway *Gateway) Analyze(context.Context, ports.QueryRequest) (semantic.Statement, error) {
	if gateway == nil || gateway.statement.Syntax() == nil {
		return semantic.Statement{}, fmt.Errorf("test GoogleSQL gateway is not initialized")
	}
	return gateway.statement, nil
}

func NewStatement(analysis Analysis) (semantic.Statement, error) {
	span, err := queryast.NewSpan(0, 10000)
	if err != nil {
		return semantic.Statement{}, err
	}
	source, err := queryast.NewSource(sourceDigest, span)
	if err != nil {
		return semantic.Statement{}, err
	}
	builder := statementBuilder{source: source}

	var syntax queryast.Statement
	if analysis.RequiresCatalogMutation {
		target := domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "table"}
		if len(analysis.MutationTargets) != 0 {
			target = analysis.MutationTargets[0]
		}
		relation, binding, relationErr := builder.table(target)
		if relationErr != nil {
			return semantic.Statement{}, relationErr
		}
		builder.bindings = append(builder.bindings, binding)
		syntax, err = queryast.NewDropTableStatement(source, relation)
	} else {
		statements := make([]queryast.Statement, 0, len(analysis.ReferencedTables)+len(analysis.MutationTargets)+1)
		for _, reference := range analysis.ReferencedTables {
			statement, statementErr := builder.selectFrom(reference)
			if statementErr != nil {
				return semantic.Statement{}, statementErr
			}
			statements = append(statements, statement)
		}
		for _, target := range analysis.MutationTargets {
			relation, binding, relationErr := builder.table(target)
			if relationErr != nil {
				return semantic.Statement{}, relationErr
			}
			builder.bindings = append(builder.bindings, binding)
			statement, statementErr := queryast.NewDeleteStatement(source, relation, nil)
			if statementErr != nil {
				return semantic.Statement{}, statementErr
			}
			statements = append(statements, statement)
		}
		if len(statements) == 0 {
			statement, statementErr := builder.selectFrom(domain.TableReference{})
			if statementErr != nil {
				return semantic.Statement{}, statementErr
			}
			statements = append(statements, statement)
		}
		if len(statements) == 1 {
			syntax = statements[0]
		} else {
			syntax, err = queryast.NewScriptStatement(source, statements)
		}
	}
	if err != nil {
		return semantic.Statement{}, err
	}

	descriptor := semantic.StatementDescriptor{
		Syntax: syntax, ResolvedKind: syntax.Kind(), RelationBindings: builder.bindings,
	}
	if analysis.ProducesRows {
		integer, typeErr := semantic.NewType(semantic.TypeDescriptor{Kind: semantic.TypeInt64})
		if typeErr != nil {
			return semantic.Statement{}, typeErr
		}
		descriptor.OutputColumns = []semantic.ColumnDescriptor{{Name: "value", Type: integer}}
	}
	return semantic.NewStatement(descriptor)
}

type statementBuilder struct {
	source   queryast.Source
	ordinal  int
	bindings []semantic.RelationBindingDescriptor
}

func (builder *statementBuilder) key(kind string) (queryast.NodeKey, error) {
	builder.ordinal++
	span, err := queryast.NewSpan(builder.ordinal, builder.ordinal+1)
	if err != nil {
		return queryast.NodeKey{}, err
	}
	return queryast.NewNodeKey(sourceDigest, span, kind, builder.ordinal)
}

func (builder *statementBuilder) table(reference domain.TableReference) (*queryast.TableRelation, semantic.RelationBindingDescriptor, error) {
	if reference.ProjectID == "" {
		reference.ProjectID = "test-project"
	}
	if reference.DatasetID == "" {
		reference.DatasetID = "dataset"
	}
	if reference.TableID == "" {
		reference.TableID = "table"
	}
	identifiers := make([]queryast.Identifier, 0, 3)
	for _, part := range []string{reference.ProjectID, reference.DatasetID, reference.TableID} {
		identifier, err := queryast.NewIdentifier(part)
		if err != nil {
			return nil, semantic.RelationBindingDescriptor{}, err
		}
		identifiers = append(identifiers, identifier)
	}
	path, err := queryast.NewIdentifierPath(identifiers)
	if err != nil {
		return nil, semantic.RelationBindingDescriptor{}, err
	}
	key, err := builder.key("TABLE_RELATION")
	if err != nil {
		return nil, semantic.RelationBindingDescriptor{}, err
	}
	relation, err := queryast.NewTableRelation(key, path, nil)
	if err != nil {
		return nil, semantic.RelationBindingDescriptor{}, err
	}
	return relation, semantic.RelationBindingDescriptor{
		Key: key, Kind: semantic.RelationPhysical, Reference: reference,
	}, nil
}

func (builder *statementBuilder) selectFrom(reference domain.TableReference) (queryast.Statement, error) {
	key, err := builder.key("INTEGER_LITERAL")
	if err != nil {
		return nil, err
	}
	literal, err := queryast.NewIntegerLiteral(key, "1")
	if err != nil {
		return nil, err
	}
	item, err := queryast.NewSelectItem(literal, nil)
	if err != nil {
		return nil, err
	}
	var from queryast.Relation
	if reference != (domain.TableReference{}) {
		relation, binding, relationErr := builder.table(reference)
		if relationErr != nil {
			return nil, relationErr
		}
		builder.bindings = append(builder.bindings, binding)
		from = relation
	}
	body, err := queryast.NewSelectQuery(false, []queryast.SelectItem{item}, from, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	query, err := queryast.NewQuery(nil, false, body, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return queryast.NewSelectStatement(builder.source, query)
}
