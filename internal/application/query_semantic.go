package application

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

func (prepared preparedQuery) statementType() string {
	if !prepared.valid || prepared.statement.Syntax() == nil {
		return ""
	}
	return string(prepared.statement.Kind())
}

func (s *QueryService) prepareQueryAdmission(
	ctx context.Context,
	request ports.QueryRequest,
) (preparedQuery, error) {
	if s.googleSQLGateway == nil {
		analysis, err := s.analyzeQueryAdmission(ctx, request)
		return preparedQuery{analysis: analysis}, err
	}
	statement, err := s.googleSQLGateway.Analyze(ctx, request)
	if err != nil {
		return preparedQuery{}, err
	}
	analysis := queryAnalysisFromStatement(statement)
	if analysis.RequiresCatalogMutation && s.ddlExecutor == nil {
		return preparedQuery{}, unsupportedDDL("catalog DDL executor is not configured")
	}
	return preparedQuery{statement: statement, analysis: analysis, valid: true}, nil
}

func queryAnalysisFromStatement(statement semantic.Statement) ports.QueryAnalysis {
	references := statement.ReferencedTables()
	targets := statement.MutationTargets()
	kind := statement.Kind()
	if kind == queryast.StatementCreateTable {
		references = referencesWithoutTargets(references, targets)
	}
	return ports.QueryAnalysis{
		ReferencedTables: references,
		MutationTargets:  targets,
		ProducesRows:     len(statement.OutputColumns()) != 0,
		RequiresCatalogMutation: kind == queryast.StatementCreateTable ||
			kind == queryast.StatementDropTable ||
			kind == queryast.StatementAlterTable ||
			kind == queryast.StatementTruncateTable,
	}
}

func referencesWithoutTargets(
	references []domain.TableReference,
	targets []domain.TableReference,
) []domain.TableReference {
	filtered := make([]domain.TableReference, 0, len(references))
	for _, reference := range references {
		isTarget := false
		for _, target := range targets {
			if reference == target {
				isTarget = true
				break
			}
		}
		if !isTarget {
			filtered = append(filtered, reference)
		}
	}
	return filtered
}

func (s *QueryService) executeAnalyzedStatement(
	ctx context.Context,
	statement semantic.Statement,
	correlationID string,
) (domain.QueryResult, error) {
	switch statement.Kind() {
	case queryast.StatementCreateTable, queryast.StatementDropTable,
		queryast.StatementAlterTable, queryast.StatementTruncateTable:
		command, err := ddlCommandFromStatement(statement)
		if err != nil {
			return domain.QueryResult{}, err
		}
		if s.ddlExecutor == nil {
			return domain.QueryResult{}, unsupportedDDL("catalog DDL executor is not configured")
		}
		if err := s.ddlExecutor.ExecuteDDL(ctx, command, correlationID); err != nil {
			return domain.QueryResult{}, err
		}
		return domain.QueryResult{}, nil
	default:
		return s.statementExecutor.ExecuteStatement(ctx, statement)
	}
}
