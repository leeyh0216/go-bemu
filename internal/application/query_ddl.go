package application

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

// WithQueryDDLParser installs the legacy DDL syntax boundary for test-only
// compositions that have not installed GoogleSQLGateway.
func WithQueryDDLParser(parser ports.DDLParser) QueryOption {
	return func(service *QueryService) { service.ddlParser = parser }
}

// WithQueryDDLExecutor installs the catalog-owned semantic DDL use case.
func WithQueryDDLExecutor(executor ports.DDLExecutor) QueryOption {
	return func(service *QueryService) { service.ddlExecutor = executor }
}

func (s *QueryService) analyzeQueryAdmission(
	ctx context.Context,
	request ports.QueryRequest,
) (ports.QueryAnalysis, error) {
	if s.ddlParser != nil {
		command, matched, err := s.ddlParser.ParseDDL(ctx, request)
		if err != nil {
			return ports.QueryAnalysis{}, err
		}
		if matched {
			if s.ddlExecutor == nil {
				return ports.QueryAnalysis{}, unsupportedDDL("catalog DDL executor is not configured")
			}
			return ports.QueryAnalysis{
				MutationTargets: []domain.TableReference{command.Table()}, RequiresCatalogMutation: true,
			}, nil
		}
	}
	if s.analyzer == nil {
		return ports.QueryAnalysis{}, nil
	}
	analysis, err := s.analyzer.AnalyzeQuery(ctx, request)
	if err != nil {
		return ports.QueryAnalysis{}, err
	}
	if analysis.RequiresCatalogMutation {
		return ports.QueryAnalysis{}, fmt.Errorf(
			"%w: query DDL requires a supported GoogleSQL semantic command; capability=%s",
			domain.ErrUnsupported, domain.GapQueryDDLCatalogSyncV1,
		)
	}
	return analysis, nil
}

func (s *QueryService) executeDDLOrGenericQuery(
	ctx context.Context,
	request ports.QueryRequest,
	correlationID string,
) (domain.QueryResult, error) {
	if s.ddlParser != nil {
		command, matched, err := s.ddlParser.ParseDDL(ctx, request)
		if err != nil {
			return domain.QueryResult{}, err
		}
		if matched {
			if s.ddlExecutor == nil {
				return domain.QueryResult{}, unsupportedDDL("catalog DDL executor is not configured")
			}
			if err := s.ddlExecutor.ExecuteDDL(ctx, command, correlationID); err != nil {
				return domain.QueryResult{}, err
			}
			return domain.QueryResult{}, nil
		}
	}
	return s.warehouse.Query(ctx, request)
}

func (s *QueryService) executeQueryWithoutDestination(ctx context.Context, request ports.QueryRequest) (domain.QueryResult, error) {
	return s.executeQueryWithoutDestinationForJob(ctx, request, "query")
}

func (s *QueryService) executeQueryWithoutDestinationForJob(
	ctx context.Context,
	request ports.QueryRequest,
	correlationID string,
) (domain.QueryResult, error) {
	return s.executeDDLOrGenericQuery(ctx, request, correlationID)
}
