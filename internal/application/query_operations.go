package application

// Connector semantic operations are prepared at the application boundary so
// the backend receives canonical catalog metadata rather than inferring it from
// physical tables. Generic queries continue through QueryEngine unchanged.
//
// Spark 0.44.2 dynamic partition overwrite source:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func (s *QueryService) executeQueryWithoutDestination(ctx context.Context, request ports.QueryRequest) (domain.QueryResult, error) {
	analyzer, supportsOperations := s.analyzer.(ports.QueryOperationAnalyzer)
	if !supportsOperations {
		return s.warehouse.Query(ctx, request)
	}
	operation, matched, err := analyzer.AnalyzeQueryOperation(ctx, request)
	if err != nil {
		return domain.QueryResult{}, err
	}
	if !matched {
		return s.warehouse.Query(ctx, request)
	}
	if s.destinations == nil {
		return domain.QueryResult{}, fmt.Errorf(
			"%w: connector semantic operation requires canonical destination catalog metadata; model_version=%s",
			domain.ErrPrecondition, operation.ModelVersion,
		)
	}
	executor, ok := s.warehouse.(ports.QueryOperationEngine)
	if !ok {
		return domain.QueryResult{}, fmt.Errorf(
			"%w: query backend does not implement the analyzed connector operation; model_version=%s",
			domain.ErrPrecondition, operation.ModelVersion,
		)
	}
	catalog, ok := s.destinations.(ports.QueryOperationCatalog)
	if !ok {
		return domain.QueryResult{}, fmt.Errorf(
			"%w: connector semantic operation requires a transaction-coordinating catalog; model_version=%s",
			domain.ErrPrecondition, operation.ModelVersion,
		)
	}
	return catalog.WithCanonicalTables(ctx, operation.Destination, operation.Source, func(canonicalDestination, canonicalSource domain.Table) (domain.QueryResult, error) {
		return executor.ExecuteQueryOperation(ctx, request, operation, canonicalDestination, canonicalSource)
	})
}
