package application

// Connector semantic operations are prepared at the application boundary so
// the backend receives canonical catalog metadata rather than inferring it from
// physical tables. Generic queries continue through QueryEngine unchanged.
//
// Spark 0.44.2 dynamic partition overwrite source:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func (s *QueryService) executeQueryWithoutDestination(ctx context.Context, request ports.QueryRequest) (domain.QueryResult, error) {
	return s.executeQueryWithoutDestinationForJob(ctx, request, "query-operation")
}

func (s *QueryService) executeQueryWithoutDestinationForJob(
	ctx context.Context,
	request ports.QueryRequest,
	correlationID string,
) (domain.QueryResult, error) {
	operation, matched, err := s.operationAnalyzer.AnalyzeQueryOperation(ctx, request)
	if err != nil {
		return domain.QueryResult{}, err
	}
	if !matched {
		return s.executeDDLOrGenericQuery(ctx, request, correlationID)
	}
	if err := s.operationAnalyzer.VerifyQueryOperation(request, operation); err != nil {
		return domain.QueryResult{}, err
	}
	return s.operationCatalog.WithCanonicalTables(
		ctx, operation.Destination(), operation.Source(),
		func(canonicalDestination, canonicalSource domain.Table) (domain.QueryResult, error) {
			return s.operationExecutor.ExecuteQueryOperation(ctx, request, operation, canonicalDestination, canonicalSource)
		})
}
