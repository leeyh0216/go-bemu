package application

import (
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantictest"
)

func withTestQueryAnalysis(analysis ports.QueryAnalysis) QueryOption {
	gateway, err := semantictest.NewGateway(semantictest.Analysis{
		ReferencedTables: analysis.ReferencedTables, MutationTargets: analysis.MutationTargets,
		ProducesRows: analysis.ProducesRows, RequiresCatalogMutation: analysis.RequiresCatalogMutation,
	})
	if err != nil {
		panic(err)
	}
	return WithGoogleSQLGateway(gateway)
}

func newTestQueryService(
	jobs ports.JobRepository,
	executor ports.StatementExecutor,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...QueryOption,
) *QueryService {
	gateway, err := semantictest.NewGateway(semantictest.Analysis{})
	if err != nil {
		panic(err)
	}
	defaults := []QueryOption{WithGoogleSQLGateway(gateway), WithStatementExecutor(executor)}
	service, err := NewQueryService(jobs, clock, ids, append(defaults, options...)...)
	if err != nil {
		panic(err)
	}
	return service
}
