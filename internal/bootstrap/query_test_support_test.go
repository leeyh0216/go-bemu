package bootstrap

import (
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantictest"
)

func newMainTestQueryService(
	jobs ports.JobRepository,
	executor ports.StatementExecutor,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...application.QueryOption,
) *application.QueryService {
	gateway, err := semantictest.NewGateway(semantictest.Analysis{})
	if err != nil {
		panic(err)
	}
	defaults := []application.QueryOption{
		application.WithGoogleSQLGateway(gateway),
		application.WithStatementExecutor(executor),
	}
	service, err := application.NewQueryService(jobs, clock, ids, append(defaults, options...)...)
	if err != nil {
		panic(err)
	}
	return service
}
