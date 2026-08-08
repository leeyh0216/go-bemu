package rest

import (
	"context"

	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantictest"
)

func newRESTGoogleSQLGateway(catalog ports.GoogleSQLCatalogReader) ports.GoogleSQLGateway {
	gateway, err := googlesqladapter.NewGateway(catalog)
	if err != nil {
		panic(err)
	}
	return gateway
}

type noopRESTStatementExecutor struct{}

func (noopRESTStatementExecutor) ExecuteStatement(context.Context, semantic.Statement) (domain.QueryResult, error) {
	return domain.QueryResult{}, nil
}

func newRESTTestQueryService(
	jobs ports.JobRepository,
	backend any,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...application.QueryOption,
) *application.QueryService {
	gateway, err := semantictest.NewGateway(semantictest.Analysis{})
	if err != nil {
		panic(err)
	}
	var executor ports.StatementExecutor = noopRESTStatementExecutor{}
	if candidate, ok := backend.(ports.StatementExecutor); ok {
		executor = candidate
	}
	defaults := []application.QueryOption{
		application.WithGoogleSQLGateway(gateway), application.WithStatementExecutor(executor),
	}
	service, err := application.NewQueryService(jobs, clock, ids, append(defaults, options...)...)
	if err != nil {
		panic(err)
	}
	return service
}
