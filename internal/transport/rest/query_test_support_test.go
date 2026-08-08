package rest

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type unavailableRESTQueryEngine struct{}

func (unavailableRESTQueryEngine) Query(context.Context, ports.QueryRequest) (domain.QueryResult, error) {
	return domain.QueryResult{}, fmt.Errorf("%w: query execution is not configured for this transport test", domain.ErrPrecondition)
}

func newRESTTestQueryService(
	jobs ports.JobRepository,
	backend any,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...application.QueryOption,
) *application.QueryService {
	var warehouse ports.QueryEngine
	if candidate, ok := backend.(ports.QueryEngine); ok {
		warehouse = candidate
	} else {
		warehouse = unavailableRESTQueryEngine{}
	}
	service, err := application.NewQueryService(jobs, warehouse, clock, ids, options...)
	if err != nil {
		panic(err)
	}
	return service
}
