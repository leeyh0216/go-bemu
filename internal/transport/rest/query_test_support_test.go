package rest

import (
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func newRESTTestQueryService(
	jobs ports.JobRepository,
	warehouse ports.QueryEngine,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...application.QueryOption,
) *application.QueryService {
	service, err := application.NewQueryService(jobs, warehouse, clock, ids, options...)
	if err != nil {
		panic(err)
	}
	return service
}
