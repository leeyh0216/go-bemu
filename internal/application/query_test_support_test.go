package application

import (
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func newTestQueryService(
	jobs ports.JobRepository,
	backend any,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...QueryOption,
) *QueryService {
	var warehouse ports.QueryEngine
	if candidate, ok := backend.(ports.QueryEngine); ok {
		warehouse = candidate
	}
	service, err := NewQueryService(jobs, warehouse, clock, ids, options...)
	if err != nil {
		panic(err)
	}
	return service
}
