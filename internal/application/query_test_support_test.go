package application

import (
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func newTestQueryService(
	jobs ports.JobRepository,
	warehouse ports.QueryEngine,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...QueryOption,
) *QueryService {
	service, err := NewQueryService(jobs, warehouse, clock, ids, options...)
	if err != nil {
		panic(err)
	}
	return service
}
