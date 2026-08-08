package rest

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type disabledRESTQueryOperations struct{}

func (disabledRESTQueryOperations) AnalyzeQueryOperation(
	context.Context,
	ports.QueryRequest,
) (ports.QueryOperation, bool, error) {
	return ports.QueryOperation{}, false, nil
}

func (disabledRESTQueryOperations) VerifyQueryOperation(ports.QueryRequest, ports.QueryOperation) error {
	return fmt.Errorf("%w: disabled semantic query operation", domain.ErrPrecondition)
}

func (disabledRESTQueryOperations) ExecuteQueryOperation(
	context.Context,
	ports.QueryRequest,
	ports.QueryOperation,
	domain.Table,
	domain.Table,
) (domain.QueryResult, error) {
	return domain.QueryResult{}, fmt.Errorf("%w: disabled semantic query operation", domain.ErrPrecondition)
}

func (disabledRESTQueryOperations) WithCanonicalTables(
	context.Context,
	domain.TableReference,
	domain.TableReference,
	func(domain.Table, domain.Table) (domain.QueryResult, error),
) (domain.QueryResult, error) {
	return domain.QueryResult{}, fmt.Errorf("%w: disabled semantic query operation", domain.ErrPrecondition)
}

func newRESTTestQueryService(
	jobs ports.JobRepository,
	warehouse ports.QueryEngine,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...application.QueryOption,
) *application.QueryService {
	disabled := disabledRESTQueryOperations{}
	service, err := application.NewQueryService(jobs, warehouse, disabled, disabled, disabled, clock, ids, options...)
	if err != nil {
		panic(err)
	}
	return service
}
