package application

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type disabledQueryOperations struct{}

func (disabledQueryOperations) AnalyzeQueryOperation(
	context.Context,
	ports.QueryRequest,
) (ports.QueryOperation, bool, error) {
	return ports.QueryOperation{}, false, nil
}

func (disabledQueryOperations) VerifyQueryOperation(ports.QueryRequest, ports.QueryOperation) error {
	return fmt.Errorf("%w: disabled semantic query operation", domain.ErrPrecondition)
}

func (disabledQueryOperations) ExecuteQueryOperation(
	context.Context,
	ports.QueryRequest,
	ports.QueryOperation,
	domain.Table,
	domain.Table,
) (domain.QueryResult, error) {
	return domain.QueryResult{}, fmt.Errorf("%w: disabled semantic query operation", domain.ErrPrecondition)
}

func (disabledQueryOperations) WithCanonicalTables(
	context.Context,
	domain.TableReference,
	domain.TableReference,
	func(domain.Table, domain.Table) (domain.QueryResult, error),
) (domain.QueryResult, error) {
	return domain.QueryResult{}, fmt.Errorf("%w: disabled semantic query operation", domain.ErrPrecondition)
}

func newTestQueryService(
	jobs ports.JobRepository,
	warehouse ports.QueryEngine,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...QueryOption,
) *QueryService {
	disabled := disabledQueryOperations{}
	service, err := NewQueryService(jobs, warehouse, disabled, disabled, disabled, clock, ids, options...)
	if err != nil {
		panic(err)
	}
	return service
}
