package ports

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

// DDLExecutor owns canonical metadata coordination. CorrelationID identifies
// the query job without exposing SQL to storage planning.
type DDLExecutor interface {
	ExecuteDDL(context.Context, domain.DDLCommand, string) error
}

// ViewDDLExecutor receives the analyzed definition and the original request
// solely for canonical view-definition persistence. Storage receives only the
// semantic statement through LogicalViewStorage.
type ViewDDLExecutor interface {
	ExecuteViewDDL(context.Context, semantic.Statement, string, string) error
}

// DDLStorage is the planned physical mutation boundary. Every execution must
// revalidate the immutable plan against the supplied semantic mutation before
// producing engine SQL or starting a write.
type DDLStorage interface {
	PlanTableMutation(context.Context, engine.TableMutation) (engine.TablePlan, error)
	ApplyTableMutation(context.Context, engine.TablePlan, engine.TableMutation) error
	PlanTableTruncation(context.Context, engine.DataReplacement) (engine.DataReplacementPlan, error)
	ApplyTableTruncation(context.Context, engine.DataReplacementPlan, engine.DataReplacement) error
}
