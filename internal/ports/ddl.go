package ports

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
)

// DDLExecutor owns canonical metadata coordination. CorrelationID identifies
// the query job without exposing SQL to storage planning.
type DDLExecutor interface {
	ExecuteDDL(context.Context, domain.DDLCommand, string) error
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
