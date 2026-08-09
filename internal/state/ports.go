package state

import (
	"context"
	"time"
)

const MaxPendingList = 1000

// MutationJournal persists the boundary between canonical BQEMU state and a
// physical engine. Implementations must use compare-and-swap transitions so a
// concurrent APPLIED and FAILED decision cannot both succeed.
type MutationJournal interface {
	Begin(context.Context, BeginMutation) (Mutation, error)
	Get(context.Context, string) (Mutation, error)
	ListPending(context.Context, int) ([]Mutation, error)
	MarkFailed(context.Context, string, Failure, time.Time) (Mutation, error)
}

// CanonicalMutationJournal atomically publishes the persisted After table and
// transitions its PREPARED journal entry to APPLIED. This stronger contract is
// required for mutations spanning BQEMU's SQLite catalog and a physical engine.
type CanonicalMutationJournal interface {
	MutationJournal
	CommitTableChange(context.Context, string, time.Time) (Mutation, error)
}
