package ports

import (
	"context"
	"errors"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
)

var (
	ErrStateNotFound       = errors.New("Storage Write state not found")
	ErrStateConflict       = errors.New("Storage Write state compare-and-swap conflict")
	ErrReceiptConflict     = errors.New("Storage Write append receipt conflict")
	ErrExactReceipt        = errors.New("Storage Write append receipt already prepared with the same identity")
	ErrCommitGroupConflict = errors.New("Storage Write commit group conflict")
)

// StateRepository is owned by the Storage Write consumer. It exposes only
// lifecycle compare-and-swap operations; SQLite handles and transaction types
// never cross this boundary.
type StateRepository interface {
	ReconcileStartup(context.Context, time.Time) (domain.StartupSnapshot, error)
	CreateStream(context.Context, domain.StreamRecord) error
	GetStream(context.Context, string) (domain.StreamRecord, error)
	UpdateStream(context.Context, int64, domain.StreamRecord) error
	DeleteStream(context.Context, string, int64) error

	PrepareAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error
	MarkAppendUnresolved(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error
	CompleteAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error
	AbortAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error

	PrepareCommit(context.Context, map[string]int64, []domain.StreamRecord, domain.CommitGroup) error
	MarkCommitUnresolved(context.Context, map[string]int64, []domain.StreamRecord, domain.CommitGroup) error
	CompleteCommit(context.Context, map[string]int64, []domain.StreamRecord, domain.CommitGroup) error
	AbortCommit(context.Context, map[string]int64, []domain.StreamRecord, domain.CommitGroup) error
}
