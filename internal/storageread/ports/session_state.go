package ports

import (
	"context"
	"errors"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

var (
	ErrSessionStateNotFound = errors.New("Storage Read session state not found")
	ErrSessionStateConflict = errors.New("Storage Read session state conflict")
)

// SessionStateRepository owns payload-free Storage Read lifecycle metadata.
// Implementations must make CreateSession, TransitionSessions, and
// ReconcileActive atomic. Snapshot bytes remain behind ReadSnapshot.
type SessionStateRepository interface {
	CreateSession(context.Context, domain.SessionRecord) error
	TransitionSessions(context.Context, []string, domain.SessionLifecycle, domain.SessionLifecycle, time.Time) error
	ReconcileActive(context.Context, time.Time) (int64, error)
	GetStream(context.Context, string) (domain.PersistedStream, error)
}
