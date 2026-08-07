package ports

import (
	"context"
	"io"
	"time"
)

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() string
}

// ObjectStorage is the boundary used by future load and extract job adapters.
type ObjectStorage interface {
	Open(context.Context, string) (io.ReadCloser, error)
}
