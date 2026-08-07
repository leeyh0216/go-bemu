package ports

import (
	"context"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() string
}

// SnapshotMaterializer applies projection/filter/snapshot-time once and fixes
// stable row ordinals before streams are partitioned.
type SnapshotMaterializer interface {
	Materialize(context.Context, domain.MaterializeRequest) (ReadSnapshot, error)
}

// ReadSnapshot is an immutable, concurrently readable materialized result.
// Implementations may use DuckDB, another database, or staged files. OpenRange
// must return batches that exactly cover [startOffset, endOffset), in order,
// with no overlap or gap. Each payload is one bare Arrow IPC record-batch
// message or concatenated raw Avro datums as selected by Metadata().Schema.
//
// Close is called after session expiry or service shutdown, after active reads
// have completed.
type ReadSnapshot interface {
	Metadata() domain.SnapshotMetadata
	OpenRange(ctx context.Context, startOffset, endOffset, maxRowsPerBatch int64) (BatchIterator, error)
	Close(context.Context) error
}

type BatchIterator interface {
	// Next returns io.EOF only after the requested range is fully emitted.
	Next(context.Context) (domain.EncodedBatch, error)
	Close() error
}
