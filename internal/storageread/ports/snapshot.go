package ports

import (
	"context"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() string
}

// TableSchemaResolver supplies canonical BQEMU metadata to a physical snapshot
// adapter. Implementations must not infer logical types from engine catalogs.
type TableSchemaResolver interface {
	GetTable(context.Context, string, string, string) (catalogdomain.Table, error)
}

// SnapshotMaterializerConfig contains consumer-owned resource and protocol
// limits. It deliberately contains no engine connection or SQL settings.
type SnapshotMaterializerConfig struct {
	TempDir              string
	TempFilePattern      string
	SpillThresholdBytes  int64
	MaxRowBytes          int64
	MaxBatchBytes        int
	MaxSchemaBytes       int
	MaxSnapshotBytes     int64
	MaxSnapshotRows      int64
	ProtocolModelVersion string
}

// SnapshotMaterializerFactory lets the composition root construct a Storage
// Read adapter without passing a concrete engine into another module boundary.
type SnapshotMaterializerFactory interface {
	NewSnapshotMaterializer(TableSchemaResolver, SnapshotMaterializerConfig) (SnapshotMaterializer, error)
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
// have completed. Metadata().RetainedBytes must remain stable until Close and
// is the value charged to the application-wide snapshot byte budget.
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
