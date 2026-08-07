package ports

// The atomicity required of Coordinator.CommitPending is defined by the
// official BatchCommitWriteStreams response contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#batchcommitwritestreamsresponse

import (
	"context"
	"errors"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
)

var ErrTableNotFound = errors.New("storage write destination table not found")

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() string
}

// AppendBatch contains opaque ProtoRows bytes. The application records their
// order, while the adapter decodes them using Descriptor before touching its
// backend. Logs may include counts, fingerprints, and digests, but never Rows.
type AppendBatch struct {
	StreamName        string
	Table             domain.TableReference
	StartOffset       int64
	Descriptor        []byte
	Rows              [][]byte
	SchemaFingerprint string
	PayloadDigest     string
	TraceID           string
}

type CommitRequest struct {
	Parent      domain.TableReference
	StreamNames []string
	CommitTime  time.Time
}

// Coordinator is deliberately serializable. Implementations may execute every
// database operation through one worker while the application independently
// negotiates and tracks many logical streams. StagePending must not expose rows;
// CommitPending must make every named stream visible in one transaction or make
// none visible. A failed call must be retryable without partial state changes.
type Coordinator interface {
	DescribeTable(context.Context, domain.TableReference) (domain.TableSchema, error)
	AppendDefault(context.Context, AppendBatch) error
	StagePending(context.Context, AppendBatch) error
	CommitPending(context.Context, CommitRequest) error
	DiscardPending(context.Context, string) error
}
