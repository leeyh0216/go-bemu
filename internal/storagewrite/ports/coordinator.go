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

var (
	ErrTableNotFound     = errors.New("storage write destination table not found")
	ErrResourceExhausted = errors.New("storage write byte admission exhausted")
	ErrQueueWaitTimeout  = errors.New("storage write coordinator queue wait timed out")
	ErrOperationTimeout  = errors.New("storage write coordinator operation timed out")
	ErrStreamNotFound    = errors.New("storage write stream not found")
	ErrStreamExists      = errors.New("storage write stream already exists")
	ErrStreamConflict    = errors.New("storage write stream state conflict")
	ErrReceiptNotFound   = errors.New("storage write append receipt not found")
	ErrReceiptConflict   = errors.New("storage write append receipt conflict")
)

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() string
}

// AppendBatch contains opaque ProtoRows bytes. The application records their
// order, while the adapter decodes them using Descriptor before touching its
// backend. WireBytes is the complete AppendRowsRequest envelope used for
// transient weighted admission; staged storage uses a deterministic serialized
// row estimate. Logs may include counts, fingerprints, and digests, but never
// Descriptor or Rows.
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest
type AppendBatch struct {
	StreamName        string
	Table             domain.TableReference
	StartOffset       int64
	WireBytes         int64
	Descriptor        []byte
	Rows              [][]byte
	SchemaFingerprint string
	PayloadDigest     string
	TraceID           string
}

type CommitRequest struct {
	Parent      domain.TableReference
	StreamNames []string
	RowCounts   map[string]int64
}

type PhysicalStreamState struct {
	PendingExists bool
	PendingRows   int64
	AppliedExists bool
	AppliedRows   int64
}

type PhysicalExpectation struct {
	StreamName  string
	StagedBytes int64
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

// DurableCoordinator exposes the physical reconciliation boundary needed when
// canonical stream state is persisted independently from staged rows.
type DurableCoordinator interface {
	Coordinator
	InspectPhysical(context.Context, []PhysicalExpectation) (map[string]PhysicalStreamState, error)
	AcknowledgeApplied(context.Context, []string) error
}

// StreamRepository is the canonical Storage Write metadata boundary. Every
// mutating method compares Revision and changes the stream row together with
// its receipt rows in one repository transaction.
type StreamRepository interface {
	CreateWriteStream(context.Context, domain.StreamRecord, int64) error
	GetWriteStream(context.Context, string) (domain.StreamRecord, error)
	ListWriteStreams(context.Context) ([]domain.StreamRecord, error)
	CountActivePendingStreams(context.Context) (int64, error)
	SaveWriteStream(context.Context, int64, domain.StreamRecord) error
	SaveWriteStreams(context.Context, map[string]int64, []domain.StreamRecord) error
	DeleteWriteStream(context.Context, string, int64) error
	PrepareAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error
	CompleteAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error
	AbortAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error
	GetWriteAppendReceipt(context.Context, string, int64) (domain.AppendReceipt, error)
	ListWriteAppendReceipts(context.Context, string) ([]domain.AppendReceipt, error)
}
