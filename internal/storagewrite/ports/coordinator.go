package ports

// The atomicity required of Coordinator.CommitPending is defined by the
// official BatchCommitWriteStreams response contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#batchcommitwritestreamsresponse

import (
	"context"
	"errors"
	"fmt"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
)

var (
	ErrTableNotFound     = errors.New("storage write destination table not found")
	ErrUnsupportedSchema = errors.New("storage write destination schema is unsupported")
	ErrResourceExhausted = errors.New("storage write byte admission exhausted")
	ErrQueueWaitTimeout  = errors.New("storage write coordinator queue wait timed out")
	ErrOperationTimeout  = errors.New("storage write coordinator operation timed out")
	// ErrInvalidRows classifies adapter validation failures caused by the
	// caller's ProtoSchema, ProtoRows, or decimal values. Application and
	// transport layers must preserve this as INVALID_ARGUMENT.
	ErrInvalidRows = errors.New("storage write rows are invalid")
)

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() string
}

// TableSchemaResolver supplies canonical BQEMU metadata. A coordinator must
// not reconstruct NUMERIC, BIGNUMERIC, STRUCT, or LIST identity from physical
// engine type names.
type TableSchemaResolver interface {
	GetTable(context.Context, string, string, string) (catalogdomain.Table, error)
}

// CoordinatorConfig contains consumer-owned scheduling and byte budgets. It
// carries no engine connection, SQL, or physical type information.
type CoordinatorConfig struct {
	QueueCapacity             int
	QueueWaitTimeout          time.Duration
	OperationTimeout          time.Duration
	MaxInFlightBytes          int64
	MaxInFlightBytesPerStream int64
	MaxStagedBytes            int64
	MaxStagedBytesPerStream   int64
}

func (config CoordinatorConfig) Validate() error {
	if config.QueueCapacity <= 0 {
		return fmt.Errorf("Storage Write operation queue capacity must be positive")
	}
	if config.QueueWaitTimeout <= 0 || config.OperationTimeout <= 0 {
		return fmt.Errorf("Storage Write queue wait and operation timeouts must be positive")
	}
	if config.MaxInFlightBytesPerStream <= 0 || config.MaxInFlightBytes < config.MaxInFlightBytesPerStream {
		return fmt.Errorf("Storage Write in-flight byte limits must satisfy 0 < per-stream <= global")
	}
	if config.MaxStagedBytesPerStream <= 0 || config.MaxStagedBytes < config.MaxStagedBytesPerStream {
		return fmt.Errorf("Storage Write staged byte limits must satisfy 0 < per-stream <= global")
	}
	return nil
}

// AppendBatch contains opaque ProtoRows bytes. The application records their
// order, while the adapter decodes them using Descriptor before touching its
// backend. WireBytes is the complete AppendRowsRequest envelope used for
// transient weighted admission; staged storage uses a deterministic serialized
// row estimate. Diagnostic logs may include Descriptor and Rows alongside
// counts and identity digests.
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
	CDC               bool
}

type CommitRequest struct {
	Parent            domain.TableReference
	StreamNames       []string
	GroupID           string
	ExpectedRowCounts map[string]int64
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

// CoordinatorRuntime adds the lifecycle needed only by the composition root.
// The Storage Write application continues to depend on Coordinator alone.
type CoordinatorRuntime interface {
	Coordinator
	Close(context.Context) error
}

// CDCApplyState is an optional read-only diagnostic surface for the local CDC
// apply frontier. It does not expose CDC pseudocolumns through table reads.
type CDCApplyState struct {
	AppliedAt time.Time
	KeyCount  int64
}

type CDCStateInspector interface {
	CDCApplyState(context.Context, domain.TableReference) (CDCApplyState, error)
}

type CoordinatorFactory interface {
	NewCoordinator(context.Context, TableSchemaResolver, CoordinatorConfig) (CoordinatorRuntime, error)
}
