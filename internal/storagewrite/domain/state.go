package domain

import "time"

// OperationKind identifies a cross-store side effect currently owned by a
// stream ledger entry. NONE is explicit so SQLite can enforce token/phase
// invariants without relying on NULL semantics.
type OperationKind string

const (
	OperationNone   OperationKind = "NONE"
	OperationAppend OperationKind = "APPEND"
	OperationCommit OperationKind = "COMMIT"
)

// OperationPhase separates a recorded intent from an outcome that could not
// be observed. Startup reconciliation conservatively changes PREPARED to
// UNRESOLVED before orphan cleanup is allowed to inspect the stream.
type OperationPhase string

const (
	OperationPhaseNone       OperationPhase = "NONE"
	OperationPhasePrepared   OperationPhase = "PREPARED"
	OperationPhaseUnresolved OperationPhase = "UNRESOLVED"
)

type CleanupPhase string

const (
	CleanupActive  CleanupPhase = "ACTIVE"
	CleanupPending CleanupPhase = "PENDING"
)

type ReceiptPhase string

const (
	ReceiptPrepared   ReceiptPhase = "PREPARED"
	ReceiptUnresolved ReceiptPhase = "UNRESOLVED"
	ReceiptApplied    ReceiptPhase = "APPLIED"
)

type CommitPhase string

const (
	CommitPrepared   CommitPhase = "PREPARED"
	CommitUnresolved CommitPhase = "UNRESOLVED"
	CommitApplied    CommitPhase = "APPLIED"
	CommitAborted    CommitPhase = "ABORTED"
)

// StreamRecord is the payload-free canonical Storage Write ledger. Schema is
// the canonical BigQuery schema (including decimal rounding-mode omission),
// while protobuf descriptors and row bytes remain outside SQLite.
type StreamRecord struct {
	Stream          WriteStream
	Operation       OperationKind
	OperationPhase  OperationPhase
	OperationToken  string
	CleanupPhase    CleanupPhase
	CleanupAttempts uint64
	Revision        int64
}

// AppendReceipt is an exact retry identity. PayloadDigest proves equality
// without retaining ProtoRows, and SchemaFingerprint proves descriptor
// equality without retaining the descriptor.
type AppendReceipt struct {
	StreamName        string
	StartOffset       int64
	RowCount          int64
	SchemaFingerprint string
	PayloadDigest     string
	Phase             ReceiptPhase
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type CommitMember struct {
	StreamName       string
	ExpectedRowCount int64
}

type CommitGroup struct {
	ID               string
	Parent           TableReference
	Members          []CommitMember
	ExpectedRowCount int64
	Phase            CommitPhase
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CommitTime       *time.Time
}

type StartupSnapshot struct {
	Streams      []StreamRecord
	Receipts     []AppendReceipt
	CommitGroups []CommitGroup
}
