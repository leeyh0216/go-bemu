package domain

import "time"

// Format is the immutable row encoding selected for a read session.
type Format uint8

const (
	FormatUnspecified Format = iota
	FormatArrow
	FormatAvro
)

func (f Format) String() string {
	switch f {
	case FormatArrow:
		return "ARROW"
	case FormatAvro:
		return "AVRO"
	default:
		return "UNSPECIFIED"
	}
}

// ReferenceSchema is returned with CreateReadSession and with the first
// ReadRows response. For Arrow, Serialized is one IPC schema message. For
// Avro, it is the UTF-8 JSON schema.
//
// Protocol sources:
//   - ArrowSchema: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#arrowschema
//   - AvroSchema: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#avroschema
type ReferenceSchema struct {
	Format      Format
	Serialized  []byte
	Fingerprint string
}

// MaterializeRequest describes the projection and filter that must be applied
// before row ordinals and stream ranges are fixed.
type MaterializeRequest struct {
	Table          string
	Format         Format
	SelectedFields []string
	RowRestriction string
	SnapshotTime   *time.Time
}

// SnapshotMetadata describes one immutable materialized result. All streams in
// a session share this result; adapters must not rerun the source query per
// stream. RetainedBytes is the adapter-defined storage charge held for the
// snapshot lifetime (memory payload or spill-file bytes). Container and
// allocator overhead is deliberately excluded because it is not stable across
// adapters or Go versions. RetainedBytes is distinct from EstimatedBytes,
// which populates the protocol's estimated bytes scanned.
type SnapshotMetadata struct {
	Schema         ReferenceSchema
	RowCount       int64
	EstimatedBytes int64
	RetainedBytes  int64
	// SelectedFields contains the canonical top-level field names resolved by
	// the materializer. It is lifecycle metadata, not snapshot payload.
	SelectedFields []string
	FilterShape    FilterShape
}

// FilterShape records non-sensitive structural facts about a validated row
// restriction. Literal values and the restriction text never cross the
// lifecycle-state port.
type FilterShape struct {
	PredicateCount       int
	LogicalOperatorCount int
}

// EncodedBatch is an exact contiguous range in a materialized snapshot.
// SerializedRows is one bare Arrow IPC record-batch message or concatenated
// raw Avro binary datums. It must not contain an Arrow stream/file wrapper or
// an Avro object-container header.
//
// Protocol sources:
//   - ArrowRecordBatch: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#arrowrecordbatch
//   - AvroRows: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#avrorows
type EncodedBatch struct {
	Offset         int64
	RowCount       int64
	SerializedRows []byte
}

type CreateSessionRequest struct {
	Parent                  string
	Table                   string
	Format                  Format
	SelectedFields          []string
	RowRestriction          string
	SnapshotTime            *time.Time
	MaxStreamCount          int32
	PreferredMinStreamCount int32
	TraceID                 string
}

type Stream struct {
	Name        string
	StartOffset int64
	EndOffset   int64
}

func (s Stream) RowCount() int64 { return s.EndOffset - s.StartOffset }

type Session struct {
	Name                  string
	Table                 string
	Format                Format
	Schema                ReferenceSchema
	Streams               []Stream
	ExpireTime            time.Time
	EstimatedRowCount     int64
	EstimatedBytesScanned int64
	SelectedFields        []string
	RowRestriction        string
	SnapshotTime          *time.Time
	TraceID               string
}

// SessionLifecycle is canonical metadata for a Storage Read session. ACTIVE
// means its snapshot belongs to the current process. Terminal states are kept
// after snapshot bytes have gone away so an old stream cannot be confused with
// a newly materialized result.
type SessionLifecycle string

const (
	SessionActive      SessionLifecycle = "ACTIVE"
	SessionExpired     SessionLifecycle = "EXPIRED"
	SessionUnavailable SessionLifecycle = "UNAVAILABLE"
)

// SessionRecord is the durable, payload-free representation of one read
// session. RowRestrictionDigest is SHA-256 over the client predicate and is
// deliberately not reversible. SelectedFields are canonical names returned by
// the snapshot materializer.
type SessionRecord struct {
	Name                  string
	Table                 string
	Format                Format
	SelectedFields        []string
	RowRestrictionDigest  string
	RowRestrictionBytes   int
	FilterShape           FilterShape
	Streams               []Stream
	CreatedAt             time.Time
	ExpireTime            time.Time
	SnapshotTime          *time.Time
	RetainedRowCount      int64
	RetainedBytes         int64
	EstimatedBytesScanned int64
	SchemaFingerprint     string
	Lifecycle             SessionLifecycle
	LifecycleUpdatedAt    time.Time
}

// PersistedStream identifies the terminal or active lifecycle metadata for an
// exact stream name. It does not provide access to snapshot bytes.
type PersistedStream struct {
	Name      string
	Session   string
	Lifecycle SessionLifecycle
	ExpiresAt time.Time
}

type ReadRowsRequest struct {
	StreamName string
	Offset     int64
}

// ReadChunk is independent of protobuf but maps one-to-one to a ReadRowsResponse.
// Schema is non-nil only for the first emitted chunk of an RPC.
type ReadChunk struct {
	Format        Format
	Schema        *ReferenceSchema
	Batch         EncodedBatch
	ProgressStart float64
	ProgressEnd   float64
}
