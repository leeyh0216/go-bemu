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
