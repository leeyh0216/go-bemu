package domain

// Resource names and stream state follow the official WriteStream contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#writestream

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var resourceSegmentPattern = regexp.MustCompile(`^[^/]+$`)

type StreamType string

const (
	StreamTypeDefault StreamType = "DEFAULT"
	StreamTypePending StreamType = "PENDING"
)

type StreamState string

const (
	StreamStateOpen      StreamState = "OPEN"
	StreamStateFinalized StreamState = "FINALIZED"
	StreamStateCommitted StreamState = "COMMITTED"
	StreamStateFailed    StreamState = "FAILED"
)

type StreamOperation string

const (
	StreamOperationNone   StreamOperation = ""
	StreamOperationAppend StreamOperation = "APPEND"
	StreamOperationCommit StreamOperation = "COMMIT"
)

type CleanupState string

const (
	CleanupStateActive  CleanupState = "ACTIVE"
	CleanupStatePending CleanupState = "PENDING"
)

type AppendReceiptState string

const (
	AppendReceiptPrepared AppendReceiptState = "PREPARED"
	AppendReceiptApplied  AppendReceiptState = "APPLIED"
)

type Field struct {
	Name        string
	Type        string
	Mode        string
	Description string
	Precision   *int64
	Scale       *int64
	Fields      []Field
}

type TableSchema struct {
	Fields []Field
}

type TableReference struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func (r TableReference) Name() string {
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", r.ProjectID, r.DatasetID, r.TableID)
}

func ParseTableName(name string) (TableReference, error) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "datasets" || parts[4] != "tables" {
		return TableReference{}, fmt.Errorf("invalid table resource %q", name)
	}
	for _, segment := range []string{parts[1], parts[3], parts[5]} {
		if !resourceSegmentPattern.MatchString(segment) || segment == "." || segment == ".." {
			return TableReference{}, fmt.Errorf("invalid table resource segment")
		}
	}
	return TableReference{ProjectID: parts[1], DatasetID: parts[3], TableID: parts[5]}, nil
}

// ParseStreamName accepts the official default resource as well as the legacy
// table/_default spelling emitted by spark-bigquery-connector 0.44.2. It always
// returns the official canonical form so both aliases share one ledger.
//
// Official spelling: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest
// Connector spelling: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java
func ParseStreamName(name string) (TableReference, string, bool, error) {
	if strings.HasSuffix(name, "/_default") {
		prefix := strings.TrimSuffix(name, "/_default")
		prefix = strings.TrimSuffix(prefix, "/streams")
		table, err := ParseTableName(prefix)
		if err != nil {
			return TableReference{}, "", false, err
		}
		return table, table.Name() + "/streams/_default", true, nil
	}
	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[6] != "streams" || !resourceSegmentPattern.MatchString(parts[7]) {
		return TableReference{}, "", false, fmt.Errorf("invalid write stream resource %q", name)
	}
	table, err := ParseTableName(strings.Join(parts[:6], "/"))
	if err != nil {
		return TableReference{}, "", false, err
	}
	return table, name, false, nil
}

type WriteStream struct {
	Name              string
	Parent            TableReference
	Type              StreamType
	State             StreamState
	CreateTime        time.Time
	CommitTime        *time.Time
	Location          string
	Schema            TableSchema
	RowCount          int64
	NextOffset        int64
	SchemaFingerprint string
	TableFingerprint  string
	LastActivity      time.Time
	FailureCode       string
	FailureDigest     string
}

// StreamRecord is the canonical Storage Write ledger entry. WriterDescriptor
// is schema metadata, not row payload. Operation and Revision support bounded
// compare-and-swap transitions across emulator processes.
type StreamRecord struct {
	Stream           WriteStream
	WriterDescriptor []byte
	Operation        StreamOperation
	OperationToken   string
	CleanupState     CleanupState
	CleanupAttempts  uint64
	Revision         int64
}

// AppendReceipt contains only retry identity and accounting metadata. Raw
// ProtoRows remain exclusively in the physical storage adapter.
type AppendReceipt struct {
	StreamName        string
	StartOffset       int64
	RowCount          int64
	StagedBytes       int64
	SchemaFingerprint string
	PayloadDigest     string
	State             AppendReceiptState
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type CreateStreamRequest struct {
	Parent TableReference
	Type   StreamType
}

type AppendRequest struct {
	StreamName        string
	Offset            *int64
	Descriptor        []byte
	Rows              [][]byte
	PayloadBytes      int
	WireBytes         int
	SchemaFingerprint string
	PayloadDigest     string
	TraceID           string
}

type AppendResult struct {
	StreamName  string
	StartOffset int64
	HasOffset   bool
	RowCount    int64
}

type BatchCommitResult struct {
	CommitTime   *time.Time
	StreamErrors []StreamError
}

const GapCDC = "GAP-STORAGE-WRITE-CDC-001"
