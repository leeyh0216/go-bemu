package domain

// Resource names and stream state follow the official WriteStream contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#writestream

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
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
)

type Field = catalogdomain.Field

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

// ParseStreamName accepts only the current v1 resource form documented by the
// Storage Write API, including /streams/_default for the default stream.
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest
func ParseStreamName(name string) (TableReference, string, bool, error) {
	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[6] != "streams" || !resourceSegmentPattern.MatchString(parts[7]) {
		return TableReference{}, "", false, fmt.Errorf("invalid write stream resource %q", name)
	}
	table, err := ParseTableName(strings.Join(parts[:6], "/"))
	if err != nil {
		return TableReference{}, "", false, err
	}
	return table, name, parts[7] == "_default", nil
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
	LastActivity      time.Time
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
