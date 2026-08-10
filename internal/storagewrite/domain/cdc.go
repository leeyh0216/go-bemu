package domain

import (
	"fmt"
	"strconv"
	"strings"
)

// CDCChangeType is the Storage Write CDC pseudocolumn value. It is deliberately
// separate from ordinary INSERT ingestion so callers cannot silently mix the
// two modes on a connection.
// https://cloud.google.com/bigquery/docs/change-data-capture#specify_changes_to_existing_records
type CDCChangeType string

const (
	CDCChangeTypeUpsert CDCChangeType = "UPSERT"
	CDCChangeTypeDelete CDCChangeType = "DELETE"
)

func ParseCDCChangeType(value string) (CDCChangeType, error) {
	switch CDCChangeType(value) {
	case CDCChangeTypeUpsert, CDCChangeTypeDelete:
		return CDCChangeType(value), nil
	default:
		return "", fmt.Errorf("invalid CDC change type %q: expected UPSERT or DELETE", value)
	}
}

// CDCSequenceNumber is the syntactically validated user-supplied ordering key.
// BigQuery permits one through four unsigned hexadecimal sections, each with no
// more than sixteen hexadecimal digits. It intentionally does not decide
// precedence: the documented page does not define the result when equal prefixes
// have different section counts, and a CDC apply ledger must retain ingestion
// time for equal ordering keys.
// https://cloud.google.com/bigquery/docs/change-data-capture#_change_sequence_number_format
type CDCSequenceNumber struct {
	sections [4]uint64
	count    int
}

func ParseCDCSequenceNumber(value string) (CDCSequenceNumber, error) {
	parts := strings.Split(value, "/")
	if len(parts) < 1 || len(parts) > len((CDCSequenceNumber{}).sections) {
		return CDCSequenceNumber{}, fmt.Errorf("invalid CDC sequence number %q: expected one to four hexadecimal sections", value)
	}
	sequence := CDCSequenceNumber{count: len(parts)}
	for index, part := range parts {
		if len(part) == 0 || len(part) > 16 {
			return CDCSequenceNumber{}, fmt.Errorf("invalid CDC sequence number %q: section %d must contain one to sixteen hexadecimal characters", value, index+1)
		}
		parsed, err := strconv.ParseUint(part, 16, 64)
		if err != nil {
			return CDCSequenceNumber{}, fmt.Errorf("invalid CDC sequence number %q: section %d is not hexadecimal", value, index+1)
		}
		sequence.sections[index] = parsed
	}
	return sequence, nil
}

func (s CDCSequenceNumber) SectionCount() int {
	return s.count
}

// Section exposes a parsed unsigned section to the adapter-owned CDC ledger.
// Callers must use only indexes below SectionCount.
func (s CDCSequenceNumber) Section(index int) uint64 { return s.sections[index] }
