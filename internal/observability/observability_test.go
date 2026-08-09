package observability

import (
	"fmt"
	"strings"
	"testing"
)

func TestPayloadAndErrorAttributesRetainRawDiagnostics(t *testing.T) {
	payload := `SELECT 'diagnostic-value'`
	errorMessage := `request failed: {"row":"invalid-value"}`
	attrs := fmt.Sprint(PayloadAttrs("query", []byte(payload)), ErrorAttrs(fmt.Errorf("%s", errorMessage)))
	for _, expected := range []string{payload, errorMessage, "query_bytes", "error_type"} {
		if !strings.Contains(attrs, expected) {
			t.Fatalf("diagnostic attributes omitted %q: %s", expected, attrs)
		}
	}
}

func TestMetadataEntriesRetainValues(t *testing.T) {
	entries := fmt.Sprint(MetadataEntries(map[string][]string{"authorization": {"Bearer diagnostic-token"}, "content-type": {"application/grpc"}}))
	for _, expected := range []string{"authorization=Bearer diagnostic-token", "content-type=application/grpc"} {
		if !strings.Contains(entries, expected) {
			t.Fatalf("metadata entries omitted %q: %s", expected, entries)
		}
	}
}
