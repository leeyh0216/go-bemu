package integrationcontract

import (
	"strings"
	"testing"
)

func TestRenderCapabilityCoverageIsCompactAndExplainsEvidenceBoundary(t *testing.T) {
	index := CapabilityIndex{
		Cases: []CapabilityCase{{
			ID: "SBQ-READ-EXAMPLE-V1", State: CapabilityCasePartial, Summary: "Example read",
			Issue: "https://github.com/leeyh0216/go-bemu/issues/6", Limitation: "Nested values are bounded.",
		}},
		APICoverage: []CapabilityAPICoverage{{OperationID: "bigquery.tables.get", CaseIDs: []string{"SBQ-READ-EXAMPLE-V1"}}},
	}
	contents := string(renderCapabilityCoverage(index, "en"))
	for _, want := range []string{
		"Only behaviors in this table are support claims.",
		"runtime traces are not compared",
		"SBQ-READ-EXAMPLE-V1",
		"bigquery.tables.get",
	} {
		if !strings.Contains(contents, want) {
			t.Fatalf("coverage document lacks %q:\n%s", want, contents)
		}
	}
}
