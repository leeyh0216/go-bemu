package contracttest

import "testing"

// Operation links a test to one public operation. The manifest compiler reads
// the literal operation ID from the test source; runtime metadata is not used.
func Operation(t testing.TB, operationID string) {
	t.Helper()
	if operationID == "" {
		t.Fatal("operation ID must not be empty")
	}
}
