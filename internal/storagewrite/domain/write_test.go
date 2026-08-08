package domain

import "testing"

func TestParseStreamNameAcceptsV1DefaultStream(t *testing.T) {
	const parent = "projects/test-project/datasets/analytics/tables/events"
	table, canonical, isDefault, err := ParseStreamName(parent + "/streams/_default")
	if err != nil {
		t.Fatal(err)
	}
	if !isDefault || table.Name() != parent || canonical != parent+"/streams/_default" {
		t.Fatalf("unexpected parse result: table=%q stream=%q default=%t", table.Name(), canonical, isDefault)
	}
}

func TestParseStreamNameRejectsMalformedResources(t *testing.T) {
	for _, name := range []string{"", "projects/p/datasets/d/tables/t", "projects/p/datasets/d/tables/t/_default", "projects/p/datasets/d/tables/t/streams/", "projects/p/datasets/d/tables/t/streams/a/extra"} {
		if _, _, _, err := ParseStreamName(name); err == nil {
			t.Fatalf("expected %q to be rejected", name)
		}
	}
}
