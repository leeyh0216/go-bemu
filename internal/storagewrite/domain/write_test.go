package domain

import "testing"

func TestParseStreamNameCanonicalizesDefaultAliases(t *testing.T) {
	const parent = "projects/test-project/datasets/analytics/tables/events"
	for _, name := range []string{parent + "/streams/_default", parent + "/_default"} {
		table, canonical, isDefault, err := ParseStreamName(name)
		if err != nil {
			t.Fatal(err)
		}
		if !isDefault || table.Name() != parent || canonical != parent+"/streams/_default" {
			t.Fatalf("unexpected parse result: table=%q stream=%q default=%t", table.Name(), canonical, isDefault)
		}
	}
}

func TestParseStreamNameRejectsMalformedResources(t *testing.T) {
	for _, name := range []string{"", "projects/p/datasets/d/tables/t", "projects/p/datasets/d/tables/t/streams/", "projects/p/datasets/d/tables/t/streams/a/extra"} {
		if _, _, _, err := ParseStreamName(name); err == nil {
			t.Fatalf("expected %q to be rejected", name)
		}
	}
}
