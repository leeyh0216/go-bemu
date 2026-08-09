package domain

import "testing"

func TestLoadMutationIDBindsJobAndConfiguration(t *testing.T) {
	reference := JobReference{ProjectID: "test-project", Location: "US", JobID: "load-1"}
	first, err := LoadMutationID(reference, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadMutationID(reference, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	if !ValidLoadMutationID(first) || first == second {
		t.Fatalf("mutation identities = %q / %q", first, second)
	}
	repeated, err := LoadMutationID(reference, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil || repeated != first {
		t.Fatalf("repeated mutation identity = %q, %v", repeated, err)
	}
}
