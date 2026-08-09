package observability

import "testing"

func TestEventVocabularyAndTransitionContract(t *testing.T) {
	if err := BoundaryEnter.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := EventKind("side_effect.pre").Validate(); err == nil {
		t.Fatal("legacy event accepted")
	}
	if err := (Transition{Aggregate: "job", From: "pending", To: "running", Reason: "admitted", CorrelationID: "request"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Transition{}).Validate(); err == nil {
		t.Fatal("incomplete transition accepted")
	}
	if _, err := NewTransition("", "old", "new", "reason", "request"); err == nil {
		t.Fatal("constructor accepted an incomplete transition")
	}
}
