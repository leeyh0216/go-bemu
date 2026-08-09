package state

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestMutationValidation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 1, 2, 3, 0, time.UTC)
	before := domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "events", Type: "TABLE",
		Schema: []domain.Field{{Name: "value", Type: "STRING"}}, CreatedAt: now, UpdatedAt: now}
	after := before
	after.Schema = []domain.Field{{Name: "value", Type: "INT64"}}
	after.UpdatedAt = now.Add(time.Second)
	intent := BeginMutation{
		ID: "mutation-001", ResourceKey: TableResourceKey(before), Kind: MutationKindTableSchema,
		ExpectedCanonicalRevision: TableRevision(before), BeforePhysicalFingerprint: Fingerprint([]byte("before")),
		AfterPhysicalFingerprint: Fingerprint([]byte("after")), TableChange: TableChange{Before: before, After: after}, PreparedAt: now,
	}
	if err := intent.Validate(); err != nil {
		t.Fatal(err)
	}
	record := Mutation{
		ID: intent.ID, ResourceKey: intent.ResourceKey, Kind: intent.Kind,
		ExpectedCanonicalRevision: intent.ExpectedCanonicalRevision,
		BeforePhysicalFingerprint: intent.BeforePhysicalFingerprint,
		AfterPhysicalFingerprint:  intent.AfterPhysicalFingerprint,
		TableChange:               intent.TableChange,
		State:                     MutationPrepared,
		PreparedAt:                now,
		UpdatedAt:                 now,
	}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	record.State = MutationFailed
	record.Failure = Failure{Code: "physical.ddl_failed", Digest: Fingerprint([]byte("safe error class"))}
	record.UpdatedAt = now.Add(time.Second)
	record.CompletedAt = record.UpdatedAt
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}

	invalid := intent
	invalid.ID = "contains space"
	if err := invalid.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid ID error = %v", err)
	}
	invalid = intent
	invalid.BeforePhysicalFingerprint = "SHA256:not-canonical"
	if err := invalid.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid fingerprint error = %v", err)
	}
	invalid = intent
	invalid.BeforePhysicalFingerprint = ""
	invalid.AfterPhysicalFingerprint = ""
	if err := invalid.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing fingerprints error = %v", err)
	}
	if err := (Failure{Code: "Raw Error", Digest: Fingerprint(nil)}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid failure error = %v", err)
	}
}

func TestValidationErrorsDoNotIncludeValues(t *testing.T) {
	t.Parallel()
	secretID := "mutation-sensitive-id with spaces"
	secretResource := "project-sensitive/dataset-sensitive/table-sensitive"
	err := (BeginMutation{ID: secretID, ResourceKey: secretResource}).Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if strings.Contains(err.Error(), secretID) || strings.Contains(err.Error(), secretResource) {
		t.Fatalf("validation error disclosed mutation values: %v", err)
	}
}
