// Package state defines BQEMU-owned durable state contracts.
package state

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

const (
	MaxMutationIDBytes   = 256
	MaxResourceKeyBytes  = 4096
	MaxMutationKindBytes = 128
	MaxFailureCodeBytes  = 128
)

var (
	ErrInvalid           = errors.New("invalid state mutation")
	ErrNotFound          = errors.New("state mutation not found")
	ErrConflict          = errors.New("state mutation conflicts with an existing mutation")
	ErrInvalidTransition = errors.New("invalid state mutation transition")
)

type MutationState string

const (
	MutationPrepared MutationState = "PREPARED"
	MutationApplied  MutationState = "APPLIED"
	MutationFailed   MutationState = "FAILED"
)

func (state MutationState) Valid() bool {
	return state == MutationPrepared || state == MutationApplied || state == MutationFailed
}

type MutationKind string

const MutationKindTableSchema MutationKind = "table.schema"

// TableChange is the canonical, engine-independent intent for one table schema
// mutation. It deliberately stores domain metadata rather than backend SQL so
// a pending change can be replanned safely after a restart.
type TableChange struct {
	Before domain.Table `json:"before"`
	After  domain.Table `json:"after"`
}

func (change TableChange) Validate() error {
	if err := change.Before.Validate(); err != nil {
		return fmt.Errorf("%w: before_table", ErrInvalid)
	}
	if err := change.After.Validate(); err != nil {
		return fmt.Errorf("%w: after_table", ErrInvalid)
	}
	if tableIdentity(change.Before) != tableIdentity(change.After) {
		return invalidField("table_identity")
	}
	if reflect.DeepEqual(change.Before.Schema, change.After.Schema) {
		return invalidField("table_schema_change")
	}
	beforeMetadata, afterMetadata := change.Before, change.After
	beforeMetadata.Schema, afterMetadata.Schema = nil, nil
	beforeMetadata.UpdatedAt, afterMetadata.UpdatedAt = time.Time{}, time.Time{}
	if !reflect.DeepEqual(beforeMetadata, afterMetadata) {
		return invalidField("non_schema_table_metadata")
	}
	if change.Before.UpdatedAt.IsZero() || change.After.UpdatedAt.IsZero() ||
		!change.After.UpdatedAt.After(change.Before.UpdatedAt) {
		return invalidField("canonical_revision")
	}
	return nil
}

func tableIdentity(table domain.Table) string {
	return table.ProjectID + "\x00" + table.DatasetID + "\x00" + table.ID
}

func TableResourceKey(table domain.Table) string {
	return "projects/" + table.ProjectID + "/datasets/" + table.DatasetID + "/tables/" + table.ID
}

// TableRevision maps the catalog's durable updated timestamp to the optimistic
// revision recorded in the journal. Catalog publication additionally compares
// the complete canonical Before value inside the SQLite transaction.
func TableRevision(table domain.Table) int64 {
	if table.UpdatedAt.IsZero() || table.UpdatedAt.UnixNano() < 0 {
		return 0
	}
	return table.UpdatedAt.UnixNano()
}

// BeginMutation contains the immutable table-schema intent written before a
// physical-engine change. Both fingerprints bind it to one engine mapping.
type BeginMutation struct {
	ID                        string
	ResourceKey               string
	Kind                      MutationKind
	ExpectedCanonicalRevision int64
	BeforePhysicalFingerprint string
	AfterPhysicalFingerprint  string
	TableChange               TableChange
	PreparedAt                time.Time
}

func (mutation BeginMutation) Validate() error {
	if !validOpaqueID(mutation.ID, MaxMutationIDBytes) {
		return invalidField("mutation_id")
	}
	if !validResourceKey(mutation.ResourceKey) {
		return invalidField("resource_key")
	}
	if !validCode(string(mutation.Kind), MaxMutationKindBytes) {
		return invalidField("mutation_kind")
	}
	if mutation.ExpectedCanonicalRevision < 0 {
		return invalidField("expected_canonical_revision")
	}
	if !ValidFingerprint(mutation.BeforePhysicalFingerprint) {
		return invalidField("before_physical_fingerprint")
	}
	if !ValidFingerprint(mutation.AfterPhysicalFingerprint) {
		return invalidField("after_physical_fingerprint")
	}
	if mutation.Kind != MutationKindTableSchema {
		return invalidField("mutation_kind")
	}
	if err := mutation.TableChange.Validate(); err != nil {
		return err
	}
	if mutation.ResourceKey != TableResourceKey(mutation.TableChange.Before) {
		return invalidField("resource_key")
	}
	if mutation.ExpectedCanonicalRevision != TableRevision(mutation.TableChange.Before) {
		return invalidField("expected_canonical_revision")
	}
	if mutation.PreparedAt.IsZero() {
		return invalidField("prepared_at")
	}
	return nil
}

type Failure struct {
	Code   string
	Digest string
}

func (failure Failure) Validate() error {
	if !validCode(failure.Code, MaxFailureCodeBytes) {
		return invalidField("failure_code")
	}
	if !ValidFingerprint(failure.Digest) {
		return invalidField("failure_digest")
	}
	return nil
}

// Mutation contains a durable journal record. Failure stores a stable code and
// digest only; raw errors, SQL, row payloads, and identifiers never belong in
// its failure fields.
type Mutation struct {
	ID                        string
	ResourceKey               string
	Kind                      MutationKind
	ExpectedCanonicalRevision int64
	BeforePhysicalFingerprint string
	AfterPhysicalFingerprint  string
	TableChange               TableChange
	State                     MutationState
	Failure                   Failure
	PreparedAt                time.Time
	UpdatedAt                 time.Time
	CompletedAt               time.Time
}

func (mutation Mutation) Validate() error {
	intent := BeginMutation{
		ID: mutation.ID, ResourceKey: mutation.ResourceKey, Kind: mutation.Kind,
		ExpectedCanonicalRevision: mutation.ExpectedCanonicalRevision,
		BeforePhysicalFingerprint: mutation.BeforePhysicalFingerprint,
		AfterPhysicalFingerprint:  mutation.AfterPhysicalFingerprint,
		TableChange:               mutation.TableChange,
		PreparedAt:                mutation.PreparedAt,
	}
	if err := intent.Validate(); err != nil {
		return err
	}
	if !mutation.State.Valid() || mutation.UpdatedAt.IsZero() || mutation.UpdatedAt.Before(mutation.PreparedAt) {
		return invalidField("state_timestamps")
	}
	switch mutation.State {
	case MutationPrepared:
		if mutation.Failure != (Failure{}) || !mutation.CompletedAt.IsZero() {
			return invalidField("prepared_state")
		}
	case MutationApplied:
		if mutation.Failure != (Failure{}) || mutation.CompletedAt.IsZero() ||
			mutation.CompletedAt.Before(mutation.PreparedAt) || !mutation.CompletedAt.Equal(mutation.UpdatedAt) {
			return invalidField("applied_state")
		}
	case MutationFailed:
		if err := mutation.Failure.Validate(); err != nil {
			return err
		}
		if mutation.CompletedAt.IsZero() || mutation.CompletedAt.Before(mutation.PreparedAt) ||
			!mutation.CompletedAt.Equal(mutation.UpdatedAt) {
			return invalidField("failed_state")
		}
	}
	return nil
}

func (mutation Mutation) SameIntent(intent BeginMutation) bool {
	return mutation.ID == intent.ID && mutation.ResourceKey == intent.ResourceKey &&
		mutation.Kind == intent.Kind && mutation.ExpectedCanonicalRevision == intent.ExpectedCanonicalRevision &&
		mutation.BeforePhysicalFingerprint == intent.BeforePhysicalFingerprint &&
		mutation.AfterPhysicalFingerprint == intent.AfterPhysicalFingerprint &&
		reflect.DeepEqual(mutation.TableChange, intent.TableChange)
}

func Fingerprint(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func ValidFingerprint(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	encoded := strings.TrimPrefix(value, "sha256:")
	if encoded != strings.ToLower(encoded) {
		return false
	}
	_, err := hex.DecodeString(encoded)
	return err == nil
}

func validOpaqueID(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func validResourceKey(value string) bool {
	if strings.TrimSpace(value) == "" || len(value) > MaxResourceKeyBytes {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func validCode(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') &&
			character != '.' && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func invalidField(field string) error {
	return fmt.Errorf("%w: %s", ErrInvalid, field)
}
