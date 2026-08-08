package static

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
	authports "github.com/leeyh0216/go-bemu/internal/auth/ports"
)

const (
	ManifestAPIVersion = authdomain.ModelVersion
	ManifestKind       = "StaticTokenSet"

	defaultMaxFileBytes      int64 = 1024 * 1024
	defaultMaxTokens               = 1024
	defaultMinTokenBytes           = 1
	defaultMaxTokenBytes           = 16 * 1024
	defaultMaxPrincipalBytes       = 512
)

type Options struct {
	MaxFileBytes      int64
	MaxTokens         int
	MinTokenBytes     int
	MaxTokenBytes     int
	MaxPrincipalBytes int
	Clock             authports.Clock
	Logger            *slog.Logger
}

func DefaultOptions() Options {
	return Options{
		MaxFileBytes:      defaultMaxFileBytes,
		MaxTokens:         defaultMaxTokens,
		MinTokenBytes:     defaultMinTokenBytes,
		MaxTokenBytes:     defaultMaxTokenBytes,
		MaxPrincipalBytes: defaultMaxPrincipalBytes,
		Clock:             systemClock{},
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func (o Options) validate() error {
	if o.MaxFileBytes < 1 || o.MaxFileBytes == math.MaxInt64 || o.MaxTokens < 1 || o.MinTokenBytes < 1 ||
		o.MaxTokenBytes < o.MinTokenBytes || o.MaxPrincipalBytes < 1 || o.Clock == nil {
		return authdomain.NewError(
			authdomain.ReasonVerifierUnavailable,
			authdomain.DiagnosticStaticOptionsInvalid,
			nil,
		)
	}
	return nil
}

type SnapshotState string

const (
	SnapshotActive  SnapshotState = "active"
	SnapshotDenyAll SnapshotState = "deny-all"
)

type SnapshotMetadata struct {
	State       SnapshotState
	TokenCount  int
	Revision    string
	LoadedAt    time.Time
	LoadAttempt uint64
}

type tokenRecord struct {
	tokenDigest [sha256.Size]byte
	principal   authdomain.Principal
}

type snapshot struct {
	metadata SnapshotMetadata
	records  []tokenRecord
}

// Verifier atomically publishes immutable snapshots. Reload is serialized so
// an older, slower read can never overwrite the result of a later attempt.
// https://pkg.go.dev/sync/atomic#Pointer
type Verifier struct {
	source   authports.TokenSetSource
	options  Options
	snapshot atomic.Pointer[snapshot]
	reloadMu sync.Mutex
	attempts atomic.Uint64
}

func New(ctx context.Context, source authports.TokenSetSource, options Options) (*Verifier, error) {
	if source == nil {
		return nil, authdomain.NewError(
			authdomain.ReasonVerifierUnavailable,
			authdomain.DiagnosticStaticSourceMissing,
			nil,
		)
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if err := options.validate(); err != nil {
		return nil, err
	}

	verifier := &Verifier{source: source, options: options}
	if err := verifier.loadAndPublish(ctx, "initial-load", false); err != nil {
		return nil, err
	}
	return verifier, nil
}

func (v *Verifier) Policy() authdomain.Policy { return authdomain.PolicyStatic }
func (v *Verifier) CredentialKind() authdomain.CredentialKind {
	return authdomain.CredentialStatic
}

func (v *Verifier) Revision() string {
	current := v.snapshot.Load()
	if current == nil {
		return uninitializedRevision()
	}
	return current.metadata.Revision
}

func (v *Verifier) SnapshotMetadata() SnapshotMetadata {
	current := v.snapshot.Load()
	if current == nil {
		return SnapshotMetadata{State: SnapshotDenyAll, Revision: uninitializedRevision()}
	}
	return current.metadata
}

func (v *Verifier) Verify(ctx context.Context, token []byte) (authdomain.Verification, error) {
	current := v.snapshot.Load()
	verification := authdomain.Verification{VerifierRevision: uninitializedRevision()}
	if current != nil {
		verification.VerifierRevision = current.metadata.Revision
	}
	if err := ctx.Err(); err != nil {
		return verification, authdomain.NewError(
			authdomain.ReasonVerifierUnavailable,
			authdomain.DiagnosticStaticVerifyContextEnded,
			err,
		)
	}
	if err := authdomain.ValidateBearerToken(token, v.options.MinTokenBytes, v.options.MaxTokenBytes); err != nil {
		return verification, err
	}

	if current == nil || current.metadata.State != SnapshotActive {
		return verification, authdomain.NewError(
			authdomain.ReasonVerifierUnavailable,
			authdomain.DiagnosticStaticDenyAll,
			nil,
		)
	}

	digest := sha256.Sum256(token)
	matched := 0
	matchedIndex := 0
	// Every configured digest is compared so lookup time does not reveal which
	// entry matched. Duplicate token digests are rejected while loading.
	// https://pkg.go.dev/crypto/subtle#ConstantTimeCompare
	for index := range current.records {
		equal := subtle.ConstantTimeCompare(digest[:], current.records[index].tokenDigest[:])
		matchedIndex = subtle.ConstantTimeSelect(equal, index, matchedIndex)
		matched = subtle.ConstantTimeSelect(equal, 1, matched)
	}
	if matched != 1 {
		return verification, authdomain.NewError(
			authdomain.ReasonInvalidCredential,
			authdomain.DiagnosticStaticTokenUnknown,
			nil,
		)
	}
	verification.Principal = current.records[matchedIndex].principal
	return verification, nil
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

var _ authports.TokenVerifier = (*Verifier)(nil)

func uninitializedRevision() string {
	return authdomain.Digest([]byte(authdomain.ModelVersion + ":static:uninitialized"))
}
