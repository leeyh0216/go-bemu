package ports

import (
	"context"
	"time"

	issuerdomain "github.com/leeyh0216/go-bemu/internal/auth/issuer/domain"
)

type Clock interface {
	Now() time.Time
}

// Entropy fills destination with cryptographically secure random bytes. An
// implementation must either fill the complete slice or return an error.
type Entropy interface {
	Fill(context.Context, []byte) error
}

type RefreshCredential struct {
	ClientID     []byte
	ClientSecret []byte
	RefreshToken []byte
}

type ActorCredential struct {
	TokenType string
	Token     []byte
}

type SubjectTokenCredential struct {
	TokenType string
	Token     []byte
	Audience  string
	Resource  string
	Scopes    []string
	Actor     *ActorCredential
	Options   []byte
}

// CredentialVerifier is deliberately independent of fixture files or an
// identity provider. A later adapter can validate real OIDC, SAML, or AWS
// subject tokens without changing the grant parser or token store.
type CredentialVerifier interface {
	VerifyRefresh(context.Context, RefreshCredential) (issuerdomain.Subject, error)
	VerifySubjectToken(context.Context, SubjectTokenCredential) (issuerdomain.Subject, error)
}

// SignatureInput contains the minimum information needed to select a local
// fixture key and verify the JWS signature. The application owns JWT parsing,
// claim validation, audience policy, time policy, and replay protection.
type SignatureInput struct {
	Algorithm    string
	Issuer       []byte
	KeyID        []byte
	SigningInput []byte
	Signature    []byte
}

type SignatureVerifier interface {
	Verify(context.Context, SignatureInput) error
}

// IssuedTokenStore atomically commits a digest-only token record and optional
// replay marker. If either digest already exists, neither record may be
// modified. That invariant makes concurrent JWT replay checks race-free.
type IssuedTokenStore interface {
	Commit(context.Context, issuerdomain.IssuedToken, *issuerdomain.ReplayMarker) error
	Lookup(context.Context, string, time.Time) (issuerdomain.IssuedToken, bool, error)
	Revoke(context.Context, string) error
}
