package ports

import (
	"context"
	"time"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
)

// TokenVerifier accepts an already parsed RFC 6750 bearer token. Implementors
// must not retain or log the raw token. Verify binds its result to the exact
// SHA-256 revision used for lookup; Revision reports the current digest for
// denials that happen before lookup, such as malformed Authorization data.
type TokenVerifier interface {
	Policy() authdomain.Policy
	CredentialKind() authdomain.CredentialKind
	Verify(context.Context, []byte) (authdomain.Verification, error)
	Revision() string
}

// TokenSetSource transfers ownership of Payload to the caller. Implementations
// must apply maxBytes while reading rather than trusting file metadata, content
// length, or another preflight value.
type TokenSetSource interface {
	Read(context.Context, int64) ([]byte, error)
}

// Clock makes reload logging and future token-expiry rules deterministic in
// tests without weakening context deadlines at the transport boundary.
type Clock interface {
	Now() time.Time
}
