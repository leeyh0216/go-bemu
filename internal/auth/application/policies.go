package application

import (
	"context"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
	authports "github.com/leeyh0216/go-bemu/internal/auth/ports"
)

// DisabledVerifier and PresenceVerifier are policy adapters without outbound
// dependencies. Static and future OAuth/STS verification live in separate
// adapter packages behind ports.TokenVerifier. Their stable labels are hashed
// because verifier revisions can cross directly into structured logs.
// https://pkg.go.dev/crypto/sha256
type DisabledVerifier struct{}

func (DisabledVerifier) Policy() authdomain.Policy { return authdomain.PolicyDisabled }
func (DisabledVerifier) CredentialKind() authdomain.CredentialKind {
	return authdomain.CredentialAnonymous
}
func (DisabledVerifier) Revision() string {
	return authdomain.Digest([]byte(authdomain.ModelVersion + ":disabled"))
}
func (verifier DisabledVerifier) Verify(context.Context, []byte) (authdomain.Verification, error) {
	principal, err := authdomain.NewPrincipal(authdomain.CredentialAnonymous, []byte("anonymous"))
	return authdomain.Verification{Principal: principal, VerifierRevision: verifier.Revision()}, err
}

type PresenceVerifier struct{}

func (PresenceVerifier) Policy() authdomain.Policy { return authdomain.PolicyBearerPresent }
func (PresenceVerifier) CredentialKind() authdomain.CredentialKind {
	return authdomain.CredentialBearerPresent
}
func (PresenceVerifier) Revision() string {
	return authdomain.Digest([]byte(authdomain.ModelVersion + ":bearer-present"))
}
func (verifier PresenceVerifier) Verify(_ context.Context, token []byte) (authdomain.Verification, error) {
	if len(token) == 0 {
		return authdomain.Verification{VerifierRevision: verifier.Revision()}, authdomain.NewError(
			authdomain.ReasonMissingCredential,
			authdomain.DiagnosticPresenceTokenMissing,
			nil,
		)
	}
	principal, err := authdomain.NewPrincipal(authdomain.CredentialBearerPresent, token)
	return authdomain.Verification{Principal: principal, VerifierRevision: verifier.Revision()}, err
}

var (
	_ authports.TokenVerifier = DisabledVerifier{}
	_ authports.TokenVerifier = PresenceVerifier{}
)
