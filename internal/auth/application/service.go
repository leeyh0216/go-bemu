package application

import (
	"context"
	"io"
	"log/slog"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
	authports "github.com/leeyh0216/go-bemu/internal/auth/ports"
)

const (
	defaultMinTokenBytes         = 1
	defaultMaxTokenBytes         = 16 * 1024
	defaultMaxAuthorizationBytes = 32 * 1024
)

// Config bounds attacker-controlled values before an adapter receives them.
// The defaults accommodate Google OAuth access tokens while remaining
// overrideable for deployments with a stricter local contract.
type Config struct {
	MinTokenBytes         int
	MaxTokenBytes         int
	MaxAuthorizationBytes int
}

func DefaultConfig() Config {
	return Config{
		MinTokenBytes:         defaultMinTokenBytes,
		MaxTokenBytes:         defaultMaxTokenBytes,
		MaxAuthorizationBytes: defaultMaxAuthorizationBytes,
	}
}

func (c Config) validate() error {
	if c.MinTokenBytes < 1 || c.MaxTokenBytes < c.MinTokenBytes ||
		c.MaxAuthorizationBytes < len("Bearer ") ||
		c.MaxTokenBytes > c.MaxAuthorizationBytes-len("Bearer ") {
		return authdomain.NewError(
			authdomain.ReasonVerifierUnavailable,
			authdomain.DiagnosticServiceBoundsInvalid,
			nil,
		)
	}
	return nil
}

type Service struct {
	config   Config
	verifier authports.TokenVerifier
	logger   *slog.Logger
}

func New(config Config, verifier authports.TokenVerifier, logger *slog.Logger) (*Service, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if verifier == nil {
		return nil, authdomain.NewError(
			authdomain.ReasonVerifierUnavailable,
			authdomain.DiagnosticServiceVerifierMissing,
			nil,
		)
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Service{config: config, verifier: verifier, logger: logger}, nil
}

// Authenticate accepts all values associated with the Authorization metadata
// key. Passing the complete slice lets this layer reject ambiguous duplicate
// credentials consistently for HTTP and gRPC.
func (s *Service) Authenticate(ctx context.Context, authorizationValues []string) (_ context.Context, decision authdomain.Decision, resultErr error) {
	decision = s.baseDecision()
	defer func() {
		attrs := append([]any{"event", "auth.decision"}, decision.SafeLogAttrs()...)
		if resultErr != nil {
			attrs = append(attrs, authdomain.SafeErrorAttrs(resultErr)...)
		}
		s.logger.InfoContext(ctx, "authentication decision", attrs...)
	}()

	policy, kind, revision, err := verifierMetadata(s.verifier)
	decision.Policy = policy
	decision.CredentialKind = kind
	decision.VerifierRevision = revision
	if err != nil {
		decision.Reason = authdomain.ReasonVerifierUnavailable
		return ctx, decision, err
	}

	// Disabled mode deliberately ignores credentials. This prevents malformed
	// development credentials from changing an explicitly anonymous contract.
	if policy == authdomain.PolicyDisabled {
		verification, verifyErr := s.verifier.Verify(ctx, nil)
		verifyErr = authdomain.NormalizeError(
			verifyErr, authdomain.ReasonVerifierUnavailable, authdomain.DiagnosticVerifierFailure,
		)
		if !authdomain.ValidRevision(verification.VerifierRevision) {
			verifyErr = invalidVerifierResult(authdomain.DiagnosticVerificationRevisionInvalid, verifyErr)
		} else {
			decision.VerifierRevision = verification.VerifierRevision
		}
		principal := verification.Principal
		if verifyErr != nil || !principal.Valid() || principal.CredentialKind() != kind {
			if verifyErr == nil {
				verifyErr = invalidVerifierResult(authdomain.DiagnosticDisabledPrincipalInvalid, nil)
			}
			decision.Reason = authdomain.ReasonOf(verifyErr)
			return ctx, decision, verifyErr
		}
		decision.Result = authdomain.ResultAllow
		decision.Reason = authdomain.ReasonAuthenticationOff
		decision.PrincipalDigest = principal.Digest()
		return withAuthentication(ctx, principal, decision), decision, nil
	}

	parsed, parseErr := s.parseAuthorization(authorizationValues)
	decision.AuthorizationFields = parsed.authorizationFields
	decision.AuthorizationBytes = parsed.authorizationBytes
	decision.AuthorizationDigest = parsed.authorizationDigest
	decision.TokenDigest = parsed.tokenDigest
	decision.TokenBytes = parsed.tokenBytes
	if parseErr != nil {
		decision.Reason = authdomain.ReasonOf(parseErr)
		return ctx, decision, parseErr
	}
	token := parsed.token
	defer clear(token)

	verification, verifyErr := s.verifier.Verify(ctx, token)
	verifyErr = authdomain.NormalizeError(
		verifyErr, authdomain.ReasonVerifierUnavailable, authdomain.DiagnosticVerifierFailure,
	)
	if !authdomain.ValidRevision(verification.VerifierRevision) {
		verifyErr = invalidVerifierResult(authdomain.DiagnosticVerificationRevisionInvalid, verifyErr)
		decision.VerifierRevision = "invalid"
	} else {
		decision.VerifierRevision = verification.VerifierRevision
	}
	if verifyErr != nil {
		decision.Reason = authdomain.ReasonOf(verifyErr)
		return ctx, decision, verifyErr
	}
	principal := verification.Principal
	if !principal.Valid() || principal.CredentialKind() != kind {
		verifyErr = invalidVerifierResult(authdomain.DiagnosticVerificationPrincipalInvalid, nil)
		decision.Reason = authdomain.ReasonVerifierUnavailable
		return ctx, decision, verifyErr
	}

	decision.Result = authdomain.ResultAllow
	decision.Reason = authdomain.ReasonAllowed
	decision.PrincipalDigest = principal.Digest()
	return withAuthentication(ctx, principal, decision), decision, nil
}

func (s *Service) baseDecision() authdomain.Decision {
	return authdomain.Decision{
		Policy:         "",
		Result:         authdomain.ResultDeny,
		Reason:         authdomain.ReasonVerifierUnavailable,
		CredentialKind: authdomain.CredentialUnknown,
	}
}

func verifierMetadata(verifier authports.TokenVerifier) (authdomain.Policy, authdomain.CredentialKind, string, error) {
	policy := verifier.Policy()
	kind := verifier.CredentialKind()
	revision := verifier.Revision()
	wantKind := map[authdomain.Policy]authdomain.CredentialKind{
		authdomain.PolicyDisabled:      authdomain.CredentialAnonymous,
		authdomain.PolicyBearerPresent: authdomain.CredentialBearerPresent,
		authdomain.PolicyStatic:        authdomain.CredentialStatic,
	}[policy]
	if !policy.Valid() || !kind.Valid() || kind != wantKind || !authdomain.ValidRevision(revision) {
		return "", authdomain.CredentialUnknown, "invalid", invalidVerifierResult(authdomain.DiagnosticVerifierMetadataInvalid, nil)
	}
	return policy, kind, revision, nil
}

func invalidVerifierResult(diagnostic authdomain.DiagnosticCode, cause error) error {
	return authdomain.NewError(
		authdomain.ReasonVerifierUnavailable,
		diagnostic,
		cause,
	)
}
