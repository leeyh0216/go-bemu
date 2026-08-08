package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

const ModelVersion = "auth.bqemu.dev/v1alpha1"

type Policy string

const (
	PolicyDisabled      Policy = "disabled"
	PolicyBearerPresent Policy = "bearer-present"
	PolicyStatic        Policy = "static"
)

func (p Policy) Valid() bool {
	switch p {
	case PolicyDisabled, PolicyBearerPresent, PolicyStatic:
		return true
	default:
		return false
	}
}

type CredentialKind string

const (
	CredentialAnonymous     CredentialKind = "anonymous"
	CredentialBearerPresent CredentialKind = "bearer-present"
	CredentialStatic        CredentialKind = "static"
	CredentialUnknown       CredentialKind = "unknown"
)

func (k CredentialKind) Valid() bool {
	switch k {
	case CredentialAnonymous, CredentialBearerPresent, CredentialStatic:
		return true
	default:
		return false
	}
}

// Principal retains only a digest of the stable local identity. CredentialKind
// is deliberately separate so two credential acquisition paths can represent
// the same principal without producing different principal digests.
type Principal struct {
	credentialKind CredentialKind
	digest         string
}

func NewPrincipal(kind CredentialKind, stableIdentity []byte) (Principal, error) {
	if !kind.Valid() || len(stableIdentity) == 0 {
		return Principal{}, NewError(
			ReasonVerifierUnavailable,
			DiagnosticPrincipalInvalid,
			nil,
		)
	}
	return Principal{credentialKind: kind, digest: Digest(stableIdentity)}, nil
}

func (p Principal) CredentialKind() CredentialKind { return p.credentialKind }
func (p Principal) Digest() string                 { return p.digest }

func (p Principal) Valid() bool {
	return p.credentialKind.Valid() && ValidDigest(p.digest)
}

// Verification binds a principal to the exact immutable verifier revision
// used for the lookup. Returning both values from TokenVerifier.Verify avoids
// attributing a decision to a snapshot published concurrently after lookup.
type Verification struct {
	Principal        Principal
	VerifierRevision string
}

type Result string

const (
	ResultAllow Result = "allow"
	ResultDeny  Result = "deny"
)

func (r Result) Valid() bool { return r == ResultAllow || r == ResultDeny }

type Reason string

const (
	ReasonAllowed               Reason = "allowed"
	ReasonAuthenticationOff     Reason = "authentication-disabled"
	ReasonMissingCredential     Reason = "missing-credential"
	ReasonMultipleCredentials   Reason = "multiple-credentials"
	ReasonMalformedCredential   Reason = "malformed-credential"
	ReasonUnsupportedScheme     Reason = "unsupported-scheme"
	ReasonCredentialTooLarge    Reason = "credential-too-large"
	ReasonInvalidCredential     Reason = "invalid-credential"
	ReasonVerifierUnavailable   Reason = "verifier-unavailable"
	ReasonInvalidTokenSet       Reason = "invalid-token-set"
	ReasonTokenSetSourceFailure Reason = "token-set-source-failure"
)

func (r Reason) Valid() bool {
	switch r {
	case ReasonAllowed, ReasonAuthenticationOff, ReasonMissingCredential,
		ReasonMultipleCredentials, ReasonMalformedCredential, ReasonUnsupportedScheme,
		ReasonCredentialTooLarge, ReasonInvalidCredential, ReasonVerifierUnavailable,
		ReasonInvalidTokenSet, ReasonTokenSetSourceFailure:
		return true
	default:
		return false
	}
}

// Decision is safe to attach to structured logs and request contexts. It never
// contains an Authorization value, a token, or the source principal string.
type Decision struct {
	Policy              Policy
	Result              Result
	Reason              Reason
	CredentialKind      CredentialKind
	PrincipalDigest     string
	AuthorizationFields int
	AuthorizationBytes  int
	AuthorizationDigest string
	TokenDigest         string
	TokenBytes          int
	VerifierRevision    string
}

func (d Decision) Allowed() bool { return d.Result == ResultAllow }

func (d Decision) SafeLogAttrs() []any {
	policy := d.Policy
	if !policy.Valid() {
		policy = ""
	}
	result := d.Result
	if !result.Valid() {
		result = ResultDeny
	}
	reason := d.Reason
	if !reason.Valid() {
		reason = ReasonVerifierUnavailable
	}
	kind := d.CredentialKind
	if !kind.Valid() {
		kind = CredentialUnknown
	}
	principalDigest := validDigestOr(d.PrincipalDigest, "none")
	authorizationDigest := validDigestOr(d.AuthorizationDigest, "none")
	tokenDigest := validDigestOr(d.TokenDigest, "none")
	revision := d.VerifierRevision
	if !ValidRevision(revision) {
		revision = "invalid"
	}
	return []any{
		"model_version", ModelVersion,
		"policy", emptyAs(string(policy), "unknown"),
		"policy_result", string(result),
		"reason", string(reason),
		"credential_kind", string(kind),
		"principal_digest", principalDigest,
		"authorization_fields", nonNegative(d.AuthorizationFields),
		"authorization_bytes", nonNegative(d.AuthorizationBytes),
		"authorization_digest", authorizationDigest,
		"token_shape", "opaque_bearer",
		"token_bytes", nonNegative(d.TokenBytes),
		"token_digest", tokenDigest,
		"verifier_revision", revision,
	}
}

// AuthError intentionally omits the wrapped error message. Parser, file, and
// decoder errors can contain credentials or paths; callers may inspect Cause
// with errors.Is/As, while logs and user-facing errors retain stable safe fields.
type AuthError struct {
	reason     Reason
	diagnostic diagnosticDefinition
	cause      error
}

func NewError(reason Reason, diagnosticCode DiagnosticCode, cause error) *AuthError {
	if !reason.Valid() {
		reason = ReasonVerifierUnavailable
	}
	return &AuthError{
		reason: reason, diagnostic: resolveDiagnostic(diagnosticCode), cause: cause,
	}
}

func (e *AuthError) Error() string {
	diagnostic := safeErrorDiagnostic(e)
	return fmt.Sprintf(
		"authentication failure: model_version=%s reason=%s diagnostic=%s diagnostic_fingerprint=%s operation=%s shape=%s fix_hint=%s",
		ModelVersion,
		safeErrorReason(e),
		diagnostic.code,
		diagnostic.fingerprint,
		diagnostic.operation,
		diagnostic.shape,
		diagnostic.fixHint,
	)
}

func (e *AuthError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func ReasonOf(err error) Reason {
	var authError *AuthError
	if errors.As(err, &authError) {
		return safeErrorReason(authError)
	}
	return ReasonVerifierUnavailable
}

// SafeErrorAttrs exposes only the stable diagnostic contract owned by this
// package. In particular, it never emits the wrapped decoder, filesystem, or
// verifier error text.
func SafeErrorAttrs(err error) []any {
	var authError *AuthError
	if !errors.As(err, &authError) {
		diagnostic := resolveDiagnostic(DiagnosticUnknown)
		return []any{
			"error_reason", ReasonVerifierUnavailable,
			"error_diagnostic", diagnostic.code,
			"diagnostic_fingerprint", diagnostic.fingerprint,
			"error_operation", diagnostic.operation,
			"error_shape", diagnostic.shape,
			"fix_hint", diagnostic.fixHint,
			"error_digest", Digest([]byte("unclassified-authentication-error")),
		}
	}
	diagnostic := safeErrorDiagnostic(authError)
	return []any{
		"error_reason", safeErrorReason(authError),
		"error_diagnostic", diagnostic.code,
		"diagnostic_fingerprint", diagnostic.fingerprint,
		"error_operation", diagnostic.operation,
		"error_shape", diagnostic.shape,
		"fix_hint", diagnostic.fixHint,
		"error_digest", Digest([]byte(authError.Error())),
	}
}

// NormalizeError preserves already-safe direct AuthError values. Every other
// error is wrapped so its Error text can never cross the boundary, while
// errors.Is/errors.As can still inspect the original cause through Unwrap.
// https://pkg.go.dev/errors#Is
func NormalizeError(err error, reason Reason, fallback DiagnosticCode) error {
	if err == nil {
		return nil
	}
	if _, safe := err.(*AuthError); safe {
		return err
	}
	return NewError(reason, fallback, err)
}

func Digest(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ValidDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || value[:len("sha256:")] != "sha256:" {
		return false
	}
	_, err := hex.DecodeString(value[len("sha256:"):])
	return err == nil
}

// Revisions cross an adapter-to-log trust boundary, so only canonical SHA-256
// digests are accepted. Labels belong in code; snapshots and adapter-provided
// values are represented opaquely.
// https://pkg.go.dev/crypto/sha256
func ValidRevision(value string) bool {
	return ValidDigest(value)
}

// ValidateBearerToken applies the b64token grammar from RFC 6750 section 2.1.
// Padding is permitted only as a trailing run of '=' characters. Callers own
// the independently configurable byte bounds.
// https://www.rfc-editor.org/rfc/rfc6750.html#section-2.1
func ValidateBearerToken(token []byte, minBytes, maxBytes int) error {
	if minBytes < 1 || maxBytes < minBytes {
		return NewError(
			ReasonVerifierUnavailable,
			DiagnosticBearerBoundsInvalid,
			nil,
		)
	}
	if len(token) < minBytes {
		return NewError(
			ReasonMalformedCredential,
			DiagnosticBearerBelowMinimum,
			nil,
		)
	}
	if len(token) > maxBytes {
		return NewError(
			ReasonCredentialTooLarge,
			DiagnosticBearerAboveMaximum,
			nil,
		)
	}
	padding := false
	dataCharacters := 0
	for _, character := range token {
		if character == '=' {
			padding = true
			continue
		}
		if padding || !isBearerCharacter(character) {
			return NewError(
				ReasonMalformedCredential,
				DiagnosticBearerGrammarInvalid,
				nil,
			)
		}
		dataCharacters++
	}
	if dataCharacters == 0 {
		return NewError(
			ReasonMalformedCredential,
			DiagnosticBearerPaddingOnly,
			nil,
		)
	}
	return nil
}

func isBearerCharacter(character byte) bool {
	return (character >= 'a' && character <= 'z') ||
		(character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9') ||
		character == '-' || character == '.' || character == '_' || character == '~' ||
		character == '+' || character == '/'
}

func emptyAs(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func validDigestOr(value, fallback string) string {
	if !ValidDigest(value) {
		return fallback
	}
	return value
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func safeErrorReason(err *AuthError) Reason {
	if err == nil || !err.reason.Valid() {
		return ReasonVerifierUnavailable
	}
	return err.reason
}

func safeErrorDiagnostic(err *AuthError) diagnosticDefinition {
	if err == nil || err.diagnostic.code == "" {
		return resolveDiagnostic(DiagnosticUnknown)
	}
	return err.diagnostic
}
