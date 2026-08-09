package domain

import (
	"errors"
	"fmt"
)

const ModelVersion = "auth-issuer.bqemu.dev/v1alpha1"

// OAuthErrorCode is the stable wire-level error vocabulary defined by OAuth
// and RFC 8693. Diagnostics remain a separate local, payload-free contract.
type OAuthErrorCode string

const (
	ErrorInvalidRequest       OAuthErrorCode = "invalid_request"
	ErrorInvalidGrant         OAuthErrorCode = "invalid_grant"
	ErrorUnsupportedGrantType OAuthErrorCode = "unsupported_grant_type"
	ErrorInvalidTarget        OAuthErrorCode = "invalid_target"
	ErrorServer               OAuthErrorCode = "server_error"
)

func (c OAuthErrorCode) Valid() bool {
	switch c {
	case ErrorInvalidRequest, ErrorInvalidGrant, ErrorUnsupportedGrantType,
		ErrorInvalidTarget, ErrorServer:
		return true
	default:
		return false
	}
}

// Diagnostic selects reviewed error metadata. Callers cannot inject request,
// credential, key identifier, filesystem, or dependency text into Error() or
// SafeErrorAttrs.
type Diagnostic string

const (
	DiagnosticUnknown Diagnostic = "unknown"

	DiagnosticConfigInvalid       Diagnostic = "issuer.config.invalid"
	DiagnosticDependencyMissing   Diagnostic = "issuer.dependency.missing"
	DiagnosticContextEnded        Diagnostic = "issuer.context.ended"
	DiagnosticContentTypeMissing  Diagnostic = "form.content-type.missing"
	DiagnosticContentTypeInvalid  Diagnostic = "form.content-type.invalid"
	DiagnosticContentTypeRejected Diagnostic = "form.content-type.rejected"
	DiagnosticBodyEmpty           Diagnostic = "form.body.empty"
	DiagnosticBodyTooLarge        Diagnostic = "form.body.too-large"
	DiagnosticBodyEncoding        Diagnostic = "form.body.encoding.invalid"
	DiagnosticFormMalformed       Diagnostic = "form.encoding.invalid"
	DiagnosticFieldCount          Diagnostic = "form.field-count.invalid"
	DiagnosticFieldName           Diagnostic = "form.field-name.invalid"
	DiagnosticFieldDuplicate      Diagnostic = "form.field.duplicate"
	DiagnosticFieldUnknown        Diagnostic = "form.field.unknown"
	DiagnosticFieldMissing        Diagnostic = "form.field.missing"
	DiagnosticFieldValue          Diagnostic = "form.field-value.invalid"
	DiagnosticGrantUnsupported    Diagnostic = "grant.unsupported"
	DiagnosticVerifierFailure     Diagnostic = "grant.verifier.failure"
	DiagnosticRefreshRejected     Diagnostic = "refresh.credential.rejected"
	DiagnosticJWTMalformed        Diagnostic = "jwt.assertion.malformed"
	DiagnosticJWTHeader           Diagnostic = "jwt.header.invalid"
	DiagnosticJWTClaims           Diagnostic = "jwt.claims.invalid"
	DiagnosticJWTAlgorithm        Diagnostic = "jwt.algorithm.unsupported"
	DiagnosticJWTSignature        Diagnostic = "jwt.signature.rejected"
	DiagnosticJWTAudience         Diagnostic = "jwt.audience.rejected"
	DiagnosticJWTTime             Diagnostic = "jwt.time.rejected"
	DiagnosticJWTScope            Diagnostic = "jwt.scope.invalid"
	DiagnosticJWTReplay           Diagnostic = "jwt.assertion.replayed"
	DiagnosticSTSRequest          Diagnostic = "sts.request.invalid"
	DiagnosticSTSRequestedType    Diagnostic = "sts.requested-token-type.unsupported"
	DiagnosticSTSSubjectRejected  Diagnostic = "sts.subject.rejected"
	DiagnosticSTSActorRejected    Diagnostic = "sts.actor.rejected"
	DiagnosticSTSTargetRejected   Diagnostic = "sts.target.rejected"
	DiagnosticSubjectInvalid      Diagnostic = "issuer.subject.invalid"
	DiagnosticEntropyFailure      Diagnostic = "issuer.entropy.failure"
	DiagnosticTokenCollision      Diagnostic = "issuer.token.collision"
	DiagnosticIssueAttempts       Diagnostic = "issuer.attempts.exhausted"
	DiagnosticStoreFailure        Diagnostic = "issuer.store.failure"
	DiagnosticStoreCapacity       Diagnostic = "issuer.store.capacity"
	DiagnosticStoreRecord         Diagnostic = "issuer.store.record.invalid"
	DiagnosticStoreConfig         Diagnostic = "issuer.store.config.invalid"
	DiagnosticFixtureConfig       Diagnostic = "issuer.fixture.config.invalid"
	DiagnosticFixtureKeyUnknown   Diagnostic = "issuer.fixture.key.unknown"
)

type diagnosticDefinition struct {
	code        Diagnostic
	operation   string
	shape       string
	fixHint     string
	fingerprint string
}

var diagnosticCatalog = map[Diagnostic]diagnosticDefinition{
	DiagnosticUnknown:             diagnostic("issue-token", "unknown", "inspect-issuer-contract"),
	DiagnosticConfigInvalid:       diagnostic("construct-issuer", "invalid-local-bounds", "configure-positive-ordered-issuer-bounds"),
	DiagnosticDependencyMissing:   diagnostic("construct-issuer", "nil-required-port", "configure-all-required-issuer-ports"),
	DiagnosticContextEnded:        diagnostic("issue-token", "operation-context-ended", "retry-with-an-active-context"),
	DiagnosticContentTypeMissing:  diagnostic("parse-token-form", "missing-content-type", "send-application-x-www-form-urlencoded"),
	DiagnosticContentTypeInvalid:  diagnostic("parse-token-form", "malformed-content-type", "send-application-x-www-form-urlencoded"),
	DiagnosticContentTypeRejected: diagnostic("parse-token-form", "unsupported-content-type", "send-application-x-www-form-urlencoded-utf8"),
	DiagnosticBodyEmpty:           diagnostic("parse-token-form", "empty-body", "send-a-bounded-token-form"),
	DiagnosticBodyTooLarge:        diagnostic("parse-token-form", "body-above-maximum", "reduce-the-token-form-size"),
	DiagnosticBodyEncoding:        diagnostic("parse-token-form", "non-utf8-body", "send-a-utf8-token-form"),
	DiagnosticFormMalformed:       diagnostic("parse-token-form", "malformed-form-encoding", "send-rfc6749-form-encoding"),
	DiagnosticFieldCount:          diagnostic("parse-token-form", "field-count-above-maximum", "reduce-the-token-form-fields"),
	DiagnosticFieldName:           diagnostic("parse-token-form", "invalid-field-name", "send-only-documented-token-fields"),
	DiagnosticFieldDuplicate:      diagnostic("parse-token-form", "duplicate-field", "send-each-token-field-once"),
	DiagnosticFieldUnknown:        diagnostic("parse-token-form", "unknown-field", "send-only-documented-token-fields"),
	DiagnosticFieldMissing:        diagnostic("parse-token-form", "required-field-missing", "send-all-required-token-fields"),
	DiagnosticFieldValue:          diagnostic("parse-token-form", "invalid-or-oversize-field-value", "send-bounded-token-field-values"),
	DiagnosticGrantUnsupported:    diagnostic("select-grant", "unsupported-grant-type", "use-refresh-jwt-bearer-or-token-exchange"),
	DiagnosticVerifierFailure:     diagnostic("verify-grant", "unclassified-verifier-failure", "inspect-the-credential-verifier-adapter"),
	DiagnosticRefreshRejected:     diagnostic("verify-refresh-grant", "credential-not-registered", "use-a-registered-local-refresh-credential"),
	DiagnosticJWTMalformed:        diagnostic("parse-jwt-assertion", "non-compact-jws", "send-a-bounded-three-segment-jwt"),
	DiagnosticJWTHeader:           diagnostic("parse-jwt-assertion", "invalid-protected-header", "send-a-strict-rs256-jwt-header"),
	DiagnosticJWTClaims:           diagnostic("parse-jwt-assertion", "invalid-claims", "send-required-bounded-service-account-claims"),
	DiagnosticJWTAlgorithm:        diagnostic("verify-jwt-assertion", "algorithm-not-rs256", "sign-the-fixture-assertion-with-rs256"),
	DiagnosticJWTSignature:        diagnostic("verify-jwt-assertion", "signature-rejected", "use-a-registered-fixture-public-key"),
	DiagnosticJWTAudience:         diagnostic("verify-jwt-assertion", "audience-not-allowed", "configure-and-send-an-allowed-token-audience"),
	DiagnosticJWTTime:             diagnostic("verify-jwt-assertion", "expired-future-or-overlong-assertion", "send-a-current-bounded-lifetime-assertion"),
	DiagnosticJWTScope:            diagnostic("verify-jwt-assertion", "invalid-scope-set", "send-bounded-rfc6749-scope-tokens"),
	DiagnosticJWTReplay:           diagnostic("commit-issued-token", "assertion-already-consumed", "sign-a-new-service-account-assertion"),
	DiagnosticSTSRequest:          diagnostic("parse-sts-form", "invalid-rfc8693-request", "send-a-complete-rfc8693-token-exchange-form"),
	DiagnosticSTSRequestedType:    diagnostic("parse-sts-form", "requested-type-not-access-token", "request-an-oauth-access-token"),
	DiagnosticSTSSubjectRejected:  diagnostic("verify-sts-subject", "subject-token-not-registered", "use-a-registered-local-subject-token"),
	DiagnosticSTSActorRejected:    diagnostic("verify-sts-subject", "actor-token-not-registered", "use-a-matching-registered-local-actor-token"),
	DiagnosticSTSTargetRejected:   diagnostic("verify-sts-subject", "audience-resource-or-scope-rejected", "use-the-registered-local-target-and-scopes"),
	DiagnosticSubjectInvalid:      diagnostic("issue-token", "invalid-digest-only-subject", "return-a-valid-subject-from-the-verifier"),
	DiagnosticEntropyFailure:      diagnostic("generate-access-token", "entropy-source-failed", "inspect-the-entropy-adapter"),
	DiagnosticTokenCollision:      diagnostic("commit-issued-token", "opaque-token-digest-collision", "retry-with-fresh-cryptographic-entropy"),
	DiagnosticIssueAttempts:       diagnostic("commit-issued-token", "collision-retry-limit-reached", "increase-entropy-or-inspect-the-entropy-adapter"),
	DiagnosticStoreFailure:        diagnostic("commit-issued-token", "unclassified-store-failure", "inspect-the-issued-token-store-adapter"),
	DiagnosticStoreCapacity:       diagnostic("commit-issued-token", "bounded-store-capacity-reached", "increase-capacity-or-wait-for-expiry"),
	DiagnosticStoreRecord:         diagnostic("commit-issued-token", "invalid-digest-only-record", "inspect-the-issuer-store-contract"),
	DiagnosticStoreConfig:         diagnostic("construct-issued-token-store", "invalid-local-bounds", "configure-positive-store-capacities"),
	DiagnosticFixtureConfig:       diagnostic("construct-fixture-verifier", "invalid-or-duplicate-fixture", "configure-bounded-unique-fixture-credentials-and-keys"),
	DiagnosticFixtureKeyUnknown:   diagnostic("verify-jwt-signature", "issuer-or-key-id-not-registered", "register-the-fixture-public-key"),
}

func diagnostic(operation, shape, fixHint string) diagnosticDefinition {
	return diagnosticDefinition{operation: operation, shape: shape, fixHint: fixHint}
}

func resolveDiagnostic(code Diagnostic) diagnosticDefinition {
	if definition, ok := diagnosticCatalog[code]; ok {
		definition.code = code
		definition.fingerprint = Digest([]byte(code))
		return definition
	}
	definition := diagnosticCatalog[DiagnosticUnknown]
	definition.code = DiagnosticUnknown
	definition.fingerprint = Digest([]byte(code))
	return definition
}

// Error deliberately omits the wrapped cause. URL decoders, crypto providers,
// stores, and fixture readers may include credential material in their errors.
type Error struct {
	code       OAuthErrorCode
	diagnostic diagnosticDefinition
	cause      error
}

func NewError(code OAuthErrorCode, diagnosticCode Diagnostic, cause error) *Error {
	if !code.Valid() {
		code = ErrorServer
	}
	return &Error{code: code, diagnostic: resolveDiagnostic(diagnosticCode), cause: cause}
}

func (e *Error) Error() string {
	definition := safeDefinition(e)
	return fmt.Sprintf(
		"token issuer failure: model_version=%s oauth_error=%s diagnostic=%s diagnostic_fingerprint=%s operation=%s shape=%s fix_hint=%s",
		ModelVersion, safeCode(e), definition.code, definition.fingerprint,
		definition.operation, definition.shape, definition.fixHint,
	)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func CodeOf(err error) OAuthErrorCode {
	var issuerError *Error
	if errors.As(err, &issuerError) {
		return safeCode(issuerError)
	}
	return ErrorServer
}

func DiagnosticOf(err error) Diagnostic {
	var issuerError *Error
	if errors.As(err, &issuerError) {
		return safeDefinition(issuerError).code
	}
	return DiagnosticUnknown
}

func NormalizeError(err error, code OAuthErrorCode, fallback Diagnostic) error {
	if err == nil {
		return nil
	}
	if _, safe := err.(*Error); safe {
		return err
	}
	return NewError(code, fallback, err)
}

func SafeErrorAttrs(err error) []any {
	var issuerError *Error
	if !errors.As(err, &issuerError) {
		definition := resolveDiagnostic(DiagnosticUnknown)
		return []any{
			"oauth_error", ErrorServer,
			"error_diagnostic", definition.code,
			"diagnostic_fingerprint", definition.fingerprint,
			"error_operation", definition.operation,
			"error_shape", definition.shape,
			"fix_hint", definition.fixHint,
			"error_digest", Digest([]byte("unclassified-token-issuer-error")),
		}
	}
	definition := safeDefinition(issuerError)
	return []any{
		"oauth_error", safeCode(issuerError),
		"error_diagnostic", definition.code,
		"diagnostic_fingerprint", definition.fingerprint,
		"error_operation", definition.operation,
		"error_shape", definition.shape,
		"fix_hint", definition.fixHint,
		"error_digest", Digest([]byte(issuerError.Error())),
	}
}

func safeCode(err *Error) OAuthErrorCode {
	if err != nil && err.code.Valid() {
		return err.code
	}
	return ErrorServer
}

func safeDefinition(err *Error) diagnosticDefinition {
	if err == nil {
		return resolveDiagnostic(DiagnosticUnknown)
	}
	return resolveDiagnostic(err.diagnostic.code)
}

var (
	ErrTokenCollision = errors.New("issued token digest collision")
	ErrReplay         = errors.New("grant replay rejected")
	ErrStoreCapacity  = errors.New("issued token store capacity reached")
)
