package domain

// DiagnosticCode selects a reviewed, payload-free error description. Callers
// cannot inject operation, shape, or fix_hint text into logs: an unregistered
// code is discarded and represented only by a SHA-256 fingerprint.
//
// Logging guidance requires identifiers and metadata rather than sensitive
// request data:
// https://cloud.google.com/logging/docs/audit/best-practices
type DiagnosticCode string

const (
	DiagnosticUnknown DiagnosticCode = "unknown"

	DiagnosticPrincipalInvalid     DiagnosticCode = "principal.invalid"
	DiagnosticBearerBoundsInvalid  DiagnosticCode = "bearer.bounds.invalid"
	DiagnosticBearerBelowMinimum   DiagnosticCode = "bearer.size.below-minimum"
	DiagnosticBearerAboveMaximum   DiagnosticCode = "bearer.size.above-maximum"
	DiagnosticBearerGrammarInvalid DiagnosticCode = "bearer.grammar.invalid"
	DiagnosticBearerPaddingOnly    DiagnosticCode = "bearer.padding.without-data"

	DiagnosticServiceBoundsInvalid           DiagnosticCode = "service.bounds.invalid"
	DiagnosticServiceVerifierMissing         DiagnosticCode = "service.verifier.missing"
	DiagnosticAuthorizationMissing           DiagnosticCode = "authorization.missing"
	DiagnosticAuthorizationMultiple          DiagnosticCode = "authorization.multiple"
	DiagnosticAuthorizationTooLarge          DiagnosticCode = "authorization.size.above-maximum"
	DiagnosticAuthorizationEncodingInvalid   DiagnosticCode = "authorization.encoding.invalid"
	DiagnosticAuthorizationSeparatorMissing  DiagnosticCode = "authorization.separator.missing"
	DiagnosticAuthorizationSchemeUnsupported DiagnosticCode = "authorization.scheme.unsupported"
	DiagnosticAuthorizationTokenMissing      DiagnosticCode = "authorization.token.missing"
	DiagnosticVerifierMetadataInvalid        DiagnosticCode = "verifier.metadata.invalid"
	DiagnosticVerificationRevisionInvalid    DiagnosticCode = "verification.revision.invalid"
	DiagnosticDisabledPrincipalInvalid       DiagnosticCode = "verification.disabled-principal.invalid"
	DiagnosticVerificationPrincipalInvalid   DiagnosticCode = "verification.principal.invalid"
	DiagnosticVerifierFailure                DiagnosticCode = "verifier.failure"
	DiagnosticPresenceTokenMissing           DiagnosticCode = "presence.token.missing"

	DiagnosticStaticOptionsInvalid       DiagnosticCode = "static.options.invalid"
	DiagnosticStaticSourceMissing        DiagnosticCode = "static.source.missing"
	DiagnosticStaticVerifyContextEnded   DiagnosticCode = "static.verify.context-ended"
	DiagnosticStaticDenyAll              DiagnosticCode = "static.snapshot.deny-all"
	DiagnosticStaticTokenUnknown         DiagnosticCode = "static.token.unknown"
	DiagnosticTokenSourceInvalidBound    DiagnosticCode = "static.source.bound.invalid"
	DiagnosticTokenSourceContextBefore   DiagnosticCode = "static.source.context-before-open"
	DiagnosticTokenSourceOpenFailed      DiagnosticCode = "static.source.open-failed"
	DiagnosticTokenSourceCloseFailed     DiagnosticCode = "static.source.close-failed"
	DiagnosticTokenSourceReadFailed      DiagnosticCode = "static.source.read-failed"
	DiagnosticTokenSourcePayloadTooLarge DiagnosticCode = "static.source.payload-too-large"
	DiagnosticTokenSourceContextAfter    DiagnosticCode = "static.source.context-after-read"
	DiagnosticTokenSourceUnknown         DiagnosticCode = "static.source.unknown-failure"
	DiagnosticTokenSourceContextEnded    DiagnosticCode = "static.source.context-ended"

	DiagnosticManifestPayloadTooLarge   DiagnosticCode = "static.manifest.payload-too-large"
	DiagnosticManifestYAMLDecode        DiagnosticCode = "static.manifest.yaml-decode"
	DiagnosticManifestMultipleDocuments DiagnosticCode = "static.manifest.multiple-documents"
	DiagnosticManifestTrailingYAML      DiagnosticCode = "static.manifest.trailing-yaml"
	DiagnosticManifestIdentityMismatch  DiagnosticCode = "static.manifest.identity-mismatch"
	DiagnosticManifestEmpty             DiagnosticCode = "static.manifest.empty"
	DiagnosticManifestTokenCount        DiagnosticCode = "static.manifest.token-count"
	DiagnosticManifestPrincipalSize     DiagnosticCode = "static.manifest.principal-size"
	DiagnosticManifestPrincipalChars    DiagnosticCode = "static.manifest.principal-characters"
	DiagnosticManifestPrincipalBuild    DiagnosticCode = "static.manifest.principal-build"
	DiagnosticManifestTokenInvalid      DiagnosticCode = "static.manifest.token-invalid"
	DiagnosticManifestTokenDuplicate    DiagnosticCode = "static.manifest.token-duplicate"
)

type diagnosticDefinition struct {
	code        DiagnosticCode
	operation   string
	shape       string
	fixHint     string
	fingerprint string
}

var diagnosticCatalog = map[DiagnosticCode]diagnosticDefinition{
	DiagnosticUnknown: diagnostic("unknown", "unknown", "inspect-authentication-contract"),

	DiagnosticPrincipalInvalid:     diagnostic("construct-principal", "credential-kind+non-empty-identity", "return-a-valid-digest-only-principal"),
	DiagnosticBearerBoundsInvalid:  diagnostic("validate-bearer-token", "invalid-local-bounds", "configure-positive-ordered-token-bounds"),
	DiagnosticBearerBelowMinimum:   diagnostic("validate-bearer-token", "token-below-minimum", "send-a-non-empty-rfc6750-bearer-token"),
	DiagnosticBearerAboveMaximum:   diagnostic("validate-bearer-token", "token-above-maximum", "reduce-the-bearer-token-size"),
	DiagnosticBearerGrammarInvalid: diagnostic("validate-bearer-token", "non-rfc6750-b64token", "send-an-rfc6750-bearer-token"),
	DiagnosticBearerPaddingOnly:    diagnostic("validate-bearer-token", "padding-without-b64token-data", "send-an-rfc6750-bearer-token"),

	DiagnosticServiceBoundsInvalid:           diagnostic("construct-authentication-service", "invalid-local-bounds", "configure-positive-ordered-authorization-bounds"),
	DiagnosticServiceVerifierMissing:         diagnostic("construct-authentication-service", "nil-token-verifier", "configure-an-authentication-verifier"),
	DiagnosticAuthorizationMissing:           diagnostic("parse-authorization", "missing-authorization-field", "send-one-authorization-bearer-field"),
	DiagnosticAuthorizationMultiple:          diagnostic("parse-authorization", "multiple-authorization-fields", "send-exactly-one-authorization-field"),
	DiagnosticAuthorizationTooLarge:          diagnostic("parse-authorization", "authorization-field-above-maximum", "reduce-the-authorization-field-size"),
	DiagnosticAuthorizationEncodingInvalid:   diagnostic("parse-authorization", "non-utf8-authorization-field", "send-one-rfc6750-bearer-credential"),
	DiagnosticAuthorizationSeparatorMissing:  diagnostic("parse-authorization", "missing-bearer-separator", "send-one-rfc6750-bearer-credential"),
	DiagnosticAuthorizationSchemeUnsupported: diagnostic("parse-authorization", "non-bearer-authorization-scheme", "use-the-bearer-authorization-scheme"),
	DiagnosticAuthorizationTokenMissing:      diagnostic("parse-authorization", "missing-bearer-token", "send-one-rfc6750-bearer-credential"),
	DiagnosticVerifierMetadataInvalid:        diagnostic("authenticate", "policy-kind-or-revision", "inspect-the-token-verifier-adapter"),
	DiagnosticVerificationRevisionInvalid:    diagnostic("authenticate", "unsafe-verification-revision", "inspect-the-token-verifier-adapter"),
	DiagnosticDisabledPrincipalInvalid:       diagnostic("authenticate", "disabled-principal", "inspect-the-token-verifier-adapter"),
	DiagnosticVerificationPrincipalInvalid:   diagnostic("authenticate", "principal-kind-or-digest", "inspect-the-token-verifier-adapter"),
	DiagnosticVerifierFailure:                diagnostic("authenticate", "non-auth-verifier-error", "inspect-the-token-verifier-adapter"),
	DiagnosticPresenceTokenMissing:           diagnostic("verify-bearer-presence", "empty-token", "send-a-non-empty-bearer-token"),

	DiagnosticStaticOptionsInvalid:       diagnostic("construct-static-token-verifier", "invalid-local-bounds-or-dependency", "configure-positive-ordered-static-token-bounds-and-clock"),
	DiagnosticStaticSourceMissing:        diagnostic("construct-static-token-verifier", "nil-token-set-source", "configure-a-static-token-set-source"),
	DiagnosticStaticVerifyContextEnded:   diagnostic("verify-static-token", "request-context-ended", "retry-with-an-active-request-context"),
	DiagnosticStaticDenyAll:              diagnostic("verify-static-token", "token-set-deny-all", "repair-the-static-token-set-and-reload"),
	DiagnosticStaticTokenUnknown:         diagnostic("verify-static-token", "unknown-token-digest", "use-a-token-in-the-active-static-token-set"),
	DiagnosticTokenSourceInvalidBound:    diagnostic("read-static-token-set", "invalid-byte-bound", "configure-a-positive-bounded-token-set-source"),
	DiagnosticTokenSourceContextBefore:   diagnostic("read-static-token-set", "context-ended-before-open", "retry-the-token-set-load"),
	DiagnosticTokenSourceOpenFailed:      diagnostic("read-static-token-set", "open-failed", "repair-the-token-set-source-and-reload"),
	DiagnosticTokenSourceCloseFailed:     diagnostic("read-static-token-set", "close-failed", "repair-the-token-set-source-and-reload"),
	DiagnosticTokenSourceReadFailed:      diagnostic("read-static-token-set", "read-failed", "repair-the-token-set-source-and-reload"),
	DiagnosticTokenSourcePayloadTooLarge: diagnostic("read-static-token-set", "payload-above-maximum", "reduce-the-static-token-set-size"),
	DiagnosticTokenSourceContextAfter:    diagnostic("read-static-token-set", "context-ended-after-read", "retry-the-token-set-load"),
	DiagnosticTokenSourceUnknown:         diagnostic("read-static-token-set", "source-read-failed", "repair-the-token-set-source-and-reload"),
	DiagnosticTokenSourceContextEnded:    diagnostic("read-static-token-set", "request-context-ended", "retry-the-token-set-load"),

	DiagnosticManifestPayloadTooLarge:   diagnostic("decode-static-token-set", "payload-above-maximum", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestYAMLDecode:        diagnostic("decode-static-token-set", "yaml-decode-failed", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestMultipleDocuments: diagnostic("decode-static-token-set", "multiple-yaml-documents", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestTrailingYAML:      diagnostic("decode-static-token-set", "trailing-yaml-decode-failed", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestIdentityMismatch:  diagnostic("decode-static-token-set", "api-version-or-kind-mismatch", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestEmpty:             diagnostic("decode-static-token-set", "empty-token-set", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestTokenCount:        diagnostic("decode-static-token-set", "token-count-above-maximum", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestPrincipalSize:     diagnostic("decode-static-token-set", "invalid-principal-size-or-encoding", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestPrincipalChars:    diagnostic("decode-static-token-set", "principal-contains-whitespace-or-control", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestPrincipalBuild:    diagnostic("decode-static-token-set", "principal-construction-failed", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestTokenInvalid:      diagnostic("decode-static-token-set", "invalid-rfc6750-token", "provide-a-bounded-versioned-static-token-set"),
	DiagnosticManifestTokenDuplicate:    diagnostic("decode-static-token-set", "duplicate-token", "provide-a-bounded-versioned-static-token-set"),
}

func diagnostic(operation, shape, fixHint string) diagnosticDefinition {
	return diagnosticDefinition{operation: operation, shape: shape, fixHint: fixHint}
}

func resolveDiagnostic(code DiagnosticCode) diagnosticDefinition {
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
