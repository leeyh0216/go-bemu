package domain

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestPrincipalAndDecisionExposeDigestsOnly(t *testing.T) {
	identity := []byte("local-user@example.invalid")
	principal, err := NewPrincipal(CredentialStatic, identity)
	if err != nil {
		t.Fatal(err)
	}
	if !principal.Valid() || principal.Digest() != Digest(identity) {
		t.Fatalf("principal = %#v", principal)
	}
	decision := Decision{
		Policy: PolicyStatic, Result: ResultAllow, Reason: ReasonAllowed,
		CredentialKind: CredentialStatic, PrincipalDigest: principal.Digest(),
		TokenDigest: Digest([]byte("secret-token-value")), TokenBytes: 18,
	}
	formatted := fmt.Sprint(decision.SafeLogAttrs())
	if strings.Contains(formatted, string(identity)) || strings.Contains(formatted, "secret-token-value") {
		t.Fatalf("safe attributes exposed an identity or token: %s", formatted)
	}
	for _, expected := range []string{"policy_result allow", "principal_digest sha256:", "token_digest sha256:"} {
		if !strings.Contains(formatted, expected) {
			t.Fatalf("safe attributes missing %q: %s", expected, formatted)
		}
	}
}

func TestSafeLogContractsSanitizeInvalidCallerValues(t *testing.T) {
	secret := "raw-secret-value"
	decision := Decision{
		Policy: Policy(secret), Result: Result(secret), Reason: Reason(secret),
		CredentialKind: CredentialKind(secret), PrincipalDigest: secret,
		AuthorizationFields: -1, AuthorizationBytes: -2, AuthorizationDigest: secret,
		TokenBytes: -3, TokenDigest: secret, VerifierRevision: secret,
	}
	formatted := fmt.Sprint(decision.SafeLogAttrs())
	if strings.Contains(formatted, secret) {
		t.Fatalf("safe decision attributes exposed invalid caller value: %s", formatted)
	}

	cause := errors.New(secret)
	err := NewError(Reason(secret), DiagnosticCode(secret), cause)
	formatted = err.Error() + fmt.Sprint(SafeErrorAttrs(err))
	if strings.Contains(formatted, secret) {
		t.Fatalf("safe error contract exposed invalid caller value: %s", formatted)
	}
	if !strings.Contains(formatted, Digest([]byte(secret))) {
		t.Fatalf("unknown diagnostic lacks digest-only fallback: %s", formatted)
	}
	if !errors.Is(err, cause) {
		t.Fatal("unknown diagnostic discarded the inspectable cause")
	}
}

func TestValidateBearerTokenUsesRFC6750GrammarAndBounds(t *testing.T) {
	for _, token := range []string{"abc", "a-b.c_d~e+f/g", "abc==", "header.payload.signature"} {
		if err := ValidateBearerToken([]byte(token), 1, 128); err != nil {
			t.Errorf("valid token %q rejected: %v", token, err)
		}
	}
	for name, token := range map[string]string{
		"empty": "", "space": "abc def", "tab": "abc\tdef", "padding-middle": "ab=c", "padding-only": "===", "colon": "abc:def",
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateBearerToken([]byte(token), 1, 128); err == nil {
				t.Fatalf("invalid token %q accepted", token)
			}
		})
	}
	if reason := ReasonOf(ValidateBearerToken([]byte("too-long"), 1, 4)); reason != ReasonCredentialTooLarge {
		t.Fatalf("large token reason = %s", reason)
	}
	if reason := ReasonOf(ValidateBearerToken([]byte("short"), 8, 16)); reason != ReasonMalformedCredential {
		t.Fatalf("short token reason = %s", reason)
	}
}

func TestAuthErrorOmitsWrappedSecretAndRemainsInspectable(t *testing.T) {
	cause := errors.New("decoder exposed secret-token-value")
	err := NewError(ReasonInvalidTokenSet, DiagnosticManifestYAMLDecode, cause)
	if strings.Contains(err.Error(), "secret-token-value") {
		t.Fatalf("error exposed wrapped cause: %s", err)
	}
	if !errors.Is(err, cause) || ReasonOf(err) != ReasonInvalidTokenSet {
		t.Fatalf("wrapped error contract failed: %v", err)
	}
	formattedAttrs := fmt.Sprint(SafeErrorAttrs(err))
	if strings.Contains(formattedAttrs, "secret-token-value") {
		t.Fatalf("safe error attributes exposed wrapped cause: %s", formattedAttrs)
	}
	for _, expected := range []string{
		"error_reason invalid-token-set", "error_diagnostic static.manifest.yaml-decode",
		"diagnostic_fingerprint sha256:", "error_operation decode-static-token-set",
		"error_shape yaml-decode-failed", "fix_hint provide-a-bounded-versioned-static-token-set",
		"error_digest sha256:",
	} {
		if !strings.Contains(formattedAttrs, expected) {
			t.Fatalf("safe error attributes missing %q: %s", expected, formattedAttrs)
		}
	}
}

func TestNormalizeErrorHidesRawVerifierTextAndPreservesCause(t *testing.T) {
	secret := "printable-raw-verifier-secret"
	cause := errors.New(secret)
	normalized := NormalizeError(cause, ReasonVerifierUnavailable, DiagnosticVerifierFailure)
	if !errors.Is(normalized, cause) {
		t.Fatal("normalized error did not preserve errors.Is")
	}
	formatted := normalized.Error() + fmt.Sprint(SafeErrorAttrs(normalized))
	if strings.Contains(formatted, secret) {
		t.Fatalf("normalized error exposed raw verifier text: %s", formatted)
	}

	safe := NewError(ReasonInvalidCredential, DiagnosticStaticTokenUnknown, cause)
	if got := NormalizeError(safe, ReasonVerifierUnavailable, DiagnosticVerifierFailure); got != safe {
		t.Fatal("direct AuthError was unnecessarily replaced")
	}
}

func TestRevisionMustBeCanonicalDigest(t *testing.T) {
	if !ValidRevision(Digest([]byte("snapshot"))) {
		t.Fatal("canonical digest revision rejected")
	}
	for _, revision := range []string{"snapshot-v1", "printable-secret-token", "sha256:not-hex", ""} {
		if ValidRevision(revision) {
			t.Fatalf("non-digest revision accepted: %q", revision)
		}
	}
}

func TestDiagnosticCatalogIsCompleteAndStable(t *testing.T) {
	const expectedDiagnostics = 48
	if len(diagnosticCatalog) != expectedDiagnostics {
		t.Fatalf("diagnostic catalog size = %d, want %d", len(diagnosticCatalog), expectedDiagnostics)
	}
	for code := range diagnosticCatalog {
		definition := resolveDiagnostic(code)
		if definition.code != code || definition.operation == "" || definition.shape == "" ||
			definition.fixHint == "" || !ValidDigest(definition.fingerprint) {
			t.Fatalf("invalid diagnostic definition for %q: %#v", code, definition)
		}
	}
}
