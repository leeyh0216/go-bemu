package application

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
)

func TestDisabledPolicyIgnoresAuthorizationAndPublishesAnonymousContext(t *testing.T) {
	var logs bytes.Buffer
	service := newTestService(t, DefaultConfig(), DisabledVerifier{}, &logs)

	authenticated, decision, err := service.Authenticate(
		context.Background(),
		[]string{"Basic raw-disabled-secret", "Bearer another-disabled-secret"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Allowed() || decision.Reason != authdomain.ReasonAuthenticationOff ||
		decision.AuthorizationFields != 0 || decision.TokenDigest != "" {
		t.Fatalf("decision = %#v", decision)
	}
	principal, ok := PrincipalFromContext(authenticated)
	if !ok || principal.CredentialKind() != authdomain.CredentialAnonymous {
		t.Fatalf("principal = %#v, %t", principal, ok)
	}
	storedDecision, ok := DecisionFromContext(authenticated)
	if !ok || storedDecision.PrincipalDigest != principal.Digest() {
		t.Fatalf("context decision = %#v, %t", storedDecision, ok)
	}
	assertSafeDecisionLog(t, logs.String(), "raw-disabled-secret", "another-disabled-secret")
}

func TestPresencePolicyParsesRFC6750AndEmitsDigestOnlyDecision(t *testing.T) {
	var logs bytes.Buffer
	service := newTestService(t, DefaultConfig(), PresenceVerifier{}, &logs)
	rawToken := "local-token-value"
	header := "bEaReR   " + rawToken

	authenticated, decision, err := service.Authenticate(context.Background(), []string{header})
	if err != nil {
		t.Fatal(err)
	}
	principal, ok := PrincipalFromContext(authenticated)
	if !ok || principal.Digest() != authdomain.Digest([]byte(rawToken)) {
		t.Fatalf("principal = %#v, %t", principal, ok)
	}
	if decision.AuthorizationFields != 1 || decision.AuthorizationBytes != len(header) ||
		decision.AuthorizationDigest != authdomain.Digest([]byte(header)) ||
		decision.TokenBytes != len(rawToken) || decision.TokenDigest != authdomain.Digest([]byte(rawToken)) {
		t.Fatalf("decision metadata = %#v", decision)
	}
	assertSafeDecisionLog(t, logs.String(), rawToken, header)
}

func TestBuiltInVerifierRevisionsAreDigestOnly(t *testing.T) {
	for name, verifier := range map[string]interface{ Revision() string }{
		"disabled": DisabledVerifier{},
		"presence": PresenceVerifier{},
	} {
		t.Run(name, func(t *testing.T) {
			if revision := verifier.Revision(); !authdomain.ValidRevision(revision) {
				t.Fatalf("built-in revision is not a digest: %q", revision)
			}
		})
	}
}

func TestAuthenticateRejectsMalformedOrAmbiguousAuthorization(t *testing.T) {
	config := DefaultConfig()
	config.MinTokenBytes = 4
	config.MaxTokenBytes = 12
	config.MaxAuthorizationBytes = 20

	tests := []struct {
		name   string
		values []string
		reason authdomain.Reason
	}{
		{name: "missing", reason: authdomain.ReasonMissingCredential},
		{name: "duplicates", values: []string{"Bearer valid", "Bearer valid"}, reason: authdomain.ReasonMultipleCredentials},
		{name: "unsupported", values: []string{"Basic value"}, reason: authdomain.ReasonUnsupportedScheme},
		{name: "non-ascii-scheme", values: []string{"Beare\u017f value"}, reason: authdomain.ReasonUnsupportedScheme},
		{name: "tab-separator", values: []string{"Bearer\tvalue"}, reason: authdomain.ReasonMalformedCredential},
		{name: "leading-space", values: []string{" Bearer value"}, reason: authdomain.ReasonMalformedCredential},
		{name: "missing-token", values: []string{"Bearer    "}, reason: authdomain.ReasonMalformedCredential},
		{name: "trailing-space", values: []string{"Bearer value "}, reason: authdomain.ReasonMalformedCredential},
		{name: "middle-padding", values: []string{"Bearer va=lue"}, reason: authdomain.ReasonMalformedCredential},
		{name: "invalid-character", values: []string{"Bearer val:ue"}, reason: authdomain.ReasonMalformedCredential},
		{name: "below-minimum", values: []string{"Bearer abc"}, reason: authdomain.ReasonMalformedCredential},
		{name: "above-token-maximum", values: []string{"Bearer abcdefghijklm"}, reason: authdomain.ReasonCredentialTooLarge},
		{name: "above-header-maximum", values: []string{"Bearer abcdefghijklmn"}, reason: authdomain.ReasonCredentialTooLarge},
		{name: "non-utf8", values: []string{"Bearer " + string([]byte{0xff})}, reason: authdomain.ReasonMalformedCredential},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			service := newTestService(t, config, PresenceVerifier{}, &logs)
			resultContext, decision, err := service.Authenticate(context.Background(), test.values)
			if err == nil || authdomain.ReasonOf(err) != test.reason || decision.Reason != test.reason {
				t.Fatalf("err/decision = %v / %#v, want reason %s", err, decision, test.reason)
			}
			if decision.Allowed() {
				t.Fatalf("denial marked allowed: %#v", decision)
			}
			if test.name == "above-header-maximum" && decision.AuthorizationDigest != "" {
				t.Fatalf("oversize Authorization was digested: %#v", decision)
			}
			if _, ok := PrincipalFromContext(resultContext); ok {
				t.Fatal("denied request received an authenticated principal")
			}
			if count := strings.Count(logs.String(), `"event":"auth.decision"`); count != 1 {
				t.Fatalf("decision log count = %d: %s", count, logs.String())
			}
			for _, value := range test.values {
				if value != "" && strings.Contains(logs.String(), value) {
					t.Fatalf("log exposed Authorization value %q: %s", value, logs.String())
				}
			}
		})
	}
}

func TestStaticVerifierContractIsTransportIndependent(t *testing.T) {
	principal, err := authdomain.NewPrincipal(authdomain.CredentialStatic, []byte("static-principal-secret"))
	if err != nil {
		t.Fatal(err)
	}
	verifier := &fakeVerifier{
		policy:    authdomain.PolicyStatic,
		kind:      authdomain.CredentialStatic,
		revision:  authdomain.Digest([]byte("snapshot")),
		principal: principal,
	}
	service := newTestService(t, DefaultConfig(), verifier, nil)

	_, decision, err := service.Authenticate(context.Background(), []string{"Bearer static-token-secret"})
	if err != nil || !decision.Allowed() || verifier.calls != 1 {
		t.Fatalf("decision/error/calls = %#v / %v / %d", decision, err, verifier.calls)
	}
	if verifier.seenDigest != authdomain.Digest([]byte("static-token-secret")) {
		t.Fatalf("verifier token digest = %s", verifier.seenDigest)
	}
}

func TestVerifierFailuresFailClosedWithoutLoggingCausesOrUnsafeMetadata(t *testing.T) {
	secret := "wrapped-verifier-secret"
	tests := []struct {
		name     string
		verifier *fakeVerifier
	}{
		{
			name: "verify-error",
			verifier: &fakeVerifier{
				policy: authdomain.PolicyStatic, kind: authdomain.CredentialStatic,
				revision: authdomain.Digest([]byte("snapshot")),
				err:      authdomain.NewError(authdomain.ReasonInvalidCredential, authdomain.DiagnosticStaticTokenUnknown, errors.New(secret)),
			},
		},
		{
			name: "policy-kind-mismatch",
			verifier: &fakeVerifier{
				policy: authdomain.PolicyStatic, kind: authdomain.CredentialBearerPresent,
				revision: authdomain.Digest([]byte("snapshot")),
			},
		},
		{
			name: "unsafe-revision",
			verifier: &fakeVerifier{
				policy: authdomain.PolicyStatic, kind: authdomain.CredentialStatic,
				revision: secret,
			},
		},
		{
			name: "unsafe-verification-revision",
			verifier: &fakeVerifier{
				policy: authdomain.PolicyStatic, kind: authdomain.CredentialStatic,
				revision:             authdomain.Digest([]byte("snapshot")),
				verificationRevision: secret,
				principal:            mustPrincipal(t, authdomain.CredentialStatic, "principal"),
			},
		},
		{
			name: "wrong-principal-kind",
			verifier: &fakeVerifier{
				policy: authdomain.PolicyStatic, kind: authdomain.CredentialStatic,
				revision:  authdomain.Digest([]byte("snapshot")),
				principal: mustPrincipal(t, authdomain.CredentialBearerPresent, "wrong-kind"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			service := newTestService(t, DefaultConfig(), test.verifier, &logs)
			_, decision, err := service.Authenticate(context.Background(), []string{"Bearer request-token-secret"})
			if err == nil || decision.Allowed() {
				t.Fatalf("unsafe verifier result allowed: %#v / %v", decision, err)
			}
			assertSafeDecisionLog(t, logs.String(), secret, "request-token-secret")
			if !strings.Contains(logs.String(), `"error_operation":`) ||
				!strings.Contains(logs.String(), `"error_shape":`) ||
				!strings.Contains(logs.String(), `"fix_hint":`) {
				t.Fatalf("denial log lacks actionable diagnostics: %s", logs.String())
			}
		})
	}
}

func TestRawVerifierErrorIsNormalizedAtServiceBoundary(t *testing.T) {
	secret := "raw-printable-verifier-error"
	cause := errors.New(secret)
	verifier := &fakeVerifier{
		policy: authdomain.PolicyStatic, kind: authdomain.CredentialStatic,
		revision: authdomain.Digest([]byte("snapshot")), err: cause,
	}
	var logs bytes.Buffer
	service := newTestService(t, DefaultConfig(), verifier, &logs)
	_, decision, err := service.Authenticate(context.Background(), []string{"Bearer request-token-secret"})
	if err == nil || !errors.Is(err, cause) || authdomain.ReasonOf(err) != authdomain.ReasonVerifierUnavailable {
		t.Fatalf("normalized verifier error = %v, decision = %#v", err, decision)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
		t.Fatalf("raw verifier error crossed boundary: error=%v log=%s", err, logs.String())
	}
}

func TestInvalidVerificationRevisionStillPreservesNormalizedCause(t *testing.T) {
	secret := "raw-verifier-and-revision-secret"
	cause := errors.New(secret)
	verifier := &fakeVerifier{
		policy: authdomain.PolicyStatic, kind: authdomain.CredentialStatic,
		revision:             authdomain.Digest([]byte("metadata-snapshot")),
		verificationRevision: secret,
		err:                  cause,
	}
	var logs bytes.Buffer
	service := newTestService(t, DefaultConfig(), verifier, &logs)
	_, _, err := service.Authenticate(context.Background(), []string{"Bearer request-token-secret"})
	if err == nil || !errors.Is(err, cause) {
		t.Fatalf("revision rejection discarded normalized cause: %v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
		t.Fatalf("revision/error secret crossed boundary: error=%v log=%s", err, logs.String())
	}
}

func TestNewValidatesLocalDependenciesAndBounds(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for name, config := range map[string]Config{
		"zero-min":              {MinTokenBytes: 0, MaxTokenBytes: 1, MaxAuthorizationBytes: 8},
		"unordered-token-bound": {MinTokenBytes: 2, MaxTokenBytes: 1, MaxAuthorizationBytes: 8},
		"small-header-bound":    {MinTokenBytes: 2, MaxTokenBytes: 4, MaxAuthorizationBytes: 8},
		"overflowing-bound":     {MinTokenBytes: maxInt, MaxTokenBytes: maxInt, MaxAuthorizationBytes: maxInt},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(config, PresenceVerifier{}, nil); err == nil {
				t.Fatalf("invalid config accepted: %#v", config)
			}
		})
	}
	if _, err := New(DefaultConfig(), nil, nil); err == nil {
		t.Fatal("nil verifier accepted")
	}
}

type fakeVerifier struct {
	policy               authdomain.Policy
	kind                 authdomain.CredentialKind
	revision             string
	verificationRevision string
	principal            authdomain.Principal
	err                  error
	calls                int
	seenDigest           string
}

func (v *fakeVerifier) Policy() authdomain.Policy                 { return v.policy }
func (v *fakeVerifier) CredentialKind() authdomain.CredentialKind { return v.kind }
func (v *fakeVerifier) Revision() string                          { return v.revision }
func (v *fakeVerifier) Verify(_ context.Context, token []byte) (authdomain.Verification, error) {
	v.calls++
	v.seenDigest = authdomain.Digest(token)
	revision := v.verificationRevision
	if revision == "" {
		revision = v.revision
	}
	return authdomain.Verification{Principal: v.principal, VerifierRevision: revision}, v.err
}

func newTestService(t *testing.T, config Config, verifier interface {
	Policy() authdomain.Policy
	CredentialKind() authdomain.CredentialKind
	Verify(context.Context, []byte) (authdomain.Verification, error)
	Revision() string
}, logs *bytes.Buffer) *Service {
	t.Helper()
	var logger *slog.Logger
	if logs != nil {
		logger = slog.New(slog.NewJSONHandler(logs, nil))
	}
	service, err := New(config, verifier, logger)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func mustPrincipal(t *testing.T, kind authdomain.CredentialKind, identity string) authdomain.Principal {
	t.Helper()
	principal, err := authdomain.NewPrincipal(kind, []byte(identity))
	if err != nil {
		t.Fatal(err)
	}
	return principal
}

func assertSafeDecisionLog(t *testing.T, log string, forbidden ...string) {
	t.Helper()
	for _, value := range forbidden {
		if value != "" && strings.Contains(log, value) {
			t.Fatalf("decision log exposed %q: %s", value, log)
		}
	}
	for _, required := range []string{
		`"event":"auth.decision"`, `"model_version":"` + authdomain.ModelVersion + `"`,
		`"policy_result":`, `"reason":`, `"credential_kind":`,
	} {
		if !strings.Contains(log, required) {
			t.Fatalf("decision log missing %q: %s", required, log)
		}
	}
}
