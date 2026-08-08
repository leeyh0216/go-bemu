package static

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
)

func TestVerifierLoadsDigestOnlySnapshotAndVerifiesTokens(t *testing.T) {
	principal := "local-principal-secret"
	token := "local-token-secret"
	source := &memorySource{payload: tokenSetYAML(tokenEntry{Principal: principal, Token: token})}
	verifier := newTestVerifier(t, source, testOptions(nil))

	matched, err := verifier.Verify(context.Background(), []byte(token))
	if err != nil {
		t.Fatal(err)
	}
	if matched.Principal.CredentialKind() != authdomain.CredentialStatic ||
		matched.Principal.Digest() != authdomain.Digest([]byte(principal)) ||
		matched.VerifierRevision != verifier.Revision() {
		t.Fatalf("principal = %#v", matched)
	}
	if _, err := verifier.Verify(context.Background(), []byte("unknown-token")); err == nil || authdomain.ReasonOf(err) != authdomain.ReasonInvalidCredential {
		t.Fatalf("unknown token error = %v", err)
	}

	metadata := verifier.SnapshotMetadata()
	if metadata.State != SnapshotActive || metadata.TokenCount != 1 ||
		!authdomain.ValidDigest(metadata.Revision) || metadata.LoadAttempt != 1 {
		t.Fatalf("metadata = %#v", metadata)
	}
	formattedSnapshot := fmt.Sprintf("%#v", verifier.snapshot.Load())
	if strings.Contains(formattedSnapshot, principal) || strings.Contains(formattedSnapshot, token) {
		t.Fatalf("snapshot retained raw credential material: %s", formattedSnapshot)
	}
	if !allZero(source.lastPayload()) {
		t.Fatal("verifier did not clear the source payload after decoding")
	}
}

func TestReloadPublishesNewSnapshotFailsClosedAndRecovers(t *testing.T) {
	source := &memorySource{payload: tokenSetYAML(tokenEntry{Principal: "principal-a", Token: "token-value-a"})}
	verifier := newTestVerifier(t, source, testOptions(nil))
	assertTokenResult(t, verifier, "token-value-a", true, authdomain.ReasonAllowed)

	source.set(tokenSetYAML(tokenEntry{Principal: "principal-b", Token: "token-value-b"}), nil)
	if err := verifier.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTokenResult(t, verifier, "token-value-a", false, authdomain.ReasonInvalidCredential)
	assertTokenResult(t, verifier, "token-value-b", true, authdomain.ReasonAllowed)
	if metadata := verifier.SnapshotMetadata(); metadata.State != SnapshotActive || metadata.LoadAttempt != 2 {
		t.Fatalf("metadata after valid reload = %#v", metadata)
	}

	source.set([]byte("apiVersion: wrong\nkind: StaticTokenSet\ntokens: []\n"), nil)
	if err := verifier.Reload(context.Background()); err == nil ||
		authdomain.ReasonOf(err) != authdomain.ReasonInvalidTokenSet {
		t.Fatalf("invalid reload error = %v", err)
	}
	assertTokenResult(t, verifier, "token-value-a", false, authdomain.ReasonVerifierUnavailable)
	assertTokenResult(t, verifier, "token-value-b", false, authdomain.ReasonVerifierUnavailable)
	if metadata := verifier.SnapshotMetadata(); metadata.State != SnapshotDenyAll ||
		metadata.TokenCount != 0 || metadata.LoadAttempt != 3 {
		t.Fatalf("metadata after invalid reload = %#v", metadata)
	}

	source.set(tokenSetYAML(tokenEntry{Principal: "principal-c", Token: "token-value-c"}), nil)
	if err := verifier.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTokenResult(t, verifier, "token-value-b", false, authdomain.ReasonInvalidCredential)
	assertTokenResult(t, verifier, "token-value-c", true, authdomain.ReasonAllowed)
	if metadata := verifier.SnapshotMetadata(); metadata.State != SnapshotActive || metadata.LoadAttempt != 4 {
		t.Fatalf("metadata after recovery = %#v", metadata)
	}
}

func TestInvalidStartupIsFatal(t *testing.T) {
	tests := map[string][]byte{
		"empty":        nil,
		"malformed":    []byte("apiVersion: ["),
		"empty-tokens": []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens: []\n"),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if verifier, err := New(context.Background(), &memorySource{payload: payload}, testOptions(nil)); err == nil || verifier != nil || authdomain.ReasonOf(err) != authdomain.ReasonInvalidTokenSet {
				t.Fatalf("verifier/error = %#v / %v", verifier, err)
			}
		})
	}

	sourceFailure := errors.New("source cause contains raw-token-secret")
	if verifier, err := New(
		context.Background(),
		&memorySource{err: sourceFailure},
		testOptions(nil),
	); err == nil || verifier != nil || !errors.Is(err, sourceFailure) ||
		authdomain.ReasonOf(err) != authdomain.ReasonTokenSetSourceFailure || strings.Contains(err.Error(), "raw-token-secret") {
		t.Fatalf("source startup verifier/error = %#v / %v", verifier, err)
	}
}

func TestManifestValidationIsStrictAndBounded(t *testing.T) {
	valid := tokenSetYAML(tokenEntry{Principal: "principal", Token: "token-value"})
	tests := []struct {
		name    string
		payload []byte
		mutate  func(*Options)
	}{
		{name: "top-level-scalar", payload: []byte("not-a-token-set\n")},
		{name: "top-level-sequence", payload: []byte("- apiVersion\n- kind\n- tokens\n")},
		{name: "custom-top-level-tag", payload: []byte("!credential\napiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens: []\n")},
		{name: "unknown-top-level-field", payload: append(bytes.Clone(valid), []byte("unknown: value\n")...)},
		{name: "duplicate-top-level-field", payload: append(bytes.Clone(valid), []byte("kind: StaticTokenSet\n")...)},
		{name: "numeric-api-version", payload: []byte("apiVersion: 123\nkind: StaticTokenSet\ntokens: []\n")},
		{name: "boolean-kind", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: true\ntokens: []\n")},
		{name: "tokens-mapping", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens: {}\n")},
		{name: "custom-tokens-tag", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens: !credential\n  - principal: principal\n    token: token-value\n")},
		{name: "custom-entry-tag", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n  - !credential\n    principal: principal\n    token: token-value\n")},
		{name: "unknown-entry-field", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n  - principal: principal\n    token: token-value\n    unknown: value\n")},
		{name: "duplicate-entry-field", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n  - principal: principal\n    principal: another\n    token: token-value\n")},
		{name: "numeric-token", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n  - principal: principal\n    token: 123456\n")},
		{name: "boolean-principal", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n  - principal: true\n    token: token-value\n")},
		{name: "null-token", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n  - principal: principal\n    token: null\n")},
		{name: "wrong-version", payload: []byte("apiVersion: auth.bqemu.dev/v9\nkind: StaticTokenSet\ntokens:\n  - principal: principal\n    token: token-value\n")},
		{name: "wrong-kind", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: Other\ntokens:\n  - principal: principal\n    token: token-value\n")},
		{name: "missing-token", payload: []byte("apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n  - principal: principal\n")},
		{name: "middle-padding", payload: tokenSetYAML(tokenEntry{Principal: "principal", Token: "tok=en"})},
		{name: "token-space", payload: tokenSetYAML(tokenEntry{Principal: "principal", Token: "token value"})},
		{name: "duplicate-token", payload: tokenSetYAML(
			tokenEntry{Principal: "principal-a", Token: "same-token"},
			tokenEntry{Principal: "principal-b", Token: "same-token"},
		)},
		{name: "principal-space", payload: tokenSetYAML(tokenEntry{Principal: "principal value", Token: "token-value"})},
		{name: "principal-control", payload: tokenSetYAML(tokenEntry{Principal: "principal\tvalue", Token: "token-value"})},
		{name: "principal-too-large", payload: tokenSetYAML(tokenEntry{Principal: "principal", Token: "token-value"}), mutate: func(options *Options) { options.MaxPrincipalBytes = 3 }},
		{name: "token-too-short", payload: tokenSetYAML(tokenEntry{Principal: "principal", Token: "token"}), mutate: func(options *Options) { options.MinTokenBytes = 6 }},
		{name: "token-too-large", payload: tokenSetYAML(tokenEntry{Principal: "principal", Token: "token-value"}), mutate: func(options *Options) { options.MaxTokenBytes = 5 }},
		{name: "too-many-tokens", payload: tokenSetYAML(
			tokenEntry{Principal: "principal-a", Token: "token-a"},
			tokenEntry{Principal: "principal-b", Token: "token-b"},
		), mutate: func(options *Options) { options.MaxTokens = 1 }},
		{name: "multiple-documents", payload: append(bytes.Clone(valid), []byte("---\napiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens: []\n")...)},
		{name: "non-utf8", payload: append(bytes.Clone(valid), 0xff)},
		{name: "source-violates-bound", payload: valid, mutate: func(options *Options) { options.MaxFileBytes = int64(len(valid) - 1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := testOptions(nil)
			if test.mutate != nil {
				test.mutate(&options)
			}
			verifier, err := New(context.Background(), &memorySource{payload: test.payload}, options)
			if err == nil || verifier != nil || authdomain.ReasonOf(err) != authdomain.ReasonInvalidTokenSet {
				t.Fatalf("strict manifest verifier/error = %#v / %v", verifier, err)
			}
		})
	}

	// Multiple rotating tokens for one principal are intentional and produce
	// the same digest-only identity.
	verifier := newTestVerifier(t, &memorySource{payload: tokenSetYAML(
		tokenEntry{Principal: "same-principal", Token: "rotating-token-a"},
		tokenEntry{Principal: "same-principal", Token: "rotating-token-b"},
	)}, testOptions(nil))
	first, err := verifier.Verify(context.Background(), []byte("rotating-token-a"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := verifier.Verify(context.Background(), []byte("rotating-token-b"))
	if err != nil || first.Principal.Digest() != second.Principal.Digest() {
		t.Fatalf("rotating principals = %#v / %#v / %v", first, second, err)
	}
}

func TestFileSourceEnforcesReadBoundAndSafeErrors(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "deployment-secret-token-set.yaml")
	if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := NewFileSource(path)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := source.Read(context.Background(), 3)
	if err != nil || string(payload) != "abc" {
		t.Fatalf("payload/error = %q / %v", payload, err)
	}
	clear(payload)
	if _, err := source.Read(context.Background(), 2); err == nil ||
		strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "abc") {
		t.Fatalf("unsafe bounded-read error = %v", err)
	}

	missingPath := filepath.Join(directory, "missing-secret-file.yaml")
	missing, err := NewFileSource(missingPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missing.Read(context.Background(), 100); err == nil ||
		strings.Contains(err.Error(), missingPath) || strings.Contains(err.Error(), directory) {
		t.Fatalf("unsafe missing-file error = %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Read(cancelled, 3); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read error = %v", err)
	}
	if _, err := NewFileSource(""); err == nil {
		t.Fatal("empty file path accepted")
	}
}

func TestStructuredLoadLogsContainStateButNoCredentialMaterial(t *testing.T) {
	var logs bytes.Buffer
	principal := "log-principal-secret"
	token := "log-token-secret"
	pathOrCause := "path=/deployment/credential-secret.yaml"
	source := &memorySource{payload: tokenSetYAML(tokenEntry{Principal: principal, Token: token})}
	options := testOptions(&logs)
	verifier := newTestVerifier(t, source, options)

	source.set(nil, errors.New(pathOrCause+" token="+token))
	if err := verifier.Reload(context.Background()); err == nil {
		t.Fatal("source failure reload succeeded")
	}
	logOutput := logs.String()
	for _, forbidden := range []string{principal, token, pathOrCause, "credential-secret.yaml"} {
		if strings.Contains(logOutput, forbidden) {
			t.Fatalf("structured log exposed %q: %s", forbidden, logOutput)
		}
	}
	for _, required := range []string{
		`"event":"side_effect.pre"`, `"event":"side_effect.post"`,
		`"component":"auth.static-token-set"`, `"operation":"initial-load"`,
		`"operation":"reload"`, `"tx_state":"committed"`,
		`"tx_state":"deny-all"`, `"source_fingerprint":"sha256:`,
		`"error_reason":"token-set-source-failure"`, `"error_digest":"sha256:`,
	} {
		if !strings.Contains(logOutput, required) {
			t.Fatalf("structured log missing %q: %s", required, logOutput)
		}
	}
}

func TestReloadKeepsOldSnapshotUntilAtomicPublish(t *testing.T) {
	source := newBlockingSource(tokenSetYAML(tokenEntry{Principal: "principal-a", Token: "token-value-a"}))
	verifier := newTestVerifier(t, source, testOptions(nil))

	entered, release := source.blockNext(tokenSetYAML(tokenEntry{Principal: "principal-b", Token: "token-value-b"}))
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- verifier.Reload(context.Background()) }()
	<-entered

	const readers = 32
	var readersDone sync.WaitGroup
	errorsSeen := make(chan error, readers)
	for range readers {
		readersDone.Add(1)
		go func() {
			defer readersDone.Done()
			if _, err := verifier.Verify(context.Background(), []byte("token-value-a")); err != nil {
				errorsSeen <- fmt.Errorf("old token before publish: %w", err)
				return
			}
			if _, err := verifier.Verify(context.Background(), []byte("token-value-b")); err == nil || authdomain.ReasonOf(err) != authdomain.ReasonInvalidCredential {
				errorsSeen <- fmt.Errorf("new token before publish: %w", err)
			}
		}()
	}
	readersDone.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
	close(release)
	if err := <-reloadDone; err != nil {
		t.Fatal(err)
	}
	assertTokenResult(t, verifier, "token-value-a", false, authdomain.ReasonInvalidCredential)
	assertTokenResult(t, verifier, "token-value-b", true, authdomain.ReasonAllowed)
}

func TestOptionsRequireExplicitValidBoundsAndDependencies(t *testing.T) {
	validSource := &memorySource{payload: tokenSetYAML(tokenEntry{Principal: "principal", Token: "token-value"})}
	for name, mutate := range map[string]func(*Options){
		"file-bound":      func(options *Options) { options.MaxFileBytes = 0 },
		"token-count":     func(options *Options) { options.MaxTokens = 0 },
		"minimum-token":   func(options *Options) { options.MinTokenBytes = 0 },
		"token-order":     func(options *Options) { options.MinTokenBytes, options.MaxTokenBytes = 2, 1 },
		"principal-bound": func(options *Options) { options.MaxPrincipalBytes = 0 },
		"clock":           func(options *Options) { options.Clock = nil },
	} {
		t.Run(name, func(t *testing.T) {
			options := testOptions(nil)
			mutate(&options)
			if verifier, err := New(context.Background(), validSource, options); err == nil || verifier != nil {
				t.Fatalf("invalid options accepted: %#v", options)
			}
		})
	}
	if verifier, err := New(context.Background(), nil, testOptions(nil)); err == nil || verifier != nil {
		t.Fatalf("nil source accepted: %#v / %v", verifier, err)
	}
}

type memorySource struct {
	mu           sync.Mutex
	payload      []byte
	err          error
	lastReturned []byte
}

func (s *memorySource) Read(_ context.Context, _ int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	payload := bytes.Clone(s.payload)
	s.lastReturned = payload
	return payload, nil
}

func (s *memorySource) set(payload []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.payload = bytes.Clone(payload)
	s.err = err
}

func (s *memorySource) lastPayload() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.lastReturned)
}

type blockingSource struct {
	mu      sync.Mutex
	payload []byte
	entered chan struct{}
	release chan struct{}
}

func newBlockingSource(payload []byte) *blockingSource {
	return &blockingSource{payload: bytes.Clone(payload)}
}

func (s *blockingSource) Read(ctx context.Context, _ int64) ([]byte, error) {
	s.mu.Lock()
	payload := bytes.Clone(s.payload)
	entered := s.entered
	release := s.release
	s.entered = nil
	s.release = nil
	s.mu.Unlock()
	if entered != nil {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			clear(payload)
			return nil, ctx.Err()
		}
	}
	return payload, nil
}

func (s *blockingSource) blockNext(payload []byte) (<-chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.payload = bytes.Clone(payload)
	s.entered = make(chan struct{})
	s.release = make(chan struct{})
	return s.entered, s.release
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func testOptions(logs *bytes.Buffer) Options {
	options := DefaultOptions()
	options.Clock = fixedClock{now: time.Date(2026, 8, 8, 1, 2, 3, 0, time.UTC)}
	if logs != nil {
		options.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	}
	return options
}

func newTestVerifier(t *testing.T, source interface {
	Read(context.Context, int64) ([]byte, error)
}, options Options) *Verifier {
	t.Helper()
	verifier, err := New(context.Background(), source, options)
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

func assertTokenResult(t *testing.T, verifier *Verifier, token string, allowed bool, reason authdomain.Reason) {
	t.Helper()
	_, err := verifier.Verify(context.Background(), []byte(token))
	if allowed && err != nil {
		t.Fatalf("token %q denied: %v", token, err)
	}
	if !allowed && (err == nil || authdomain.ReasonOf(err) != reason) {
		t.Fatalf("token %q error = %v, want reason %s", token, err, reason)
	}
}

func tokenSetYAML(entries ...tokenEntry) []byte {
	var document strings.Builder
	document.WriteString("apiVersion: ")
	document.WriteString(ManifestAPIVersion)
	document.WriteString("\nkind: ")
	document.WriteString(ManifestKind)
	document.WriteString("\ntokens:\n")
	for _, entry := range entries {
		document.WriteString("  - principal: ")
		document.WriteString(strconv.Quote(entry.Principal))
		document.WriteString("\n    token: ")
		document.WriteString(strconv.Quote(entry.Token))
		document.WriteByte('\n')
	}
	return []byte(document.String())
}

func allZero(payload []byte) bool {
	for _, value := range payload {
		if value != 0 {
			return false
		}
	}
	return true
}
