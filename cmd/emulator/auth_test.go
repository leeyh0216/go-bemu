package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authapp "github.com/leeyh0216/go-bemu/internal/auth/application"
	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
	"github.com/leeyh0216/go-bemu/internal/config"
)

func TestComposeAuthenticationSupportsConfiguredPoliciesAndFailsStaticStartupClosed(t *testing.T) {
	ctx, cancel := authRuntimeTestContext(t)
	defer cancel()

	for _, test := range []struct {
		name          string
		mode          string
		authorization []string
		wantKind      authdomain.CredentialKind
	}{
		{name: "disabled", mode: "disabled", wantKind: authdomain.CredentialAnonymous},
		{name: "bearer-present", mode: "bearer-present", authorization: []string{"Bearer present-token"}, wantKind: authdomain.CredentialBearerPresent},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.Defaults().Auth
			cfg.Mode = test.mode
			runtime, err := composeAuthentication(ctx, cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			authenticated, decision, err := runtime.Service().Authenticate(ctx, test.authorization)
			if err != nil || !decision.Allowed() {
				t.Fatalf("authentication decision = %#v, err=%v", decision, err)
			}
			principal, ok := authenticationPrincipal(authenticated)
			if !ok || principal.CredentialKind() != test.wantKind {
				t.Fatalf("principal = %#v, present=%t", principal, ok)
			}
		})
	}

	directory := t.TempDir()
	path := filepath.Join(directory, "private-token-set.yaml")
	privatePayload := "raw-private-token-material"
	if err := os.WriteFile(path, []byte("not: [valid\n"+privatePayload), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults().Auth
	cfg.Mode = "static"
	cfg.StaticTokensFile = path
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	if runtime, err := composeAuthentication(ctx, cfg, logger); err == nil || runtime != nil {
		t.Fatalf("invalid static startup runtime=%#v err=%v", runtime, err)
	} else {
		for _, secret := range []string{path, privatePayload} {
			if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
				t.Fatalf("startup failure leaked %q: err=%v logs=%s", secret, err, logs.String())
			}
		}
	}
}

func TestStaticAuthenticationReloadFailsClosedRecoversAndStops(t *testing.T) {
	ctx, cancel := authRuntimeTestContext(t)
	defer cancel()
	directory := t.TempDir()
	path := filepath.Join(directory, "static-token-set.yaml")
	firstToken := "private-first-token"
	secondToken := "private-second-token"
	firstPrincipal := "private-first-principal"
	secondPrincipal := "private-second-principal"
	writeStaticTokenSet(t, path, firstPrincipal, firstToken)

	cfg := config.Defaults().Auth
	cfg.Mode = "static"
	cfg.StaticTokensFile = path
	var logs bytes.Buffer
	runtime, err := composeAuthentication(ctx, cfg, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	assertBearerAllowed(t, ctx, runtime, firstToken, firstPrincipal)

	reloadContext, stopReloads := context.WithCancel(ctx)
	ticks := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runtime.runReloadLoop(reloadContext, ticks)
	}()

	invalidPayload := "apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens: [private-invalid-token"
	if err := os.WriteFile(path, []byte(invalidPayload), 0o600); err != nil {
		t.Fatal(err)
	}
	sendAuthReloadTick(t, ctx, ticks)
	waitForAuthSnapshot(t, ctx, runtime, 2, "deny-all")
	if _, decision, err := runtime.Service().Authenticate(ctx, []string{"Bearer " + firstToken}); err == nil || decision.Allowed() {
		t.Fatalf("invalid reload retained old allow decision: %#v, err=%v", decision, err)
	}

	writeStaticTokenSet(t, path, secondPrincipal, secondToken)
	sendAuthReloadTick(t, ctx, ticks)
	waitForAuthSnapshot(t, ctx, runtime, 3, "active")
	assertBearerAllowed(t, ctx, runtime, secondToken, secondPrincipal)
	if _, decision, err := runtime.Service().Authenticate(ctx, []string{"Bearer " + firstToken}); err == nil || decision.Allowed() {
		t.Fatalf("recovered snapshot retained retired token: %#v, err=%v", decision, err)
	}

	stopReloads()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("reload loop did not stop: %v", ctx.Err())
	}
	output := logs.String()
	for _, secret := range []string{path, firstToken, secondToken, firstPrincipal, secondPrincipal, invalidPayload} {
		if strings.Contains(output, secret) {
			t.Fatalf("reload logs leaked %q: %s", secret, output)
		}
	}
	for _, marker := range []string{`"tx_state":"deny-all"`, `"tx_state":"committed"`, `"source_fingerprint":"sha256:`} {
		if !strings.Contains(output, marker) {
			t.Fatalf("reload logs lack %q: %s", marker, output)
		}
	}
}

func TestStaticAuthenticationSchedulerShutdownIsIdempotent(t *testing.T) {
	ctx, cancel := authRuntimeTestContext(t)
	directory := t.TempDir()
	path := filepath.Join(directory, "tokens.yaml")
	writeStaticTokenSet(t, path, "scheduler-principal", "scheduler-token")
	cfg := config.Defaults().Auth
	cfg.Mode = "static"
	cfg.StaticTokensFile = path
	runtime, err := composeAuthentication(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	stop := runtime.Start(ctx)
	cancel()
	stop()
	stop()
}

func authenticationPrincipal(ctx context.Context) (authdomain.Principal, bool) {
	return authapp.PrincipalFromContext(ctx)
}

func assertBearerAllowed(t *testing.T, ctx context.Context, runtime *authenticationRuntime, token, identity string) {
	t.Helper()
	authenticated, decision, err := runtime.Service().Authenticate(ctx, []string{"Bearer " + token})
	if err != nil || !decision.Allowed() {
		t.Fatalf("token authentication decision = %#v, err=%v", decision, err)
	}
	principal, ok := authenticationPrincipal(authenticated)
	if !ok || principal.Digest() != authdomain.Digest([]byte(identity)) {
		t.Fatalf("principal digest = %q, present=%t", principal.Digest(), ok)
	}
}

func writeStaticTokenSet(t *testing.T, path, principal, token string) {
	t.Helper()
	payload := "apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n" +
		"  - principal: " + principal + "\n    token: " + token + "\n"
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForAuthSnapshot(t *testing.T, ctx context.Context, runtime *authenticationRuntime, attempt uint64, state string) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		metadata := runtime.reloadVerifier.SnapshotMetadata()
		if metadata.LoadAttempt >= attempt && string(metadata.State) == state {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("snapshot attempt/state = %d/%s, want >=%d/%s: %v", metadata.LoadAttempt, metadata.State, attempt, state, ctx.Err())
		case <-ticker.C:
		}
	}
}

func sendAuthReloadTick(t *testing.T, ctx context.Context, ticks chan<- time.Time) {
	t.Helper()
	select {
	case ticks <- time.Now():
	case <-ctx.Done():
		t.Fatalf("reload tick was not accepted: %v", ctx.Err())
	}
}

func authRuntimeTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 5 * time.Second
	if configured := os.Getenv("BQEMU_AUTH_RUNTIME_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_AUTH_RUNTIME_TEST_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	return context.WithTimeout(t.Context(), timeout)
}
