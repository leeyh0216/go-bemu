package rest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	authstatic "github.com/leeyh0216/go-bemu/internal/auth/adapters/static"
	authapp "github.com/leeyh0216/go-bemu/internal/auth/application"
	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
)

func TestRESTAuthenticationRejectsBeforeBodyAndHandlerSideEffects(t *testing.T) {
	ctx, cancel := restAuthTestContext(t)
	defer cancel()
	var logs bytes.Buffer
	authentication := newRESTStaticAuthentication(t, ctx, &logs, "private-rest-principal", "private-valid-rest-token")
	var handlerCalls atomic.Int64
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls.Add(1)
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	})
	handler := authenticationMiddleware(authentication, next)

	for _, test := range []struct {
		name   string
		values []string
	}{
		{name: "missing"},
		{name: "unsupported", values: []string{"Basic private-invalid-rest-token"}},
		{name: "unknown", values: []string{"Bearer private-unknown-rest-token"}},
		{name: "duplicate", values: []string{"Bearer private-valid-rest-token", "Bearer private-valid-rest-token"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := handlerCalls.Load()
			body := &authCountingBody{payload: []byte("private-request-payload")}
			request := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p/queries", nil).WithContext(ctx)
			request.Body = body
			for _, value := range test.values {
				request.Header.Add("Authorization", value)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401: %s", response.Code, response.Body.String())
			}
			if handlerCalls.Load() != before || body.reads.Load() != 0 || body.closes.Load() != 1 {
				t.Fatalf("side effects handler=%d/%d reads=%d closes=%d", handlerCalls.Load(), before, body.reads.Load(), body.closes.Load())
			}
			if response.Header().Get("WWW-Authenticate") != `Bearer realm="bqemu"` {
				t.Fatalf("WWW-Authenticate = %q", response.Header().Get("WWW-Authenticate"))
			}
			for _, secret := range append(test.values, "private-request-payload") {
				if secret != "" && strings.Contains(response.Body.String(), secret) {
					t.Fatalf("response leaked credential/payload %q: %s", secret, response.Body.String())
				}
			}
		})
	}
}

func TestRESTAuthenticationPropagatesPrincipalAndOnlyExemptsHealthPaths(t *testing.T) {
	ctx, cancel := restAuthTestContext(t)
	defer cancel()
	var logs bytes.Buffer
	identity := "private-rest-principal"
	token := "private-valid-rest-token"
	authentication := newRESTStaticAuthentication(t, ctx, &logs, identity, token)
	var handlerCalls atomic.Int64
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls.Add(1)
		principal, principalOK := authapp.PrincipalFromContext(r.Context())
		decision, decisionOK := authapp.DecisionFromContext(r.Context())
		if r.URL.Path == "/protected" {
			if !principalOK || !decisionOK || principal.Digest() != authdomain.Digest([]byte(identity)) || !decision.Allowed() {
				t.Errorf("authenticated context principal=%#v/%t decision=%#v/%t", principal, principalOK, decision, decisionOK)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	handler := authenticationMiddleware(authentication, next)

	valid := httptest.NewRequest(http.MethodGet, "/protected", nil).WithContext(ctx)
	valid.Header.Set("Authorization", "Bearer "+token)
	validResponse := httptest.NewRecorder()
	handler.ServeHTTP(validResponse, valid)
	if validResponse.Code != http.StatusNoContent || handlerCalls.Load() != 1 {
		t.Fatalf("valid status/calls = %d/%d", validResponse.Code, handlerCalls.Load())
	}

	for _, path := range []string{"/healthz", "/readyz"} {
		request := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("public path %s status = %d", path, response.Code)
		}
	}
	for _, path := range []string{"/$discovery/rest", "/discovery/v1/apis/bigquery/v2/rest", "/healthz/"} {
		request := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("protected path %s status = %d", path, response.Code)
		}
	}

	disabled, err := authapp.New(authapp.DefaultConfig(), authapp.DisabledVerifier{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	disabledRequest := httptest.NewRequest(http.MethodGet, "/protected", nil).WithContext(ctx)
	disabledRequest.Header.Set("Authorization", "malformed private-disabled-value")
	disabledResponse := httptest.NewRecorder()
	disabledNext := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := authapp.PrincipalFromContext(r.Context())
		if !ok || principal.CredentialKind() != authdomain.CredentialAnonymous {
			t.Errorf("disabled principal = %#v, present=%t", principal, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	authenticationMiddleware(disabled, disabledNext).ServeHTTP(disabledResponse, disabledRequest)
	if disabledResponse.Code != http.StatusNoContent {
		t.Fatalf("disabled policy status = %d", disabledResponse.Code)
	}

	output := logs.String()
	for _, secret := range []string{identity, token, "private-unknown-rest-token"} {
		if strings.Contains(output, secret) {
			t.Fatalf("auth logs leaked %q: %s", secret, output)
		}
	}
}

func TestRESTAuthenticationPublicEdgeProtectsDiscoveryAndExemptsProbes(t *testing.T) {
	ctx, cancel := restAuthTestContext(t)
	defer cancel()
	token := "public-edge-token"
	authentication := newRESTStaticAuthentication(t, ctx, &bytes.Buffer{}, "public-edge-principal", token)
	warehouse := &catalogTestWarehouse{}
	catalog := application.NewCatalogService(
		memory.NewCatalogRepository(), warehouse, catalogTestClock{},
	)
	server := httptest.NewServer(NewCatalogServer(
		catalog, warehouse, "", WithAuthentication(authentication),
	).Handler())
	t.Cleanup(server.Close)

	for _, path := range []string{"/healthz", "/readyz"} {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("public probe %s status = %d", path, response.StatusCode)
		}
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/$discovery/rest", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous discovery status = %d, want 401", response.StatusCode)
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/$discovery/rest", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("authenticated discovery status = %d, want 200", response.StatusCode)
	}
}

func TestRESTRecoveryNeverExposesCredentialLikePanicDiagnostics(t *testing.T) {
	ctx, cancel := restAuthTestContext(t)
	defer cancel()
	secret := "private-authorization-panic-value"
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	handler := recoverMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(secret)
	}))
	request := httptest.NewRequest(http.MethodGet, "/protected", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("panic status = %d, want 500", response.Code)
	}
	if strings.Contains(response.Body.String(), secret) || strings.Contains(logs.String(), secret) {
		t.Fatalf("panic diagnostic leaked: response=%s logs=%s", response.Body.String(), logs.String())
	}
	if !strings.Contains(logs.String(), `"event":"boundary.panic"`) || !strings.Contains(logs.String(), `"panic_type":"string"`) {
		t.Fatalf("safe panic log fields missing: %s", logs.String())
	}
}

func newRESTStaticAuthentication(t *testing.T, ctx context.Context, logs *bytes.Buffer, principal, token string) *authapp.Service {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.yaml")
	payload := "apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n" +
		"  - principal: " + principal + "\n    token: " + token + "\n"
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := authstatic.NewFileSource(path)
	if err != nil {
		t.Fatal(err)
	}
	options := authstatic.DefaultOptions()
	options.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	verifier, err := authstatic.New(ctx, source, options)
	if err != nil {
		t.Fatal(err)
	}
	service, err := authapp.New(authapp.DefaultConfig(), verifier, options.Logger)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func restAuthTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 5 * time.Second
	if configured := os.Getenv("BQEMU_AUTH_TRANSPORT_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_AUTH_TRANSPORT_TEST_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	return context.WithTimeout(t.Context(), timeout)
}

type authCountingBody struct {
	payload []byte
	reads   atomic.Int64
	closes  atomic.Int64
}

func (body *authCountingBody) Read(destination []byte) (int, error) {
	body.reads.Add(1)
	if len(body.payload) == 0 {
		return 0, io.EOF
	}
	written := copy(destination, body.payload)
	body.payload = body.payload[written:]
	return written, nil
}

func (body *authCountingBody) Close() error {
	body.closes.Add(1)
	return nil
}
