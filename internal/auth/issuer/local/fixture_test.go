package local

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func TestGenerateCreatesProtectedStrictClientCredentials(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "credentials")
	manifest := generateTestFixture(t, directory, "https://localhost:9052")

	if runtime.GOOS != "windows" {
		assertMode(t, directory, 0o700)
		for _, path := range []string{
			manifest.ServerKey, manifest.ServiceAccount, manifest.AuthorizedUser,
			manifest.ExternalAccount, manifest.SubjectToken, filepath.Join(directory, "manifest.json"),
		} {
			assertMode(t, path, 0o600)
		}
		assertMode(t, manifest.CACertificate, 0o644)
		assertMode(t, manifest.ServerCertificate, 0o644)
	}

	serviceFields := strictObject(t, manifest.ServiceAccount)
	assertExactFields(t, serviceFields,
		"type", "project_id", "private_key_id", "private_key", "client_email", "client_id",
		"auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url", "universe_domain")
	if serviceFields["type"] != "service_account" || serviceFields["token_uri"] != manifest.OAuthTokenURL {
		t.Fatal("service-account credential has incorrect type or endpoint")
	}
	userFields := strictObject(t, manifest.AuthorizedUser)
	assertExactFields(t, userFields, "type", "client_id", "client_secret", "refresh_token", "token_uri", "quota_project_id")
	if userFields["type"] != "authorized_user" || userFields["token_uri"] != manifest.OAuthTokenURL {
		t.Fatal("authorized-user credential has incorrect type or endpoint")
	}
	wifFields := strictObject(t, manifest.ExternalAccount)
	assertExactFields(t, wifFields, "type", "audience", "subject_token_type", "token_url", "credential_source", "quota_project_id")
	if wifFields["type"] != "external_account" || wifFields["token_url"] != manifest.STSTokenURL {
		t.Fatal("external-account credential has incorrect type or endpoint")
	}
	credentialSource, ok := wifFields["credential_source"].(map[string]any)
	if !ok || credentialSource["file"] != manifest.SubjectToken {
		t.Fatal("external-account credential does not reference the generated subject token")
	}

	certificatePEM, err := os.ReadFile(manifest.ServerCertificate)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certificatePEM)
	if block == nil {
		t.Fatal("server certificate is not PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"localhost", "127.0.0.1", "::1"} {
		if err := certificate.VerifyHostname(name); err != nil {
			t.Errorf("server certificate does not cover %s: %v", name, err)
		}
	}

	for _, path := range []string{manifest.ServiceAccount, manifest.AuthorizedUser, manifest.ExternalAccount} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := google.CredentialsFromJSON(context.Background(), contents, "https://www.googleapis.com/auth/bigquery"); err != nil {
			t.Errorf("official Google credential parser rejected %s: %v", filepath.Base(path), err)
		}
	}
}

func TestTLSServerSupportsOfficialOAuthAndSTSFlows(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	directory := filepath.Join(t.TempDir(), "credentials")
	manifest := generateTestFixture(t, directory, "https://127.0.0.1:"+strconv.Itoa(port))
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	issuer, err := NewServer(filepath.Join(directory, "manifest.json"), logger)
	if err != nil {
		t.Fatal(err)
	}
	httpServer, err := issuer.HTTPServer(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- httpServer.ServeTLS(listener, manifest.ServerCertificate, manifest.ServerKey) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		httpServer.Shutdown(ctx)
		<-serveResult
	})

	client := trustedClient(t, manifest.CACertificate)
	waitForHealth(t, client, manifest.BaseURL+"/healthz")

	var issuedTokens []string
	for _, path := range []string{manifest.ServiceAccount, manifest.AuthorizedUser, manifest.ExternalAccount} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
		credentials, err := google.CredentialsFromJSON(ctx, contents, "https://www.googleapis.com/auth/bigquery")
		if err != nil {
			t.Fatalf("parse %s: %v", filepath.Base(path), err)
		}
		token, err := credentials.TokenSource.Token()
		if err != nil {
			t.Fatalf("exchange %s: %v", filepath.Base(path), err)
		}
		if token.AccessToken == "" || token.TokenType != "Bearer" || !token.Expiry.After(time.Now()) {
			t.Errorf("invalid token returned for %s", filepath.Base(path))
		}
		issuedTokens = append(issuedTokens, token.AccessToken)
	}

	request, err := http.NewRequest(http.MethodPost, manifest.STSTokenURL, strings.NewReader("grant_type=bad"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid STS request status=%d", response.StatusCode)
	}
	body, _ := io.ReadAll(response.Body)
	if strings.Contains(string(body), issuer.subjectToken) {
		t.Fatal("OAuth error response disclosed the subject token")
	}
	for _, secret := range append(issuedTokens, issuer.user.ClientSecret, issuer.user.RefreshToken, issuer.subjectToken) {
		if strings.Contains(logOutput.String(), secret) {
			t.Fatal("credential endpoint log disclosed credential material")
		}
	}
}

func generateTestFixture(t *testing.T, directory, baseURL string) Manifest {
	t.Helper()
	manifest, err := Generate(GenerateOptions{
		OutputDir: directory, BaseURL: baseURL, Now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func assertMode(t *testing.T, path string, expected os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if actual := info.Mode().Perm(); actual != expected {
		t.Errorf("%s mode=%#o, want %#o", filepath.Base(path), actual, expected)
	}
}

func strictObject(t *testing.T, path string) map[string]any {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var result map[string]any
	if err := decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertExactFields(t *testing.T, object map[string]any, fields ...string) {
	t.Helper()
	wanted := make(map[string]bool, len(fields))
	for _, field := range fields {
		wanted[field] = true
	}
	if len(object) != len(wanted) {
		t.Errorf("field count=%d, want %d: %v", len(object), len(wanted), object)
	}
	for field := range object {
		if !wanted[field] {
			t.Errorf("unexpected field %q", field)
		}
	}
}

func trustedClient(t *testing.T, caPath string) *http.Client {
	t.Helper()
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("load fixture CA")
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
}

func waitForHealth(t *testing.T, client *http.Client, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(endpoint)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("local credential server did not become ready")
}
