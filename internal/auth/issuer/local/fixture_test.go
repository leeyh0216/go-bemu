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
	"net/url"
	"os"
	"os/exec"
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
			manifest.ExternalAccount, manifest.SubjectToken, manifest.AccessToken,
			manifest.JavaTruststore, filepath.Join(directory, "manifest.json"),
		} {
			assertMode(t, path, 0o600)
		}
		assertMode(t, manifest.CACertificate, 0o644)
		assertMode(t, manifest.ServerCertificate, 0o644)
	}
	if manifest.TruststorePassword != "changeit" {
		t.Fatalf("truststore password=%q, want documented local default", manifest.TruststorePassword)
	}
	accessToken, err := os.ReadFile(manifest.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if token := strings.TrimSpace(string(accessToken)); token == "" || strings.ContainsAny(token, " \t\r\n") {
		t.Fatal("plain access-token file has an invalid shape")
	}
	keytool, err := exec.LookPath("keytool")
	if err != nil {
		t.Fatal("test requires keytool")
	}
	command := exec.Command(
		keytool,
		"-list",
		"-alias", "bqemu-local-ca",
		"-keystore", manifest.JavaTruststore,
		"-storetype", "PKCS12",
		"-storepass", manifest.TruststorePassword,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated PKCS12 truststore is not readable: %v output_bytes=%d", err, len(output))
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
	assertExactFields(
		t,
		wifFields,
		"type",
		"audience",
		"subject_token_type",
		"token_url",
		"token_info_url",
		"credential_source",
		"quota_project_id",
	)
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
	for _, name := range []string{
		"localhost", "127.0.0.1", "::1", "bqemu",
		"oauth2.googleapis.com", "accounts.google.com",
	} {
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

func TestOAuthProxyRoutesOnlyAllowlistedTokenHosts(t *testing.T) {
	issuerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "credentials")
	manifest, err := Generate(GenerateOptions{
		OutputDir: directory,
		BaseURL:   "https://" + issuerListener.Addr().String(),
		ProxyURL:  "http://" + proxyListener.Addr().String(),
		Now:       time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	var logOutput bytes.Buffer
	issuer, err := NewServer(
		filepath.Join(directory, "manifest.json"),
		slog.New(slog.NewTextHandler(&logOutput, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	issuerServer, err := issuer.HTTPServer(issuerListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	proxyServer, err := issuer.ProxyHTTPServer(
		proxyListener.Addr().String(),
		issuerListener.Addr().String(),
	)
	if err != nil {
		t.Fatal(err)
	}
	issuerResult := make(chan error, 1)
	proxyResult := make(chan error, 1)
	go func() {
		issuerResult <- issuerServer.ServeTLS(
			issuerListener,
			manifest.ServerCertificate,
			manifest.ServerKey,
		)
	}()
	go func() { proxyResult <- proxyServer.Serve(proxyListener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		issuerServer.Shutdown(ctx)
		proxyServer.Shutdown(ctx)
		<-issuerResult
		<-proxyResult
	})

	caPEM, err := os.ReadFile(manifest.CACertificate)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("load fixture CA")
	}
	proxyURL, err := url.Parse(manifest.OAuthProxyURL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
			TLSClientConfig: &tls.Config{
				RootCAs: roots, MinVersion: tls.VersionTLS12,
			},
		},
	}
	form := url.Values{
		"grant_type":    {grantRefreshToken},
		"client_id":     {issuer.user.ClientID},
		"client_secret": {issuer.user.ClientSecret},
		"refresh_token": {issuer.user.RefreshToken},
	}
	response, err := client.Post(
		googleOAuthAudience,
		"application/x-www-form-urlencoded",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("hardcoded OAuth exchange status=%d", response.StatusCode)
	}
	if _, err := client.Get("https://example.com/token"); err == nil {
		t.Fatal("proxy unexpectedly connected to a non-allowlisted host")
	}
	for _, secret := range []string{
		issuer.user.ClientSecret,
		issuer.user.RefreshToken,
	} {
		if strings.Contains(logOutput.String(), secret) {
			t.Fatal("proxy diagnostics disclosed credential material")
		}
	}
}

func TestGenerateFailsBeforeWritingWithoutKeytool(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "credentials")
	_, err := Generate(GenerateOptions{
		OutputDir: directory,
		BaseURL:   "https://localhost:9052",
		Keytool:   filepath.Join(t.TempDir(), "missing-keytool"),
	})
	if err == nil || !strings.Contains(err.Error(), "keytool is required") {
		t.Fatalf("Generate error=%v, want missing keytool diagnostic", err)
	}
	if _, statErr := os.Stat(directory); !os.IsNotExist(statErr) {
		t.Fatalf("output directory exists after fail-fast dependency check: %v", statErr)
	}
}

func TestGenerateForceRejectsSymlinkTarget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink setup requires additional Windows privileges")
	}
	directory := filepath.Join(t.TempDir(), "credentials")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(target, []byte("unchanged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(directory, "access-token.txt")); err != nil {
		t.Fatal(err)
	}
	_, err := Generate(GenerateOptions{
		OutputDir: directory,
		BaseURL:   "https://localhost:9052",
		Force:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "target must be a regular file") {
		t.Fatalf("Generate error=%v, want symlink rejection", err)
	}
	contents, readErr := os.ReadFile(target)
	if readErr != nil || string(contents) != "unchanged\n" {
		t.Fatalf("symlink target changed: read_error=%v bytes=%d", readErr, len(contents))
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
	introspectionForm := url.Values{
		"token":           {issuedTokens[len(issuedTokens)-1]},
		"token_type_hint": {accessTokenType},
	}
	introspectionRequest, err := http.NewRequest(
		http.MethodPost,
		manifest.BaseURL+"/introspect",
		strings.NewReader(introspectionForm.Encode()),
	)
	if err != nil {
		t.Fatal(err)
	}
	introspectionRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	introspectionResponse, err := client.Do(introspectionRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer introspectionResponse.Body.Close()
	var introspection map[string]any
	if err := json.NewDecoder(introspectionResponse.Body).Decode(&introspection); err != nil {
		t.Fatal(err)
	}
	if introspectionResponse.StatusCode != http.StatusOK || introspection["active"] != true ||
		introspection["username"] != "bqemu-local-external-account" {
		t.Fatalf("introspection response has an invalid public shape: status=%d", introspectionResponse.StatusCode)
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
		OutputDir: directory,
		BaseURL:   baseURL,
		Now:       time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC),
		DNSNames:  []string{"bqemu"},
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
