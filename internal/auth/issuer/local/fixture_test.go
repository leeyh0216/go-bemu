package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
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
			filepath.Join(directory, generationMarker),
		} {
			assertMode(t, path, 0o600)
		}
		assertMode(t, manifest.CACertificate, 0o644)
		assertMode(t, manifest.ServerCertificate, 0o644)
	}
	if manifest.TruststorePassword != "changeit" {
		t.Fatalf("truststore password=%q, want documented local default", manifest.TruststorePassword)
	}
	if manifest.IssuerListen != "127.0.0.1:9052" {
		t.Fatalf("issuer listen address=%q, want generated loopback address", manifest.IssuerListen)
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
			t.Errorf(
				"official Google credential parser rejected %s: error_type=%T error=%v",
				filepath.Base(path),
				err,
				err,
			)
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
	issuer, err := NewServer(
		filepath.Join(directory, "manifest.json"),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
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

func TestGenerateForceDoesNotFollowPreviousGenerationSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink setup requires additional Windows privileges")
	}
	directory := filepath.Join(t.TempDir(), "credentials")
	manifest := generateTestFixture(t, directory, "https://localhost:9052")
	target := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(target, []byte("unchanged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(manifest.AccessToken); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, manifest.AccessToken); err != nil {
		t.Fatal(err)
	}
	replacement, err := Generate(GenerateOptions{
		OutputDir: directory,
		BaseURL:   "https://localhost:9052",
		Force:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	contents, readErr := os.ReadFile(target)
	if readErr != nil || string(contents) != "unchanged\n" {
		t.Fatalf("symlink target changed: read_error=%v bytes=%d", readErr, len(contents))
	}
	info, err := os.Lstat(replacement.AccessToken)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("replacement access token is not a regular file: %v", err)
	}
}

func TestGenerateForceFailureKeepsPreviousGeneration(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "credentials")
	generateTestFixture(t, directory, "https://localhost:9052")
	before := generationFingerprint(t, directory)

	_, err := Generate(GenerateOptions{
		OutputDir: directory,
		BaseURL:   "https://localhost:9052",
		Force:     true,
		testHook: func(stage string) error {
			if stage == "staged-authorized-user.json" {
				return errors.New("injected failure")
			}
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "staged-authorized-user.json") {
		t.Fatalf("Generate error_type=%T, want injected staged-write failure", err)
	}
	after := generationFingerprint(t, directory)
	if before != after {
		t.Fatalf("generation changed after failed replacement: before=%x after=%x", before, after)
	}
	assertNoStagingDirectories(t, directory)
}

func TestGenerateForceCrashKeepsPreviousGeneration(t *testing.T) {
	if os.Getenv("BQEMU_AUTH_CRASH_HELPER") == "1" {
		_, err := Generate(GenerateOptions{
			OutputDir: os.Getenv("BQEMU_AUTH_CRASH_OUTPUT"),
			BaseURL:   "https://localhost:9052",
			Force:     true,
			testHook: func(stage string) error {
				if stage == "staged-authorized-user.json" {
					os.Exit(91)
				}
				return nil
			},
		})
		if err != nil {
			os.Exit(92)
		}
		os.Exit(93)
	}

	directory := filepath.Join(t.TempDir(), "credentials")
	generateTestFixture(t, directory, "https://localhost:9052")
	before := generationFingerprint(t, directory)
	command := exec.Command(os.Args[0], "-test.run=^TestGenerateForceCrashKeepsPreviousGeneration$")
	command.Env = append(
		os.Environ(),
		"BQEMU_AUTH_CRASH_HELPER=1",
		"BQEMU_AUTH_CRASH_OUTPUT="+directory,
	)
	output, err := command.CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 91 {
		t.Fatalf(
			"crash helper exit_type=%T exit_code=%d output_bytes=%d output_digest=%s",
			err,
			exitCode(exitError),
			len(output),
			diagnosticDigest(string(output)),
		)
	}
	after := generationFingerprint(t, directory)
	if before != after {
		t.Fatalf("generation changed after crash: before=%x after=%x", before, after)
	}
	if matches := stagingDirectories(t, directory); len(matches) == 0 {
		t.Fatal("crash regression did not leave a recoverable staging directory")
	}
	if _, err := Generate(GenerateOptions{
		OutputDir: directory,
		BaseURL:   "https://localhost:9052",
		Force:     true,
	}); err != nil {
		t.Fatal(err)
	}
	assertNoStagingDirectories(t, directory)
}

func TestGenerateSerializesConcurrentStagingAndCleanup(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "credentials")
	ready := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		_, err := Generate(GenerateOptions{
			OutputDir: directory,
			BaseURL:   "https://localhost:9052",
			testHook: func(stage string) error {
				if stage == "staged-ca.pem" {
					close(ready)
					<-release
				}
				return nil
			},
		})
		firstResult <- err
	}()

	select {
	case <-ready:
	case err := <-firstResult:
		t.Fatalf("first generation ended before staging: error_type=%T", err)
	case <-time.After(30 * time.Second):
		close(release)
		err := <-firstResult
		t.Fatalf("first generation did not reach staging: error_type=%T", err)
	}
	_, concurrentErr := Generate(GenerateOptions{
		OutputDir: directory,
		BaseURL:   "https://localhost:9052",
		Force:     true,
	})
	close(release)
	firstErr := <-firstResult
	if firstErr != nil {
		t.Fatalf("first generation error_type=%T error=%v", firstErr, firstErr)
	}
	if concurrentErr == nil || !strings.Contains(concurrentErr.Error(), "already active") {
		t.Fatalf("concurrent generation error_type=%T, want active-generation diagnostic", concurrentErr)
	}
	if _, err := LoadManifest(filepath.Join(directory, "manifest.json")); err != nil {
		t.Fatal("completed generation manifest is invalid")
	}
	assertNoStagingDirectories(t, directory)
}

func TestGenerateDoesNotFollowGenerationLockSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink setup requires additional Windows privileges")
	}
	parent := t.TempDir()
	directory := filepath.Join(parent, "credentials")
	target := filepath.Join(t.TempDir(), "outside-lock-target")
	if err := os.WriteFile(target, []byte("unchanged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(parent, generationLockName(filepath.Base(directory)))
	if err := os.Symlink(target, lockPath); err != nil {
		t.Fatal(err)
	}
	_, err := Generate(GenerateOptions{
		OutputDir: directory,
		BaseURL:   "https://localhost:9052",
	})
	if err == nil {
		t.Fatal("generation unexpectedly followed a lock-file symlink")
	}
	contents, readErr := os.ReadFile(target)
	info, statErr := os.Stat(target)
	if readErr != nil || statErr != nil || string(contents) != "unchanged\n" || info.Mode().Perm() != 0o644 {
		t.Fatalf(
			"lock symlink target changed: read_error_type=%T stat_error_type=%T bytes=%d mode=%#o",
			readErr,
			statErr,
			len(contents),
			infoMode(info),
		)
	}
	if _, statErr := os.Lstat(directory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("output directory exists after lock rejection: error_type=%T", statErr)
	}
}

func TestGeneratedManifestServesCustomIssuerPortByDefault(t *testing.T) {
	port := freeLoopbackPort(t)
	directory := filepath.Join(t.TempDir(), "credentials")
	manifest := generateTestFixture(
		t,
		directory,
		"https://localhost:"+strconv.Itoa(port),
	)
	issuer, err := NewServer(filepath.Join(directory, "manifest.json"), nil)
	if err != nil {
		t.Fatal(err)
	}
	httpServer, err := issuer.HTTPServer("")
	if err != nil {
		t.Fatal(err)
	}
	wantAddress := "127.0.0.1:" + strconv.Itoa(port)
	if httpServer.Addr != wantAddress || manifest.IssuerListen != wantAddress {
		t.Fatalf(
			"issuer address mismatch: server=%q manifest=%q want=%q",
			httpServer.Addr,
			manifest.IssuerListen,
			wantAddress,
		)
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- issuer.ServeTLS(httpServer) }()
	client := trustedClient(t, manifest.CACertificate)
	waitForHealth(t, client, manifest.BaseURL+"/healthz")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serveResult; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("issuer serve error_type=%T error=%v", err, err)
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
			t.Fatalf(
				"parse %s: error_type=%T error=%v",
				filepath.Base(path),
				err,
				err,
			)
		}
		token, err := credentials.TokenSource.Token()
		if err != nil {
			t.Fatalf(
				"exchange %s: error_type=%T error=%v",
				filepath.Base(path),
				err,
				err,
			)
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
	for _, value := range []string{
		issuer.user.ClientSecret,
		issuer.user.RefreshToken,
		issuer.subjectToken,
		issuedTokens[len(issuedTokens)-1],
	} {
		if !strings.Contains(logOutput.String(), value) {
			t.Fatalf("credential endpoint log omitted raw diagnostic %q", value)
		}
	}
	if !strings.Contains(logOutput.String(), strconv.Quote(string(body))) {
		t.Fatalf("credential endpoint log omitted structured response body %q", string(body))
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
		t.Error(fieldSetDiagnostic(object, wanted))
	}
	for field := range object {
		if !wanted[field] {
			t.Errorf("unexpected field %q", field)
		}
	}
}

func fieldSetDiagnostic(object map[string]any, wanted map[string]bool) string {
	actualFields := make([]string, 0, len(object))
	for field := range object {
		actualFields = append(actualFields, field)
	}
	sort.Strings(actualFields)
	digest := diagnosticDigest(strings.Join(actualFields, "\x00"))
	return fmt.Sprintf(
		"field count=%d, want=%d actual_fields=%v field_name_digest=%s",
		len(object),
		len(wanted),
		actualFields,
		digest,
	)
}

func generationFingerprint(t *testing.T, root string) [sha256.Size]byte {
	t.Helper()
	hash := sha256.New()
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fmt.Fprintf(hash, "%s\x00%o\x00", filepath.ToSlash(relative), info.Mode())
		if !info.Mode().IsRegular() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		return errors.Join(copyErr, closeErr)
	})
	if err != nil {
		t.Fatal(err)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func stagingDirectories(t *testing.T, outputDir string) []string {
	t.Helper()
	matches, err := filepath.Glob(
		filepath.Join(
			filepath.Dir(outputDir),
			stagingPrefix(filepath.Base(outputDir))+"*",
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func assertNoStagingDirectories(t *testing.T, outputDir string) {
	t.Helper()
	if matches := stagingDirectories(t, outputDir); len(matches) != 0 {
		t.Fatalf("staging directory count=%d, want=0", len(matches))
	}
}

func freeLoopbackPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func diagnosticDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("sha256:%x", digest)
}

func exitCode(error *exec.ExitError) int {
	if error == nil {
		return -1
	}
	return error.ExitCode()
}

func infoMode(info os.FileInfo) os.FileMode {
	if info == nil {
		return 0
	}
	return info.Mode().Perm()
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
