// Package local creates and serves credentials for local client-library tests.
// It does not participate in BQEMU request authentication or authorization.
package local

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
)

const (
	ManifestVersion     = "auth-fixture.bqemu.dev/v1alpha3"
	defaultBaseURL      = "https://localhost:9052"
	defaultProxyURL     = "http://127.0.0.1:9053"
	maxFixtureFileBytes = 1 << 20
	truststorePassword  = "changeit"
	generationMarker    = ".bqemu-auth-generation"
	stagingInfix        = ".stage-"
)

type GenerateOptions struct {
	OutputDir string
	BaseURL   string
	ProxyURL  string
	Force     bool
	Now       time.Time
	Keytool   string
	DNSNames  []string
	testHook  func(string) error
}

type Manifest struct {
	Version            string   `json:"version"`
	BaseURL            string   `json:"base_url"`
	IssuerListen       string   `json:"issuer_listen_address"`
	OAuthTokenURL      string   `json:"oauth_token_url"`
	STSTokenURL        string   `json:"sts_token_url"`
	OAuthProxyURL      string   `json:"oauth_proxy_url"`
	CACertificate      string   `json:"ca_certificate"`
	ServerCertificate  string   `json:"server_certificate"`
	ServerKey          string   `json:"server_key"`
	ServiceAccount     string   `json:"service_account"`
	AuthorizedUser     string   `json:"authorized_user"`
	ExternalAccount    string   `json:"external_account"`
	SubjectToken       string   `json:"subject_token"`
	AccessToken        string   `json:"access_token"`
	JavaTruststore     string   `json:"java_truststore"`
	TruststorePassword string   `json:"truststore_password"`
	TLSDNSNames        []string `json:"tls_dns_names"`
	TLSIPAddresses     []string `json:"tls_ip_addresses"`
}

type serviceAccountCredential struct {
	Type                 string `json:"type"`
	ProjectID            string `json:"project_id"`
	PrivateKeyID         string `json:"private_key_id"`
	PrivateKey           string `json:"private_key"`
	ClientEmail          string `json:"client_email"`
	ClientID             string `json:"client_id"`
	AuthURI              string `json:"auth_uri"`
	TokenURI             string `json:"token_uri"`
	AuthProviderCertURL  string `json:"auth_provider_x509_cert_url"`
	ClientCertificateURL string `json:"client_x509_cert_url"`
	UniverseDomain       string `json:"universe_domain"`
}

type authorizedUserCredential struct {
	Type         string `json:"type"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
	TokenURI     string `json:"token_uri"`
	QuotaProject string `json:"quota_project_id"`
}

type externalAccountCredential struct {
	Type             string            `json:"type"`
	Audience         string            `json:"audience"`
	SubjectTokenType string            `json:"subject_token_type"`
	TokenURL         string            `json:"token_url"`
	TokenInfoURL     string            `json:"token_info_url"`
	CredentialSource map[string]string `json:"credential_source"`
	QuotaProject     string            `json:"quota_project_id"`
	WorkforceProject string            `json:"workforce_pool_user_project,omitempty"`
}

func Generate(options GenerateOptions) (Manifest, error) {
	if options.OutputDir == "" {
		return Manifest{}, errors.New("output directory is required")
	}
	baseURL := strings.TrimRight(options.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	parsedBaseURL, err := url.Parse(baseURL)
	if err != nil || parsedBaseURL.Scheme != "https" || parsedBaseURL.Host == "" ||
		parsedBaseURL.Path != "" || parsedBaseURL.RawQuery != "" || parsedBaseURL.Fragment != "" ||
		parsedBaseURL.User != nil {
		return Manifest{}, errors.New("base URL must be an HTTPS origin without a path")
	}
	if !loopbackHost(parsedBaseURL.Hostname()) {
		return Manifest{}, errors.New("base URL host must resolve to localhost or a loopback address")
	}
	proxyURL := strings.TrimRight(options.ProxyURL, "/")
	if proxyURL == "" {
		proxyURL = defaultProxyURL
	}
	parsedProxyURL, err := url.Parse(proxyURL)
	if err != nil || parsedProxyURL.Scheme != "http" || parsedProxyURL.Host == "" ||
		parsedProxyURL.Path != "" || parsedProxyURL.RawQuery != "" ||
		parsedProxyURL.Fragment != "" || parsedProxyURL.User != nil ||
		!loopbackHost(parsedProxyURL.Hostname()) {
		return Manifest{}, errors.New("proxy URL must be a loopback HTTP origin without a path")
	}
	dnsNames, ipAddresses, err := certificateNames(parsedBaseURL.Hostname(), options.DNSNames)
	if err != nil {
		return Manifest{}, err
	}
	keytool, err := resolveKeytool(options.Keytool)
	if err != nil {
		return Manifest{}, err
	}
	outputDir, err := filepath.Abs(options.OutputDir)
	if err != nil {
		return Manifest{}, fmt.Errorf("resolve output directory: %w", err)
	}
	parentDir := filepath.Dir(outputDir)
	if err := os.MkdirAll(parentDir, 0o700); err != nil {
		return Manifest{}, fmt.Errorf("create output parent directory: %w", err)
	}
	generationLock, err := acquireGenerationLock(parentDir, filepath.Base(outputDir))
	if err != nil {
		return Manifest{}, err
	}
	defer generationLock.Close()
	replacing, err := inspectOutputDirectory(outputDir, options.Force)
	if err != nil {
		return Manifest{}, err
	}
	if err := cleanupStaleGenerations(parentDir, filepath.Base(outputDir)); err != nil {
		return Manifest{}, err
	}

	now := options.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return Manifest{}, fmt.Errorf("generate CA key: %w", err)
	}
	caDER, caCertificate, err := makeCA(now, caKey)
	if err != nil {
		return Manifest{}, err
	}
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return Manifest{}, fmt.Errorf("generate server key: %w", err)
	}
	serverDER, err := makeServerCertificate(now, caCertificate, caKey, &serverKey.PublicKey, dnsNames, ipAddresses)
	if err != nil {
		return Manifest{}, err
	}
	accountKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return Manifest{}, fmt.Errorf("generate service-account key: %w", err)
	}

	clientID, err := randomText(24)
	if err != nil {
		return Manifest{}, err
	}
	clientSecret, err := randomText(32)
	if err != nil {
		return Manifest{}, err
	}
	refreshToken, err := randomText(32)
	if err != nil {
		return Manifest{}, err
	}
	subjectToken, err := randomText(32)
	if err != nil {
		return Manifest{}, err
	}
	accessToken, err := randomText(32)
	if err != nil {
		return Manifest{}, err
	}
	privateKeyID, err := randomText(16)
	if err != nil {
		return Manifest{}, err
	}

	issuerHost := parsedBaseURL.Hostname()
	if strings.EqualFold(issuerHost, "localhost") {
		issuerHost = "127.0.0.1"
	}
	issuerPort := parsedBaseURL.Port()
	if issuerPort == "" {
		issuerPort = "443"
	}
	manifest := Manifest{
		Version:            ManifestVersion,
		BaseURL:            baseURL,
		IssuerListen:       net.JoinHostPort(issuerHost, issuerPort),
		OAuthTokenURL:      baseURL + "/oauth/token",
		STSTokenURL:        baseURL + "/sts/token",
		OAuthProxyURL:      proxyURL,
		CACertificate:      filepath.Join(outputDir, "ca.pem"),
		ServerCertificate:  filepath.Join(outputDir, "server.pem"),
		ServerKey:          filepath.Join(outputDir, "server-key.pem"),
		ServiceAccount:     filepath.Join(outputDir, "service-account.json"),
		AuthorizedUser:     filepath.Join(outputDir, "authorized-user.json"),
		ExternalAccount:    filepath.Join(outputDir, "wif.json"),
		SubjectToken:       filepath.Join(outputDir, "subject-token.txt"),
		AccessToken:        filepath.Join(outputDir, "access-token.txt"),
		JavaTruststore:     filepath.Join(outputDir, "truststore.p12"),
		TruststorePassword: truststorePassword,
		TLSDNSNames:        dnsNames,
		TLSIPAddresses:     ipStrings(ipAddresses),
	}

	account := serviceAccountCredential{
		Type: "service_account", ProjectID: "bqemu-local", PrivateKeyID: privateKeyID,
		PrivateKey:  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: mustPKCS8(accountKey)})),
		ClientEmail: "bqemu-local@bqemu-local.iam.gserviceaccount.com", ClientID: "100000000000000000001",
		AuthURI: "https://accounts.google.com/o/oauth2/auth", TokenURI: manifest.OAuthTokenURL,
		AuthProviderCertURL:  "https://www.googleapis.com/oauth2/v1/certs",
		ClientCertificateURL: "https://www.googleapis.com/robot/v1/metadata/x509/bqemu-local%40bqemu-local.iam.gserviceaccount.com",
		UniverseDomain:       "googleapis.com",
	}
	authorized := authorizedUserCredential{
		Type: "authorized_user", ClientID: clientID, ClientSecret: clientSecret,
		RefreshToken: refreshToken, TokenURI: manifest.OAuthTokenURL, QuotaProject: "bqemu-local",
	}
	external := externalAccountCredential{
		Type:             "external_account",
		Audience:         "//iam.googleapis.com/projects/100000000000/locations/global/workloadIdentityPools/bqemu/providers/local",
		SubjectTokenType: "urn:ietf:params:oauth:token-type:jwt",
		TokenURL:         manifest.STSTokenURL,
		TokenInfoURL:     baseURL + "/introspect",
		CredentialSource: map[string]string{"file": manifest.SubjectToken},
		QuotaProject:     "bqemu-local",
	}

	stagingDir, err := os.MkdirTemp(parentDir, stagingPrefix(filepath.Base(outputDir)))
	if err != nil {
		return Manifest{}, fmt.Errorf("create staging directory: %w", err)
	}
	defer os.RemoveAll(stagingDir)
	if err := os.Chmod(stagingDir, 0o700); err != nil {
		return Manifest{}, fmt.Errorf("protect staging directory: %w", err)
	}
	if err := writeFile(filepath.Join(stagingDir, generationMarker), []byte(ManifestVersion+"\n"), 0o600, false); err != nil {
		return Manifest{}, err
	}

	writes := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644},
		{"server.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), 0o644},
		{"server-key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: mustPKCS8(serverKey)}), 0o600},
		{"service-account.json", mustJSON(account), 0o600},
		{"authorized-user.json", mustJSON(authorized), 0o600},
		{"wif.json", mustJSON(external), 0o600},
		{"subject-token.txt", []byte(subjectToken), 0o600},
		{"access-token.txt", []byte(accessToken + "\n"), 0o600},
	}
	for _, write := range writes {
		if err := writeFile(filepath.Join(stagingDir, write.name), write.data, write.mode, false); err != nil {
			return Manifest{}, err
		}
		if err := runGenerationHook(options, "staged-"+write.name); err != nil {
			return Manifest{}, err
		}
	}
	stagedCA := filepath.Join(stagingDir, "ca.pem")
	stagedTruststore := filepath.Join(stagingDir, "truststore.p12")
	if err := createJavaTruststore(keytool, stagedTruststore, stagedCA, false); err != nil {
		return Manifest{}, err
	}
	if err := runGenerationHook(options, "staged-truststore.p12"); err != nil {
		return Manifest{}, err
	}
	if err := writeFile(filepath.Join(stagingDir, "manifest.json"), mustJSON(manifest), 0o600, false); err != nil {
		return Manifest{}, err
	}
	if err := validateStagedGeneration(stagingDir, manifest, keytool); err != nil {
		return Manifest{}, err
	}
	if err := runGenerationHook(options, "before-install"); err != nil {
		return Manifest{}, err
	}
	if err := installGeneration(stagingDir, outputDir, replacing); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func LoadManifest(path string) (Manifest, error) {
	var manifest Manifest
	if err := readStrictJSON(path, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	if manifest.Version != ManifestVersion || manifest.BaseURL == "" || manifest.IssuerListen == "" ||
		manifest.OAuthTokenURL == "" ||
		manifest.STSTokenURL == "" || manifest.OAuthProxyURL == "" ||
		manifest.CACertificate == "" || manifest.ServerCertificate == "" ||
		manifest.ServerKey == "" || manifest.ServiceAccount == "" || manifest.AuthorizedUser == "" ||
		manifest.ExternalAccount == "" || manifest.SubjectToken == "" || manifest.AccessToken == "" ||
		manifest.JavaTruststore == "" || manifest.TruststorePassword != truststorePassword ||
		len(manifest.TLSDNSNames) == 0 || len(manifest.TLSIPAddresses) == 0 {
		return Manifest{}, errors.New("manifest is incomplete or has an unsupported version")
	}
	return manifest, nil
}

func inspectOutputDirectory(path string, force bool) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect output directory: %w", err)
	}
	if !force {
		return false, errors.New("output directory already exists; use --force to replace it")
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return false, errors.New("replace output directory: target must be a generated directory")
	}
	markerPath := filepath.Join(path, generationMarker)
	markerInfo, err := os.Lstat(markerPath)
	if err != nil || !markerInfo.Mode().IsRegular() {
		return false, errors.New("replace output directory: generation marker is missing")
	}
	marker, err := readBoundedFile(markerPath)
	if err != nil || string(marker) != ManifestVersion+"\n" {
		return false, errors.New("replace output directory: generation marker is invalid")
	}
	return true, nil
}

func cleanupStaleGenerations(parentDir, outputBase string) error {
	entries, err := os.ReadDir(parentDir)
	if err != nil {
		return fmt.Errorf("find stale staging directories: %w", err)
	}
	prefix := stagingPrefix(outputBase)
	removed := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		candidate := filepath.Join(parentDir, entry.Name())
		info, statErr := os.Lstat(candidate)
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		markerInfo, markerErr := os.Lstat(filepath.Join(candidate, generationMarker))
		if markerErr != nil || !markerInfo.Mode().IsRegular() {
			continue
		}
		marker, readErr := readBoundedFile(filepath.Join(candidate, generationMarker))
		if readErr != nil || string(marker) != ManifestVersion+"\n" {
			continue
		}
		if err := os.RemoveAll(candidate); err != nil {
			return fmt.Errorf("remove stale staging directory: %w", err)
		}
		removed = true
	}
	if removed {
		if err := syncDirectory(parentDir); err != nil {
			return fmt.Errorf("sync stale staging cleanup: %w", err)
		}
	}
	return nil
}

func stagingPrefix(outputBase string) string {
	return "." + normalizedOutputBase(outputBase) + stagingInfix
}

func generationLockName(outputBase string) string {
	return "." + normalizedOutputBase(outputBase) + ".lock"
}

func normalizedOutputBase(outputBase string) string {
	normalized := strings.TrimLeft(outputBase, ".")
	if normalized == "" {
		return "credentials"
	}
	return normalized
}

func runGenerationHook(options GenerateOptions, stage string) error {
	if options.testHook == nil {
		return nil
	}
	if err := options.testHook(stage); err != nil {
		return fmt.Errorf("generation interrupted at %s: %w", stage, err)
	}
	return nil
}

func validateStagedGeneration(stagingDir string, expected Manifest, keytool string) error {
	expectedModes := map[string]os.FileMode{
		generationMarker:       0o600,
		"manifest.json":        0o600,
		"ca.pem":               0o644,
		"server.pem":           0o644,
		"server-key.pem":       0o600,
		"service-account.json": 0o600,
		"authorized-user.json": 0o600,
		"wif.json":             0o600,
		"subject-token.txt":    0o600,
		"access-token.txt":     0o600,
		"truststore.p12":       0o600,
	}
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		return fmt.Errorf("read staged generation: %w", err)
	}
	actualNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		actualNames = append(actualNames, entry.Name())
	}
	sort.Strings(actualNames)
	expectedNames := make([]string, 0, len(expectedModes))
	for name := range expectedModes {
		expectedNames = append(expectedNames, name)
	}
	sort.Strings(expectedNames)
	if !reflect.DeepEqual(actualNames, expectedNames) {
		return fmt.Errorf(
			"staged generation file set is incomplete: actual_count=%d expected_count=%d",
			len(actualNames),
			len(expectedNames),
		)
	}
	for name, mode := range expectedModes {
		path := filepath.Join(stagingDir, name)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("staged %s is not a regular file", name)
		}
		if info.Mode().Perm() != mode {
			return fmt.Errorf("staged %s has an invalid file mode", name)
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open staged %s: %w", name, err)
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil || closeErr != nil {
			return fmt.Errorf("sync staged %s: %w", name, errors.Join(syncErr, closeErr))
		}
	}

	var observed Manifest
	if err := readStrictJSON(filepath.Join(stagingDir, "manifest.json"), &observed); err != nil {
		return fmt.Errorf("validate staged manifest: %w", err)
	}
	if !reflect.DeepEqual(observed, expected) {
		return errors.New("staged manifest does not match the generated file set")
	}
	var account serviceAccountCredential
	if err := readStrictJSON(filepath.Join(stagingDir, "service-account.json"), &account); err != nil {
		return errors.New("validate staged service-account credential")
	}
	var user authorizedUserCredential
	if err := readStrictJSON(filepath.Join(stagingDir, "authorized-user.json"), &user); err != nil {
		return errors.New("validate staged authorized-user credential")
	}
	var external externalAccountCredential
	if err := readStrictJSON(filepath.Join(stagingDir, "wif.json"), &external); err != nil {
		return errors.New("validate staged external-account credential")
	}
	if account.TokenURI != expected.OAuthTokenURL || user.TokenURI != expected.OAuthTokenURL ||
		external.TokenURL != expected.STSTokenURL ||
		external.CredentialSource["file"] != expected.SubjectToken {
		return errors.New("staged credential endpoints do not match the manifest")
	}

	if _, err := tls.LoadX509KeyPair(
		filepath.Join(stagingDir, "server.pem"),
		filepath.Join(stagingDir, "server-key.pem"),
	); err != nil {
		return errors.New("validate staged TLS certificate and key")
	}
	caPEM, err := readBoundedFile(filepath.Join(stagingDir, "ca.pem"))
	if err != nil {
		return errors.New("validate staged CA certificate")
	}
	serverPEM, err := readBoundedFile(filepath.Join(stagingDir, "server.pem"))
	if err != nil {
		return errors.New("validate staged server certificate")
	}
	caBlock, _ := pem.Decode(caPEM)
	serverBlock, _ := pem.Decode(serverPEM)
	if caBlock == nil || serverBlock == nil {
		return errors.New("validate staged certificate chain")
	}
	caCertificate, caErr := x509.ParseCertificate(caBlock.Bytes)
	serverCertificate, serverErr := x509.ParseCertificate(serverBlock.Bytes)
	if caErr != nil || serverErr != nil || serverCertificate.CheckSignatureFrom(caCertificate) != nil {
		return errors.New("validate staged certificate chain")
	}

	for _, name := range []string{"subject-token.txt", "access-token.txt"} {
		value, err := readBoundedFile(filepath.Join(stagingDir, name))
		trimmed := strings.TrimSpace(string(value))
		if err != nil || trimmed == "" || strings.ContainsAny(trimmed, " \t\r\n") {
			return fmt.Errorf("validate staged %s", name)
		}
	}
	if err := verifyJavaTruststore(keytool, filepath.Join(stagingDir, "truststore.p12")); err != nil {
		return err
	}
	if err := syncDirectory(stagingDir); err != nil {
		return fmt.Errorf("sync staged generation: %w", err)
	}
	return nil
}

func verifyJavaTruststore(keytool, truststore string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(
		ctx,
		keytool,
		"-list",
		"-alias", "bqemu-local-ca",
		"-keystore", truststore,
		"-storetype", "PKCS12",
		"-storepass", truststorePassword,
	)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return errors.New("keytool timed out while validating truststore.p12")
		}
		return errors.New("keytool failed while validating truststore.p12")
	}
	return nil
}

func installGeneration(stagingDir, outputDir string, replacing bool) error {
	parentDir := filepath.Dir(outputDir)
	if !replacing {
		if err := os.Rename(stagingDir, outputDir); err != nil {
			return fmt.Errorf("install generated directory: %w", err)
		}
		if err := syncDirectory(parentDir); err != nil {
			rollbackErr := os.Rename(outputDir, stagingDir)
			syncErr := syncDirectory(parentDir)
			return fmt.Errorf(
				"sync generated directory: %w",
				errors.Join(err, rollbackErr, syncErr),
			)
		}
		return nil
	}

	if err := atomicSwapDirectories(stagingDir, outputDir); err != nil {
		return fmt.Errorf("atomically replace generated directory: %w", err)
	}
	if err := syncDirectory(parentDir); err != nil {
		rollbackErr := atomicSwapDirectories(stagingDir, outputDir)
		syncErr := syncDirectory(parentDir)
		return fmt.Errorf(
			"sync replaced generated directory: %w",
			errors.Join(err, rollbackErr, syncErr),
		)
	}
	if err := os.RemoveAll(stagingDir); err == nil {
		_ = syncDirectory(parentDir)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func makeCA(now time.Time, key *rsa.PrivateKey) ([]byte, *x509.Certificate, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "BQEMU Local CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(5, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true, IsCA: true, MaxPathLenZero: true,
		SubjectKeyId: publicKeyID(&key.PublicKey),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create CA certificate: %w", err)
	}
	return der, template, nil
}

func makeServerCertificate(
	now time.Time,
	ca *x509.Certificate,
	caKey *rsa.PrivateKey,
	publicKey *rsa.PublicKey,
	dnsNames []string,
	ipAddresses []net.IP,
) ([]byte, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: dnsNames[0]},
		DNSNames: dnsNames, IPAddresses: ipAddresses,
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(2, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		SubjectKeyId: publicKeyID(publicKey),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, publicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create server certificate: %w", err)
	}
	return der, nil
}

func publicKeyID(publicKey *rsa.PublicKey) []byte {
	digest := sha256.Sum256(x509.MarshalPKCS1PublicKey(publicKey))
	return digest[:]
}

func certificateNames(baseHost string, additionalDNSNames []string) ([]string, []net.IP, error) {
	if len(additionalDNSNames) > 15 {
		return nil, nil, errors.New("at most 15 additional TLS DNS names are allowed")
	}
	dnsNames := []string{"localhost", "oauth2.googleapis.com", "accounts.google.com"}
	seen := map[string]struct{}{
		"localhost": {}, "oauth2.googleapis.com": {}, "accounts.google.com": {},
	}
	for _, candidate := range additionalDNSNames {
		name := strings.ToLower(strings.TrimSpace(candidate))
		if !validDNSName(name) {
			return nil, nil, errors.New("TLS DNS names must be bounded host names without wildcards")
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		dnsNames = append(dnsNames, name)
	}
	ipAddresses := []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	if baseIP := net.ParseIP(baseHost); baseIP != nil && baseIP.IsLoopback() {
		found := false
		for _, existing := range ipAddresses {
			found = found || existing.Equal(baseIP)
		}
		if !found {
			ipAddresses = append(ipAddresses, baseIP)
		}
	}
	return dnsNames, ipAddresses, nil
}

func validDNSName(name string) bool {
	if name == "" || len(name) > 253 || strings.Contains(name, "*") {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') &&
				(character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func ipStrings(addresses []net.IP) []string {
	result := make([]string, len(addresses))
	for index, address := range addresses {
		result[index] = address.String()
	}
	return result
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate certificate serial: %w", err)
	}
	return serial, nil
}

func randomText(bytes int) (string, error) {
	buffer := make([]byte, bytes)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate credential: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func mustPKCS8(key *rsa.PrivateKey) []byte {
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}
	return encoded
}

func mustJSON(value any) []byte {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(encoded, '\n')
}

func writeFile(path string, data []byte, mode os.FileMode, force bool) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !force {
		flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	}
	file, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return fmt.Errorf("protect %s: %w", filepath.Base(path), err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
	}
	return nil
}

func resolveKeytool(configured string) (string, error) {
	name := configured
	if name == "" {
		name = "keytool"
	}
	path, err := exec.LookPath(name)
	if err != nil {
		return "", errors.New("keytool is required to generate truststore.p12")
	}
	return path, nil
}

func createJavaTruststore(keytool, target, caPath string, force bool) error {
	temporary, err := os.CreateTemp(filepath.Dir(target), ".bqemu-truststore-*.p12")
	if err != nil {
		return fmt.Errorf("prepare Java truststore: %w", err)
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		os.Remove(temporaryPath)
		return fmt.Errorf("prepare Java truststore: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("prepare Java truststore: %w", err)
	}
	defer os.Remove(temporaryPath)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(
		ctx,
		keytool,
		"-importcert",
		"-noprompt",
		"-alias", "bqemu-local-ca",
		"-file", caPath,
		"-keystore", temporaryPath,
		"-storetype", "PKCS12",
		"-storepass", truststorePassword,
	)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return errors.New("keytool timed out while generating truststore.p12")
		}
		return errors.New("keytool failed while generating truststore.p12")
	}
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return fmt.Errorf("protect truststore.p12: %w", err)
	}
	if force {
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("replace truststore.p12: %w", err)
		}
	}
	if err := os.Rename(temporaryPath, target); err != nil {
		return fmt.Errorf("install truststore.p12: %w", err)
	}
	return nil
}

func readStrictJSON(path string, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxFixtureFileBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values are not allowed")
	}
	if info, err := file.Stat(); err != nil || info.Size() > maxFixtureFileBytes {
		return errors.New("JSON file exceeds the size limit")
	}
	return nil
}

func readBoundedFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maxFixtureFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(contents) > maxFixtureFileBytes {
		return nil, errors.New("file exceeds the size limit")
	}
	return contents, nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
