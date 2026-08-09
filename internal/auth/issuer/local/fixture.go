// Package local creates and serves credentials for local client-library tests.
// It does not participate in BQEMU request authentication or authorization.
package local

import (
	"crypto/rand"
	"crypto/rsa"
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
	"path/filepath"
	"strings"
	"time"
)

const (
	ManifestVersion     = "auth-fixture.bqemu.dev/v1alpha1"
	defaultBaseURL      = "https://localhost:9052"
	maxFixtureFileBytes = 1 << 20
)

type GenerateOptions struct {
	OutputDir string
	BaseURL   string
	Force     bool
	Now       time.Time
}

type Manifest struct {
	Version           string `json:"version"`
	BaseURL           string `json:"base_url"`
	OAuthTokenURL     string `json:"oauth_token_url"`
	STSTokenURL       string `json:"sts_token_url"`
	CACertificate     string `json:"ca_certificate"`
	ServerCertificate string `json:"server_certificate"`
	ServerKey         string `json:"server_key"`
	ServiceAccount    string `json:"service_account"`
	AuthorizedUser    string `json:"authorized_user"`
	ExternalAccount   string `json:"external_account"`
	SubjectToken      string `json:"subject_token"`
	TLSName           string `json:"tls_name"`
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
	if err != nil || parsedBaseURL.Scheme != "https" || parsedBaseURL.Host == "" || parsedBaseURL.Path != "" {
		return Manifest{}, errors.New("base URL must be an HTTPS origin without a path")
	}
	if !loopbackHost(parsedBaseURL.Hostname()) {
		return Manifest{}, errors.New("base URL host must resolve to localhost or a loopback address")
	}

	outputDir, err := filepath.Abs(options.OutputDir)
	if err != nil {
		return Manifest{}, fmt.Errorf("resolve output directory: %w", err)
	}
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		return Manifest{}, fmt.Errorf("create output directory: %w", err)
	}
	if err := os.Chmod(outputDir, 0o700); err != nil {
		return Manifest{}, fmt.Errorf("protect output directory: %w", err)
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
	serverDER, err := makeServerCertificate(now, caCertificate, caKey, &serverKey.PublicKey)
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
	privateKeyID, err := randomText(16)
	if err != nil {
		return Manifest{}, err
	}

	manifest := Manifest{
		Version:           ManifestVersion,
		BaseURL:           baseURL,
		OAuthTokenURL:     baseURL + "/oauth/token",
		STSTokenURL:       baseURL + "/sts/token",
		CACertificate:     filepath.Join(outputDir, "ca.pem"),
		ServerCertificate: filepath.Join(outputDir, "server.pem"),
		ServerKey:         filepath.Join(outputDir, "server-key.pem"),
		ServiceAccount:    filepath.Join(outputDir, "service-account.json"),
		AuthorizedUser:    filepath.Join(outputDir, "authorized-user.json"),
		ExternalAccount:   filepath.Join(outputDir, "wif.json"),
		SubjectToken:      filepath.Join(outputDir, "subject-token.txt"),
		TLSName:           "localhost",
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
		CredentialSource: map[string]string{"file": manifest.SubjectToken},
		QuotaProject:     "bqemu-local",
	}

	writes := []struct {
		path string
		data []byte
		mode os.FileMode
	}{
		{manifest.CACertificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644},
		{manifest.ServerCertificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), 0o644},
		{manifest.ServerKey, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: mustPKCS8(serverKey)}), 0o600},
		{manifest.ServiceAccount, mustJSON(account), 0o600},
		{manifest.AuthorizedUser, mustJSON(authorized), 0o600},
		{manifest.ExternalAccount, mustJSON(external), 0o600},
		{manifest.SubjectToken, []byte(subjectToken + "\n"), 0o600},
	}
	for _, write := range writes {
		if err := writeFile(write.path, write.data, write.mode, options.Force); err != nil {
			return Manifest{}, err
		}
	}
	manifestPath := filepath.Join(outputDir, "manifest.json")
	if err := writeFile(manifestPath, mustJSON(manifest), 0o600, options.Force); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func LoadManifest(path string) (Manifest, error) {
	var manifest Manifest
	if err := readStrictJSON(path, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	if manifest.Version != ManifestVersion || manifest.BaseURL == "" || manifest.OAuthTokenURL == "" ||
		manifest.STSTokenURL == "" || manifest.CACertificate == "" || manifest.ServerCertificate == "" ||
		manifest.ServerKey == "" || manifest.ServiceAccount == "" || manifest.AuthorizedUser == "" ||
		manifest.ExternalAccount == "" || manifest.SubjectToken == "" || manifest.TLSName != "localhost" {
		return Manifest{}, errors.New("manifest is incomplete or has an unsupported version")
	}
	return manifest, nil
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
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create CA certificate: %w", err)
	}
	return der, template, nil
}

func makeServerCertificate(now time.Time, ca *x509.Certificate, caKey *rsa.PrivateKey, publicKey *rsa.PublicKey) ([]byte, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(2, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, publicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create server certificate: %w", err)
	}
	return der, nil
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
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
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
