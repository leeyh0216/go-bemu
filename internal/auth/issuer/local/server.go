package local

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	maxTokenRequestBytes = 64 << 10
	grantRefreshToken    = "refresh_token"
	grantJWTBearer       = "urn:ietf:params:oauth:grant-type:jwt-bearer"
	grantTokenExchange   = "urn:ietf:params:oauth:grant-type:token-exchange"
	accessTokenType      = "urn:ietf:params:oauth:token-type:access_token"
	googleOAuthAudience  = "https://oauth2.googleapis.com/token"
	legacyOAuthAudience  = "https://accounts.google.com/o/oauth2/token"
	issuedTokenPrefix    = "bqemu-local-issued-"
)

type tokenResponse struct {
	AccessToken     string `json:"access_token"`
	TokenType       string `json:"token_type"`
	ExpiresIn       int64  `json:"expires_in"`
	Scope           string `json:"scope,omitempty"`
	IssuedTokenType string `json:"issued_token_type,omitempty"`
}

type Server struct {
	manifest     Manifest
	account      serviceAccountCredential
	user         authorizedUserCredential
	external     externalAccountCredential
	subjectToken string
	accountKey   *rsa.PrivateKey
	logger       *slog.Logger
	tokensMu     sync.Mutex
	tokens       map[[sha256.Size]byte]time.Time
}

func NewServer(manifestPath string, logger *slog.Logger) (*Server, error) {
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	server := &Server{
		manifest: manifest,
		logger:   logger,
		tokens:   make(map[[sha256.Size]byte]time.Time),
	}
	if err := readStrictJSON(manifest.ServiceAccount, &server.account); err != nil {
		return nil, errors.New("read service-account credential")
	}
	if err := readStrictJSON(manifest.AuthorizedUser, &server.user); err != nil {
		return nil, errors.New("read authorized-user credential")
	}
	if err := readStrictJSON(manifest.ExternalAccount, &server.external); err != nil {
		return nil, errors.New("read external-account credential")
	}
	subjectToken, err := readBoundedFile(manifest.SubjectToken)
	if err != nil {
		return nil, errors.New("read subject-token credential")
	}
	server.subjectToken = strings.TrimSpace(string(subjectToken))
	block, _ := pem.Decode([]byte(server.account.PrivateKey))
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("service-account private key is invalid")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("service-account private key is invalid")
	}
	var ok bool
	server.accountKey, ok = key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("service-account private key is not RSA")
	}
	if server.account.TokenURI != manifest.OAuthTokenURL || server.user.TokenURI != manifest.OAuthTokenURL ||
		server.external.TokenURL != manifest.STSTokenURL ||
		server.external.TokenInfoURL != manifest.BaseURL+"/introspect" ||
		server.external.CredentialSource["file"] != manifest.SubjectToken {
		return nil, errors.New("credential endpoint does not match manifest")
	}
	return server, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /oauth/token", s.oauthToken)
	mux.HandleFunc("POST /token", s.oauthToken)
	mux.HandleFunc("POST /o/oauth2/token", s.oauthToken)
	mux.HandleFunc("POST /sts/token", s.stsToken)
	mux.HandleFunc("POST /introspect", s.introspectToken)
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		io.WriteString(writer, "{\"status\":\"ok\"}\n")
	})
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		var requestBody []byte
		var requestBodyError error
		if request.Body != nil {
			requestBody, requestBodyError = io.ReadAll(io.LimitReader(request.Body, maxTokenRequestBytes+1))
			_ = request.Body.Close()
			request.Body = io.NopCloser(bytes.NewReader(requestBody))
		}
		statusWriter := &responseStatusWriter{ResponseWriter: writer, status: http.StatusOK}
		mux.ServeHTTP(statusWriter, request)
		s.logger.Info("local credential endpoint request",
			"method", request.Method, "path", request.URL.Path, "query", request.URL.RawQuery,
			"headers", request.Header, "request_body", string(requestBody), "request_body_error", requestBodyError,
			"status", statusWriter.status, "response_headers", statusWriter.Header(),
			"response_body", statusWriter.body.String(),
			"duration_ms", time.Since(started).Milliseconds())
	})
}

func (s *Server) HTTPServer(address string) (*http.Server, error) {
	if address == "" {
		var err error
		address, err = s.IssuerListenAddress()
		if err != nil {
			return nil, err
		}
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil || !loopbackHost(host) {
		return nil, errors.New("issuer address must be a loopback host and port")
	}
	if err := issuerAddressMismatch(address, s.manifest.BaseURL); err != nil {
		return nil, err
	}
	return &http.Server{
		Addr: address, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10,
	}, nil
}

func (s *Server) IssuerListenAddress() (string, error) {
	if !validLoopbackAddress(s.manifest.IssuerListen) {
		return "", errors.New("manifest issuer listen address must be a loopback host and port")
	}
	if err := issuerAddressMismatch(s.manifest.IssuerListen, s.manifest.BaseURL); err != nil {
		return "", err
	}
	return s.manifest.IssuerListen, nil
}

func (s *Server) ServeTLS(httpServer *http.Server) error {
	if httpServer == nil {
		return errors.New("HTTP server is required")
	}
	return httpServer.ListenAndServeTLS(s.manifest.ServerCertificate, s.manifest.ServerKey)
}

func (s *Server) oauthToken(writer http.ResponseWriter, request *http.Request) {
	form, err := readForm(writer, request)
	if err != nil {
		return
	}
	switch form.Get("grant_type") {
	case grantRefreshToken:
		if !allowedFields(form, "grant_type", "client_id", "client_secret", "refresh_token", "scope") ||
			!constantEqual(form.Get("client_id"), s.user.ClientID) ||
			!constantEqual(form.Get("client_secret"), s.user.ClientSecret) ||
			!constantEqual(form.Get("refresh_token"), s.user.RefreshToken) {
			writeOAuthError(writer, http.StatusBadRequest, "invalid_grant")
			return
		}
		s.writeToken(writer, false, form.Get("scope"))
	case grantJWTBearer:
		if !allowedFields(form, "grant_type", "assertion") || s.verifyAssertion(form.Get("assertion"), time.Now()) != nil {
			writeOAuthError(writer, http.StatusBadRequest, "invalid_grant")
			return
		}
		s.writeToken(writer, false, "")
	default:
		writeOAuthError(writer, http.StatusBadRequest, "unsupported_grant_type")
	}
}

func (s *Server) stsToken(writer http.ResponseWriter, request *http.Request) {
	form, err := readForm(writer, request)
	if err != nil {
		return
	}
	if !allowedFields(form, "grant_type", "audience", "resource", "scope", "requested_token_type", "subject_token", "subject_token_type", "actor_token", "actor_token_type", "options") ||
		form.Get("grant_type") != grantTokenExchange ||
		form.Get("requested_token_type") != accessTokenType ||
		form.Get("subject_token_type") != s.external.SubjectTokenType ||
		form.Get("audience") != s.external.Audience ||
		!constantEqual(form.Get("subject_token"), s.subjectToken) {
		writeOAuthError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	s.writeToken(writer, true, form.Get("scope"))
}

func (s *Server) writeToken(writer http.ResponseWriter, sts bool, scope string) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		writeOAuthError(writer, http.StatusInternalServerError, "server_error")
		return
	}
	accessToken := issuedTokenPrefix + base64.RawURLEncoding.EncodeToString(tokenBytes)
	if !s.rememberToken(accessToken, time.Now().Add(time.Hour)) {
		writeOAuthError(writer, http.StatusInternalServerError, "server_error")
		return
	}
	response := tokenResponse{
		AccessToken: accessToken,
		TokenType:   "Bearer", ExpiresIn: 3600, Scope: scope,
	}
	if sts {
		response.IssuedTokenType = accessTokenType
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Pragma", "no-cache")
	json.NewEncoder(writer).Encode(response)
}

func (s *Server) introspectToken(writer http.ResponseWriter, request *http.Request) {
	form, err := readForm(writer, request)
	if err != nil {
		return
	}
	if !allowedFields(form, "token", "token_type_hint", "client_id", "client_secret") ||
		form.Get("token") == "" ||
		(form.Get("token_type_hint") != "" && form.Get("token_type_hint") != accessTokenType) {
		writeOAuthError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	expiresAt, active := s.activeToken(form.Get("token"), time.Now())
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	response := map[string]any{"active": active}
	if active {
		response["username"] = "bqemu-local-external-account"
		response["token_type"] = "Bearer"
		response["exp"] = expiresAt.Unix()
	}
	_ = json.NewEncoder(writer).Encode(response)
}

func (s *Server) rememberToken(token string, expiresAt time.Time) bool {
	digest := sha256.Sum256([]byte(token))
	now := time.Now()
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()
	for existing, expiration := range s.tokens {
		if !expiration.After(now) {
			delete(s.tokens, existing)
		}
	}
	if len(s.tokens) >= 1024 {
		return false
	}
	s.tokens[digest] = expiresAt
	return true
}

func (s *Server) activeToken(token string, now time.Time) (time.Time, bool) {
	digest := sha256.Sum256([]byte(token))
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()
	expiresAt, ok := s.tokens[digest]
	if !ok || !expiresAt.After(now) {
		delete(s.tokens, digest)
		return time.Time{}, false
	}
	return expiresAt, true
}

func (s *Server) verifyAssertion(assertion string, now time.Time) error {
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		return errors.New("malformed assertion")
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
		Type      string `json:"typ"`
	}
	var claims struct {
		Issuer    string `json:"iss"`
		Subject   string `json:"sub"`
		Audience  string `json:"aud"`
		Scope     string `json:"scope"`
		IssuedAt  int64  `json:"iat"`
		ExpiresAt int64  `json:"exp"`
	}
	if decodeJWTPart(parts[0], &header) != nil || decodeJWTPart(parts[1], &claims) != nil ||
		header.Algorithm != "RS256" || header.KeyID != s.account.PrivateKeyID ||
		claims.Issuer != s.account.ClientEmail || !s.allowedJWTAudience(claims.Audience) ||
		claims.Scope == "" || claims.IssuedAt > now.Add(time.Minute).Unix() ||
		claims.ExpiresAt <= now.Unix() || claims.ExpiresAt-claims.IssuedAt > 3600 {
		return errors.New("invalid assertion")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return errors.New("invalid assertion")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&s.accountKey.PublicKey, crypto.SHA256, digest[:], signature); err != nil {
		return errors.New("invalid assertion")
	}
	return nil
}

func (s *Server) allowedJWTAudience(audience string) bool {
	return audience == s.manifest.OAuthTokenURL ||
		audience == googleOAuthAudience ||
		audience == legacyOAuthAudience
}

func decodeJWTPart(part string, destination any) error {
	decoded, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(decoded)))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

func readForm(writer http.ResponseWriter, request *http.Request) (url.Values, error) {
	mediaType, _, mediaTypeError := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if mediaTypeError != nil || mediaType != "application/x-www-form-urlencoded" {
		writeOAuthError(writer, http.StatusBadRequest, "invalid_request")
		return nil, errors.New("invalid content type")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxTokenRequestBytes+1))
	if err != nil || len(body) == 0 || len(body) > maxTokenRequestBytes {
		writeOAuthError(writer, http.StatusBadRequest, "invalid_request")
		return nil, errors.New("invalid body")
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		writeOAuthError(writer, http.StatusBadRequest, "invalid_request")
		return nil, errors.New("invalid form")
	}
	for _, values := range form {
		if len(values) != 1 {
			writeOAuthError(writer, http.StatusBadRequest, "invalid_request")
			return nil, errors.New("duplicate field")
		}
	}
	return form, nil
}

func allowedFields(form url.Values, fields ...string) bool {
	allowed := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		allowed[field] = struct{}{}
	}
	for field := range form {
		if _, ok := allowed[field]; !ok {
			return false
		}
	}
	return true
}

func constantEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func writeOAuthError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	json.NewEncoder(writer).Encode(map[string]string{"error": string(code)})
}

type responseStatusWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (writer *responseStatusWriter) WriteHeader(status int) {
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *responseStatusWriter) Write(payload []byte) (int, error) {
	remaining := maxTokenRequestBytes - writer.body.Len()
	if remaining > 0 {
		if len(payload) < remaining {
			remaining = len(payload)
		}
		_, _ = writer.body.Write(payload[:remaining])
	}
	return writer.ResponseWriter.Write(payload)
}

func Shutdown(ctx context.Context, server *http.Server) error {
	if server == nil {
		return fmt.Errorf("HTTP server is required")
	}
	return server.Shutdown(ctx)
}
