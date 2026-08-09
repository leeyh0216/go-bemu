package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

const (
	GrantTypeRefreshToken  = "refresh_token"
	GrantTypeJWTBearer     = "urn:ietf:params:oauth:grant-type:jwt-bearer"
	GrantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"

	TokenTypeAccessToken = "urn:ietf:params:oauth:token-type:access_token"
	BearerTokenType      = "Bearer"
)

type Endpoint string

const (
	EndpointOAuthToken Endpoint = "oauth-token"
	EndpointSTS        Endpoint = "sts-token-exchange"
)

func (e Endpoint) Valid() bool { return e == EndpointOAuthToken || e == EndpointSTS }

type GrantKind string

const (
	GrantRefreshToken  GrantKind = "refresh_token"
	GrantJWTBearer     GrantKind = "jwt_bearer"
	GrantTokenExchange GrantKind = "token_exchange"
)

func (g GrantKind) Valid() bool {
	return g == GrantRefreshToken || g == GrantJWTBearer || g == GrantTokenExchange
}

type ReplayKind string

const ReplayJWTAssertion ReplayKind = "jwt_assertion"

func (k ReplayKind) Valid() bool { return k == ReplayJWTAssertion }

// Subject contains only a digest of the local identity. Scopes are protocol
// output, not credential material, but every caller must still bound and
// validate them before a response or store mutation.
type Subject struct {
	PrincipalDigest string
	Scopes          []string
}

func (s Subject) Valid() bool { return ValidDigest(s.PrincipalDigest) }

func (s Subject) Clone() Subject {
	s.Scopes = append([]string(nil), s.Scopes...)
	return s
}

type IssuedToken struct {
	TokenDigest     string
	PrincipalDigest string
	Grant           GrantKind
	ScopeDigest     string
	ScopeCount      int
	IssuedAt        time.Time
	ExpiresAt       time.Time
}

func (t IssuedToken) Valid() bool {
	return ValidDigest(t.TokenDigest) &&
		ValidDigest(t.PrincipalDigest) &&
		ValidDigest(t.ScopeDigest) &&
		t.Grant.Valid() && t.ScopeCount >= 0 && !t.IssuedAt.IsZero() &&
		t.ExpiresAt.After(t.IssuedAt)
}

func (t IssuedToken) SafeLogAttrs() []any {
	grant := t.Grant
	if !grant.Valid() {
		grant = ""
	}
	return []any{
		"model_version", ModelVersion,
		"grant", string(grant),
		"token_digest", validDigestOr(t.TokenDigest, "invalid"),
		"principal_digest", validDigestOr(t.PrincipalDigest, "invalid"),
		"scope_digest", validDigestOr(t.ScopeDigest, "invalid"),
		"scope_count", nonNegative(t.ScopeCount),
		"expires_in_seconds", nonNegative64(int64(t.ExpiresAt.Sub(t.IssuedAt) / time.Second)),
	}
}

type ReplayMarker struct {
	Kind      ReplayKind
	Digest    string
	ExpiresAt time.Time
}

func (r ReplayMarker) Valid(issuedAt time.Time) bool {
	return r.Kind.Valid() && ValidDigest(r.Digest) && r.ExpiresAt.After(issuedAt)
}

// TokenResponse matches RFC 6749 section 5.1 and RFC 8693 section 2.2.1.
// IssuedTokenType is populated only for the STS exchange endpoint.
type TokenResponse struct {
	AccessToken     string `json:"access_token"`
	TokenType       string `json:"token_type"`
	ExpiresIn       int64  `json:"expires_in"`
	Scope           string `json:"scope,omitempty"`
	IssuedTokenType string `json:"issued_token_type,omitempty"`
}

func ScopeDigest(scopes []string) string {
	return Digest([]byte(strings.Join(scopes, " ")))
}

func Digest(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func ValidDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

// ValidScopeToken applies the scope-token grammar from RFC 6749 section 3.3:
// visible ASCII except quote, backslash, and space (which is the delimiter).
// https://www.rfc-editor.org/rfc/rfc6749.html#section-3.3
func ValidScopeToken(scope string) bool {
	if scope == "" {
		return false
	}
	for index := 0; index < len(scope); index++ {
		character := scope[index]
		if character != 0x21 && (character < 0x23 || character > 0x5b) &&
			(character < 0x5d || character > 0x7e) {
			return false
		}
	}
	return true
}

func validDigestOr(value, fallback string) string {
	if ValidDigest(value) {
		return value
	}
	return fallback
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func nonNegative64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
