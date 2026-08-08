package application

import (
	"strings"
	"unicode/utf8"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
)

// RFC 6750 defines credentials as "Bearer" 1*SP b64token. ABNF literal
// strings are case-insensitive, SP is ASCII space (not a tab), and query/body
// token transports are deliberately outside this parser.
// https://www.rfc-editor.org/rfc/rfc6750.html#section-2.1
// Authorization field semantics: https://www.rfc-editor.org/rfc/rfc9110.html#section-11.6.2
type parsedAuthorization struct {
	token               []byte
	authorizationFields int
	authorizationBytes  int
	authorizationDigest string
	tokenBytes          int
	tokenDigest         string
}

func (s *Service) parseAuthorization(values []string) (parsedAuthorization, error) {
	parsed := parsedAuthorization{authorizationFields: len(values)}
	if len(values) == 0 {
		return parsed, authdomain.NewError(
			authdomain.ReasonMissingCredential,
			authdomain.DiagnosticAuthorizationMissing,
			nil,
		)
	}
	if len(values) != 1 {
		return parsed, authdomain.NewError(
			authdomain.ReasonMultipleCredentials,
			authdomain.DiagnosticAuthorizationMultiple,
			nil,
		)
	}

	value := values[0]
	parsed.authorizationBytes = len(value)
	if len(value) > s.config.MaxAuthorizationBytes {
		return parsed, authdomain.NewError(
			authdomain.ReasonCredentialTooLarge,
			authdomain.DiagnosticAuthorizationTooLarge,
			nil,
		)
	}
	parsed.authorizationDigest = authdomain.Digest([]byte(value))
	if !utf8.ValidString(value) {
		return parsed, malformedAuthorization(authdomain.DiagnosticAuthorizationEncodingInvalid)
	}

	separator := strings.IndexByte(value, ' ')
	if separator <= 0 {
		return parsed, malformedAuthorization(authdomain.DiagnosticAuthorizationSeparatorMissing)
	}
	if !isBearerScheme(value[:separator]) {
		return parsed, authdomain.NewError(
			authdomain.ReasonUnsupportedScheme,
			authdomain.DiagnosticAuthorizationSchemeUnsupported,
			nil,
		)
	}

	tokenStart := separator + 1
	for tokenStart < len(value) && value[tokenStart] == ' ' {
		tokenStart++
	}
	if tokenStart == len(value) {
		return parsed, malformedAuthorization(authdomain.DiagnosticAuthorizationTokenMissing)
	}
	token := []byte(value[tokenStart:])
	parsed.tokenBytes = len(token)
	parsed.tokenDigest = authdomain.Digest(token)
	if err := authdomain.ValidateBearerToken(token, s.config.MinTokenBytes, s.config.MaxTokenBytes); err != nil {
		clear(token)
		return parsed, err
	}
	parsed.token = token
	return parsed, nil
}

func isBearerScheme(value string) bool {
	if len(value) != len("Bearer") {
		return false
	}
	for index, expected := range []byte("bearer") {
		character := value[index]
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}
		if character != expected {
			return false
		}
	}
	return true
}

func malformedAuthorization(diagnostic authdomain.DiagnosticCode) error {
	return authdomain.NewError(
		authdomain.ReasonMalformedCredential,
		diagnostic,
		nil,
	)
}
