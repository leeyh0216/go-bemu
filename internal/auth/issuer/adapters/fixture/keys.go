package fixture

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"math/big"

	issuerdomain "github.com/leeyh0216/go-bemu/internal/auth/issuer/domain"
	issuerports "github.com/leeyh0216/go-bemu/internal/auth/issuer/ports"
)

const (
	defaultMaxKeys           = 1_000
	defaultMinRSABits        = 2_048
	defaultMaxKeyIssuerBytes = 4 * 1024
	defaultMaxKeyIDBytes     = 4 * 1024
)

type KeyOptions struct {
	MaxKeys        int
	MinRSABits     int
	MaxIssuerBytes int
	MaxKeyIDBytes  int
}

func DefaultKeyOptions() KeyOptions {
	return KeyOptions{
		MaxKeys:        defaultMaxKeys,
		MinRSABits:     defaultMinRSABits,
		MaxIssuerBytes: defaultMaxKeyIssuerBytes,
		MaxKeyIDBytes:  defaultMaxKeyIDBytes,
	}
}

type RSAKeyRegistration struct {
	Issuer    string
	KeyID     string
	PublicKey *rsa.PublicKey
}

type rsaKeyRecord struct {
	issuer [sha256.Size]byte
	keyID  [sha256.Size]byte
	key    *rsa.PublicKey
}

type RSAKeyVerifier struct {
	options KeyOptions
	keys    []rsaKeyRecord
}

func NewRSAKeyVerifier(registrations []RSAKeyRegistration, options KeyOptions) (*RSAKeyVerifier, error) {
	if options.MaxKeys < 1 || options.MinRSABits < 2_048 || options.MaxIssuerBytes < 1 ||
		options.MaxKeyIDBytes < 1 || len(registrations) == 0 || len(registrations) > options.MaxKeys {
		return nil, fixtureConfigError()
	}

	verifier := &RSAKeyVerifier{options: options}
	seen := make(map[[sha256.Size * 2]byte]struct{}, len(registrations))
	for _, registration := range registrations {
		if !boundedText(registration.Issuer, options.MaxIssuerBytes, false) ||
			!boundedText(registration.KeyID, options.MaxKeyIDBytes, true) ||
			!validPublicKey(registration.PublicKey, options.MinRSABits) {
			return nil, fixtureConfigError()
		}
		record := rsaKeyRecord{
			issuer: sha256.Sum256([]byte(registration.Issuer)),
			keyID:  sha256.Sum256([]byte(registration.KeyID)),
			key:    clonePublicKey(registration.PublicKey),
		}
		var key [sha256.Size * 2]byte
		copy(key[:sha256.Size], record.issuer[:])
		copy(key[sha256.Size:], record.keyID[:])
		if _, duplicate := seen[key]; duplicate {
			return nil, fixtureConfigError()
		}
		seen[key] = struct{}{}
		verifier.keys = append(verifier.keys, record)
	}
	return verifier, nil
}

// Verify implements only the local RS256 fixture contract used by service
// account credentials. It intentionally does not fetch Google JWKS, call IAM,
// apply key rotation policy, or establish production Google identity parity.
//
// Pinned Java client request construction:
// https://github.com/googleapis/google-auth-library-java/blob/v1.43.0/oauth2_http/java/com/google/auth/oauth2/ServiceAccountCredentials.java#L522-L533
func (v *RSAKeyVerifier) Verify(ctx context.Context, input issuerports.SignatureInput) error {
	if err := activeContext(ctx); err != nil {
		return err
	}
	if input.Algorithm != "RS256" || !boundedCredential(input.Issuer, v.options.MaxIssuerBytes) ||
		len(input.KeyID) > v.options.MaxKeyIDBytes || !utf8OrEmpty(input.KeyID) ||
		len(input.SigningInput) == 0 || len(input.Signature) == 0 {
		return issuerdomain.NewError(
			issuerdomain.ErrorInvalidGrant,
			issuerdomain.DiagnosticJWTSignature,
			nil,
		)
	}

	issuer := sha256.Sum256(input.Issuer)
	keyID := sha256.Sum256(input.KeyID)
	matched := 0
	matchedIndex := 0
	for index := range v.keys {
		equal := subtle.ConstantTimeCompare(issuer[:], v.keys[index].issuer[:]) &
			subtle.ConstantTimeCompare(keyID[:], v.keys[index].keyID[:])
		matchedIndex = subtle.ConstantTimeSelect(equal, index, matchedIndex)
		matched = subtle.ConstantTimeSelect(equal, 1, matched)
	}
	if matched != 1 {
		return issuerdomain.NewError(
			issuerdomain.ErrorInvalidGrant,
			issuerdomain.DiagnosticFixtureKeyUnknown,
			nil,
		)
	}

	hash := sha256.Sum256(input.SigningInput)
	if err := rsa.VerifyPKCS1v15(v.keys[matchedIndex].key, crypto.SHA256, hash[:], input.Signature); err != nil {
		return issuerdomain.NewError(
			issuerdomain.ErrorInvalidGrant,
			issuerdomain.DiagnosticJWTSignature,
			err,
		)
	}
	return activeContext(ctx)
}

func validPublicKey(key *rsa.PublicKey, minBits int) bool {
	return key != nil && key.N != nil && key.N.Sign() > 0 && key.N.BitLen() >= minBits && key.E >= 3 && key.E%2 == 1
}

func clonePublicKey(key *rsa.PublicKey) *rsa.PublicKey {
	return &rsa.PublicKey{N: new(big.Int).Set(key.N), E: key.E}
}

func utf8OrEmpty(value []byte) bool {
	if len(value) == 0 {
		return true
	}
	return boundedCredential(value, len(value))
}

var _ issuerports.SignatureVerifier = (*RSAKeyVerifier)(nil)
