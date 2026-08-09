package random

import (
	"context"
	cryptorand "crypto/rand"
	"io"

	issuerdomain "github.com/leeyh0216/go-bemu/internal/auth/issuer/domain"
	issuerports "github.com/leeyh0216/go-bemu/internal/auth/issuer/ports"
)

type Source struct {
	reader io.Reader
}

func NewCryptoSource() *Source { return &Source{reader: cryptorand.Reader} }

// NewSource permits deterministic or fault-injecting readers in tests. Runtime
// composition must use NewCryptoSource unless it deliberately supplies an
// equivalent cryptographic provider.
func NewSource(reader io.Reader) (*Source, error) {
	if reader == nil {
		return nil, issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticDependencyMissing,
			nil,
		)
	}
	return &Source{reader: reader}, nil
}

// Fill requires a complete read. Partial entropy must never be encoded into an
// access token. crypto/rand.Reader is the Go standard-library CSPRNG boundary.
// https://pkg.go.dev/crypto/rand#Reader
func (s *Source) Fill(ctx context.Context, destination []byte) error {
	if ctx == nil || ctx.Err() != nil {
		var cause error
		if ctx != nil {
			cause = ctx.Err()
		}
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticContextEnded,
			cause,
		)
	}
	if len(destination) == 0 {
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticEntropyFailure,
			nil,
		)
	}
	if _, err := io.ReadFull(s.reader, destination); err != nil {
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticEntropyFailure,
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		clear(destination)
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticContextEnded,
			err,
		)
	}
	return nil
}

var _ issuerports.Entropy = (*Source)(nil)
