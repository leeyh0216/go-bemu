package memory

import (
	"context"
	"sync"
	"time"

	issuerdomain "github.com/leeyh0216/go-bemu/internal/auth/issuer/domain"
	issuerports "github.com/leeyh0216/go-bemu/internal/auth/issuer/ports"
)

const (
	defaultMaxTokens        = 100_000
	defaultMaxReplayMarkers = 100_000
)

type Config struct {
	MaxTokens        int
	MaxReplayMarkers int
}

func DefaultConfig() Config {
	return Config{MaxTokens: defaultMaxTokens, MaxReplayMarkers: defaultMaxReplayMarkers}
}

type Store struct {
	mu      sync.Mutex
	config  Config
	tokens  map[string]issuerdomain.IssuedToken
	replays map[string]issuerdomain.ReplayMarker
}

func New(config Config) (*Store, error) {
	if config.MaxTokens < 1 || config.MaxReplayMarkers < 1 {
		return nil, issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticStoreConfig,
			nil,
		)
	}
	return &Store{
		config:  config,
		tokens:  make(map[string]issuerdomain.IssuedToken),
		replays: make(map[string]issuerdomain.ReplayMarker),
	}, nil
}

// Commit performs collision detection and replay consumption atomically. The
// replay marker is inserted only after every validation and capacity check has
// passed, so a failed token write cannot burn a valid assertion.
// https://www.rfc-editor.org/rfc/rfc7523.html#section-3
func (s *Store) Commit(ctx context.Context, token issuerdomain.IssuedToken, replay *issuerdomain.ReplayMarker) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if !token.Valid() || (replay != nil && !replay.Valid(token.IssuedAt)) {
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticStoreRecord,
			nil,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return err
	}
	s.purgeExpired(token.IssuedAt)

	if _, exists := s.tokens[token.TokenDigest]; exists {
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticTokenCollision,
			issuerdomain.ErrTokenCollision,
		)
	}
	if replay != nil {
		if _, exists := s.replays[replay.Digest]; exists {
			return issuerdomain.NewError(
				issuerdomain.ErrorInvalidGrant,
				issuerdomain.DiagnosticJWTReplay,
				issuerdomain.ErrReplay,
			)
		}
	}
	if len(s.tokens) >= s.config.MaxTokens ||
		(replay != nil && len(s.replays) >= s.config.MaxReplayMarkers) {
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticStoreCapacity,
			issuerdomain.ErrStoreCapacity,
		)
	}

	s.tokens[token.TokenDigest] = token
	if replay != nil {
		s.replays[replay.Digest] = *replay
	}
	return nil
}

func (s *Store) Lookup(ctx context.Context, tokenDigest string, now time.Time) (issuerdomain.IssuedToken, bool, error) {
	if err := contextError(ctx); err != nil {
		return issuerdomain.IssuedToken{}, false, err
	}
	if !issuerdomain.ValidDigest(tokenDigest) || now.IsZero() {
		return issuerdomain.IssuedToken{}, false, issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticStoreRecord,
			nil,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return issuerdomain.IssuedToken{}, false, err
	}
	token, exists := s.tokens[tokenDigest]
	if !exists {
		return issuerdomain.IssuedToken{}, false, nil
	}
	if !token.ExpiresAt.After(now) {
		delete(s.tokens, tokenDigest)
		return issuerdomain.IssuedToken{}, false, nil
	}
	return token, true, nil
}

func (s *Store) Revoke(ctx context.Context, tokenDigest string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if !issuerdomain.ValidDigest(tokenDigest) {
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticStoreRecord,
			nil,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return err
	}
	delete(s.tokens, tokenDigest)
	return nil
}

type Stats struct {
	TokenCount  int
	ReplayCount int
}

func (s *Store) Stats(now time.Time) Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpired(now)
	return Stats{TokenCount: len(s.tokens), ReplayCount: len(s.replays)}
}

func (s *Store) purgeExpired(now time.Time) {
	for digest, token := range s.tokens {
		if !token.ExpiresAt.After(now) {
			delete(s.tokens, digest)
		}
	}
	for digest, replay := range s.replays {
		if !replay.ExpiresAt.After(now) {
			delete(s.replays, digest)
		}
	}
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticContextEnded,
			nil,
		)
	}
	if err := ctx.Err(); err != nil {
		return issuerdomain.NewError(
			issuerdomain.ErrorServer,
			issuerdomain.DiagnosticContextEnded,
			err,
		)
	}
	return nil
}

var _ issuerports.IssuedTokenStore = (*Store)(nil)
