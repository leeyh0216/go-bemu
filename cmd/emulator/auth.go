package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	authstatic "github.com/leeyh0216/go-bemu/internal/auth/adapters/static"
	authapp "github.com/leeyh0216/go-bemu/internal/auth/application"
	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
	authports "github.com/leeyh0216/go-bemu/internal/auth/ports"
	"github.com/leeyh0216/go-bemu/internal/config"
)

// authenticationRuntime owns the replaceable verifier adapter and its process
// lifecycle. REST and gRPC receive Service(), so credential parsing, principal
// construction, decisions, and logs cannot diverge between public protocols.
//
// Credential files used by ADC (service_account, authorized_user, and
// external_account) obtain bearer tokens before this boundary. StaticTokenSet
// is deliberately a local verifier contract rather than an implementation of
// Google IAM or Security Token Service semantics.
//
// Official sources:
//   - https://cloud.google.com/docs/authentication/application-default-credentials
//   - https://cloud.google.com/iam/docs/workload-identity-federation
//   - https://www.rfc-editor.org/rfc/rfc6750#section-2.1
type authenticationRuntime struct {
	service        *authapp.Service
	reloadVerifier *authstatic.Verifier
	reloadInterval time.Duration
	logger         *slog.Logger
}

func composeAuthentication(ctx context.Context, cfg config.AuthConfig, logger *slog.Logger) (*authenticationRuntime, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	var verifier authports.TokenVerifier
	var reloadVerifier *authstatic.Verifier
	switch cfg.Mode {
	case string(authdomain.PolicyDisabled):
		verifier = authapp.DisabledVerifier{}
	case string(authdomain.PolicyBearerPresent):
		verifier = authapp.PresenceVerifier{}
	case string(authdomain.PolicyStatic):
		if cfg.StaticTokensReloadInterval.Value() <= 0 {
			return nil, fmt.Errorf("configure authentication reload: interval must be positive")
		}
		source, err := authstatic.NewFileSource(cfg.StaticTokensFile)
		if err != nil {
			return nil, fmt.Errorf("configure static token source: %w", err)
		}
		options := authstatic.DefaultOptions()
		options.MaxFileBytes = cfg.StaticTokensMaxFileBytes
		options.MaxTokens = cfg.StaticTokensMaxEntries
		options.MinTokenBytes = cfg.MinTokenBytes
		options.MaxTokenBytes = cfg.MaxTokenBytes
		options.MaxPrincipalBytes = cfg.StaticPrincipalMaxBytes
		options.Logger = logger
		reloadVerifier, err = authstatic.New(ctx, source, options)
		if err != nil {
			return nil, fmt.Errorf("load initial static token snapshot: %w", err)
		}
		verifier = reloadVerifier
	default:
		return nil, fmt.Errorf("configure authentication: unsupported mode")
	}

	service, err := authapp.New(authapp.Config{
		MinTokenBytes:         cfg.MinTokenBytes,
		MaxTokenBytes:         cfg.MaxTokenBytes,
		MaxAuthorizationBytes: cfg.MaxAuthorizationBytes,
	}, verifier, logger)
	if err != nil {
		return nil, fmt.Errorf("configure authentication service: %w", err)
	}
	logger.InfoContext(ctx, "authentication configured",
		"event", "runtime.authentication.configured",
		"model_version", authdomain.ModelVersion,
		"policy", cfg.Mode,
		"static_reload_enabled", reloadVerifier != nil,
		"static_reload_interval_ms", cfg.StaticTokensReloadInterval.Value().Milliseconds(),
	)
	return &authenticationRuntime{
		service: service, reloadVerifier: reloadVerifier,
		reloadInterval: cfg.StaticTokensReloadInterval.Value(), logger: logger,
	}, nil
}

func (runtime *authenticationRuntime) Service() *authapp.Service {
	if runtime == nil {
		return nil
	}
	return runtime.service
}

// Start begins periodic static-file reload and returns an idempotent shutdown
// function that waits until the goroutine has observed cancellation. time.Ticker
// is process wiring; runReloadLoop accepts a channel so reload/failure/recovery
// tests can drive every transition without wall-clock sleeps.
// https://pkg.go.dev/time#Ticker
func (runtime *authenticationRuntime) Start(ctx context.Context) func() {
	if runtime == nil || runtime.reloadVerifier == nil {
		return func() {}
	}
	reloadContext, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(runtime.reloadInterval)
	done := make(chan struct{})
	runtime.logger.InfoContext(ctx, "static token reload scheduler started",
		"event", "domain.transition", "component", "auth.static-token-reload",
		"state_from", "stopped", "state_to", "running",
		"model_version", authdomain.ModelVersion,
		"reload_interval_ms", runtime.reloadInterval.Milliseconds(),
	)
	go func() {
		defer close(done)
		defer ticker.Stop()
		runtime.runReloadLoop(reloadContext, ticker.C)
		runtime.logger.InfoContext(context.Background(), "static token reload scheduler stopped",
			"event", "domain.transition", "component", "auth.static-token-reload",
			"state_from", "running", "state_to", "stopped",
			"model_version", authdomain.ModelVersion,
		)
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

func (runtime *authenticationRuntime) runReloadLoop(ctx context.Context, ticks <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, open := <-ticks:
			if !open {
				return
			}
			// Reload itself records safe pre/post side-effect logs and atomically
			// installs deny-all on failure. The next tick is always attempted so
			// a corrected mounted file recovers without process restart.
			_ = runtime.reloadVerifier.Reload(ctx)
		}
	}
}
