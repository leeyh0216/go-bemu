package static

import (
	"context"
	"time"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
)

// Reload fails closed. Any unsuccessful attempt atomically replaces the prior
// valid snapshot with deny-all state; a subsequent valid call publishes a new
// active snapshot and recovers service without a process restart.
func (v *Verifier) Reload(ctx context.Context) error {
	return v.loadAndPublish(ctx, "reload", true)
}

func (v *Verifier) loadAndPublish(ctx context.Context, operation string, denyOnFailure bool) error {
	v.reloadMu.Lock()
	defer v.reloadMu.Unlock()

	attempt := v.attempts.Add(1)
	started := v.options.Clock.Now()
	v.options.Logger.InfoContext(ctx, "static token snapshot load started",
		"event", "side_effect.pre",
		"component", "auth.static-token-set",
		"operation", operation,
		"tx_state", "before",
		"model_version", authdomain.ModelVersion,
		"load_attempt", attempt,
	)

	next, sourceFingerprint, err := v.readSnapshot(ctx, attempt)
	if err != nil {
		state := "rejected"
		if denyOnFailure {
			state = string(SnapshotDenyAll)
			v.snapshot.Store(denyAllSnapshot(v.options.Clock.Now(), attempt, sourceFingerprint, err))
		}
		v.logLoadEnd(ctx, operation, started, attempt, state, sourceFingerprint, 0, err)
		return err
	}

	v.snapshot.Store(next)
	v.logLoadEnd(ctx, operation, started, attempt, "committed", sourceFingerprint, len(next.records), nil)
	return nil
}

func (v *Verifier) readSnapshot(ctx context.Context, attempt uint64) (*snapshot, string, error) {
	payload, err := v.source.Read(ctx, v.options.MaxFileBytes)
	if err != nil {
		clear(payload)
		return nil, "none", authdomain.NewError(
			authdomain.ReasonTokenSetSourceFailure,
			sourceDiagnostic(err),
			err,
		)
	}
	defer clear(payload)

	fingerprint := authdomain.Digest(payload)
	if int64(len(payload)) > v.options.MaxFileBytes {
		return nil, fingerprint, invalidTokenSet(authdomain.DiagnosticManifestPayloadTooLarge, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, fingerprint, authdomain.NewError(
			authdomain.ReasonTokenSetSourceFailure,
			authdomain.DiagnosticTokenSourceContextEnded,
			err,
		)
	}

	records, err := decodeRecords(payload, v.options)
	if err != nil {
		return nil, fingerprint, err
	}
	loadedAt := v.options.Clock.Now()
	return &snapshot{
		metadata: SnapshotMetadata{
			State: SnapshotActive, TokenCount: len(records), Revision: fingerprint,
			LoadedAt: loadedAt, LoadAttempt: attempt,
		},
		records: records,
	}, fingerprint, nil
}

func (v *Verifier) logLoadEnd(ctx context.Context, operation string, started time.Time, attempt uint64, state, fingerprint string, count int, err error) {
	duration := v.options.Clock.Now().Sub(started)
	if duration < 0 {
		duration = 0
	}
	attrs := []any{
		"event", "side_effect.post",
		"component", "auth.static-token-set",
		"operation", operation,
		"tx_state", state,
		"success", err == nil,
		"duration_ms", duration.Milliseconds(),
		"model_version", authdomain.ModelVersion,
		"load_attempt", attempt,
		"token_count", count,
		"source_fingerprint", fingerprint,
	}
	if err != nil {
		attrs = append(attrs, authdomain.SafeErrorAttrs(err)...)
	}
	v.options.Logger.InfoContext(ctx, "static token snapshot load completed", attrs...)
}

func denyAllSnapshot(loadedAt time.Time, attempt uint64, fingerprint string, err error) *snapshot {
	if fingerprint == "" || fingerprint == "none" {
		fingerprint = authdomain.Digest([]byte(err.Error()))
	}
	return &snapshot{metadata: SnapshotMetadata{
		State: SnapshotDenyAll, TokenCount: 0, Revision: fingerprint,
		LoadedAt: loadedAt, LoadAttempt: attempt,
	}}
}
