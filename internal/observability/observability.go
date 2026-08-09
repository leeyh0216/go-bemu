package observability

// Official protobuf source: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

type contextKey string

const (
	requestIDKey contextKey = "request_id"
	traceIDKey   contextKey = "trace_id"
)

func WithRequestMetadata(ctx context.Context, requestID, traceID string) context.Context {
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	ctx = context.WithValue(ctx, traceIDKey, traceID)
	return ctx
}

func ContextAttrs(ctx context.Context) []any {
	return []any{
		"request_id", valueFromContext(ctx, requestIDKey),
		"trace_id", valueFromContext(ctx, traceIDKey),
	}
}

func valueFromContext(ctx context.Context, key contextKey) string {
	value, _ := ctx.Value(key).(string)
	return value
}

func NewID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}

func SafeID(value string) string {
	if len(value) > 128 {
		value = value[:128]
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("-_./", character) {
			continue
		}
		return ""
	}
	return value
}

func Digest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func PayloadAttrs(name string, payload []byte) []any {
	return []any{
		name + "_bytes", len(payload),
		name, string(payload),
	}
}

func ErrorAttrs(err error) []any {
	if err == nil {
		return nil
	}
	return []any{
		"error_type", fmt.Sprintf("%T", err),
		"error", err.Error(),
	}
}

func MetadataEntries(values map[string][]string) []string {
	entries := make([]string, 0, len(values))
	for key := range values {
		for _, value := range values[key] {
			entries = append(entries, strings.ToLower(key)+"="+value)
		}
	}
	sort.Strings(entries)
	return entries
}

func LogSideEffectStart(ctx context.Context, component, operation string, attrs ...any) time.Time {
	started := time.Now()
	base := append(ContextAttrs(ctx), "event", SideEffectBefore, "component", component, "operation", operation, "tx_state", "before")
	slog.InfoContext(ctx, "side effect", append(base, attrs...)...)
	return started
}

func LogSideEffectEnd(ctx context.Context, component, operation string, started time.Time, err error, attrs ...any) {
	state := "committed"
	event := SideEffectAfter
	if err != nil {
		state = "rolled_back"
		event = SideEffectError
	}
	base := append(ContextAttrs(ctx),
		"event", event, "component", component, "operation", operation,
		"tx_state", state, "success", err == nil, "duration_ms", time.Since(started).Milliseconds(),
	)
	if err != nil {
		base = append(base, ErrorAttrs(err)...)
	}
	slog.InfoContext(ctx, "side effect", append(base, attrs...)...)
}
