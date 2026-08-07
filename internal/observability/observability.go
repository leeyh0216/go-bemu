package observability

// Official protobuf source: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type contextKey string

const (
	requestIDKey contextKey = "request_id"
	traceIDKey   contextKey = "trace_id"
)

var unsafePayloads atomic.Bool

var sensitiveText = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(bearer|basic)\s+[^\s,;]+`),
	regexp.MustCompile(`(?is)-----BEGIN (RSA |EC )?PRIVATE KEY-----.*?-----END (RSA |EC )?PRIVATE KEY-----`),
	regexp.MustCompile(`(?i)(["']?(authorization|proxy[_-]?authorization|access[_-]?token|refresh[_-]?token|id[_-]?token|password|private[_-]?key|credential|client[_-]?secret|api[_-]?key|cookie)["']?\s*[:=]\s*["']?)[^\s,;&}"']+`),
}

func Configure(allowUnsafePayloads bool) {
	unsafePayloads.Store(allowUnsafePayloads)
	if allowUnsafePayloads {
		slog.Warn("unsafe payload logging enabled; data values may be written to logs, credentials remain redacted")
	}
}

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
	attrs := []any{name + "_bytes", len(payload), name + "_digest", Digest(payload)}
	if unsafePayloads.Load() {
		attrs = append(attrs, name, RedactText(string(payload)))
	}
	return attrs
}

func ErrorAttrs(err error) []any {
	if err == nil {
		return nil
	}
	message := err.Error()
	attrs := []any{"error_type", fmt.Sprintf("%T", err), "error_digest", Digest([]byte(message))}
	if unsafePayloads.Load() {
		attrs = append(attrs, "error", RedactText(message))
	}
	return attrs
}

func RedactText(value string) string {
	var document any
	if json.Unmarshal([]byte(value), &document) == nil {
		if encoded, err := json.Marshal(redactJSON(document)); err == nil {
			value = string(encoded)
		}
	}
	redacted := value
	for _, pattern := range sensitiveText {
		redacted = pattern.ReplaceAllStringFunc(redacted, func(match string) string {
			lower := strings.ToLower(match)
			if strings.HasPrefix(lower, "bearer ") {
				return "Bearer [REDACTED]"
			}
			if strings.HasPrefix(lower, "basic ") {
				return "Basic [REDACTED]"
			}
			if strings.Contains(lower, "private key-----") {
				return "[REDACTED PRIVATE KEY]"
			}
			separator := strings.IndexAny(match, ":=")
			if separator < 0 {
				return "[REDACTED]"
			}
			return match[:separator+1] + "[REDACTED]"
		})
	}
	return redacted
}

func redactJSON(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if isSensitiveName(strings.ToLower(key)) {
				typed[key] = "[REDACTED]"
				continue
			}
			typed[key] = redactJSON(child)
		}
		return typed
	case []any:
		for index, child := range typed {
			typed[index] = redactJSON(child)
		}
		return typed
	case string:
		return redactNonJSONText(typed)
	default:
		return value
	}
}

func redactNonJSONText(value string) string {
	redacted := value
	for _, pattern := range sensitiveText {
		redacted = pattern.ReplaceAllString(redacted, "[REDACTED]")
	}
	return redacted
}

func MetadataKeys(values map[string][]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		lower := strings.ToLower(key)
		if isSensitiveName(lower) {
			keys = append(keys, lower+"=[REDACTED]")
			continue
		}
		keys = append(keys, lower)
	}
	sort.Strings(keys)
	return keys
}

func isSensitiveName(name string) bool {
	for _, fragment := range []string{
		"authorization", "cookie", "token", "credential", "password", "private-key", "private_key",
		"api-key", "api_key", "client-secret", "client_secret", "service-account-key", "service_account_key",
	} {
		if strings.Contains(name, fragment) {
			return true
		}
	}
	return false
}

func ProtoAttrs(message any) []any {
	protobuf, ok := message.(proto.Message)
	if !ok || !protobuf.ProtoReflect().IsValid() {
		return nil
	}
	wire, err := proto.Marshal(protobuf)
	if err != nil {
		return []any{
			"grpc_message", string(protobuf.ProtoReflect().Descriptor().FullName()),
			"marshal_error_digest", Digest([]byte(err.Error())),
		}
	}
	attrs := []any{
		"grpc_message", string(protobuf.ProtoReflect().Descriptor().FullName()),
		"wire_bytes", len(wire), "payload_digest", Digest(wire),
	}
	attrs = append(attrs, reflectedMetrics(protobuf.ProtoReflect(), 0)...)
	if unsafePayloads.Load() {
		if payload, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(protobuf); err == nil {
			attrs = append(attrs, "payload", RedactText(string(payload)))
		}
	}
	return attrs
}

func reflectedMetrics(message protoreflect.Message, depth int) []any {
	if depth > 3 {
		return nil
	}
	attrs := make([]any, 0)
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		name := string(field.Name())
		if field.IsList() {
			list := value.List()
			if name == "serialized_rows" {
				bytes := 0
				for i := 0; i < list.Len(); i++ {
					bytes += len(list.Get(i).Bytes())
				}
				attrs = append(attrs, "row_count", list.Len(), "row_bytes", bytes)
			}
			return true
		}
		switch field.Kind() {
		case protoreflect.MessageKind:
			if strings.Contains(name, "schema") || strings.Contains(name, "descriptor") {
				if schema, err := proto.Marshal(value.Message().Interface()); err == nil {
					attrs = append(attrs, "schema_fingerprint", Digest(schema), "schema_bytes", len(schema))
				}
				break
			}
			attrs = append(attrs, reflectedMetrics(value.Message(), depth+1)...)
		case protoreflect.StringKind:
			if name == "write_stream" || name == "read_stream" || name == "parent" || (depth == 0 && name == "name") {
				attrs = append(attrs, name, value.String())
			}
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			if name == "offset" || name == "row_count" {
				attrs = append(attrs, name, value.Int())
			}
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			if name == "offset" || name == "row_count" {
				attrs = append(attrs, name, value.Uint())
			}
		case protoreflect.BytesKind:
			if strings.Contains(name, "schema") {
				attrs = append(attrs, "schema_fingerprint", Digest(value.Bytes()))
			} else if strings.Contains(name, "serialized") {
				attrs = append(attrs, name+"_bytes", len(value.Bytes()), name+"_digest", Digest(value.Bytes()))
			}
		}
		return true
	})
	return attrs
}

func LogSideEffectStart(ctx context.Context, component, operation string, attrs ...any) time.Time {
	started := time.Now()
	base := append(ContextAttrs(ctx), "event", "side_effect.pre", "component", component, "operation", operation, "tx_state", "before")
	slog.InfoContext(ctx, "side effect", append(base, attrs...)...)
	return started
}

func LogSideEffectEnd(ctx context.Context, component, operation string, started time.Time, err error, attrs ...any) {
	state := "committed"
	if err != nil {
		state = "rolled_back"
	}
	base := append(ContextAttrs(ctx),
		"event", "side_effect.post", "component", component, "operation", operation,
		"tx_state", state, "success", err == nil, "duration_ms", time.Since(started).Milliseconds(),
	)
	if err != nil {
		base = append(base, ErrorAttrs(err)...)
	}
	slog.InfoContext(ctx, "side effect", append(base, attrs...)...)
}
