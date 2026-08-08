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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func ProtoAttrs(message any) []any {
	protobuf, ok := message.(proto.Message)
	if !ok || !protobuf.ProtoReflect().IsValid() {
		return nil
	}
	wire, err := proto.Marshal(protobuf)
	if err != nil {
		return []any{
			"grpc_message", string(protobuf.ProtoReflect().Descriptor().FullName()),
			"marshal_error", err.Error(),
		}
	}
	attrs := []any{
		"grpc_message", string(protobuf.ProtoReflect().Descriptor().FullName()),
		"wire_bytes", len(wire), "payload", protobuf,
	}
	attrs = append(attrs, reflectedMetrics(protobuf.ProtoReflect(), 0)...)
	return attrs
}

func reflectedMetrics(message protoreflect.Message, depth int) []any {
	if depth > 3 {
		return nil
	}
	attrs := make([]any, 0)
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		name := string(field.Name())
		if field.IsMap() {
			attrs = append(attrs, name+"_count", value.Map().Len())
			return true
		}
		if field.IsList() {
			list := value.List()
			attrs = append(attrs, name+"_count", list.Len())
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
				payload := []byte(value.String())
				attrs = append(attrs,
					name+"_shape", "resource_name",
					name+"_bytes", len(payload),
					name+"_fingerprint", Digest(payload),
				)
			} else {
				payload := []byte(value.String())
				attrs = append(attrs, name+"_bytes", len(payload), name+"_digest", Digest(payload))
			}
		case protoreflect.EnumKind:
			enum := field.Enum().Values().ByNumber(value.Enum())
			if enum == nil {
				attrs = append(attrs, name+"_shape", "unknown_enum")
				break
			}
			attrs = append(attrs, name, string(enum.Name()))
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
				attrs = append(attrs, "schema_fingerprint", Digest(value.Bytes()), "schema_bytes", len(value.Bytes()))
			} else {
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
