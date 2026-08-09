package grpcserver

// This adapter owns gRPC and protobuf inspection. The observability core stays
// protocol-neutral so applications can use its event contract without taking a
// transport dependency.

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/leeyh0216/go-bemu/internal/observability"
)

func UnaryServerInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx = grpcContext(ctx)
	started := time.Now()
	attrs := append(observability.ContextAttrs(ctx), "event", observability.BoundaryEnter, "boundary", "grpc.unary", "rpc", info.FullMethod)
	attrs = append(attrs, grpcMetadataAttrs(ctx)...)
	attrs = append(attrs, protoAttrs(request)...)
	slog.InfoContext(ctx, "grpc request", attrs...)
	timeline := observability.ProcessTimeline()
	recordProtobuf(timeline, grpcEvent(ctx, "grpc", info.FullMethod, "request", "", 0), request)
	response, err := handler(ctx, request)
	exitAttrs := append(observability.ContextAttrs(ctx), "event", observability.BoundaryExit, "boundary", "grpc.unary", "rpc", info.FullMethod, "grpc_code", status.Code(err).String(), "duration_ms", time.Since(started).Milliseconds())
	exitAttrs = append(exitAttrs, protoAttrs(response)...)
	if err != nil {
		exitAttrs = append(exitAttrs, observability.ErrorAttrs(err)...)
	}
	slog.InfoContext(ctx, "grpc response", exitAttrs...)
	recordProtobuf(timeline, grpcEvent(ctx, "grpc", info.FullMethod, "response", status.Code(err).String(), time.Since(started).Nanoseconds(), observability.ErrorAttrs(err)...), response)
	return response, err
}

func StreamServerInterceptor(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := grpcContext(stream.Context())
	started := time.Now()
	wrapper := &loggingServerStream{ServerStream: stream, ctx: ctx, rpc: info.FullMethod}
	attrs := append(observability.ContextAttrs(ctx), "event", observability.BoundaryEnter, "boundary", "grpc.stream", "rpc", info.FullMethod, "client_stream", info.IsClientStream, "server_stream", info.IsServerStream)
	attrs = append(attrs, grpcMetadataAttrs(ctx)...)
	slog.InfoContext(ctx, "grpc stream", attrs...)
	observability.ProcessTimeline().Record(grpcEvent(ctx, "grpc.stream", info.FullMethod, "open", "", 0), nil)
	err := handler(service, wrapper)
	exitAttrs := append(observability.ContextAttrs(ctx), "event", observability.BoundaryExit, "boundary", "grpc.stream", "rpc", info.FullMethod, "grpc_code", status.Code(err).String(), "recv_messages", wrapper.recvMessages, "recv_bytes", wrapper.recvBytes, "send_messages", wrapper.sendMessages, "send_bytes", wrapper.sendBytes, "duration_ms", time.Since(started).Milliseconds())
	if err != nil {
		exitAttrs = append(exitAttrs, observability.ErrorAttrs(err)...)
	}
	slog.InfoContext(ctx, "grpc stream", exitAttrs...)
	observability.ProcessTimeline().Record(grpcEvent(ctx, "grpc.stream", info.FullMethod, "close", status.Code(err).String(), time.Since(started).Nanoseconds(), observability.ErrorAttrs(err)...), nil)
	return err
}

type loggingServerStream struct {
	grpc.ServerStream
	ctx                                              context.Context
	rpc                                              string
	recvMessages, recvBytes, sendMessages, sendBytes int64
}

func (s *loggingServerStream) Context() context.Context { return s.ctx }
func (s *loggingServerStream) RecvMsg(message any) error {
	err := s.ServerStream.RecvMsg(message)
	attrs := protoAttrs(message)
	s.recvBytes += wireBytes(attrs)
	if err == nil {
		s.recvMessages++
	}
	logAttrs := append(observability.ContextAttrs(s.ctx), "event", observability.SideEffectAfter, "operation", "grpc.stream.recv", "rpc", s.rpc, "success", err == nil)
	logAttrs = append(logAttrs, attrs...)
	slog.DebugContext(s.ctx, "grpc stream message", logAttrs...)
	phase := "recv"
	if err == io.EOF {
		phase = "half_close"
	}
	recordProtobuf(observability.ProcessTimeline(), grpcEvent(s.ctx, "grpc.stream", s.rpc, phase, status.Code(err).String(), 0, observability.ErrorAttrs(err)...), message)
	return err
}
func (s *loggingServerStream) SendMsg(message any) error {
	attrs := protoAttrs(message)
	s.sendBytes += wireBytes(attrs)
	err := s.ServerStream.SendMsg(message)
	if err == nil {
		s.sendMessages++
	}
	logAttrs := append(observability.ContextAttrs(s.ctx), "event", observability.SideEffectAfter, "operation", "grpc.stream.send", "rpc", s.rpc, "success", err == nil)
	logAttrs = append(logAttrs, attrs...)
	slog.DebugContext(s.ctx, "grpc stream message", logAttrs...)
	recordProtobuf(observability.ProcessTimeline(), grpcEvent(s.ctx, "grpc.stream", s.rpc, "send", status.Code(err).String(), 0, observability.ErrorAttrs(err)...), message)
	return err
}
func wireBytes(attrs []any) int64 {
	for i := 0; i+1 < len(attrs); i += 2 {
		if attrs[i] == "wire_bytes" {
			if n, ok := attrs[i+1].(int); ok {
				return int64(n)
			}
		}
	}
	return 0
}

func grpcEvent(ctx context.Context, protocol, rpc, phase, outcome string, duration int64, errorAttrs ...any) observability.DiagnosticEvent {
	event := observability.DiagnosticEvent{Protocol: protocol, OperationID: rpc, RPCMethod: rpc, Phase: phase, Status: outcome, DurationNanos: duration}
	for i := 0; i+1 < len(observability.ContextAttrs(ctx)); i += 2 {
		if observability.ContextAttrs(ctx)[i] == "request_id" {
			event.RequestID, _ = observability.ContextAttrs(ctx)[i+1].(string)
		}
		if observability.ContextAttrs(ctx)[i] == "trace_id" {
			event.TraceID, _ = observability.ContextAttrs(ctx)[i+1].(string)
		}
	}
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		event.Headers = map[string][]string{}
		for k, v := range incoming {
			event.Headers[k] = append([]string(nil), v...)
		}
	}
	if remote, ok := peer.FromContext(ctx); ok && remote.Addr != nil {
		event.Peer = remote.Addr.String()
	}
	for i := 0; i+1 < len(errorAttrs); i += 2 {
		if errorAttrs[i] == "error" {
			event.Error, _ = errorAttrs[i+1].(string)
		}
	}
	return event
}
func recordProtobuf(timeline *observability.Timeline, event observability.DiagnosticEvent, message any) {
	if protobuf, ok := message.(proto.Message); ok && protobuf != nil {
		if payload, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(protobuf); err == nil {
			event.PayloadJSON = string(payload)
		}
		if wire, err := proto.Marshal(protobuf); err == nil {
			timeline.Record(event, wire)
			return
		}
	}
	timeline.Record(event, nil)
}
func grpcContext(ctx context.Context) context.Context {
	requestID, traceID := "", ""
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		requestID = firstSafe(incoming.Get("x-request-id"))
		if values := incoming.Get("traceparent"); len(values) > 0 {
			parts := strings.Split(values[0], "-")
			if len(parts) >= 2 {
				traceID = observability.SafeID(parts[1])
			}
		}
	}
	if requestID == "" {
		requestID = observability.NewID()
	}
	if traceID == "" {
		traceID = observability.NewID()
	}
	return observability.WithRequestMetadata(ctx, requestID, traceID)
}
func grpcMetadataAttrs(ctx context.Context) []any {
	attrs := []any{}
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		values := map[string][]string{}
		for k, v := range incoming {
			values[k] = v
		}
		attrs = append(attrs, "metadata", observability.MetadataEntries(values))
	}
	if remote, ok := peer.FromContext(ctx); ok && remote.Addr != nil {
		attrs = append(attrs, "peer", remote.Addr.String())
	}
	return attrs
}
func firstSafe(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return observability.SafeID(values[0])
}

func protoAttrs(message any) []any {
	protobuf, ok := message.(proto.Message)
	if !ok || !protobuf.ProtoReflect().IsValid() {
		return nil
	}
	wire, err := proto.Marshal(protobuf)
	if err != nil {
		return []any{"grpc_message", string(protobuf.ProtoReflect().Descriptor().FullName()), "marshal_error", err.Error()}
	}
	attrs := []any{"grpc_message", string(protobuf.ProtoReflect().Descriptor().FullName()), "wire_bytes", len(wire), "payload", protobuf}
	return append(attrs, reflectedMetrics(protobuf.ProtoReflect(), 0)...)
}
func reflectedMetrics(message protoreflect.Message, depth int) []any {
	if depth > 3 {
		return nil
	}
	attrs := []any{}
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		name := string(field.Name())
		if field.IsMap() {
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
					attrs = append(attrs, "schema_fingerprint", observability.Digest(schema), "schema_bytes", len(schema))
				}
			} else {
				attrs = append(attrs, reflectedMetrics(value.Message(), depth+1)...)
			}
		case protoreflect.StringKind:
			payload := []byte(value.String())
			attrs = append(attrs, name+"_bytes", len(payload), name+"_digest", observability.Digest(payload))
		case protoreflect.EnumKind:
			if enum := field.Enum().Values().ByNumber(value.Enum()); enum != nil {
				attrs = append(attrs, name, string(enum.Name()))
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
			attrs = append(attrs, name+"_bytes", len(value.Bytes()), name+"_digest", observability.Digest(value.Bytes()))
		}
		return true
	})
	return attrs
}
