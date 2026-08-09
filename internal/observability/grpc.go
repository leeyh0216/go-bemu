package observability

// The interceptors observe official protobuf messages without copying generated
// types. Logs retain the original message together with structural metrics.
// Official source: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1

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
)

func UnaryServerInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx = grpcContext(ctx)
	started := time.Now()
	attrs := append(ContextAttrs(ctx), "event", "boundary.enter", "boundary", "grpc.unary", "rpc", info.FullMethod)
	attrs = append(attrs, grpcMetadataAttrs(ctx)...)
	attrs = append(attrs, ProtoAttrs(request)...)
	slog.InfoContext(ctx, "grpc request", attrs...)
	timeline := ProcessTimeline()
	recordProtobuf(timeline, grpcEvent(ctx, "grpc", info.FullMethod, "request", "", 0), request)
	response, err := handler(ctx, request)
	exitAttrs := append(ContextAttrs(ctx),
		"event", "boundary.exit", "boundary", "grpc.unary", "rpc", info.FullMethod,
		"grpc_code", status.Code(err).String(), "duration_ms", time.Since(started).Milliseconds(),
	)
	exitAttrs = append(exitAttrs, ProtoAttrs(response)...)
	if err != nil {
		exitAttrs = append(exitAttrs, ErrorAttrs(err)...)
	}
	slog.InfoContext(ctx, "grpc response", exitAttrs...)
	recordProtobuf(timeline, grpcEvent(ctx, "grpc", info.FullMethod, "response", status.Code(err).String(), time.Since(started).Nanoseconds(), ErrorAttrs(err)...), response)
	return response, err
}

func StreamServerInterceptor(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := grpcContext(stream.Context())
	started := time.Now()
	wrapper := &loggingServerStream{ServerStream: stream, ctx: ctx, rpc: info.FullMethod}
	attrs := append(ContextAttrs(ctx),
		"event", "boundary.enter", "boundary", "grpc.stream", "rpc", info.FullMethod,
		"client_stream", info.IsClientStream, "server_stream", info.IsServerStream,
	)
	attrs = append(attrs, grpcMetadataAttrs(ctx)...)
	slog.InfoContext(ctx, "grpc stream", attrs...)
	ProcessTimeline().Record(grpcEvent(ctx, "grpc.stream", info.FullMethod, "open", "", 0), nil)
	err := handler(service, wrapper)
	exitAttrs := append(ContextAttrs(ctx),
		"event", "boundary.exit", "boundary", "grpc.stream", "rpc", info.FullMethod,
		"grpc_code", status.Code(err).String(), "recv_messages", wrapper.recvMessages,
		"recv_bytes", wrapper.recvBytes, "send_messages", wrapper.sendMessages,
		"send_bytes", wrapper.sendBytes, "duration_ms", time.Since(started).Milliseconds(),
	)
	if err != nil {
		exitAttrs = append(exitAttrs, ErrorAttrs(err)...)
	}
	slog.InfoContext(ctx, "grpc stream", exitAttrs...)
	ProcessTimeline().Record(grpcEvent(ctx, "grpc.stream", info.FullMethod, "close", status.Code(err).String(), time.Since(started).Nanoseconds(), ErrorAttrs(err)...), nil)
	return err
}

type loggingServerStream struct {
	grpc.ServerStream
	ctx          context.Context
	rpc          string
	recvMessages int64
	recvBytes    int64
	sendMessages int64
	sendBytes    int64
}

func (s *loggingServerStream) Context() context.Context { return s.ctx }

func (s *loggingServerStream) RecvMsg(message any) error {
	err := s.ServerStream.RecvMsg(message)
	attrs := ProtoAttrs(message)
	for i := 0; i+1 < len(attrs); i += 2 {
		if attrs[i] == "wire_bytes" {
			if bytes, ok := attrs[i+1].(int); ok {
				s.recvBytes += int64(bytes)
			}
		}
	}
	if err == nil {
		s.recvMessages++
	}
	logAttrs := append(ContextAttrs(s.ctx), "event", "grpc.stream.recv", "rpc", s.rpc, "success", err == nil)
	logAttrs = append(logAttrs, attrs...)
	slog.DebugContext(s.ctx, "grpc stream message", logAttrs...)
	phase := "recv"
	if err == io.EOF {
		phase = "half_close"
	}
	recordProtobuf(ProcessTimeline(), grpcEvent(s.ctx, "grpc.stream", s.rpc, phase, status.Code(err).String(), 0, ErrorAttrs(err)...), message)
	return err
}

func (s *loggingServerStream) SendMsg(message any) error {
	attrs := ProtoAttrs(message)
	for i := 0; i+1 < len(attrs); i += 2 {
		if attrs[i] == "wire_bytes" {
			if bytes, ok := attrs[i+1].(int); ok {
				s.sendBytes += int64(bytes)
			}
		}
	}
	err := s.ServerStream.SendMsg(message)
	if err == nil {
		s.sendMessages++
	}
	logAttrs := append(ContextAttrs(s.ctx), "event", "grpc.stream.send", "rpc", s.rpc, "success", err == nil)
	logAttrs = append(logAttrs, attrs...)
	slog.DebugContext(s.ctx, "grpc stream message", logAttrs...)
	recordProtobuf(ProcessTimeline(), grpcEvent(s.ctx, "grpc.stream", s.rpc, "send", status.Code(err).String(), 0, ErrorAttrs(err)...), message)
	return err
}

func grpcEvent(ctx context.Context, protocol, rpc, phase, outcome string, duration int64, errorAttrs ...any) DiagnosticEvent {
	event := DiagnosticEvent{RequestID: valueFromContext(ctx, requestIDKey), TraceID: valueFromContext(ctx, traceIDKey), Protocol: protocol, OperationID: rpc, RPCMethod: rpc, Phase: phase, Status: outcome, DurationNanos: duration}
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		event.Headers = make(map[string][]string, len(incoming))
		for key, values := range incoming {
			event.Headers[key] = append([]string(nil), values...)
		}
	}
	if remote, ok := peer.FromContext(ctx); ok && remote.Addr != nil {
		event.Peer = remote.Addr.String()
	}
	for index := 0; index+1 < len(errorAttrs); index += 2 {
		if errorAttrs[index] == "error" {
			event.Error, _ = errorAttrs[index+1].(string)
		}
	}
	return event
}

func protobufPayload(message any) []byte {
	protobuf, ok := message.(proto.Message)
	if !ok || protobuf == nil {
		return nil
	}
	payload, err := proto.Marshal(protobuf)
	if err != nil {
		return nil
	}
	return payload
}

func recordProtobuf(timeline *Timeline, event DiagnosticEvent, message any) {
	protobuf, ok := message.(proto.Message)
	if ok && protobuf != nil {
		if jsonPayload, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(protobuf); err == nil {
			event.PayloadJSON = string(jsonPayload)
		}
	}
	timeline.Record(event, protobufPayload(message))
}

func grpcContext(ctx context.Context) context.Context {
	requestID := ""
	traceID := ""
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		requestID = firstSafe(incoming.Get("x-request-id"))
		traceparents := incoming.Get("traceparent")
		if len(traceparents) > 0 {
			parts := strings.Split(traceparents[0], "-")
			if len(parts) >= 2 {
				traceID = SafeID(parts[1])
			}
		}
	}
	if requestID == "" {
		requestID = NewID()
	}
	if traceID == "" {
		traceID = NewID()
	}
	return WithRequestMetadata(ctx, requestID, traceID)
}

func grpcMetadataAttrs(ctx context.Context) []any {
	attrs := make([]any, 0, 4)
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		values := make(map[string][]string, len(incoming))
		for key, value := range incoming {
			values[key] = value
		}
		attrs = append(attrs, "metadata", MetadataEntries(values))
	}
	if remote, ok := peer.FromContext(ctx); ok {
		attrs = append(attrs, "peer", remote.Addr.String())
	}
	return attrs
}

func firstSafe(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return SafeID(values[0])
}
