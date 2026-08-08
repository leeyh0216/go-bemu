package grpcserver

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	authstatic "github.com/leeyh0216/go-bemu/internal/auth/adapters/static"
	authapp "github.com/leeyh0216/go-bemu/internal/auth/application"
	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
	readDomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

func TestGRPCAuthenticationPublicEdgeUnaryBidiHealthAndReflection(t *testing.T) {
	ctx, cancel := grpcAuthTestContext(t)
	defer cancel()
	var logs bytes.Buffer
	identity := "private-grpc-principal"
	validToken := "private-valid-grpc-token"
	invalidToken := "private-invalid-grpc-token"
	authentication := newGRPCStaticAuthentication(t, ctx, &logs, identity, validToken)
	materializer := newWireMaterializer(t, readDomain.FormatArrow, 1)
	coordinator := newWireWriteCoordinator()
	connection := startAuthenticatedGRPCTestServer(t, Services{
		Read: newWireReadService(t, materializer), Write: newWireWriteService(t, coordinator),
		Authentication: authentication,
	})

	readClient := storagepb.NewBigQueryReadClient(connection)
	request := wireCreateSessionRequest(storagepb.DataFormat_ARROW)
	for _, test := range []struct {
		name   string
		values []string
	}{
		{name: "missing"},
		{name: "invalid", values: []string{"Bearer " + invalidToken}},
		{name: "duplicate", values: []string{"Bearer " + validToken, "Bearer " + validToken}},
	} {
		t.Run("unary-"+test.name, func(t *testing.T) {
			_, err := readClient.CreateReadSession(outgoingAuthorization(ctx, test.values...), request)
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("status = %s, want Unauthenticated: %v", status.Code(err), err)
			}
			if materializer.calls != 0 {
				t.Fatalf("materializer calls = %d, want 0", materializer.calls)
			}
		})
	}
	if _, err := readClient.CreateReadSession(outgoingAuthorization(ctx, "Bearer "+validToken), request); err != nil {
		t.Fatalf("authenticated CreateReadSession: %v", err)
	}
	if materializer.calls != 1 {
		t.Fatalf("materializer calls = %d, want 1", materializer.calls)
	}

	writeClient := storagepb.NewBigQueryWriteClient(connection)
	deniedAppend, err := writeClient.AppendRows(outgoingAuthorization(ctx, "Bearer "+invalidToken))
	if err != nil {
		t.Fatalf("create denied bidi stream: %v", err)
	}
	_ = deniedAppend.Send(&storagepb.AppendRowsRequest{})
	if _, err := deniedAppend.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("denied AppendRows status = %s, want Unauthenticated: %v", status.Code(err), err)
	}
	coordinator.mu.Lock()
	visibleRows, stagedStreams := coordinator.visibleRows, len(coordinator.staged)
	coordinator.mu.Unlock()
	if visibleRows != 0 || stagedStreams != 0 {
		t.Fatalf("denied AppendRows reached coordinator: visible=%d staged=%d", visibleRows, stagedStreams)
	}

	descriptor, rows := wireProtoRows(t, 7)
	authorizedAppend, err := writeClient.AppendRows(outgoingAuthorization(ctx, "Bearer "+validToken))
	if err != nil {
		t.Fatal(err)
	}
	if err := authorizedAppend.Send(&storagepb.AppendRowsRequest{
		WriteStream: "projects/test-project/datasets/analytics/tables/events/_default",
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: descriptor},
			Rows:         &storagepb.ProtoRows{SerializedRows: rows},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if response, err := authorizedAppend.Recv(); err != nil || response.GetError() != nil {
		t.Fatalf("authenticated AppendRows response=%#v err=%v", response, err)
	}
	_ = authorizedAppend.CloseSend()
	coordinator.mu.Lock()
	visibleRows = coordinator.visibleRows
	coordinator.mu.Unlock()
	if visibleRows != 1 {
		t.Fatalf("authenticated AppendRows visible rows = %d, want 1", visibleRows)
	}

	healthClient := grpc_health_v1.NewHealthClient(connection)
	if response, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("public health Check response=%#v err=%v", response, err)
	}
	watch, err := healthClient.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := watch.Recv(); err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("public health Watch response=%#v err=%v", response, err)
	}

	reflectionV1, err := reflectionv1.NewServerReflectionClient(connection).ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = reflectionV1.Send(&reflectionv1.ServerReflectionRequest{MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{ListServices: ""}})
	if _, err := reflectionV1.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("reflection v1 status = %s, want Unauthenticated: %v", status.Code(err), err)
	}
	reflectionV1Alpha, err := reflectionv1alpha.NewServerReflectionClient(connection).ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = reflectionV1Alpha.Send(&reflectionv1alpha.ServerReflectionRequest{MessageRequest: &reflectionv1alpha.ServerReflectionRequest_ListServices{ListServices: ""}})
	if _, err := reflectionV1Alpha.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("reflection v1alpha status = %s, want Unauthenticated: %v", status.Code(err), err)
	}

	output := logs.String()
	for _, secret := range []string{identity, validToken, invalidToken} {
		if strings.Contains(output, secret) {
			t.Fatalf("gRPC auth logs leaked %q: %s", secret, output)
		}
	}
}

func TestGRPCAuthenticationInterceptorsPropagateOneContextContract(t *testing.T) {
	ctx, cancel := grpcAuthTestContext(t)
	defer cancel()
	identity := "interceptor-principal"
	token := "interceptor-token"
	authentication := newGRPCStaticAuthentication(t, ctx, &bytes.Buffer{}, identity, token)
	incoming := metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))

	var unaryCalls atomic.Int64
	_, err := authenticationUnaryServerInterceptor(authentication)(
		incoming, struct{}{}, &grpc.UnaryServerInfo{FullMethod: "/test.Auth/Unary"},
		func(handlerContext context.Context, _ any) (any, error) {
			unaryCalls.Add(1)
			assertGRPCAuthenticationContext(t, handlerContext, identity)
			return struct{}{}, nil
		},
	)
	if err != nil || unaryCalls.Load() != 1 {
		t.Fatalf("unary interceptor calls=%d err=%v", unaryCalls.Load(), err)
	}

	stream := &authTestServerStream{context: incoming}
	var streamCalls atomic.Int64
	err = authenticationStreamServerInterceptor(authentication)(
		nil, stream, &grpc.StreamServerInfo{FullMethod: "/test.Auth/Bidi", IsClientStream: true, IsServerStream: true},
		func(_ any, authenticated grpc.ServerStream) error {
			streamCalls.Add(1)
			assertGRPCAuthenticationContext(t, authenticated.Context(), identity)
			return authenticated.RecvMsg(&struct{}{})
		},
	)
	if err != io.EOF || streamCalls.Load() != 1 || stream.recvCalls.Load() != 1 {
		t.Fatalf("stream interceptor calls=%d recv=%d err=%v", streamCalls.Load(), stream.recvCalls.Load(), err)
	}

	deniedStream := &authTestServerStream{context: ctx}
	err = authenticationStreamServerInterceptor(authentication)(
		nil, deniedStream, &grpc.StreamServerInfo{FullMethod: "/test.Auth/Bidi", IsClientStream: true, IsServerStream: true},
		func(_ any, authenticated grpc.ServerStream) error {
			streamCalls.Add(1)
			return authenticated.RecvMsg(&struct{}{})
		},
	)
	if status.Code(err) != codes.Unauthenticated || deniedStream.recvCalls.Load() != 0 || streamCalls.Load() != 1 {
		t.Fatalf("denied stream status=%s recv=%d handler_calls=%d", status.Code(err), deniedStream.recvCalls.Load(), streamCalls.Load())
	}
}

func assertGRPCAuthenticationContext(t *testing.T, ctx context.Context, identity string) {
	t.Helper()
	principal, principalOK := authapp.PrincipalFromContext(ctx)
	decision, decisionOK := authapp.DecisionFromContext(ctx)
	if !principalOK || !decisionOK || principal.Digest() != authdomain.Digest([]byte(identity)) || !decision.Allowed() {
		t.Fatalf("authentication context principal=%#v/%t decision=%#v/%t", principal, principalOK, decision, decisionOK)
	}
}

func newGRPCStaticAuthentication(t *testing.T, ctx context.Context, logs *bytes.Buffer, principal, token string) *authapp.Service {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.yaml")
	payload := "apiVersion: auth.bqemu.dev/v1alpha1\nkind: StaticTokenSet\ntokens:\n" +
		"  - principal: " + principal + "\n    token: " + token + "\n"
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := authstatic.NewFileSource(path)
	if err != nil {
		t.Fatal(err)
	}
	options := authstatic.DefaultOptions()
	options.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	verifier, err := authstatic.New(ctx, source, options)
	if err != nil {
		t.Fatal(err)
	}
	service, err := authapp.New(authapp.DefaultConfig(), verifier, options.Logger)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func startAuthenticatedGRPCTestServer(t *testing.T, services Services) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(4 * 1024 * 1024)
	server := NewWithServices(services)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///bufnet-auth",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func outgoingAuthorization(ctx context.Context, values ...string) context.Context {
	pairs := make([]string, 0, len(values)*2)
	for _, value := range values {
		pairs = append(pairs, "authorization", value)
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...))
}

func grpcAuthTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 5 * time.Second
	if configured := os.Getenv("BQEMU_AUTH_TRANSPORT_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_AUTH_TRANSPORT_TEST_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	return context.WithTimeout(t.Context(), timeout)
}

type authTestServerStream struct {
	context   context.Context
	recvCalls atomic.Int64
}

func (stream *authTestServerStream) SetHeader(metadata.MD) error  { return nil }
func (stream *authTestServerStream) SendHeader(metadata.MD) error { return nil }
func (stream *authTestServerStream) SetTrailer(metadata.MD)       {}
func (stream *authTestServerStream) Context() context.Context     { return stream.context }
func (stream *authTestServerStream) SendMsg(any) error            { return nil }
func (stream *authTestServerStream) RecvMsg(any) error {
	stream.recvCalls.Add(1)
	return io.EOF
}
