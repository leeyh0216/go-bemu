package grpcserver

import (
	"context"
	"net"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

func TestRegistersOfficialStorageServicesAndHealth(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := New()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	ctx, cancel := grpcTestContext(t)
	defer cancel()
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	healthResponse, err := grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if healthResponse.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("unexpected health status: %s", healthResponse.Status)
	}
	for _, service := range []string{storageReadServiceName, storageWriteServiceName} {
		response, err := grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: service})
		if err != nil || response.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			t.Fatalf("unconfigured %s health = %#v, %v", service, response, err)
		}
	}
	_, err = storagepb.NewBigQueryReadClient(connection).CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected explicit unimplemented Storage Read RPC, got %v", err)
	}

	services := server.GetServiceInfo()
	for _, service := range []string{
		"google.cloud.bigquery.storage.v1.BigQueryRead",
		"google.cloud.bigquery.storage.v1.BigQueryWrite",
	} {
		if _, ok := services[service]; !ok {
			t.Errorf("service %q is not registered", service)
		}
	}
}

func TestStorageHealthWatchObservesShutdownBeforeTransportStops(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	read := newWireReadService(t, newWireMaterializer(t, domain.FormatArrow, 1))
	runtime := NewRuntimeWithServices(Services{Read: read})
	server := runtime.Server()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	ctx, cancel := grpcTestContext(t)
	defer cancel()
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	watch, err := grpc_health_v1.NewHealthClient(connection).Watch(ctx, &grpc_health_v1.HealthCheckRequest{Service: storageReadServiceName})
	if err != nil {
		t.Fatal(err)
	}
	first, err := watch.Recv()
	if err != nil || first.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("initial health = %#v, %v", first, err)
	}
	runtime.MarkNotServing()
	second, err := watch.Recv()
	if err != nil || second.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("shutdown health = %#v, %v", second, err)
	}
}

func TestStorageWriteHealthRequiresInjectedApplicationService(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	write := newWireWriteService(t, newWireWriteCoordinator())
	server := NewWithServices(Services{Write: write})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	ctx, cancel := grpcTestContext(t)
	defer cancel()
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	response, err := grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: storageWriteServiceName})
	if err != nil || response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("configured write health = %#v, %v", response, err)
	}
}
