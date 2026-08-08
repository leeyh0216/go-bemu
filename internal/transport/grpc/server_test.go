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
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/leeyh0216/go-bemu/internal/contractspec"
	"github.com/leeyh0216/go-bemu/internal/contracttest"
	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

func TestRegistersOfficialStorageServicesAndHealth(t *testing.T) {
	contracttest.Operation(t, "grpc.bigquery-read.split-read-stream")
	contracttest.Operation(t, "grpc.bigquery-write.flush-rows")
	contracttest.Operation(t, "grpc.health.check")
	contracttest.Operation(t, "grpc.health.list")
	contracttest.Operation(t, "grpc.health.watch")
	contracttest.Operation(t, "grpc.reflection-v1.server-reflection-info")
	contracttest.Operation(t, "grpc.reflection-v1alpha.server-reflection-info")
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

	healthClient := grpc_health_v1.NewHealthClient(connection)
	healthResponse, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if healthResponse.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("unexpected health status: %s", healthResponse.Status)
	}
	healthList, err := healthClient.List(ctx, &grpc_health_v1.HealthListRequest{})
	if err != nil || len(healthList.GetStatuses()) == 0 {
		t.Fatalf("health list = %#v, %v", healthList, err)
	}
	healthWatch, err := healthClient.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if watched, err := healthWatch.Recv(); err != nil || watched.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health watch = %#v, %v", watched, err)
	}
	for _, service := range []string{storageReadServiceName, storageWriteServiceName} {
		response, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: service})
		if err != nil || response.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			t.Fatalf("unconfigured %s health = %#v, %v", service, response, err)
		}
	}
	_, err = storagepb.NewBigQueryReadClient(connection).CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected explicit unimplemented Storage Read RPC, got %v", err)
	}
	_, err = storagepb.NewBigQueryReadClient(connection).SplitReadStream(ctx, &storagepb.SplitReadStreamRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected explicit unimplemented SplitReadStream RPC, got %v", err)
	}
	_, err = storagepb.NewBigQueryWriteClient(connection).FlushRows(ctx, &storagepb.FlushRowsRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected explicit unimplemented FlushRows RPC, got %v", err)
	}

	reflection, err := reflectionv1.NewServerReflectionClient(connection).ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := reflection.Send(&reflectionv1.ServerReflectionRequest{
		MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{ListServices: ""},
	}); err != nil {
		t.Fatal(err)
	}
	if response, err := reflection.Recv(); err != nil || response.GetListServicesResponse() == nil {
		t.Fatalf("reflection v1 response = %#v, %v", response, err)
	}
	reflectionAlpha, err := reflectionv1alpha.NewServerReflectionClient(connection).ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := reflectionAlpha.Send(&reflectionv1alpha.ServerReflectionRequest{
		MessageRequest: &reflectionv1alpha.ServerReflectionRequest_ListServices{ListServices: ""},
	}); err != nil {
		t.Fatal(err)
	}
	if response, err := reflectionAlpha.Recv(); err != nil || response.GetListServicesResponse() == nil {
		t.Fatalf("reflection v1alpha response = %#v, %v", response, err)
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
	assertStorageDescriptorsMatchManifest(t, services)
}

func assertStorageDescriptorsMatchManifest(t *testing.T, services map[string]grpc.ServiceInfo) {
	t.Helper()
	expected := make(map[string]map[string]bool)
	for _, rpc := range contractspec.GRPCRPCs() {
		if expected[rpc.Service] == nil {
			expected[rpc.Service] = make(map[string]bool)
		}
		expected[rpc.Service][rpc.Method] = true
	}
	for serviceName, methods := range expected {
		service, exists := services[serviceName]
		if !exists {
			t.Errorf("manifest gRPC service %q is not registered", serviceName)
			continue
		}
		actual := make(map[string]bool, len(service.Methods))
		for _, method := range service.Methods {
			actual[method.Name] = true
		}
		for method := range methods {
			if !actual[method] {
				t.Errorf("manifest gRPC method %s/%s is not registered", serviceName, method)
			}
			delete(actual, method)
		}
		for method := range actual {
			t.Errorf("registered gRPC method %s/%s is absent from the operation manifest", serviceName, method)
		}
	}
	for serviceName := range services {
		if expected[serviceName] == nil {
			t.Errorf("registered gRPC service %q is absent from the operation manifest", serviceName)
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
