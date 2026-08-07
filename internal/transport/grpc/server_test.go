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
	if healthResponse.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("unexpected health status: %s", healthResponse.Status)
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
