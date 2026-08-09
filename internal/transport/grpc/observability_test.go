package grpcserver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/leeyh0216/go-bemu/internal/observability"
)

func TestGRPCBoundaryUsesSharedEventContractAndTimeline(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	timeline := observability.ConfigureTimeline(observability.TimelineConfig{MaxEvents: 10, MaxBytes: 4 << 10, MaxPayloadBytes: 64})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer grpc-token", "x-request-id", "grpc-request"))
	request := &storagepb.CreateReadSessionRequest{ReadSession: &storagepb.ReadSession{ReadOptions: &storagepb.ReadSession_TableReadOptions{SelectedFields: []string{"diagnostic_field"}, RowRestriction: "payload = 'diagnostic-row'"}}}
	_, err := UnaryServerInterceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: "/example.Service/Call"}, func(context.Context, any) (any, error) { return nil, fmt.Errorf("backend diagnostic error") })
	if err == nil {
		t.Fatal("expected error")
	}
	for _, expected := range []string{"boundary.enter", "boundary.exit", "diagnostic_field", "diagnostic-row", "backend diagnostic error", "authorization=Bearer grpc-token", "grpc-request"} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("log omitted %q: %s", expected, output.String())
		}
	}
	snapshot := timeline.Snapshot(0, 0)
	if len(snapshot.Events) != 2 || snapshot.Events[0].RPCMethod != "/example.Service/Call" {
		t.Fatalf("timeline = %#v", snapshot.Events)
	}
}
