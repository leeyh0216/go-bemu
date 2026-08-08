package grpcserver

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestPublicGRPCIgnoresAuthorizationMetadata(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	listener := bufconn.Listen(1024 * 1024)
	server := New()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///bufnet-public-authorization",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	baseContext, cancel := grpcTestContext(t)
	defer cancel()
	client := storagepb.NewBigQueryReadClient(connection)
	cases := []struct {
		name   string
		values []string
	}{
		{name: "missing"},
		{name: "arbitrary", values: []string{"Basic private-grpc-arbitrary"}},
		{name: "malformed", values: []string{"not-a-bearer private-grpc-malformed"}},
		{name: "fixture-issued", values: []string{"Bearer private-grpc-local-fixture"}},
		{name: "expired-looking", values: []string{"Bearer private-grpc-expired.fixture.value"}},
		{name: "duplicates", values: []string{"Bearer private-grpc-duplicate-a", "Bearer private-grpc-duplicate-b"}},
	}

	var baselineMessage string
	for index, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ctx := baseContext
			if len(test.values) > 0 {
				pairs := make([]string, 0, len(test.values)*2)
				for _, value := range test.values {
					pairs = append(pairs, "authorization", value)
				}
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...))
			}
			_, callErr := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{})
			if status.Code(callErr) != codes.Unimplemented {
				t.Fatalf("code = %s, error = %v", status.Code(callErr), callErr)
			}
			message := status.Convert(callErr).Message()
			if index == 0 {
				baselineMessage = message
			} else if message != baselineMessage {
				t.Fatalf("Authorization changed the RPC result: %q != %q", message, baselineMessage)
			}
			for _, secret := range test.values {
				if strings.Contains(callErr.Error(), secret) {
					t.Fatalf("error exposed authorization metadata %q", secret)
				}
			}
		})
	}

	output := logs.String()
	for _, test := range cases {
		for _, value := range test.values {
			if !strings.Contains(output, "authorization="+value) {
				t.Fatalf("logs omitted authorization metadata %q: %s", value, output)
			}
		}
	}
}
