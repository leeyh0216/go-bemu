package grpcserver

import (
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/leeyh0216/go-bemu/internal/observability"
	readapp "github.com/leeyh0216/go-bemu/internal/storageread/application"
	writeapp "github.com/leeyh0216/go-bemu/internal/storagewrite/application"
)

const (
	storageReadServiceName  = "google.cloud.bigquery.storage.v1.BigQueryRead"
	storageWriteServiceName = "google.cloud.bigquery.storage.v1.BigQueryWrite"
)

// Services keeps protocol adapters dependent on application services rather
// than concrete databases. A nil service remains explicitly UNIMPLEMENTED.
type Services struct {
	Read  *readapp.Service
	Write *writeapp.Service
}

// StorageServer binds the official Google-generated service definitions.
// Protocol source:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
type StorageServer struct {
	storagepb.UnimplementedBigQueryReadServer

	read *readapp.Service
}

var _ storagepb.BigQueryReadServer = (*StorageServer)(nil)

func New(options ...grpc.ServerOption) *grpc.Server {
	return NewWithServices(Services{}, options...)
}

func NewWithServices(services Services, options ...grpc.ServerOption) *grpc.Server {
	options = append(options,
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor),
	)
	server := grpc.NewServer(options...)
	storage := &StorageServer{read: services.Read}
	storagepb.RegisterBigQueryReadServer(server, storage)
	storagepb.RegisterBigQueryWriteServer(server, NewStorageWriteServer(services.Write))

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	readStatus := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if services.Read != nil {
		readStatus = grpc_health_v1.HealthCheckResponse_SERVING
	}
	healthServer.SetServingStatus(storageReadServiceName, readStatus)
	writeStatus := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if services.Write != nil {
		writeStatus = grpc_health_v1.HealthCheckResponse_SERVING
	}
	healthServer.SetServingStatus(storageWriteServiceName, writeStatus)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	reflection.Register(server)
	return server
}
