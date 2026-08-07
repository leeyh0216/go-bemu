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

// Runtime retains the health controller so the composition root can announce
// NOT_SERVING before draining transports. Shutdown follows the gRPC health and
// graceful-stop guidance instead of leaving a construction-time status behind.
//
// Sources:
//   - https://grpc.io/docs/guides/health-checking/
//   - https://grpc.io/docs/guides/server-graceful-stop/
type Runtime struct {
	server *grpc.Server
	health *health.Server
}

func (r *Runtime) Server() *grpc.Server { return r.server }

func (r *Runtime) MarkNotServing() { r.health.Shutdown() }

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
	return NewRuntimeWithServices(services, options...).Server()
}

func NewRuntimeWithServices(services Services, options ...grpc.ServerOption) *Runtime {
	options = append(options,
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor),
	)
	server := grpc.NewServer(options...)
	storage := &StorageServer{read: services.Read}
	storagepb.RegisterBigQueryReadServer(server, storage)
	storagepb.RegisterBigQueryWriteServer(server, NewStorageWriteServer(services.Write))

	healthServer := health.NewServer()
	overallStatus := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if services.Read != nil || services.Write != nil {
		overallStatus = grpc_health_v1.HealthCheckResponse_SERVING
	}
	healthServer.SetServingStatus("", overallStatus)
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
	return &Runtime{server: server, health: healthServer}
}
