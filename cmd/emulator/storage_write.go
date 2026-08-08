package main

// Storage Write composition preserves many logical streams while serializing
// DuckDB work through a bounded single-worker coordinator. Request bytes use
// weighted admission, while PENDING rows spill to hidden DuckDB staging tables
// until atomic commit. This allows Spark task parallelism without claiming that
// the embedded engine has BigQuery's distributed write backend.
//
// Official lifecycle:
// https://cloud.google.com/bigquery/docs/write-api-batch

import (
	"context"
	"errors"
	"log/slog"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/config"
	writeapplication "github.com/leeyh0216/go-bemu/internal/storagewrite/application"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

type storageWriteRuntime struct {
	Service     *writeapplication.Service
	coordinator *duckdb.StorageWriteCoordinator
	cancel      context.CancelFunc
	cleanupDone chan error
}

func composeStorageWrite(
	ctx context.Context,
	cfg config.Config,
	warehouse *duckdb.Warehouse,
	resolver duckdb.StorageWriteTableSchemaResolver,
	clock writeports.Clock,
	ids writeports.IDGenerator,
	logger *slog.Logger,
) (*storageWriteRuntime, error) {
	if !cfg.Storage.Write.Enabled {
		return &storageWriteRuntime{}, nil
	}
	coordinator, err := duckdb.NewStorageWriteCoordinator(ctx, warehouse, resolver, duckdb.StorageWriteCoordinatorConfig{
		QueueCapacity:             cfg.Storage.Write.QueueCapacity,
		QueueWaitTimeout:          cfg.Storage.Write.QueueWaitTimeout.Value(),
		OperationTimeout:          cfg.Storage.Write.OperationTimeout.Value(),
		MaxInFlightBytes:          cfg.Storage.Write.MaxInFlightBytes,
		MaxInFlightBytesPerStream: cfg.Storage.Write.MaxInFlightBytesPerStream,
		MaxStagedBytes:            cfg.Storage.Write.MaxStagedBytes,
		MaxStagedBytesPerStream:   cfg.Storage.Write.MaxStagedBytesPerStream,
	})
	if err != nil {
		return nil, err
	}
	service, err := writeapplication.New(writeapplication.Config{
		Location: cfg.Defaults.Location, ProtocolModelVersion: cfg.Storage.Write.ProtocolModelVersion,
		MaxStreams: cfg.Storage.Write.MaxStreams, MaxAppendBytes: cfg.Storage.Write.MaxAppendRequestBytes,
		MaxAppendEnvelopeBytes:      cfg.Storage.Write.MaxAppendEnvelopeBytes,
		MaxConcurrentAppendRequests: cfg.Storage.Write.MaxConcurrentAppendRequests,
		OrphanTTL:                   cfg.Storage.Write.OrphanTTL.Value(), CleanupInterval: cfg.Storage.Write.CleanupInterval.Value(),
	}, coordinator, clock, ids, logger)
	if err != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), cfg.Runtime.ShutdownTimeout.Value())
		defer cancel()
		_ = coordinator.Close(closeContext)
		return nil, err
	}
	cleanupContext, cancel := context.WithCancel(context.Background())
	runtime := &storageWriteRuntime{
		Service: service, coordinator: coordinator, cancel: cancel, cleanupDone: make(chan error, 1),
	}
	go func() { runtime.cleanupDone <- service.RunCleanup(cleanupContext) }()
	return runtime, nil
}

func (runtime *storageWriteRuntime) Close(ctx context.Context) error {
	if runtime == nil || runtime.Service == nil {
		return nil
	}
	runtime.cancel()
	serviceErr := runtime.Service.Close(ctx)
	coordinatorErr := runtime.coordinator.Close(ctx)
	var cleanupErr error
	select {
	case cleanupErr = <-runtime.cleanupDone:
		if errors.Is(cleanupErr, context.Canceled) {
			cleanupErr = nil
		}
	case <-ctx.Done():
		cleanupErr = ctx.Err()
	}
	runtime.Service = nil
	return errors.Join(serviceErr, coordinatorErr, cleanupErr)
}
