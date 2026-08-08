package main

// Storage Read composition binds the protocol-independent session service to
// a replaceable snapshot materializer. DuckDB is selected only here; the
// application and gRPC adapters do not import it.
//
// Official lifecycle and negotiation contracts:
//   - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest
//   - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession

import (
	"context"
	"errors"
	"log/slog"

	"github.com/leeyh0216/go-bemu/internal/config"
	readapplication "github.com/leeyh0216/go-bemu/internal/storageread/application"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

type storageReadRuntime struct {
	Service     *readapplication.Service
	cancel      context.CancelFunc
	cleanupDone chan error
}

func composeStorageRead(
	cfg config.Config,
	factory readports.SnapshotMaterializerFactory,
	predicateParser readports.RowRestrictionParser,
	resolver readports.TableSchemaResolver,
	clock readports.Clock,
	ids readports.IDGenerator,
	logger *slog.Logger,
	stateRepositories ...readports.SessionStateRepository,
) (*storageReadRuntime, error) {
	if !cfg.Storage.Read.Enabled {
		return &storageReadRuntime{}, nil
	}
	materializer, err := factory.NewSnapshotMaterializer(resolver, readports.SnapshotMaterializerConfig{
		TempDir: cfg.Database.TempDirectory, TempFilePattern: cfg.Storage.Read.TempFilePattern,
		SpillThresholdBytes: cfg.Storage.Read.SpillThresholdBytes,
		MaxRowBytes:         cfg.Storage.Read.MaxRowBytes, MaxBatchBytes: cfg.Storage.Read.MaxResponseBytes,
		MaxSchemaBytes:       cfg.Storage.Read.MaxSchemaBytes,
		MaxSnapshotBytes:     cfg.Storage.Read.MaxSnapshotBytes,
		MaxSnapshotRows:      cfg.Storage.Read.MaxSnapshotRows,
		ProtocolModelVersion: cfg.Storage.Read.ProtocolModelVersion,
	})
	if err != nil {
		return nil, err
	}
	if len(stateRepositories) > 1 {
		return nil, errors.New("at most one Storage Read session state repository may be configured")
	}
	options := make([]readapplication.Option, 0, len(stateRepositories))
	if len(stateRepositories) == 1 {
		options = append(options, readapplication.WithSessionStateRepository(stateRepositories[0]))
	}
	service, err := readapplication.New(readapplication.Config{
		Location: cfg.Defaults.Location, ProtocolModelVersion: cfg.Storage.Read.ProtocolModelVersion,
		MaxStreams: int32(cfg.Storage.Read.MaxStreams), DefaultStreamCount: int32(cfg.Storage.Read.DefaultStreamCount),
		SessionTTL: cfg.Runtime.ReadSessionTTL.Value(), CleanupInterval: cfg.Runtime.CleanupInterval.Value(),
		MaxRowsPerResponse: int64(cfg.Storage.Read.RowsPerResponse), MaxSessions: cfg.Storage.Read.MaxSessions,
		MaxSnapshotBytes: cfg.Storage.Read.MaxSnapshotBytes, MaxTotalSnapshotBytes: cfg.Storage.Read.MaxTotalSnapshotBytes,
	}, materializer, predicateParser, clock, ids, logger, options...)
	if err != nil {
		return nil, err
	}
	if err := service.ReconcilePersistedSessions(context.Background()); err != nil {
		return nil, err
	}
	cleanupContext, cancel := context.WithCancel(context.Background())
	runtime := &storageReadRuntime{Service: service, cancel: cancel, cleanupDone: make(chan error, 1)}
	go func() { runtime.cleanupDone <- service.RunCleanup(cleanupContext) }()
	return runtime, nil
}

func (runtime *storageReadRuntime) Close(ctx context.Context) error {
	if runtime == nil || runtime.Service == nil {
		return nil
	}
	runtime.cancel()
	serviceErr := runtime.Service.Close(ctx)
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
	return errors.Join(serviceErr, cleanupErr)
}
