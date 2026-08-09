package main

// The executable is the composition root for the ports-and-adapters runtime.
// Protocol handlers stay in internal/transport, while this package owns only
// configuration, adapter wiring, listener lifecycle, and graceful shutdown.
//
// Lifecycle references:
//   - net/http shutdown: https://pkg.go.dev/net/http#Server.Shutdown
//   - gRPC graceful stop: https://pkg.go.dev/google.golang.org/grpc#Server.GracefulStop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	stateadapter "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/admin"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
	grpcserver "github.com/leeyh0216/go-bemu/internal/transport/grpc"
	"github.com/leeyh0216/go-bemu/internal/transport/rest"
)

type servingEndpoint struct {
	name     string
	listener net.Listener
	serve    func() error
}

type serveResult struct {
	name string
	err  error
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		attrs := []any{"event", "runtime.exit"}
		attrs = append(attrs, observability.ErrorAttrs(err)...)
		slog.Error("BQEMU stopped", attrs...)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	loaded, err := config.Load(args)
	if err != nil {
		return err
	}
	if loaded.PrintEffective {
		_, err := stdout.Write(loaded.EffectiveYAML)
		return err
	}
	cfg := loaded.Config
	logger, err := configureLogger(cfg.Logging, os.Stderr)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	observability.Configure(cfg.Logging.UnsafePayloads)
	logger.InfoContext(ctx, "configuration loaded",
		"event", "runtime.configuration.loaded",
		"model_version", config.APIVersion,
		"config_path", emptyAs(loaded.ConfigPath, "defaults"),
		"source_fingerprint", emptyAs(loaded.SourceFingerprint, "none"),
		"effective_fingerprint", loaded.EffectiveFingerprint,
	)
	if err := prepareDirectory(ctx, cfg.Database.TempDirectory); err != nil {
		return err
	}
	stateStore, err := stateadapter.Open(ctx, stateadapter.Config{
		DataSourceName: cfg.State.DSN,
		BusyTimeout:    cfg.State.BusyTimeout.Value(),
		JournalMode:    cfg.State.JournalMode,
		Synchronous:    cfg.State.Synchronous,
	})
	if err != nil {
		return fmt.Errorf("open BQEMU state store: %w", err)
	}
	closeState := true
	defer func() {
		if closeState {
			_ = stateStore.Close()
		}
	}()
	warehouse, err := duckdb.New(cfg.Database.DSN)
	if err != nil {
		return err
	}
	closeWarehouse := true
	defer func() {
		if closeWarehouse {
			_ = warehouse.Close()
		}
	}()

	clock := system.Clock{}
	catalogService := composeCatalogService(cfg, stateStore, warehouse, clock)
	if err := catalogService.RecoverCatalogState(ctx); err != nil {
		return fmt.Errorf("recover canonical catalog state: %w", err)
	}
	if err := ensureDefaultProject(ctx, catalogService, cfg.Defaults.ProjectID); err != nil {
		return fmt.Errorf("initialize default project: %w", err)
	}
	if err := bootstrapCatalog(ctx, catalogService, cfg.Bootstrap); err != nil {
		return fmt.Errorf("bootstrap catalog: %w", err)
	}
	if err := verifyMaterializationTarget(ctx, catalogService, cfg.Query.Materialization); err != nil {
		return fmt.Errorf("verify query materialization target: %w", err)
	}
	if _, err := stateStore.ReconcileInterruptedJobs(ctx, clock.Now()); err != nil {
		return fmt.Errorf("reconcile interrupted jobs: %w", err)
	}
	ddlParser, err := googlesqladapter.NewParser()
	if err != nil {
		return fmt.Errorf("initialize GoogleSQL DDL parser: %w", err)
	}
	jobRepository := stateadapter.NewQueryJobRepository(stateStore)
	queryService := application.NewQueryService(
		jobRepository, warehouse, clock, system.IDGenerator{},
		application.WithQueryDefaultLocation(cfg.Defaults.Location),
		application.WithQueryAnalyzer(warehouse),
		application.WithQueryMaterializer(warehouse),
		application.WithQueryDestinationCatalog(catalogService),
		application.WithQueryDDLParser(ddlParser), application.WithQueryParameterValidator(ddlParser),
		application.WithQueryDDLExecutor(catalogService),
		application.WithQueryOperationTimeout(cfg.Query.OperationTimeout.Value()),
		application.WithQueryCompensationTimeout(cfg.Query.CompensationTimeout.Value()),
		application.WithAnonymousQueryTTL(cfg.Query.AnonymousResultTTL.Value()),
		application.WithQueryMaterializationTarget(application.MaterializationTarget{ProjectID: cfg.Query.Materialization.ProjectID, DatasetID: cfg.Query.Materialization.DatasetID, TTL: cfg.Query.Materialization.Expiration.Value()}),
	)
	loadService, err := composeLoadJobs(
		cfg, stateadapter.NewLoadJobRepository(stateStore), catalogService,
		warehouse, clock, system.IDGenerator{},
	)
	if err != nil {
		return err
	}
	readRuntime, err := composeStorageRead(cfg, warehouse, catalogService, clock, system.IDGenerator{}, logger, stateStore)
	if err != nil {
		return fmt.Errorf("configure Storage Read: %w", err)
	}
	defer func() {
		if readRuntime != nil {
			closeContext, cancel := context.WithTimeout(context.Background(), cfg.Runtime.ShutdownTimeout.Value())
			defer cancel()
			_ = readRuntime.Close(closeContext)
		}
	}()
	writeRuntime, err := composeStorageWrite(ctx, cfg, warehouse, catalogService, stateStore, clock, system.IDGenerator{}, logger)
	if err != nil {
		return fmt.Errorf("configure Storage Write: %w", err)
	}
	defer func() {
		if writeRuntime != nil {
			closeContext, cancel := context.WithTimeout(context.Background(), cfg.Runtime.ShutdownTimeout.Value())
			defer cancel()
			_ = writeRuntime.Close(closeContext)
		}
	}()

	restOptions := make([]rest.Option, 0, 3)
	restOptions = append(restOptions, rest.WithRequestBodyLimits(
		cfg.Server.HTTP.MaxCompressedRequestBytes, cfg.Server.HTTP.MaxUncompressedRequestBytes,
	))
	restOptions = append(restOptions, rest.WithMediaUploadMaxBytes(cfg.Load.MaxObjectBytes))
	restOptions = append(restOptions, rest.WithTableDataAPI(catalogService))
	if cfg.UI.Enabled {
		restOptions = append(restOptions, rest.WithConsoleDirectory(cfg.UI.Directory))
	}
	readiness := compositeHealthChecker{stateStore, warehouse}
	var restServer *rest.Server
	if loadService == nil {
		restServer = rest.NewServer(catalogService, queryService, readiness, cfg.Server.HTTP.PublicURL, restOptions...)
	} else {
		restServer = rest.NewServerWithLoadJobs(
			catalogService, queryService, loadService, readiness, cfg.Server.HTTP.PublicURL, restOptions...,
		)
	}
	publicHTTP := &http.Server{
		Addr:              cfg.Server.HTTP.Address,
		Handler:           restServer.Handler(),
		ReadHeaderTimeout: cfg.Server.HTTP.ReadHeaderTimeout.Value(),
		ReadTimeout:       cfg.Server.HTTP.ReadTimeout.Value(),
		WriteTimeout:      cfg.Server.HTTP.WriteTimeout.Value(),
		IdleTimeout:       cfg.Server.HTTP.IdleTimeout.Value(),
	}

	grpcOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.Server.GRPC.MaxReceiveMessageBytes),
		grpc.MaxSendMsgSize(cfg.Server.GRPC.MaxSendMessageBytes),
	}
	if cfg.Server.TLS.CertFile != "" {
		transportCredentials, err := credentials.NewServerTLSFromFile(cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile)
		if err != nil {
			return fmt.Errorf("load gRPC TLS identity: %w", err)
		}
		grpcOptions = append(grpcOptions, grpc.Creds(transportCredentials))
	}
	// Storage adapters are registered independently. A nil application service
	// retains the generated RPC surface but reports NOT_SERVING and returns
	// UNIMPLEMENTED rather than advertising a false capability.
	grpcRuntime := grpcserver.NewRuntimeWithServices(grpcserver.Services{
		Read: readRuntime.Service, Write: writeRuntime.Service,
	}, grpcOptions...)
	grpcService := grpcRuntime.Server()

	endpoints := make([]servingEndpoint, 0, 3)
	publicListener, err := net.Listen("tcp", cfg.Server.HTTP.Address)
	if err != nil {
		return fmt.Errorf("listen on public HTTP address %s: %w", cfg.Server.HTTP.Address, err)
	}
	endpoints = append(endpoints, servingEndpoint{
		name: "rest", listener: publicListener,
		serve: func() error {
			if cfg.Server.TLS.CertFile != "" {
				return publicHTTP.ServeTLS(publicListener, cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile)
			}
			return publicHTTP.Serve(publicListener)
		},
	})

	grpcListener, err := net.Listen("tcp", cfg.Server.GRPC.Address)
	if err != nil {
		closeEndpoints(endpoints)
		return fmt.Errorf("listen on gRPC address %s: %w", cfg.Server.GRPC.Address, err)
	}
	endpoints = append(endpoints, servingEndpoint{
		name: "storage-grpc", listener: grpcListener,
		serve: func() error { return grpcService.Serve(grpcListener) },
	})

	var adminHTTP *http.Server
	if cfg.Admin.Enabled {
		adminService, err := admin.New(admin.Options{
			TokenFile: cfg.Admin.TokenFile, MaxStackBytes: cfg.Admin.MaxStackBytes, Logger: logger,
		})
		if err != nil {
			closeEndpoints(endpoints)
			return fmt.Errorf("configure admin server: %w", err)
		}
		adminHTTP = &http.Server{
			Addr: cfg.Admin.Address, Handler: adminService.Handler(),
			ReadHeaderTimeout: cfg.Admin.ReadHeaderTimeout.Value(),
			ReadTimeout:       cfg.Server.HTTP.ReadTimeout.Value(),
			WriteTimeout:      cfg.Server.HTTP.WriteTimeout.Value(),
			IdleTimeout:       cfg.Server.HTTP.IdleTimeout.Value(),
		}
		adminListener, err := net.Listen("tcp", cfg.Admin.Address)
		if err != nil {
			closeEndpoints(endpoints)
			return fmt.Errorf("listen on admin address %s: %w", cfg.Admin.Address, err)
		}
		endpoints = append(endpoints, servingEndpoint{
			name: "admin", listener: adminListener,
			serve: func() error {
				if cfg.Server.TLS.CertFile != "" {
					return adminHTTP.ServeTLS(adminListener, cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile)
				}
				return adminHTTP.Serve(adminListener)
			},
		})
	}
	defer closeEndpoints(endpoints)

	results := make(chan serveResult, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint := endpoint
		logger.InfoContext(ctx, "listener starting",
			"event", "side_effect.before", "side_effect", "network.listen",
			"endpoint", endpoint.name, "address", endpoint.listener.Addr().String(),
			"tls", cfg.Server.TLS.CertFile != "",
		)
		go func() { results <- serveResult{name: endpoint.name, err: endpoint.serve()} }()
	}

	var servingFailure error
	select {
	case <-ctx.Done():
		logger.InfoContext(context.Background(), "shutdown requested",
			"event", "domain.transition", "state_from", "SERVING", "state_to", "STOPPING",
			"reason", context.Cause(ctx),
		)
	case result := <-results:
		if !isExpectedServeError(result.err) {
			servingFailure = fmt.Errorf("%s server stopped: %w", result.name, result.err)
		}
	}

	grpcRuntime.MarkNotServing()
	drainContext, drainCancel := context.WithTimeout(context.Background(), cfg.Runtime.ServerDrainTimeout.Value())
	shutdownErr := shutdownServers(drainContext, publicHTTP, adminHTTP, grpcService)
	drainCancel()
	storageContext, storageCancel := context.WithTimeout(context.Background(), cfg.Runtime.StorageCloseTimeout.Value())
	defer storageCancel()
	queryCloseErr, readCloseErr, writeCloseErr := shutdownQueryAndStorage(
		storageContext, queryService, readRuntime, writeRuntime,
	)
	readRuntime = nil
	writeRuntime = nil
	// Closing DuckDB while a query ignored cancellation would introduce a data
	// race in the adapter. On a bounded query-drain failure, leave process-owned
	// resources to OS teardown rather than crossing the still-active boundary.
	if queryCloseErr != nil {
		closeWarehouse = false
		closeState = false
	}
	if servingFailure != nil {
		return errors.Join(servingFailure, shutdownErr, queryCloseErr, readCloseErr, writeCloseErr)
	}
	return errors.Join(shutdownErr, queryCloseErr, readCloseErr, writeCloseErr)
}

func ensureDefaultProject(ctx context.Context, service *application.CatalogService, projectID string) error {
	if _, err := service.GetProject(ctx, projectID); err == nil {
		return nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	_, err := service.CreateProject(ctx, domain.Project{
		ID: projectID, FriendlyName: "BQEMU default project",
	})
	return err
}

func bootstrapCatalog(ctx context.Context, service *application.CatalogService, bootstrap config.BootstrapConfig) error {
	for _, project := range bootstrap.Projects {
		if _, err := service.GetProject(ctx, project.ID); errors.Is(err, domain.ErrNotFound) {
			if _, createErr := service.CreateProject(ctx, domain.Project{ID: project.ID}); createErr != nil {
				return createErr
			}
		} else if err != nil {
			return err
		}
		for _, dataset := range project.Datasets {
			existing, err := service.GetDataset(ctx, project.ID, dataset.ID)
			if errors.Is(err, domain.ErrNotFound) {
				_, err = service.CreateDataset(ctx, domain.Dataset{ProjectID: project.ID, ID: dataset.ID, Location: dataset.Location, Description: dataset.Description, Labels: dataset.Labels, DefaultTableExpirationMs: dataset.DefaultTableExpirationMs, DefaultPartitionExpirationMs: dataset.DefaultPartitionExpirationMs})
				if err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if !strings.EqualFold(existing.Location, dataset.Location) || existing.Description != dataset.Description || !equalStringMap(existing.Labels, dataset.Labels) || !equalInt64Pointer(existing.DefaultTableExpirationMs, dataset.DefaultTableExpirationMs) || !equalInt64Pointer(existing.DefaultPartitionExpirationMs, dataset.DefaultPartitionExpirationMs) {
				return fmt.Errorf("%w: bootstrap dataset %s/%s declaration differs", domain.ErrConflict, project.ID, dataset.ID)
			}
		}
	}
	return nil
}

// verifyMaterializationTarget makes the configured result dataset part of the
// startup contract. bootstrapCatalog runs first, so a declared bootstrap
// dataset can satisfy the target on a fresh process and after a SQLite restart.
// Per-query location matching remains in QueryService because source datasets
// can determine a query location that is different from the server default.
func verifyMaterializationTarget(ctx context.Context, service *application.CatalogService, target config.MaterializationConfig) error {
	if target.ProjectID == "" {
		return nil
	}
	if _, err := service.GetDataset(ctx, target.ProjectID, target.DatasetID); err != nil {
		return fmt.Errorf("configured dataset %s/%s: %w", target.ProjectID, target.DatasetID, err)
	}
	return nil
}

func equalStringMap(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func equalInt64Pointer(left, right *int64) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

type catalogWarehouse interface {
	ports.WarehouseAdmin
	ports.TableDataReader
	ports.TableDataWriter
}

type compositeHealthChecker []ports.HealthChecker

func (checks compositeHealthChecker) Ping(ctx context.Context) error {
	for index, check := range checks {
		if check == nil {
			return fmt.Errorf("readiness dependency %d is not configured", index)
		}
		if err := check.Ping(ctx); err != nil {
			return fmt.Errorf("readiness dependency %d: %w", index, err)
		}
	}
	return nil
}

func composeCatalogService(cfg config.Config, repository ports.CatalogRepository, warehouse catalogWarehouse, clock ports.Clock) *application.CatalogService {
	options := []application.CatalogOption{
		application.WithDefaultLocation(cfg.Defaults.Location),
		application.WithCatalogCompensationTimeout(cfg.Query.CompensationTimeout.Value()),
		application.WithTableDataReader(warehouse),
		application.WithTableDataWriter(warehouse),
		application.WithTableDataOperationTimeout(cfg.TableData.OperationTimeout.Value()),
		application.WithMaxTableDataPageRows(cfg.TableData.MaxPageRows),
		application.WithMaxTableDataResponseBytes(cfg.TableData.MaxResponseBytes),
		application.WithMaxTableDataRowBytes(cfg.TableData.MaxRowBytes),
	}
	if ledger, ok := repository.(ports.TableDataInsertIDLedger); ok {
		options = append(options, application.WithTableDataInsertIDLedger(ledger))
	}
	return application.NewCatalogService(repository, warehouse, clock, options...)
}

type runtimeCloser interface {
	Close(context.Context) error
}

// shutdownQueryAndStorage uses one configured cleanup budget. Query workers
// must relinquish the warehouse before Storage Read/Write cleanup begins.
func shutdownQueryAndStorage(ctx context.Context, query, read, write runtimeCloser) (queryErr, readErr, writeErr error) {
	started := time.Now()
	slog.InfoContext(ctx, "query service shutdown",
		"event", "side_effect.before", "side_effect", "application.query.close")
	queryErr = query.Close(ctx)
	attrs := []any{
		"event", "side_effect.after", "side_effect", "application.query.close",
		"duration_ms", time.Since(started).Milliseconds(), "success", queryErr == nil,
	}
	if queryErr != nil {
		attrs = append(attrs, observability.ErrorAttrs(queryErr)...)
	}
	slog.InfoContext(context.Background(), "query service shutdown", attrs...)
	if queryErr != nil {
		return queryErr, nil, nil
	}
	readErr = read.Close(ctx)
	writeErr = write.Close(ctx)
	return queryErr, readErr, writeErr
}

func configureLogger(cfg config.LoggingConfig, output io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", cfg.Level)
	}
	options := &slog.HandlerOptions{Level: level}
	if cfg.Format == "text" {
		return slog.New(slog.NewTextHandler(output, options)), nil
	}
	if cfg.Format != "json" {
		return nil, fmt.Errorf("unsupported log format %q", cfg.Format)
	}
	return slog.New(slog.NewJSONHandler(output, options)), nil
}

func prepareDirectory(ctx context.Context, path string) error {
	started := time.Now()
	slog.InfoContext(ctx, "temporary directory preparation",
		"event", "side_effect.before", "side_effect", "filesystem.mkdir",
		"path", path, "mode", "0700",
	)
	err := os.MkdirAll(path, 0o700)
	attrs := []any{
		"event", "side_effect.after", "side_effect", "filesystem.mkdir",
		"path", path, "duration_ms", time.Since(started).Milliseconds(), "success", err == nil,
	}
	if err != nil {
		attrs = append(attrs, observability.ErrorAttrs(err)...)
	}
	slog.InfoContext(ctx, "temporary directory preparation", attrs...)
	if err != nil {
		return fmt.Errorf("prepare temporary directory %s: %w", path, err)
	}
	return nil
}

func shutdownServers(ctx context.Context, publicHTTP, adminHTTP *http.Server, grpcService *grpc.Server) error {
	started := time.Now()
	slog.InfoContext(ctx, "server shutdown",
		"event", "side_effect.before", "side_effect", "network.shutdown",
	)
	var shutdownErrors []error
	if publicHTTP != nil {
		if err := publicHTTP.Shutdown(ctx); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("shutdown public HTTP: %w", err))
		}
	}
	if adminHTTP != nil {
		if err := adminHTTP.Shutdown(ctx); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("shutdown admin HTTP: %w", err))
		}
	}
	if grpcService != nil {
		stopped := make(chan struct{})
		go func() {
			grpcService.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-ctx.Done():
			grpcService.Stop()
			shutdownErrors = append(shutdownErrors, fmt.Errorf("graceful gRPC shutdown: %w", ctx.Err()))
		}
	}
	err := errors.Join(shutdownErrors...)
	slog.InfoContext(context.Background(), "server shutdown",
		"event", "side_effect.after", "side_effect", "network.shutdown",
		"duration_ms", time.Since(started).Milliseconds(), "success", err == nil,
	)
	return err
}

func closeEndpoints(endpoints []servingEndpoint) {
	for _, endpoint := range endpoints {
		_ = endpoint.listener.Close()
	}
}

func isExpectedServeError(err error) bool {
	return err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) ||
		errors.Is(err, net.ErrClosed)
}

func emptyAs(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
