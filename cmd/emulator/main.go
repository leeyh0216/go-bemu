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

	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	v0442 "github.com/leeyh0216/go-bemu/internal/adapters/sparkbigquery/v0442"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/admin"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/capabilityspec"
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
	storageEngine, err := composeDuckDBEngine(cfg.Database.DSN)
	if err != nil {
		return err
	}
	closeEngine := true
	defer func() {
		if closeEngine {
			_ = storageEngine.Close()
		}
	}()
	engineCapabilities := storageEngine.capabilities
	health := storageEngine.health
	catalogStorage := storageEngine.catalog
	ddlStorage := storageEngine.ddl
	queryEngine := storageEngine.query
	queryFallbackAnalyzer := storageEngine.queryAnalyzer
	queryOperationEngine := storageEngine.queryOperations
	queryMaterializer := storageEngine.queryMaterializer
	statementExecutor := storageEngine.statementExecutor
	statementMaterializer := storageEngine.statementMaterializer
	tableDataReader := storageEngine.tableData
	loader := storageEngine.loader
	readFactory := storageEngine.readFactory
	writeFactory := storageEngine.writeFactory
	logger.InfoContext(ctx, "storage engine composed",
		"event", "runtime.engine.composed",
		"engine_id", engineCapabilities.Identity().ID(),
		"engine_version", engineCapabilities.Identity().Version(),
		"capability_fingerprint", engineCapabilities.Fingerprint(),
	)

	state, err := composeStateRuntime(ctx, cfg.State.DSN)
	if err != nil {
		return err
	}
	closeState := true
	defer func() {
		if closeState {
			_ = state.Close()
		}
	}()
	catalogRepository := state.catalog
	jobRepository := state.queryJobs
	clock := system.Clock{}
	catalogService := composeCatalogService(cfg, catalogRepository, catalogStorage, ddlStorage, tableDataReader, clock)
	if err := ensureDefaultProject(ctx, catalogService, cfg.Defaults.ProjectID); err != nil {
		return fmt.Errorf("initialize default project: %w", err)
	}
	queryAnalyzer, err := v0442.NewAnalyzer(queryFallbackAnalyzer)
	if err != nil {
		return fmt.Errorf("configure Spark BigQuery query profiles: %w", err)
	}
	googleSQLGateway, err := googlesqladapter.NewGateway(catalogService)
	if err != nil {
		return fmt.Errorf("configure GoogleSQL analyzer gateway: %w", err)
	}
	queryService, err := application.NewQueryService(
		jobRepository, queryEngine, queryAnalyzer, queryOperationEngine, catalogService, clock, system.IDGenerator{},
		application.WithQueryDefaultLocation(cfg.Defaults.Location),
		application.WithQueryAnalyzer(queryAnalyzer),
		application.WithGoogleSQLGateway(googleSQLGateway),
		application.WithStatementExecutor(statementExecutor),
		application.WithStatementMaterializer(statementMaterializer),
		application.WithQueryDDLExecutor(catalogService),
		application.WithQueryMaterializer(queryMaterializer),
		application.WithQueryDestinationCatalog(catalogService),
		application.WithQueryOperationTimeout(cfg.Query.OperationTimeout.Value()),
		application.WithQueryCompensationTimeout(cfg.Query.CompensationTimeout.Value()),
		application.WithAnonymousQueryTTL(cfg.Query.AnonymousResultTTL.Value()),
	)
	if err != nil {
		return fmt.Errorf("configure query service: %w", err)
	}
	loadService, err := composeLoadJobs(cfg, state.loadJobs, catalogService, loader, clock, system.IDGenerator{})
	if err != nil {
		return err
	}
	readRuntime, err := composeStorageRead(
		cfg, readFactory, ddlParser, catalogService, clock, system.IDGenerator{}, logger, state.readSessions,
	)
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
	writeRuntime, err := composeStorageWrite(
		ctx, cfg, writeFactory, catalogService, clock, system.IDGenerator{}, logger, state.writeState,
	)
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
	restOptions = append(restOptions, rest.WithCapabilityProfiles(capabilityspec.Profiles()))
	restOptions = append(restOptions, rest.WithRequestBodyLimits(
		cfg.Server.HTTP.MaxCompressedRequestBytes, cfg.Server.HTTP.MaxUncompressedRequestBytes,
	))
	restOptions = append(restOptions, rest.WithTableDataAPI(catalogService))
	if cfg.UI.Enabled {
		restOptions = append(restOptions, rest.WithConsoleDirectory(cfg.UI.Directory))
	}
	var restServer *rest.Server
	if loadService == nil {
		restServer = rest.NewServer(catalogService, queryService, health, cfg.Server.HTTP.PublicURL, restOptions...)
	} else {
		restServer = rest.NewServerWithLoadJobs(
			catalogService, queryService, loadService, health, cfg.Server.HTTP.PublicURL, restOptions...,
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
		closeEngine = false
		closeState = false
	}
	if servingFailure != nil {
		return errors.Join(servingFailure, shutdownErr, queryCloseErr, readCloseErr, writeCloseErr)
	}
	return errors.Join(shutdownErr, queryCloseErr, readCloseErr, writeCloseErr)
}

func ensureDefaultProject(ctx context.Context, catalog *application.CatalogService, projectID string) error {
	if _, err := catalog.GetProject(ctx, projectID); err == nil {
		return nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	_, err := catalog.CreateProject(ctx, domain.Project{
		ID: projectID, FriendlyName: "BQEMU default project",
	})
	return err
}

func composeCatalogService(
	cfg config.Config,
	repository ports.CatalogRepository,
	storage ports.CatalogStorage,
	ddlStorage ports.DDLStorage,
	tableData ports.TableDataReader,
	clock ports.Clock,
) *application.CatalogService {
	return application.NewCatalogService(
		repository, storage, clock, application.WithDefaultLocation(cfg.Defaults.Location),
		application.WithCatalogCompensationTimeout(cfg.Query.CompensationTimeout.Value()),
		application.WithDDLStorage(ddlStorage),
		application.WithTableDataReader(tableData),
		application.WithTableDataOperationTimeout(cfg.TableData.OperationTimeout.Value()),
		application.WithMaxTableDataPageRows(cfg.TableData.MaxPageRows),
		application.WithMaxTableDataResponseBytes(cfg.TableData.MaxResponseBytes),
		application.WithMaxTableDataRowBytes(cfg.TableData.MaxRowBytes),
	)
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
