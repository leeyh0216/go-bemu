package main

// EngineRuntime exists only in the executable composition root. It validates
// one adapter bundle and is immediately split into consumer-owned narrow ports;
// application services never receive this aggregate or discover dependencies
// from it at run time.

import (
	"fmt"
	"reflect"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/domain"
	enginecontract "github.com/leeyh0216/go-bemu/internal/engine"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/ports"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

type engineLifecycle interface {
	Close() error
}

type engineRuntimeDescriptor struct {
	Capabilities      enginecontract.Capabilities
	Health            ports.HealthChecker
	Catalog           ports.CatalogStorage
	DDL               ports.DDLStorage
	Query             ports.QueryEngine
	QueryAnalyzer     ports.QueryAnalyzer
	QueryOperations   ports.QueryOperationEngine
	QueryMaterializer ports.QueryMaterializer
	TableData         ports.TableDataReader
	Loader            loadports.Loader
	ReadFactory       readports.SnapshotMaterializerFactory
	WriteFactory      writeports.CoordinatorFactory
	Lifecycle         engineLifecycle
}

type engineRuntime struct {
	capabilities      enginecontract.Capabilities
	health            ports.HealthChecker
	catalog           ports.CatalogStorage
	ddl               ports.DDLStorage
	query             ports.QueryEngine
	queryAnalyzer     ports.QueryAnalyzer
	queryOperations   ports.QueryOperationEngine
	queryMaterializer ports.QueryMaterializer
	tableData         ports.TableDataReader
	loader            loadports.Loader
	readFactory       readports.SnapshotMaterializerFactory
	writeFactory      writeports.CoordinatorFactory
	lifecycle         engineLifecycle
}

func newEngineRuntime(descriptor engineRuntimeDescriptor) (*engineRuntime, error) {
	capabilities, err := validateRuntimeCapabilities(descriptor.Capabilities)
	if err != nil {
		return nil, err
	}
	for _, dependency := range []struct {
		name  string
		value any
	}{
		{name: "health checker", value: descriptor.Health},
		{name: "catalog storage", value: descriptor.Catalog},
		{name: "DDL storage", value: descriptor.DDL},
		{name: "query engine", value: descriptor.Query},
		{name: "query analyzer", value: descriptor.QueryAnalyzer},
		{name: "semantic query executor", value: descriptor.QueryOperations},
		{name: "query materializer", value: descriptor.QueryMaterializer},
		{name: "table data reader", value: descriptor.TableData},
		{name: "load adapter", value: descriptor.Loader},
		{name: "Storage Read factory", value: descriptor.ReadFactory},
		{name: "Storage Write factory", value: descriptor.WriteFactory},
		{name: "engine lifecycle", value: descriptor.Lifecycle},
	} {
		if runtimeDependencyIsNil(dependency.value) {
			return nil, fmt.Errorf("%w: engine runtime %s is required", domain.ErrPrecondition, dependency.name)
		}
	}
	return &engineRuntime{
		capabilities: capabilities,
		health:       descriptor.Health, catalog: descriptor.Catalog, ddl: descriptor.DDL,
		query: descriptor.Query, queryAnalyzer: descriptor.QueryAnalyzer,
		queryOperations: descriptor.QueryOperations, queryMaterializer: descriptor.QueryMaterializer,
		tableData: descriptor.TableData, loader: descriptor.Loader,
		readFactory: descriptor.ReadFactory, writeFactory: descriptor.WriteFactory,
		lifecycle: descriptor.Lifecycle,
	}, nil
}

func composeDuckDBEngine(dsn string) (*engineRuntime, error) {
	warehouse, err := duckdb.New(dsn)
	if err != nil {
		return nil, err
	}
	runtime, err := newEngineRuntime(engineRuntimeDescriptor{
		Capabilities: warehouse.Capabilities(),
		Health:       warehouse, Catalog: warehouse, DDL: warehouse,
		Query: warehouse, QueryAnalyzer: warehouse, QueryOperations: warehouse, QueryMaterializer: warehouse,
		TableData: warehouse, Loader: warehouse, ReadFactory: warehouse, WriteFactory: warehouse,
		Lifecycle: warehouse,
	})
	if err != nil {
		_ = warehouse.Close()
		return nil, fmt.Errorf("compose DuckDB engine runtime: %w", err)
	}
	return runtime, nil
}

func validateRuntimeCapabilities(capabilities enginecontract.Capabilities) (enginecontract.Capabilities, error) {
	detached, err := enginecontract.NewCapabilities(capabilities.Descriptor())
	if err != nil || capabilities.Fingerprint() == "" || detached.Fingerprint() != capabilities.Fingerprint() {
		return enginecontract.Capabilities{}, fmt.Errorf("%w: engine runtime capabilities are invalid", domain.ErrPrecondition)
	}
	return detached, nil
}

func runtimeDependencyIsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (runtime *engineRuntime) Close() error {
	if runtime == nil || runtimeDependencyIsNil(runtime.lifecycle) {
		return nil
	}
	return runtime.lifecycle.Close()
}
