package main

import (
	"errors"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/domain"
	enginecontract "github.com/leeyh0216/go-bemu/internal/engine"
)

func TestComposeDuckDBEngineBuildsValidatedNarrowRuntime(t *testing.T) {
	runtime, err := composeDuckDBEngine("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if runtime.capabilities.Identity().ID() != "duckdb" || runtime.capabilities.Fingerprint() == "" {
		t.Fatalf("runtime capabilities = %#v", runtime.capabilities.Descriptor())
	}
	for name, dependency := range map[string]any{
		"health": runtime.health, "catalog": runtime.catalog, "DDL": runtime.ddl, "query": runtime.query,
		"query analyzer": runtime.queryAnalyzer, "query operations": runtime.queryOperations,
		"query materializer": runtime.queryMaterializer, "table data": runtime.tableData,
		"loader": runtime.loader, "read factory": runtime.readFactory, "write factory": runtime.writeFactory,
	} {
		if runtimeDependencyIsNil(dependency) {
			t.Fatalf("runtime %s port is nil", name)
		}
	}

	descriptor := runtime.capabilities.Descriptor()
	descriptor.Decimal.MaxPrecision = 1
	if runtime.capabilities.Decimal().MaxPrecision != domain.SparkDecimalMaxPrecision {
		t.Fatal("runtime exposed mutable capabilities")
	}
}

func TestEngineRuntimeRejectsZeroAndTypedNilDependencies(t *testing.T) {
	valid, err := composeDuckDBEngine("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valid.Close() })
	baseline := descriptorFromRuntime(valid)

	zeroCapabilities := baseline
	zeroCapabilities.Capabilities = enginecontract.Capabilities{}
	if runtime, err := newEngineRuntime(zeroCapabilities); runtime != nil || !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("zero capabilities runtime=%v error=%v", runtime, err)
	}

	var typedNil *duckdb.Warehouse
	for name, mutate := range map[string]func(*engineRuntimeDescriptor){
		"health":             func(value *engineRuntimeDescriptor) { value.Health = typedNil },
		"catalog":            func(value *engineRuntimeDescriptor) { value.Catalog = typedNil },
		"DDL":                func(value *engineRuntimeDescriptor) { value.DDL = typedNil },
		"query":              func(value *engineRuntimeDescriptor) { value.Query = typedNil },
		"query analyzer":     func(value *engineRuntimeDescriptor) { value.QueryAnalyzer = typedNil },
		"query operations":   func(value *engineRuntimeDescriptor) { value.QueryOperations = typedNil },
		"query materializer": func(value *engineRuntimeDescriptor) { value.QueryMaterializer = typedNil },
		"table data":         func(value *engineRuntimeDescriptor) { value.TableData = typedNil },
		"loader":             func(value *engineRuntimeDescriptor) { value.Loader = typedNil },
		"read factory":       func(value *engineRuntimeDescriptor) { value.ReadFactory = typedNil },
		"write factory":      func(value *engineRuntimeDescriptor) { value.WriteFactory = typedNil },
		"lifecycle":          func(value *engineRuntimeDescriptor) { value.Lifecycle = typedNil },
	} {
		t.Run(name, func(t *testing.T) {
			descriptor := baseline
			mutate(&descriptor)
			runtime, err := newEngineRuntime(descriptor)
			if runtime != nil || !errors.Is(err, domain.ErrPrecondition) {
				t.Fatalf("runtime=%v error=%v, want nil precondition", runtime, err)
			}
		})
	}
}

func descriptorFromRuntime(runtime *engineRuntime) engineRuntimeDescriptor {
	return engineRuntimeDescriptor{
		Capabilities: runtime.capabilities,
		Health:       runtime.health, Catalog: runtime.catalog, DDL: runtime.ddl,
		Query: runtime.query, QueryAnalyzer: runtime.queryAnalyzer,
		QueryOperations: runtime.queryOperations, QueryMaterializer: runtime.queryMaterializer,
		TableData: runtime.tableData, Loader: runtime.loader,
		ReadFactory: runtime.readFactory, WriteFactory: runtime.writeFactory,
		Lifecycle: runtime.lifecycle,
	}
}
