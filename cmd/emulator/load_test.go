package main

import (
	"testing"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	loadapplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
)

func TestComposeLoadJobsIsExplicitAndUsesConfiguredAdapters(t *testing.T) {
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := system.Clock{}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	cfg := config.Defaults()

	jobs := loadapplication.NewMemoryJobRepository()
	cfg.Load.GCSEndpoint = "http://127.0.0.1:4443"
	service, err := composeLoadJobs(cfg, jobs, catalog, warehouse, clock, system.IDGenerator{})
	if err != nil || service == nil {
		t.Fatalf("enabled load composition = %#v, %v", service, err)
	}
}
