package bootstrap

import (
	"testing"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	loadapplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
)

func TestComposeLoadJobsAlwaysUsesConfiguredGCSAdapter(t *testing.T) {
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := system.Clock{}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	cfg := config.Defaults()

	jobs := loadapplication.NewMemoryJobRepository()
	runtime, err := composeLoadJobs(
		cfg, jobs, loadapplication.NewMemoryMutationJournal(), catalog, warehouse, clock, system.IDGenerator{},
	)
	if err != nil || runtime.service == nil || runtime.media == nil {
		t.Fatalf("load composition = %#v, %v", runtime, err)
	}
}
