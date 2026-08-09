package main

// Load-job composition keeps object-store selection at the outermost adapter
// boundary. Public load sources accept only gs:// URIs through the configured
// JSON API endpoint.
//
// Official contracts:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
//   - https://cloud.google.com/storage/docs/json_api/v1/objects

import (
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/objectstore"
	"github.com/leeyh0216/go-bemu/internal/config"
	loadapplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/transport/rest"
)

func composeLoadJobs(
	cfg config.Config,
	jobs loadports.JobRepository,
	catalog rest.CatalogUseCases,
	warehouse *duckdb.Warehouse,
	clock loadports.Clock,
	ids loadports.IDGenerator,
) (rest.LoadJobUseCases, error) {
	media, err := objectstore.NewMediaStore(filepath.Join(cfg.Database.TempDirectory, "bqemu-media"), cfg.Load.MaxObjectBytes)
	if err != nil {
		return nil, fmt.Errorf("configure load media store: %w", err)
	}
	gcs, err := objectstore.NewGCSJSON(objectstore.GCSJSONConfig{
		Endpoint: cfg.Load.GCSEndpoint, Client: &http.Client{Timeout: cfg.Load.OperationTimeout.Value()},
		MaxMetadataBytes: cfg.Load.MaxMetadataBytes, MaxListedObjects: cfg.Load.MaxListedObjects,
	})
	if err != nil {
		return nil, fmt.Errorf("configure load GCS adapter: %w", err)
	}
	objects, err := objectstore.NewRouterWithMedia(gcs, media)
	if err != nil {
		return nil, fmt.Errorf("configure load object-store router: %w", err)
	}
	loadConfig := loadapplication.Config{
		DefaultLocation: cfg.Defaults.Location, OperationTimeout: cfg.Load.OperationTimeout.Value(),
		MaxObjects: cfg.Load.MaxObjects, MaxObjectBytes: cfg.Load.MaxObjectBytes,
		MaxTotalBytes: cfg.Load.MaxTotalBytes, TempDirectory: cfg.Database.TempDirectory,
	}
	service, err := loadapplication.NewService(
		jobs, objects, rest.NewLoadTableCatalog(catalog),
		warehouse, clock, ids, loadConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("configure load job service: %w", err)
	}
	return &loadRuntime{LoadJobUseCases: service, media: media}, nil
}

type loadRuntime struct {
	rest.LoadJobUseCases
	media loadports.MediaUploadStore
}

func (r *loadRuntime) MediaUploads() loadports.MediaUploadStore { return r.media }
