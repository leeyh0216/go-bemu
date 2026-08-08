package main

// Load-job composition keeps object-store selection at the outermost adapter
// boundary. The public default accepts only gs:// URIs through the configured
// JSON API endpoint; file:// access requires an explicit local-only opt-in.
//
// Official contracts:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
//   - https://cloud.google.com/storage/docs/json_api/v1/objects

import (
	"fmt"
	"net/http"

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
	loader loadports.Loader,
	clock loadports.Clock,
	ids loadports.IDGenerator,
) (rest.LoadJobUseCases, error) {
	if !cfg.Load.Enabled {
		return nil, nil
	}
	gcs, err := objectstore.NewGCSJSON(objectstore.GCSJSONConfig{
		Endpoint: cfg.Load.GCSEndpoint,
		Client: &http.Client{
			Timeout: cfg.Load.OperationTimeout.Value(),
		},
		MaxMetadataBytes: cfg.Load.MaxMetadataBytes,
		MaxListedObjects: cfg.Load.MaxListedObjects,
	})
	if err != nil {
		return nil, fmt.Errorf("configure load GCS adapter: %w", err)
	}
	var objects loadports.ObjectStore
	if cfg.Load.AllowFileSources {
		objects, err = objectstore.NewRouter(objectstore.FileSystem{}, gcs)
	} else {
		objects, err = objectstore.NewGCSOnlyRouter(gcs)
	}
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
		loader, clock, ids, loadConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("configure load job service: %w", err)
	}
	return service, nil
}
