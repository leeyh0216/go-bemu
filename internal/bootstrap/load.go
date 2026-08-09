package bootstrap

// Load-job composition binds public gs:// sources to the required
// GCS-compatible JSON endpoint at the outermost adapter boundary.
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

type loadRuntime struct {
	service *loadapplication.Service
	media   *objectstore.GCSJSON
}

func composeLoadJobs(
	cfg config.Config,
	jobs loadports.JobRepository,
	mutations loadports.MutationJournal,
	catalog rest.LoadCatalogUseCases,
	loader loadports.Loader,
	clock loadports.Clock,
	ids loadports.IDGenerator,
) (loadRuntime, error) {
	gcs, err := objectstore.NewGCSJSON(objectstore.GCSJSONConfig{
		Endpoint: cfg.Load.GCSEndpoint,
		Client: &http.Client{
			Timeout: cfg.Load.OperationTimeout.Value(),
		},
		MaxMetadataBytes: cfg.Load.MaxMetadataBytes,
		MaxListedObjects: cfg.Load.MaxListedObjects,
	})
	if err != nil {
		return loadRuntime{}, fmt.Errorf("configure load GCS adapter: %w", err)
	}
	loadConfig := loadapplication.Config{
		DefaultLocation: cfg.Defaults.Location, OperationTimeout: cfg.Load.OperationTimeout.Value(),
		MaxObjects: cfg.Load.MaxObjects, MaxObjectBytes: cfg.Load.MaxObjectBytes,
		MaxTotalBytes: cfg.Load.MaxTotalBytes, TempDirectory: cfg.Database.TempDirectory,
	}
	service, err := loadapplication.NewService(
		jobs, gcs, rest.NewLoadTableCatalog(catalog),
		loader, clock, ids, loadConfig, loadapplication.WithMutationJournal(mutations),
	)
	if err != nil {
		return loadRuntime{}, fmt.Errorf("configure load job service: %w", err)
	}
	return loadRuntime{service: service, media: gcs}, nil
}
