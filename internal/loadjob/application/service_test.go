package application

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	"github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type testClock struct{ value time.Time }

func (c testClock) Now() time.Time { return c.value }

type testIDs struct{}

func (testIDs) NewID() string { return "generated" }

type trackingJobRepository struct {
	*MemoryJobRepository
	createOrGetCalls int
}

func (r *trackingJobRepository) CreateOrGet(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	r.createOrGetCalls++
	return r.MemoryJobRepository.CreateOrGet(ctx, job)
}

type testObjectStore struct {
	mu             sync.Mutex
	objects        map[string][]byte
	sizeOffset     int64
	generation     string
	listGeneration string
	etag           string
	listETag       string
	opens          int
	opened         []ports.ObjectInfo
}

func (s *testObjectStore) Get(_ context.Context, uri string) (ports.ObjectInfo, error) {
	payload, ok := s.objects[uri]
	if !ok {
		return ports.ObjectInfo{}, domain.ErrNotFound
	}
	return ports.ObjectInfo{
		URI: uri, Size: int64(len(payload)) + s.sizeOffset,
		Generation: defaultString(s.generation, "1"), ETag: s.etag,
	}, nil
}
func (s *testObjectStore) List(_ context.Context, pattern string) ([]ports.ObjectInfo, error) {
	result := make([]ports.ObjectInfo, 0)
	for uri, payload := range s.objects {
		matched, _ := filepath.Match(pattern, uri)
		if matched {
			result = append(result, ports.ObjectInfo{
				URI: uri, Size: int64(len(payload)) + s.sizeOffset,
				Generation: defaultString(s.listGeneration, defaultString(s.generation, "1")),
				ETag:       defaultString(s.listETag, s.etag),
			})
		}
	}
	return result, nil
}
func (s *testObjectStore) Open(_ context.Context, object ports.ObjectInfo) (io.ReadCloser, error) {
	s.mu.Lock()
	s.opens++
	s.opened = append(s.opened, object)
	s.mu.Unlock()
	return io.NopCloser(bytes.NewReader(s.objects[object.URI])), nil
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

type testCatalog struct {
	table                        domain.Table
	missing                      bool
	datasetLocation              string
	defaultPartitionExpirationMs *int64
	publishErr                   error
	published                    *[]domain.Table
	updated                      *[]domain.Table
}

func (c testCatalog) GetTable(_ context.Context, reference domain.TableReference) (domain.Table, error) {
	if c.missing || reference != c.table.Reference {
		return domain.Table{}, domain.ErrNotFound
	}
	return c.table, nil
}

func (c testCatalog) GetDataset(_ context.Context, projectID, datasetID string) (domain.Dataset, error) {
	if projectID != c.table.Reference.ProjectID || datasetID != c.table.Reference.DatasetID {
		return domain.Dataset{}, domain.ErrNotFound
	}
	if c.datasetLocation != "" {
		return domain.Dataset{
			Location:                     c.datasetLocation,
			DefaultPartitionExpirationMs: catalogdomain.CloneOptionalInt64(c.defaultPartitionExpirationMs),
		}, nil
	}
	return domain.Dataset{
		Location:                     "US",
		DefaultPartitionExpirationMs: catalogdomain.CloneOptionalInt64(c.defaultPartitionExpirationMs),
	}, nil
}

func (c testCatalog) PublishTable(_ context.Context, table domain.Table) error {
	if c.publishErr != nil {
		return c.publishErr
	}
	if c.published != nil {
		*c.published = append(*c.published, table)
	}
	return nil
}

func (c testCatalog) PublishSchemaUpdate(
	_ context.Context,
	reference domain.TableReference,
	_ []domain.Field,
	updated []domain.Field,
) error {
	if c.publishErr != nil {
		return c.publishErr
	}
	if c.updated != nil {
		*c.updated = append(*c.updated, domain.Table{Reference: reference, Schema: catalogdomain.CloneFields(updated)})
	}
	return nil
}

type testLoader struct {
	testLoadPlanner
	mu             sync.Mutex
	calls          int
	paths          []string
	block          bool
	discardCalls   int
	discardErr     error
	inferredSchema []domain.Field
	inferErr       error
	inferCalls     int
	inferOptions   ports.ParquetSchemaOptions
}

type gatedLoader struct {
	testLoadPlanner
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type testLoadPlanner struct {
	capabilities  *engine.Capabilities
	plannerErr    error
	loadPlanErr   error
	plannerCalls  int
	loadPlanCalls int
	schemaPlanner *engine.SchemaPlanner
	loadPlanner   *ports.Planner
}

type testSchemaPlanAdapter struct{ planner *testLoadPlanner }

func (adapter testSchemaPlanAdapter) ValidateSchemaIntent(context.Context, engine.SchemaIntent) error {
	adapter.planner.plannerCalls++
	return adapter.planner.plannerErr
}

type testLoadPlanAdapter struct{ planner *testLoadPlanner }

func (adapter testLoadPlanAdapter) ValidateLoadRequest(context.Context, ports.LoadPlanRequest) (string, error) {
	adapter.planner.loadPlanCalls++
	if adapter.planner.loadPlanErr != nil {
		return "", adapter.planner.loadPlanErr
	}
	return strings.Repeat("a", 64), nil
}

func (planner *testLoadPlanner) PlanSchema(ctx context.Context, intent engine.SchemaIntent) (engine.SchemaPlan, error) {
	if err := planner.ensurePlanners(); err != nil {
		return engine.SchemaPlan{}, err
	}
	return planner.schemaPlanner.Plan(ctx, intent)
}

func (planner *testLoadPlanner) PlanLoad(ctx context.Context, request ports.LoadPlanRequest) (ports.LoadPlan, error) {
	if err := planner.ensurePlanners(); err != nil {
		return ports.LoadPlan{}, err
	}
	return planner.loadPlanner.Plan(ctx, request)
}

func (planner *testLoadPlanner) validateExecution(ctx context.Context, plan ports.LoadPlan, objects []ports.LocalObject) (ports.LoadPlanRequest, error) {
	if err := planner.ensurePlanners(); err != nil {
		return ports.LoadPlanRequest{}, err
	}
	return planner.loadPlanner.ValidateExecution(ctx, plan, objects)
}

func (planner *testLoadPlanner) ensurePlanners() error {
	if planner.schemaPlanner != nil && planner.loadPlanner != nil {
		return nil
	}
	capabilities := testLoaderCapabilities()
	if planner.capabilities != nil {
		capabilities = *planner.capabilities
	}
	schemaPlanner, err := engine.NewSchemaPlanner(capabilities, testSchemaPlanAdapter{planner: planner})
	if err != nil {
		return err
	}
	loadPlanner, err := ports.NewPlanner(capabilities, testLoadPlanAdapter{planner: planner})
	if err != nil {
		return err
	}
	planner.schemaPlanner, planner.loadPlanner = schemaPlanner, loadPlanner
	return nil
}

func testLoaderCapabilities() engine.Capabilities {
	identity, err := engine.NewIdentity("test-loader", "1")
	if err != nil {
		panic(err)
	}
	capabilities, err := engine.NewCapabilities(engine.CapabilitiesDescriptor{
		Identity:  identity,
		Decimal:   engine.DecimalCapabilities{Supported: true, MaxPrecision: catalogdomain.SupportedDecimalMaxPrecision, MaxScale: catalogdomain.SupportedDecimalMaxScale},
		Composite: engine.CompositeCapabilities{MaxStructDepth: 15, MaxListDepth: 15},
		Transactions: map[engine.TransactionScope]bool{
			engine.TransactionScopeSingleTable: true,
		},
		AtomicReplacements: map[engine.AtomicReplacementScope]bool{
			engine.AtomicReplacementTable: true,
		},
		DDL: map[engine.DDLOperation]engine.DDLCapability{
			engine.DDLCreateTable: {Guarantee: engine.DDLGuaranteeAtomicPhysicalTable},
			engine.DDLAddColumn: {
				Guarantee: engine.DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 15,
			},
			engine.DDLRelaxColumn: {
				Guarantee: engine.DDLGuaranteeAtomicPhysicalStatement, MaxFieldPathDepth: 15,
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return capabilities
}

func (l *gatedLoader) ExecuteLoad(ctx context.Context, plan ports.LoadPlan, objects []ports.LocalObject) (ports.LoadResult, error) {
	request, err := l.validateExecution(ctx, plan, objects)
	if err != nil {
		return ports.LoadResult{}, err
	}
	l.once.Do(func() { close(l.started) })
	select {
	case <-l.release:
		return ports.LoadResult{
			OutputRows: 3, CreatedDestination: request.CreateDestination, UpdatedDestination: request.UpdateDestination,
		}, nil
	case <-ctx.Done():
		return ports.LoadResult{}, ctx.Err()
	}
}

func (l *testLoader) ExecuteLoad(ctx context.Context, plan ports.LoadPlan, objects []ports.LocalObject) (ports.LoadResult, error) {
	request, err := l.validateExecution(ctx, plan, objects)
	if err != nil {
		return ports.LoadResult{}, err
	}
	l.mu.Lock()
	l.calls++
	for _, object := range objects {
		l.paths = append(l.paths, object.Path)
		if _, err := os.Stat(object.Path); err != nil {
			l.mu.Unlock()
			return ports.LoadResult{}, err
		}
	}
	l.mu.Unlock()
	if l.block {
		<-ctx.Done()
		return ports.LoadResult{}, ctx.Err()
	}
	return ports.LoadResult{
		OutputRows: 3, CreatedDestination: request.CreateDestination, UpdatedDestination: request.UpdateDestination,
	}, nil
}

func (l *testLoader) InferParquetSchema(
	_ context.Context,
	_ []ports.LocalObject,
	options ports.ParquetSchemaOptions,
) ([]domain.Field, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inferCalls++
	l.inferOptions = options
	if l.inferErr != nil {
		return nil, l.inferErr
	}
	return catalogdomain.CloneFields(l.inferredSchema), nil
}

func (l *testLoader) DiscardLoadedTable(context.Context, domain.TableReference) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.discardCalls++
	return l.discardErr
}

func (l *gatedLoader) DiscardLoadedTable(context.Context, domain.TableReference) error { return nil }

func (l *gatedLoader) InferParquetSchema(
	context.Context,
	[]ports.LocalObject,
	ports.ParquetSchemaOptions,
) ([]domain.Field, error) {
	return nil, domain.ErrUnsupported
}

func TestServiceIsIdempotentAndCleansWorkspace(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-1"}
	configuration := testConfiguration(domain.FormatParquet)
	first, err := service.Submit(context.Background(), reference, configuration)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Submit(context.Background(), reference, configuration)
	if err != nil {
		t.Fatal(err)
	}
	if first.Reference != second.Reference {
		t.Fatalf("idempotent references differ")
	}
	job := waitForDone(t, service, reference)
	if job.Error != nil || job.Statistics.InputFiles != 1 || job.Statistics.InputBytes != 7 ||
		job.Statistics.OutputBytes != 7 || job.Statistics.OutputRows != 3 {
		t.Fatalf("job = %+v", job)
	}
	loader.mu.Lock()
	calls, paths := loader.calls, append([]string(nil), loader.paths...)
	loader.mu.Unlock()
	if calls != 1 {
		t.Fatalf("loader calls = %d", calls)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("workspace file remains: %s (%v)", path, err)
		}
	}
	configuration.SourceURIs = []string{"gs://test-bucket/different.parquet"}
	if _, err := service.Submit(context.Background(), reference, configuration); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("configuration conflict = %v", err)
	}
}

func TestServiceCreatesAndPublishesMissingDestination(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	loader := &testLoader{}
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		Location:  "US",
		Schema:    []domain.Field{{Name: "id", Type: "INT64"}},
		RangePartitioning: &domain.RangePartitioning{
			Field: "id", Range: catalogdomain.Range{Start: 0, End: 100, Interval: 10},
		},
		ClusteringFields: []string{"id"},
	}
	published := make([]domain.Table, 0, 1)
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	service, err := NewService(
		NewMemoryJobRepository(), objects,
		testCatalog{table: table, missing: true, published: &published}, loader,
		testClock{value: time.Unix(1, 0)}, testIDs{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	configuration := testConfiguration(domain.FormatParquet)
	configuration.Schema = catalogdomain.CloneFields(table.Schema)
	configuration.RangePartitioning = &domain.RangePartitioning{
		Field: "id", Range: catalogdomain.Range{Start: 0, End: 100, Interval: 10},
	}
	configuration.ClusteringFields = []string{"id"}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "create-destination"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error != nil || job.Statistics.OutputRows != 3 {
		t.Fatalf("job = %+v", job)
	}
	if len(published) != 1 || published[0].Reference != table.Reference || !schemasEqual(published[0].Schema, table.Schema) ||
		!reflect.DeepEqual(published[0].RangePartitioning, table.RangePartitioning) ||
		!reflect.DeepEqual(published[0].ClusteringFields, table.ClusteringFields) {
		t.Fatalf("published destinations = %#v", published)
	}
	loader.mu.Lock()
	discards := loader.discardCalls
	loader.mu.Unlock()
	if discards != 0 {
		t.Fatalf("successful destination was discarded %d times", discards)
	}
}

func TestServiceRejectsExistingDestinationLayoutDriftBeforeObjectAccess(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		Location:  "US", Schema: []domain.Field{{Name: "event_time", Type: "TIMESTAMP"}, {Name: "customer_id", Type: "STRING"}},
		TimePartitioning: &catalogdomain.TimePartitioning{Type: "DAY", Field: "event_time", ExpirationMs: 86_400_000},
		ClusteringFields: []string{"customer_id"},
	}
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	loader := &testLoader{}
	service, err := NewService(
		NewMemoryJobRepository(), objects, testCatalog{table: table}, loader,
		testClock{value: time.Unix(1, 0)}, testIDs{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	configuration := testConfiguration(domain.FormatParquet)
	expiration := int64(86_400_000)
	configuration.TimePartitioning = &domain.TimePartitioning{Type: "HOUR", Field: "event_time", ExpirationMs: &expiration}
	configuration.ClusteringFields = []string{"customer_id"}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "layout-drift"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "conditionNotMet" {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	if opens != 0 || loader.calls != 0 || loader.plannerCalls != 0 || loader.loadPlanCalls != 0 {
		t.Fatalf("layout drift crossed a side-effect boundary: opens=%d loads=%d planner=%d load_plan=%d", opens, loader.calls, loader.plannerCalls, loader.loadPlanCalls)
	}
}

func TestServiceAppliesDatasetDefaultPartitionExpiration(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		Location:  "US", Schema: []domain.Field{{Name: "event_time", Type: "TIMESTAMP"}},
	}
	published := make([]domain.Table, 0, 1)
	expiration := int64(172_800_000)
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	service, err := NewService(
		NewMemoryJobRepository(), objects,
		testCatalog{table: table, missing: true, published: &published, defaultPartitionExpirationMs: &expiration},
		&testLoader{}, testClock{value: time.Unix(1, 0)}, testIDs{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	configuration := testConfiguration(domain.FormatParquet)
	configuration.Schema = catalogdomain.CloneFields(table.Schema)
	configuration.TimePartitioning = &domain.TimePartitioning{Type: "DAY", Field: "event_time"}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "partition-default"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error != nil || len(published) != 1 || published[0].TimePartitioning == nil ||
		published[0].TimePartitioning.ExpirationMs != expiration {
		t.Fatalf("published destination = %#v, job = %+v", published, job)
	}
}

func TestServiceInfersMissingParquetDestinationAfterDownload(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	inferred := []domain.Field{{Name: "items", Type: "RECORD", Mode: "REPEATED", Fields: []domain.Field{
		{Name: "id", Type: "INT64"},
	}}}
	loader := &testLoader{inferredSchema: inferred}
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		Location:  "US",
	}
	published := make([]domain.Table, 0, 1)
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	service, err := NewService(
		NewMemoryJobRepository(), objects,
		testCatalog{table: table, missing: true, published: &published}, loader,
		testClock{value: time.Unix(1, 0)}, testIDs{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	configuration := testConfiguration(domain.FormatParquet)
	configuration.ParquetOptions.EnableListInference = true
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "infer-destination"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error != nil {
		t.Fatalf("job = %+v error=%+v", job, job.Error)
	}
	if len(published) != 1 || !schemasEqual(published[0].Schema, inferred) {
		t.Fatalf("published inferred destination = %#v", published)
	}
	loader.mu.Lock()
	inferCalls, inferOptions := loader.inferCalls, loader.inferOptions
	loader.mu.Unlock()
	if inferCalls != 1 || !inferOptions.EnableListInference {
		t.Fatalf("schema inference calls=%d options=%+v", inferCalls, inferOptions)
	}
}

func TestServiceInfersAndPublishesAllowedExistingSchemaUpdate(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	before := []domain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED", Description: "stable metadata"}}
	after := []domain.Field{{Name: "id", Type: "INTEGER", Mode: "NULLABLE"}, {Name: "score", Type: "FLOAT64"}}
	loader := &testLoader{inferredSchema: after}
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		Location:  "US", Schema: before,
	}
	updated := make([]domain.Table, 0, 1)
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	service, err := NewService(
		NewMemoryJobRepository(), objects, testCatalog{table: table, updated: &updated}, loader,
		testClock{value: time.Unix(1, 0)}, testIDs{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	configuration := testConfiguration(domain.FormatParquet)
	configuration.SchemaUpdateOptions = []domain.SchemaUpdateOption{
		domain.AllowFieldAddition, domain.AllowFieldRelaxation,
	}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "update-destination"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error != nil {
		t.Fatalf("job = %+v", job)
	}
	if len(updated) != 1 || len(updated[0].Schema) != 2 || updated[0].Schema[0].Mode != "NULLABLE" ||
		updated[0].Schema[1].Name != "score" || updated[0].Schema[1].Type != "FLOAT64" {
		t.Fatalf("published schema updates = %#v", updated)
	}
	if updated[0].Schema[0].Type != "INT64" || updated[0].Schema[0].Description != "stable metadata" {
		t.Fatalf("inference replaced canonical field metadata: %#v", updated[0].Schema[0])
	}
	loader.mu.Lock()
	inferCalls := loader.inferCalls
	loader.mu.Unlock()
	if inferCalls != 1 {
		t.Fatalf("inference calls = %d", inferCalls)
	}
}

func TestMergeInferredSchemaUpdateRelaxesOnlyWhenRequested(t *testing.T) {
	current := []domain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED", Description: "canonical"}}
	inferred := []domain.Field{{Name: "id", Type: "INTEGER", Mode: "NULLABLE"}, {Name: "score", Type: "FLOAT"}}
	additionOnly := mergeInferredSchemaUpdate(current, inferred, false)
	if additionOnly[0].Mode != "REQUIRED" || additionOnly[0].Description != "canonical" || len(additionOnly) != 2 {
		t.Fatalf("addition-only inferred schema = %#v", additionOnly)
	}
	withRelaxation := mergeInferredSchemaUpdate(current, inferred, true)
	if withRelaxation[0].Mode != "NULLABLE" || withRelaxation[0].Type != "INT64" {
		t.Fatalf("relaxing inferred schema = %#v", withRelaxation)
	}
}

func TestServiceRejectsUnapprovedSchemaDriftBeforeObjectAccess(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		Location:  "US", Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	service, err := NewService(
		NewMemoryJobRepository(), objects, testCatalog{table: table}, &testLoader{},
		testClock{value: time.Unix(1, 0)}, testIDs{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	configuration := testConfiguration(domain.FormatParquet)
	configuration.Schema = []domain.Field{{Name: "id", Type: "STRING"}}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "reject-drift"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "invalid" {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	if opens != 0 {
		t.Fatalf("rejected schema drift opened %d objects", opens)
	}
}

func TestServiceEnforcesCreateNeverAndCompensatesPublicationFailure(t *testing.T) {
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		Location:  "US",
		Schema:    []domain.Field{{Name: "id", Type: "INT64"}},
	}
	newService := func(t *testing.T, catalog testCatalog, loader *testLoader, objects *testObjectStore) *Service {
		t.Helper()
		config := DefaultConfig()
		config.TempDirectory = t.TempDir()
		service, err := NewService(
			NewMemoryJobRepository(), objects, catalog, loader,
			testClock{value: time.Unix(1, 0)}, testIDs{}, config,
		)
		if err != nil {
			t.Fatal(err)
		}
		return service
	}

	t.Run("CREATE_NEVER", func(t *testing.T) {
		objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
		loader := &testLoader{}
		service := newService(t, testCatalog{table: table, missing: true}, loader, objects)
		configuration := testConfiguration(domain.FormatParquet)
		configuration.Schema = catalogdomain.CloneFields(table.Schema)
		configuration.CreateDisposition = domain.CreateNever
		reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "create-never"}
		if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
			t.Fatal(err)
		}
		job := waitForDone(t, service, reference)
		if job.Error == nil || job.Error.Reason != "notFound" {
			t.Fatalf("job = %+v", job)
		}
		objects.mu.Lock()
		opens := objects.opens
		objects.mu.Unlock()
		if opens != 0 || loader.calls != 0 {
			t.Fatalf("CREATE_NEVER crossed object/engine boundary: opens=%d loads=%d", opens, loader.calls)
		}
	})

	t.Run("publication failure", func(t *testing.T) {
		objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
		loader := &testLoader{}
		service := newService(t, testCatalog{
			table: table, missing: true, publishErr: fmt.Errorf("%w: publish", domain.ErrConflict),
		}, loader, objects)
		configuration := testConfiguration(domain.FormatParquet)
		configuration.Schema = catalogdomain.CloneFields(table.Schema)
		reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "publish-failure"}
		if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
			t.Fatal(err)
		}
		job := waitForDone(t, service, reference)
		if job.Error == nil {
			t.Fatalf("job = %+v", job)
		}
		loader.mu.Lock()
		discards := loader.discardCalls
		loader.mu.Unlock()
		if discards != 1 {
			t.Fatalf("publication failure discarded destination %d times", discards)
		}
	})
}

func TestServiceRejectsNonGCSURIWithoutPersistingJob(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{}}
	repository := &trackingJobRepository{MemoryJobRepository: NewMemoryJobRepository()}
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		Location:  "US",
		Schema:    []domain.Field{{Name: "id", Type: "INT64"}},
	}
	service, err := NewService(
		repository, objects, testCatalog{table: table}, &testLoader{},
		testClock{value: time.Unix(1, 0)}, testIDs{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	configuration := testConfiguration(domain.FormatParquet)
	configuration.SourceURIs = []string{"file:///must-not-be-read.parquet"}
	_, err = service.Submit(context.Background(), domain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: "invalid-source",
	}, configuration)
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("non-GCS source error = %v", err)
	}
	if repository.createOrGetCalls != 0 {
		t.Fatalf("invalid source persisted %d jobs", repository.createOrGetCalls)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	if opens != 0 {
		t.Fatalf("invalid source opened %d objects", opens)
	}
}

func TestServiceConcurrentIdempotentSubmissionsExecuteOnce(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-concurrent"}
	start := make(chan struct{})
	errorsChannel := make(chan error, 32)
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, err := service.Submit(context.Background(), reference, testConfiguration(domain.FormatParquet))
			errorsChannel <- err
		}()
	}
	close(start)
	group.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = waitForDone(t, service, reference)
	loader.mu.Lock()
	calls := loader.calls
	loader.mu.Unlock()
	if calls != 1 {
		t.Fatalf("loader calls = %d", calls)
	}
}

func TestServicePublishesOutputBytesToConcurrentReaders(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	loader := &gatedLoader{started: make(chan struct{}), release: make(chan struct{})}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-statistics-race"}
	if _, err := service.Submit(context.Background(), reference, testConfiguration(domain.FormatParquet)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-loader.started:
	case <-time.After(time.Second):
		t.Fatal("load did not reach the warehouse boundary")
	}

	const readers = 16
	ready := make(chan struct{}, readers)
	errorsChannel := make(chan error, readers)
	var group sync.WaitGroup
	for range readers {
		group.Add(1)
		go func() {
			defer group.Done()
			job, err := service.Get(context.Background(), reference)
			ready <- struct{}{}
			if err != nil {
				errorsChannel <- err
				return
			}
			if job.State != domain.JobRunning {
				errorsChannel <- fmt.Errorf("state before release = %s", job.State)
				return
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				job, err = service.Get(context.Background(), reference)
				if err != nil {
					errorsChannel <- err
					return
				}
				if job.State == domain.JobDone {
					if job.Error != nil || job.Statistics.InputBytes != 7 || job.Statistics.OutputBytes != 7 {
						errorsChannel <- fmt.Errorf("terminal job = %+v", job)
					}
					return
				}
				time.Sleep(100 * time.Microsecond)
			}
			errorsChannel <- errors.New("load job did not become visible as DONE")
		}()
	}
	for range readers {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("concurrent reader did not observe the running job")
		}
	}
	close(loader.release)
	group.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestServiceRecordsStrictFormatGapWithoutDownloading(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.avro": []byte("secret")}}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-avro"}
	configuration := testConfiguration(domain.FormatAvro)
	configuration.SourceURIs = []string{"gs://test-bucket/source.avro"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "notImplemented" {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	if opens != 0 || loader.calls != 0 {
		t.Fatalf("unsupported format performed IO: opens=%d loads=%d", opens, loader.calls)
	}
}

func TestServiceRecordsUnsupportedOptionsWithoutSideEffects(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-options"}
	configuration := testConfiguration(domain.FormatParquet)
	configuration.UnsupportedOptions = []string{"parquetOptions.enableListInference:fingerprint"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "notImplemented" {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	loader.mu.Lock()
	loads := loader.calls
	loader.mu.Unlock()
	if opens != 0 || loads != 0 {
		t.Fatalf("unsupported options performed side effects: opens=%d loads=%d", opens, loads)
	}
}

func TestServiceRejectsDownloadedSizeDriftBeforeLoaderExecution(t *testing.T) {
	objects := &testObjectStore{
		objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}, sizeOffset: 1,
	}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-size-drift"}
	if _, err := service.Submit(context.Background(), reference, testConfiguration(domain.FormatParquet)); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "conditionNotMet" {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	loader.mu.Lock()
	executions := loader.calls
	loader.mu.Unlock()
	if opens != 1 || executions != 0 {
		t.Fatalf("size drift boundary: opens=%d executions=%d", opens, executions)
	}
}

func TestServiceDeduplicatesOverlappingSourcesAndBindsGenerations(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{
		"gs://test-bucket/a.parquet": []byte("a"),
		"gs://test-bucket/b.parquet": []byte("bb"),
	}, generation: "7", etag: "stable"}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	configuration := testConfiguration(domain.FormatParquet)
	configuration.SourceURIs = []string{"gs://test-bucket/a.parquet", "gs://test-bucket/*.parquet"}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-overlap"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error != nil || job.Statistics.InputFiles != 2 || job.Statistics.InputBytes != 3 {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opened := append([]ports.ObjectInfo(nil), objects.opened...)
	objects.mu.Unlock()
	if len(opened) != 2 || opened[0].URI != "gs://test-bucket/a.parquet" ||
		opened[1].URI != "gs://test-bucket/b.parquet" || opened[0].Generation != "7" || opened[1].Generation != "7" {
		t.Fatalf("opened immutable objects = %#v", opened)
	}
}

func TestServiceRejectsOverlappingGenerationDriftBeforeDownload(t *testing.T) {
	objects := &testObjectStore{
		objects:    map[string][]byte{"gs://test-bucket/a.parquet": []byte("a")},
		generation: "7", listGeneration: "8",
	}
	loader := &testLoader{}
	service := newTestService(t, objects, loader, time.Second)
	configuration := testConfiguration(domain.FormatParquet)
	configuration.SourceURIs = []string{"gs://test-bucket/a.parquet", "gs://test-bucket/*.parquet"}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-generation-drift"}
	if _, err := service.Submit(context.Background(), reference, configuration); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "conditionNotMet" {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	if opens != 0 || loader.calls != 0 || loader.loadPlanCalls != 0 {
		t.Fatalf("generation drift crossed download/engine boundary: opens=%d loads=%d plans=%d", opens, loader.calls, loader.loadPlanCalls)
	}
}

func TestServicePersistsTimeoutAsTerminalError(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	loader := &testLoader{block: true}
	service := newTestService(t, objects, loader, 10*time.Millisecond)
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "load-timeout"}
	if _, err := service.Submit(context.Background(), reference, testConfiguration(domain.FormatParquet)); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "backendError" {
		t.Fatalf("job = %+v", job)
	}
}

func TestServiceRejectsReplaceableEngineSchemaBoundsBeforeObjectAccess(t *testing.T) {
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	descriptor := testLoaderCapabilities().Descriptor()
	descriptor.Decimal.MaxPrecision, descriptor.Decimal.MaxScale = 10, 4
	capabilities, err := engine.NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	loader := &testLoader{testLoadPlanner: testLoadPlanner{capabilities: &capabilities}}
	precision, scale := int64(11), int64(2)
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"}, Location: "US",
		Schema: []domain.Field{{Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale}},
	}
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	service, err := NewService(
		NewMemoryJobRepository(), objects, testCatalog{table: table}, loader,
		testClock{value: time.Unix(1, 0)}, testIDs{}, config,
	)
	if err != nil {
		t.Fatal(err)
	}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "engine-bound"}
	if _, err := service.Submit(context.Background(), reference, testConfiguration(domain.FormatParquet)); err != nil {
		t.Fatal(err)
	}
	job := waitForDone(t, service, reference)
	if job.Error == nil || job.Error.Reason != "notImplemented" {
		t.Fatalf("job = %+v", job)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	loader.mu.Lock()
	loads := loader.calls
	loader.mu.Unlock()
	if opens != 0 || loads != 0 || loader.plannerCalls != 0 || loader.loadPlanCalls != 0 {
		t.Fatalf("capability rejection crossed a side-effect boundary: opens=%d loads=%d planner=%d load_plan=%d", opens, loads, loader.plannerCalls, loader.loadPlanCalls)
	}
}

func TestServiceRejectsRecursiveSchemaDuplicatesBeforeJobPublication(t *testing.T) {
	jobs := &trackingJobRepository{MemoryJobRepository: NewMemoryJobRepository()}
	objects := &testObjectStore{objects: map[string][]byte{"gs://test-bucket/source.parquet": []byte("parquet")}}
	loader := &testLoader{}
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"}, Location: "US",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	service, err := NewService(jobs, objects, testCatalog{table: table}, loader, testClock{value: time.Unix(1, 0)}, testIDs{}, config)
	if err != nil {
		t.Fatal(err)
	}
	reference := domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "recursive-duplicate"}
	configuration := testConfiguration(domain.FormatParquet)
	configuration.Schema = []domain.Field{{
		Name: "payload", Type: "STRUCT", Fields: []domain.Field{{
			Name: "items", Type: "RECORD", Mode: "REPEATED", Fields: []domain.Field{
				{Name: "amount", Type: "NUMERIC"}, {Name: "Amount", Type: "BIGNUMERIC"},
			},
		}},
	}}
	job, err := service.Submit(context.Background(), reference, configuration)
	if !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "payload.items.Amount") {
		t.Fatalf("Submit error = %v, want synchronous recursive duplicate", err)
	}
	if job != nil || jobs.createOrGetCalls != 0 {
		t.Fatalf("invalid schema reached job publication: job=%#v repository_calls=%d", job, jobs.createOrGetCalls)
	}
	if _, err := jobs.Get(context.Background(), reference); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("invalid schema repository lookup = %v, want not found", err)
	}
	objects.mu.Lock()
	opens := objects.opens
	objects.mu.Unlock()
	if opens != 0 || loader.calls != 0 || loader.plannerCalls != 0 || loader.loadPlanCalls != 0 {
		t.Fatalf("invalid schema crossed a side-effect boundary: opens=%d loads=%d planner=%d load_plan=%d", opens, loader.calls, loader.plannerCalls, loader.loadPlanCalls)
	}
}

func TestLoadSchemaIdentityIncludesDecimalPresenceValuesAndRoundingRecursively(t *testing.T) {
	precision38, precision20, precision19, scale0, scale2 := int64(38), int64(20), int64(19), int64(0), int64(2)
	base := []domain.Field{{Name: "items", Type: "STRUCT", Mode: "REPEATED", Fields: []domain.Field{{
		Name: "amount", Type: "NUMERIC",
	}}}}
	tests := []struct {
		name   string
		mutate func([]domain.Field)
	}{
		{name: "precision presence", mutate: func(fields []domain.Field) { fields[0].Fields[0].Precision = &precision38 }},
		{name: "precision value", mutate: func(fields []domain.Field) { fields[0].Fields[0].Precision = &precision20 }},
		{name: "scale presence", mutate: func(fields []domain.Field) {
			fields[0].Fields[0].Precision = &precision20
			fields[0].Fields[0].Scale = &scale0
		}},
		{name: "scale value", mutate: func(fields []domain.Field) {
			fields[0].Fields[0].Precision = &precision20
			fields[0].Fields[0].Scale = &scale2
		}},
		{name: "rounding mode", mutate: func(fields []domain.Field) { fields[0].Fields[0].RoundingMode = catalogdomain.RoundingModeHalfEven }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := catalogdomain.CloneFields(base)
			test.mutate(changed)
			if schemasEqual(base, changed) {
				t.Fatalf("schemasEqual ignored %s", test.name)
			}
			if schemaDigest(base) == schemaDigest(changed) {
				t.Fatalf("schemaDigest ignored %s", test.name)
			}
		})
	}
	clone := catalogdomain.CloneFields(base)
	if !schemasEqual(base, clone) || schemaDigest(base) != schemaDigest(clone) {
		t.Fatal("identical recursive schema did not retain stable identity")
	}
	assertDifferent := func(name string, left, right []domain.Field) {
		t.Helper()
		if schemasEqual(left, right) || schemaDigest(left) == schemaDigest(right) {
			t.Fatalf("schema identity ignored differing %s", name)
		}
	}
	left := catalogdomain.CloneFields(base)
	right := catalogdomain.CloneFields(base)
	left[0].Fields[0].Precision, right[0].Fields[0].Precision = &precision20, &precision19
	assertDifferent("precision values", left, right)
	left = catalogdomain.CloneFields(base)
	right = catalogdomain.CloneFields(base)
	left[0].Fields[0].Precision, right[0].Fields[0].Precision = &precision20, &precision20
	left[0].Fields[0].Scale, right[0].Fields[0].Scale = &scale0, &scale2
	assertDifferent("scale values", left, right)
}

func TestTerminalErrorPreservesDecimalRoundingCapability(t *testing.T) {
	reason, message := terminalError(fmt.Errorf("%w: capability=%s", domain.ErrUnsupported, domain.CapabilityDecimalRoundingV1))
	if reason != "notImplemented" || !strings.Contains(message, domain.CapabilityDecimalRoundingV1) {
		t.Fatalf("terminal decimal capability = %q %q", reason, message)
	}
}

func newTestService(t *testing.T, objects ports.ObjectStore, loader ports.Loader, timeout time.Duration) *Service {
	t.Helper()
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"}, Location: "US",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}
	config := DefaultConfig()
	config.TempDirectory = t.TempDir()
	config.OperationTimeout = timeout
	config.MaxObjectBytes = 1024
	config.MaxTotalBytes = 2048
	service, err := NewService(NewMemoryJobRepository(), objects, testCatalog{table: table}, loader, testClock{value: time.Unix(1, 0)}, testIDs{}, config)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func testConfiguration(format domain.SourceFormat) domain.LoadConfiguration {
	return domain.LoadConfiguration{
		SourceURIs: []string{"gs://test-bucket/source.parquet"}, Destination: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		SourceFormat: format, WriteDisposition: domain.WriteAppend,
	}
}

func waitForDone(t *testing.T, service *Service, reference domain.JobReference) *domain.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := service.Get(context.Background(), reference)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == domain.JobDone {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("load job did not finish")
	return nil
}
