package application

// CatalogService coordinates metadata with the replaceable Warehouse port.
// Official resource semantics:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
//
// Physical storage is created before catalog publication and compensated if
// publication fails. Deletes remove physical state before metadata so a
// successful response never leaves queryable storage behind an absent resource.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/state"
	tabledatabudget "github.com/leeyh0216/go-bemu/internal/tabledata"
)

// CatalogService intentionally has no expiration goroutine or Close method.
// Expiration is checked synchronously by GetTable/ListTables (and therefore by
// the composed Storage Read resolver), so process shutdown has no cleanup
// worker ordering requirement. A durable bounded sweeper remains a named gap.
type CatalogService struct {
	catalog                   ports.CatalogRepository
	warehouse                 ports.WarehouseAdmin
	tableDataReader           ports.TableDataReader
	tableDataWriter           ports.TableDataWriter
	tableDataInsertIDLedger   ports.TableDataInsertIDLedger
	clock                     ports.Clock
	defaultLocation           string
	compensationTimeout       time.Duration
	tableDataOperationTimeout time.Duration
	maxTableDataPageRows      int
	maxTableDataResponseBytes int64
	maxTableDataRowBytes      int64
	mutationJournal           state.CanonicalMutationJournal
	// The repository port intentionally stays backend-agnostic and has no
	// compare-and-create primitive. These locks make the two physical/metadata
	// transactions single-writer within one emulator process.
	anonymousDatasetMu contextMutex
	// resourceMutationMu makes metadata/physical two-phase mutations linearizable
	// within one emulator process. It covers dataset deletion as well as table
	// publication so a successful query job cannot publish into a dataset that
	// was concurrently removed. It also orders tables.patch expiration changes
	// with lazy expiration cleanup.
	// https://cloud.google.com/bigquery/docs/managing-tables#update-table-expiration
	resourceMutationMu contextMutex
}

type CatalogOption func(*CatalogService)

var ErrTableDataAdapterContract = errors.New("table data reader violated table data adapter contract")

// WithDefaultLocation applies the configured project location when a dataset
// insert omits location. BigQuery locations and their case-sensitive values are
// documented at https://cloud.google.com/bigquery/docs/locations.
func WithDefaultLocation(location string) CatalogOption {
	return func(service *CatalogService) {
		if location != "" {
			service.defaultLocation = location
		}
	}
}

// WithCatalogCompensationTimeout bounds physical rollback after a metadata
// publication failure. Compensation deliberately survives cancellation of the
// originating HTTP request but never runs without a deadline.
func WithCatalogCompensationTimeout(timeout time.Duration) CatalogOption {
	return func(service *CatalogService) {
		if timeout > 0 {
			service.compensationTimeout = timeout
		}
	}
}

// WithTableDataReader enables the tabledata.list application boundary while
// keeping physical row access out of REST handlers and catalog repositories.
func WithTableDataReader(reader ports.TableDataReader) CatalogOption {
	return func(service *CatalogService) {
		service.tableDataReader = reader
	}
}

// WithTableDataWriter enables tabledata.insertAll after the catalog boundary
// has resolved the authoritative table schema.
func WithTableDataWriter(writer ports.TableDataWriter) CatalogOption {
	return func(service *CatalogService) { service.tableDataWriter = writer }
}

func WithTableDataInsertIDLedger(ledger ports.TableDataInsertIDLedger) CatalogOption {
	return func(service *CatalogService) { service.tableDataInsertIDLedger = ledger }
}

// WithTableDataOperationTimeout bounds admission to the global catalog boundary,
// metadata/TTL resolution, and the physical count-and-page operation. The
// timeout derives from the request context so upstream cancellation and an
// earlier request deadline remain authoritative.
func WithTableDataOperationTimeout(timeout time.Duration) CatalogOption {
	return func(service *CatalogService) {
		if timeout > 0 {
			service.tableDataOperationTimeout = timeout
		}
	}
}

// WithMaxTableDataPageRows caps one physical read. The REST edge may apply a
// smaller protocol default, but callers cannot bypass this application limit.
func WithMaxTableDataPageRows(maximum int) CatalogOption {
	return func(service *CatalogService) {
		if maximum > 0 {
			service.maxTableDataPageRows = maximum
		}
	}
}

// WithMaxTableDataResponseBytes sets the normal canonical and exact REST page
// boundary. BigQuery normally paginates tabledata.list near 10 MB; one row may
// cross it only up to the separately configured hard row ceiling.
// https://cloud.google.com/bigquery/docs/paging-results#api-limits
func WithMaxTableDataResponseBytes(maximum int64) CatalogOption {
	return func(service *CatalogService) {
		if maximum > 0 {
			service.maxTableDataResponseBytes = maximum
		}
	}
}

func WithMaxTableDataRowBytes(maximum int64) CatalogOption {
	return func(service *CatalogService) {
		if maximum > 0 {
			service.maxTableDataRowBytes = maximum
		}
	}
}

func NewCatalogService(catalog ports.CatalogRepository, warehouse ports.WarehouseAdmin, clock ports.Clock, options ...CatalogOption) *CatalogService {
	service := &CatalogService{
		catalog: catalog, warehouse: warehouse, clock: clock,
		defaultLocation: "US", compensationTimeout: 30 * time.Second,
		tableDataOperationTimeout: 30 * time.Second, maxTableDataPageRows: 10_000,
		maxTableDataResponseBytes: 10_000_000, maxTableDataRowBytes: 100_000_000,
	}
	if journal, ok := catalog.(state.CanonicalMutationJournal); ok {
		service.mutationJournal = journal
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *CatalogService) CreateProject(ctx context.Context, project domain.Project) (domain.Project, error) {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return domain.Project{}, err
	}
	defer s.resourceMutationMu.Unlock()

	if err := project.Validate(); err != nil {
		return domain.Project{}, err
	}
	now := s.clock.Now()
	project.CreatedAt = now
	project.UpdatedAt = now
	if err := s.catalog.CreateProject(ctx, project); err != nil {
		return domain.Project{}, err
	}
	return project, nil
}

func (s *CatalogService) GetProject(ctx context.Context, id string) (domain.Project, error) {
	return s.catalog.GetProject(ctx, id)
}

func (s *CatalogService) ListProjects(ctx context.Context) ([]domain.Project, error) {
	return s.catalog.ListProjects(ctx)
}

func (s *CatalogService) DeleteProject(ctx context.Context, id string) error {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()

	datasets, err := s.catalog.ListDatasets(ctx, id)
	if err != nil {
		return err
	}
	for _, dataset := range datasets {
		if err := s.warehouse.DropDataset(ctx, id, dataset.ID); err != nil {
			return fmt.Errorf("drop dataset storage: %w", err)
		}
	}
	return s.catalog.DeleteProject(ctx, id)
}

func (s *CatalogService) CreateDataset(ctx context.Context, dataset domain.Dataset) (domain.Dataset, error) {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return domain.Dataset{}, err
	}
	defer s.resourceMutationMu.Unlock()

	return s.createDataset(ctx, dataset)
}

func (s *CatalogService) createDataset(ctx context.Context, dataset domain.Dataset) (domain.Dataset, error) {
	if err := dataset.Validate(); err != nil {
		return domain.Dataset{}, err
	}
	if _, err := s.catalog.GetProject(ctx, dataset.ProjectID); err != nil {
		return domain.Dataset{}, err
	}
	if _, err := s.catalog.GetDataset(ctx, dataset.ProjectID, dataset.ID); err == nil {
		return domain.Dataset{}, fmt.Errorf("%w: dataset %s/%s", domain.ErrConflict, dataset.ProjectID, dataset.ID)
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.Dataset{}, err
	}
	if dataset.Location == "" {
		dataset.Location = s.defaultLocation
	}
	now := s.clock.Now()
	dataset.CreatedAt = now
	dataset.UpdatedAt = now
	if err := s.warehouse.CreateDataset(ctx, dataset.ProjectID, dataset.ID); err != nil {
		return domain.Dataset{}, err
	}
	if err := s.catalog.CreateDataset(ctx, dataset); err != nil {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), s.compensationTimeout)
		cleanupErr := s.warehouse.DropDataset(cleanupCtx, dataset.ProjectID, dataset.ID)
		cancelCleanup()
		if cleanupErr != nil {
			return domain.Dataset{}, errors.Join(err, fmt.Errorf("compensate unpublished dataset storage: %w", cleanupErr))
		}
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func (s *CatalogService) GetDataset(ctx context.Context, projectID, datasetID string) (domain.Dataset, error) {
	return s.catalog.GetDataset(ctx, projectID, datasetID)
}

// EnsureAnonymousDataset creates the hidden metadata/storage container used by
// query jobs whose callers omit destinationTable. BigQuery documents these as
// anonymous datasets whose names start with an underscore:
// https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored
func (s *CatalogService) EnsureAnonymousDataset(ctx context.Context, projectID, datasetID, location string) (domain.Dataset, error) {
	if err := s.anonymousDatasetMu.LockContext(ctx); err != nil {
		return domain.Dataset{}, err
	}
	defer s.anonymousDatasetMu.Unlock()
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return domain.Dataset{}, err
	}
	defer s.resourceMutationMu.Unlock()

	dataset, err := s.catalog.GetDataset(ctx, projectID, datasetID)
	if err == nil {
		return validateAnonymousDataset(dataset, location)
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return domain.Dataset{}, err
	}
	started := observability.LogSideEffectStart(ctx, "catalog", "ensure_anonymous_dataset",
		"project_id", projectID, "dataset_id", datasetID, "location", location,
		"capability", domain.CapabilityQueryAnonymousDestinationV1)
	dataset, err = s.createDataset(ctx, domain.Dataset{
		ProjectID: projectID,
		ID:        datasetID,
		Location:  location,
		Hidden:    true,
	})
	observability.LogSideEffectEnd(ctx, "catalog", "ensure_anonymous_dataset", started, err,
		"project_id", projectID, "dataset_id", datasetID, "location", location,
		"capability", domain.CapabilityQueryAnonymousDestinationV1)
	return dataset, err
}

func validateAnonymousDataset(dataset domain.Dataset, location string) (domain.Dataset, error) {
	if !dataset.Hidden {
		return domain.Dataset{}, fmt.Errorf("%w: anonymous dataset identity collides with a non-hidden dataset", domain.ErrConflict)
	}
	if !strings.EqualFold(dataset.Location, location) {
		return domain.Dataset{}, fmt.Errorf("%w: anonymous dataset location differs from query location", domain.ErrInvalid)
	}
	return dataset, nil
}

func (s *CatalogService) ListDatasets(ctx context.Context, projectID string) ([]domain.Dataset, error) {
	return s.catalog.ListDatasets(ctx, projectID)
}

func (s *CatalogService) DeleteDataset(ctx context.Context, projectID, datasetID string, deleteContents bool) error {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()

	if _, err := s.catalog.GetDataset(ctx, projectID, datasetID); err != nil {
		return err
	}
	// Use the application boundary so expired result tables are physically and
	// metadata-cleaned before the deleteContents emptiness decision.
	tables, err := s.listTablesLocked(ctx, projectID, datasetID)
	if err != nil {
		return err
	}
	if len(tables) != 0 && !deleteContents {
		return fmt.Errorf("%w: dataset %s/%s is not empty; set deleteContents=true", domain.ErrConflict, projectID, datasetID)
	}
	if err := s.warehouse.DropDataset(ctx, projectID, datasetID); err != nil {
		return err
	}
	return s.catalog.DeleteDataset(ctx, projectID, datasetID)
}

func (s *CatalogService) CreateTable(ctx context.Context, table domain.Table) (domain.Table, error) {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return domain.Table{}, err
	}
	defer s.resourceMutationMu.Unlock()

	return s.createTable(ctx, table)
}

func (s *CatalogService) createTable(ctx context.Context, table domain.Table) (domain.Table, error) {
	if err := table.Validate(); err != nil {
		return domain.Table{}, err
	}
	dataset, err := s.catalog.GetDataset(ctx, table.ProjectID, table.DatasetID)
	if err != nil {
		return domain.Table{}, err
	}
	if existing, err := s.catalog.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID); err == nil {
		if !tableExpired(existing, s.clock.Now()) {
			return domain.Table{}, fmt.Errorf("%w: table %s/%s/%s", domain.ErrConflict, table.ProjectID, table.DatasetID, table.ID)
		}
		if _, err := s.removeExpiredTableLocked(ctx, table.ProjectID, table.DatasetID, table.ID); err != nil {
			return domain.Table{}, err
		}
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.Table{}, err
	}
	if table.Type == "" {
		table.Type = "TABLE"
	}
	if planner, ok := s.warehouse.(ports.SchemaPlanner); ok {
		if err := planner.ValidateSchema(table.Schema); err != nil {
			return domain.Table{}, err
		}
	}
	now := s.clock.Now()
	table.Location = dataset.Location
	if table.ExpirationTime == nil && dataset.DefaultTableExpirationMs != nil {
		expiration := now.Add(time.Duration(*dataset.DefaultTableExpirationMs) * time.Millisecond)
		table.ExpirationTime = &expiration
	}
	if table.TimePartitioning != nil && table.TimePartitioning.ExpirationMs == 0 && dataset.DefaultPartitionExpirationMs != nil {
		table.TimePartitioning.ExpirationMs = *dataset.DefaultPartitionExpirationMs
	}
	table.CreatedAt = now
	table.UpdatedAt = now
	if err := s.warehouse.CreateTable(ctx, table); err != nil {
		return domain.Table{}, err
	}
	if err := s.catalog.CreateTable(ctx, table); err != nil {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), s.compensationTimeout)
		cleanupErr := s.warehouse.DropTable(cleanupCtx, table.ProjectID, table.DatasetID, table.ID)
		cancelCleanup()
		if cleanupErr != nil {
			return domain.Table{}, errors.Join(err, fmt.Errorf("compensate unpublished table storage: %w", cleanupErr))
		}
		return domain.Table{}, err
	}
	return table, nil
}

func (s *CatalogService) GetTable(ctx context.Context, projectID, datasetID, tableID string) (domain.Table, error) {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return domain.Table{}, err
	}
	defer s.resourceMutationMu.Unlock()

	return s.getTableLocked(ctx, projectID, datasetID, tableID)
}

// ListTableData validates bounded paging and resolves canonical table metadata
// under the same lock used by expiration and resource mutations. The adapter is
// called only after GetTable's lazy TTL boundary has confirmed a live table.
// startIndex may be beyond TotalRows, in which case tabledata.list returns an
// empty page rather than an error.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
func (s *CatalogService) ListTableData(ctx context.Context, projectID, datasetID, tableID string, offset int64, maximum ports.TableDataMaxResults) (ports.TableDataPage, error) {
	if offset < 0 {
		return ports.TableDataPage{}, fmt.Errorf("%w: table data offset must be non-negative", domain.ErrInvalid)
	}
	if maximum.Value < 0 {
		return ports.TableDataPage{}, fmt.Errorf("%w: table data limit must be non-negative", domain.ErrInvalid)
	}
	limit := maximum.Value
	// maxResults is an optional uint32. Only absence selects the configured
	// default; an explicit zero still resolves metadata/TotalRows but asks the
	// physical adapter for no rows.
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
	if !maximum.Present {
		limit = s.maxTableDataPageRows
	} else if limit > s.maxTableDataPageRows {
		limit = s.maxTableDataPageRows
	}
	if s.tableDataReader == nil {
		return ports.TableDataPage{}, fmt.Errorf("table data reader is not configured")
	}

	operationCtx, cancelOperation := context.WithTimeout(ctx, s.tableDataOperationTimeout)
	defer cancelOperation()
	referenceDigest := observability.Digest([]byte(strings.Join([]string{projectID, datasetID, tableID}, "\x00")))
	admissionStarted := observability.LogSideEffectStart(operationCtx, "catalog", "table_data_admission",
		"stage", "resource_mutation_gate", "table_reference_digest", referenceDigest,
		"model_version", "tabledata-catalog-admission-v1")
	admissionErr := s.resourceMutationMu.LockContext(operationCtx)
	observability.LogSideEffectEnd(operationCtx, "catalog", "table_data_admission", admissionStarted, admissionErr,
		"stage", "resource_mutation_gate", "table_reference_digest", referenceDigest,
		"model_version", "tabledata-catalog-admission-v1")
	if admissionErr != nil {
		return ports.TableDataPage{}, admissionErr
	}
	defer s.resourceMutationMu.Unlock()
	table, err := s.getTableLocked(operationCtx, projectID, datasetID, tableID)
	if err != nil {
		return ports.TableDataPage{}, err
	}
	page, err := s.tableDataReader.ListTableData(operationCtx, ports.TableDataReadRequest{
		Reference: domain.TableReference{ProjectID: projectID, DatasetID: datasetID, TableID: tableID},
		Schema:    copyFields(table.Schema), Offset: offset, Limit: limit,
		MaxResponseBytes: s.maxTableDataResponseBytes, MaxRowBytes: s.maxTableDataRowBytes,
	})
	if err != nil {
		return ports.TableDataPage{}, err
	}
	if err := validateTableDataPage(page, offset, limit); err != nil {
		return ports.TableDataPage{}, err
	}
	// Treat the port as an untrusted boundary. Replaceable adapters must honor
	// the same canonical row/page limits before a transport can serialize data.
	budget := tabledatabudget.NewAccumulator(s.maxTableDataResponseBytes)
	for _, row := range page.Rows {
		included, budgetErr := budget.Add(row, s.maxTableDataRowBytes)
		if budgetErr != nil {
			return ports.TableDataPage{}, fmt.Errorf("%w: canonical row budget", ErrTableDataAdapterContract)
		}
		if !included {
			return ports.TableDataPage{}, fmt.Errorf("%w: canonical page budget", ErrTableDataAdapterContract)
		}
	}
	page.Schema = copyFields(table.Schema)
	page.MaxResponseBytes = s.maxTableDataResponseBytes
	page.MaxRowBytes = s.maxTableDataRowBytes
	return page, nil
}

func validateTableDataPage(page ports.TableDataPage, offset int64, limit int) error {
	if page.TotalRows < 0 {
		return fmt.Errorf("%w: table data adapter returned a negative total row count", ErrTableDataAdapterContract)
	}
	if len(page.Rows) > limit {
		return fmt.Errorf("%w: table data adapter returned %d rows for effective limit %d", ErrTableDataAdapterContract, len(page.Rows), limit)
	}
	remaining := page.TotalRows
	if offset < remaining {
		remaining -= offset
	} else {
		// tabledata.list permits a startIndex beyond TotalRows and represents it
		// as an empty page. No adapter may return rows from that position.
		// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
		remaining = 0
	}
	if int64(len(page.Rows)) > remaining {
		return fmt.Errorf("%w: table data adapter page exceeds the reported total row count", ErrTableDataAdapterContract)
	}
	return nil
}

func (s *CatalogService) getTableLocked(ctx context.Context, projectID, datasetID, tableID string) (domain.Table, error) {
	table, err := s.catalog.GetTable(ctx, projectID, datasetID, tableID)
	if err != nil {
		return domain.Table{}, err
	}
	if !tableExpired(table, s.clock.Now()) {
		return table, nil
	}
	removed, err := s.removeExpiredTableLocked(ctx, projectID, datasetID, tableID)
	if err != nil {
		return domain.Table{}, err
	}
	if !removed {
		return s.catalog.GetTable(ctx, projectID, datasetID, tableID)
	}
	return domain.Table{}, fmt.Errorf("%w: table %s/%s/%s expired", domain.ErrNotFound, projectID, datasetID, tableID)
}

// PublishMaterializedTable publishes metadata for physical storage that a
// QueryMaterializer has already committed. It intentionally does not call
// WarehouseAdmin.CreateTable: query destination creation is a CTAS transaction,
// and publishing metadata before that transaction succeeds would expose a table
// clients cannot read.
//
// BigQuery documents destination creation as part of the atomic query-job
// completion update:
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
func (s *CatalogService) PublishMaterializedTable(ctx context.Context, table domain.Table) error {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()

	if err := table.Validate(); err != nil {
		return err
	}
	dataset, err := s.catalog.GetDataset(ctx, table.ProjectID, table.DatasetID)
	if err != nil {
		return err
	}
	if existing, err := s.catalog.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID); err == nil {
		if !tableExpired(existing, s.clock.Now()) {
			return fmt.Errorf("%w: table %s/%s/%s", domain.ErrConflict, table.ProjectID, table.DatasetID, table.ID)
		}
		if _, err := s.removeExpiredTableLocked(ctx, table.ProjectID, table.DatasetID, table.ID); err != nil {
			return err
		}
	} else if !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	now := s.clock.Now()
	table.Type = "TABLE"
	table.Location = dataset.Location
	table.CreatedAt = now
	table.UpdatedAt = now
	return s.catalog.CreateTable(ctx, table)
}

func (s *CatalogService) ListTables(ctx context.Context, projectID, datasetID string) ([]domain.Table, error) {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.resourceMutationMu.Unlock()

	return s.listTablesLocked(ctx, projectID, datasetID)
}

func (s *CatalogService) listTablesLocked(ctx context.Context, projectID, datasetID string) ([]domain.Table, error) {
	tables, err := s.catalog.ListTables(ctx, projectID, datasetID)
	if err != nil {
		return nil, err
	}
	live := make([]domain.Table, 0, len(tables))
	for _, table := range tables {
		if !tableExpired(table, s.clock.Now()) {
			live = append(live, table)
			continue
		}
		removed, err := s.removeExpiredTableLocked(ctx, table.ProjectID, table.DatasetID, table.ID)
		if err != nil {
			return nil, err
		}
		if !removed {
			current, err := s.catalog.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID)
			if err != nil {
				return nil, err
			}
			live = append(live, current)
		}
	}
	return live, nil
}

func tableExpired(table domain.Table, now time.Time) bool {
	return table.ExpirationTime != nil && !now.Before(*table.ExpirationTime)
}

// removeExpiredTable rechecks metadata under the cleanup lock before crossing
// either side-effect boundary. Physical-first deletion means a failed metadata
// delete cannot leave queryable storage hidden behind an absent resource.
func (s *CatalogService) removeExpiredTableLocked(ctx context.Context, projectID, datasetID, tableID string) (bool, error) {
	table, err := s.catalog.GetTable(ctx, projectID, datasetID, tableID)
	if errors.Is(err, domain.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !tableExpired(table, s.clock.Now()) {
		return false, nil
	}
	started := observability.LogSideEffectStart(ctx, "catalog", "expire_table",
		"project_id", projectID, "dataset_id", datasetID, "table_id", tableID,
		"model_version", "catalog-expiration-lazy-v1")
	if err := s.warehouse.DropTable(ctx, projectID, datasetID, tableID); err != nil && !errors.Is(err, domain.ErrNotFound) {
		observability.LogSideEffectEnd(ctx, "catalog", "expire_table", started, err,
			"project_id", projectID, "dataset_id", datasetID, "table_id", tableID,
			"model_version", "catalog-expiration-lazy-v1", "metadata_removed", false)
		return false, err
	}
	err = s.catalog.DeleteTable(ctx, projectID, datasetID, tableID)
	if errors.Is(err, domain.ErrNotFound) {
		err = nil
	}
	observability.LogSideEffectEnd(ctx, "catalog", "expire_table", started, err,
		"project_id", projectID, "dataset_id", datasetID, "table_id", tableID,
		"model_version", "catalog-expiration-lazy-v1", "metadata_removed", err == nil)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *CatalogService) DeleteTable(ctx context.Context, projectID, datasetID, tableID string) error {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()

	if _, err := s.catalog.GetTable(ctx, projectID, datasetID, tableID); err != nil {
		return err
	}
	if err := s.warehouse.DropTable(ctx, projectID, datasetID, tableID); err != nil && !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	return s.catalog.DeleteTable(ctx, projectID, datasetID, tableID)
}
