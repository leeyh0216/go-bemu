package application

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type staticQueryAnalyzer struct{ analysis ports.QueryAnalysis }

func (analyzer staticQueryAnalyzer) AnalyzeQuery(context.Context, ports.QueryRequest) (ports.QueryAnalysis, error) {
	return analyzer.analysis, nil
}

func TestQueryCatalogDDLIsRejectedBeforeJobAndEngineSideEffects(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	repository := memory.NewJobRepository()
	engine := &countingQueryEngine{}
	service := newTestQueryService(
		repository, engine, fixedClock{now: time.Unix(1, 0)}, fixedQueryID("ddl"),
		WithQueryAnalyzer(staticQueryAnalyzer{analysis: ports.QueryAnalysis{RequiresCatalogMutation: true}}),
	)

	_, err := service.RunSync(ctx, QueryInput{ProjectID: "test-project", SQL: "DROP TABLE hidden"})
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(err.Error(), domain.GapQueryDDLCatalogSyncV1) {
		t.Fatalf("DDL error = %v, want unsupported capability %s", err, domain.GapQueryDDLCatalogSyncV1)
	}
	if jobs, listErr := repository.List(ctx, "test-project", ""); listErr != nil || len(jobs) != 0 {
		t.Fatalf("DDL created jobs = %#v, error=%v", jobs, listErr)
	}
	if engine.calls.Load() != 0 {
		t.Fatalf("DDL reached query engine %d times", engine.calls.Load())
	}
}

func TestQueryLocationIsInferredBeforeJobInsertion(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	clock := fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := NewCatalogService(memory.NewCatalogRepository(), &fakeWarehouse{}, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "data-project"}); err != nil {
		t.Fatal(err)
	}
	for id, location := range map[string]string{"eu_source": "EU", "eu_lookup": "EU", "us_source": "US"} {
		if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: id, Location: location}); err != nil {
			t.Fatal(err)
		}
	}
	for datasetID, tableID := range map[string]string{"eu_source": "events", "eu_lookup": "names", "us_source": "events"} {
		if _, err := catalog.CreateTable(ctx, domain.Table{
			ProjectID: "test-project", DatasetID: datasetID, ID: tableID,
			Schema: []domain.Field{{Name: "id", Type: "INT64"}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "data-project", ID: "cross_source", Location: "EU"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "data-project", DatasetID: "cross_source", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name       string
		input      QueryInput
		references []domain.TableReference
		want       string
	}{
		{
			name:  "source tables",
			input: QueryInput{ProjectID: "test-project", JobID: "source-location", SQL: "SELECT 1"},
			references: []domain.TableReference{
				{ProjectID: "test-project", DatasetID: "eu_source", TableID: "events"},
				{ProjectID: "test-project", DatasetID: "eu_lookup", TableID: "names"},
			},
			want: "EU",
		},
		{
			name:  "default dataset",
			input: QueryInput{ProjectID: "test-project", JobID: "default-location", DefaultDataset: "eu_source", SQL: "SELECT 1"},
			want:  "EU",
		},
		{
			name: "cross-project default dataset",
			input: QueryInput{
				ProjectID: "test-project", JobID: "cross-default-location",
				DefaultProjectID: "data-project", DefaultDataset: "cross_source", SQL: "SELECT 1",
			},
			want: "EU",
		},
		{
			name: "destination dataset",
			input: QueryInput{
				ProjectID: "test-project", JobID: "destination-location", SQL: "SELECT 1",
				Destination: &domain.TableReference{ProjectID: "test-project", DatasetID: "eu_lookup", TableID: "output"},
			},
			want: "EU",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := newTestQueryService(
				memory.NewJobRepository(), &countingQueryEngine{}, clock, fixedQueryID("generated"),
				WithQueryAnalyzer(staticQueryAnalyzer{analysis: ports.QueryAnalysis{ReferencedTables: test.references}}),
				WithQueryDestinationCatalog(catalog),
			)
			job, created, err := service.newJob(ctx, test.input)
			if err != nil {
				t.Fatal(err)
			}
			if !created || job.Reference.Location != test.want {
				t.Fatalf("created=%v location=%q, want %q", created, job.Reference.Location, test.want)
			}
		})
	}
}

func TestDMLCannotTargetAnonymousCachedResultTable(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	clock := fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := NewCatalogService(memory.NewCatalogRepository(), &fakeWarehouse{}, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.EnsureAnonymousDataset(ctx, "test-project", "_bqemu_anonymous_us", "US"); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "_bqemu_anonymous_us", ID: "_bqemu_query_cached",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}); err != nil {
		t.Fatal(err)
	}
	reference := domain.TableReference{
		ProjectID: "test-project", DatasetID: "_bqemu_anonymous_us", TableID: "_bqemu_query_cached",
	}
	repository := memory.NewJobRepository()
	service := newTestQueryService(
		repository, &countingQueryEngine{}, clock, fixedQueryID("generated"),
		WithQueryAnalyzer(staticQueryAnalyzer{analysis: ports.QueryAnalysis{
			ReferencedTables: []domain.TableReference{reference}, MutationTargets: []domain.TableReference{reference},
		}}),
		WithQueryDestinationCatalog(catalog),
	)
	if _, _, err := service.newJob(ctx, QueryInput{
		ProjectID: "test-project", JobID: "mutate-cache", SQL: "DELETE FROM `_bqemu_anonymous_us._bqemu_query_cached`",
	}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("anonymous cached-result DML error = %v, want invalid", err)
	}
	jobs, err := repository.List(ctx, "test-project", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("rejected cached-result DML persisted jobs: %#v", jobs)
	}
}

func TestQueryLocationMismatchIsRejectedBeforeJobInsertion(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	clock := fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := NewCatalogService(memory.NewCatalogRepository(), &fakeWarehouse{}, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	for id, location := range map[string]string{"eu_source": "EU", "us_source": "US"} {
		if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: id, Location: location}); err != nil {
			t.Fatal(err)
		}
		if _, err := catalog.CreateTable(ctx, domain.Table{
			ProjectID: "test-project", DatasetID: id, ID: "events",
			Schema: []domain.Field{{Name: "id", Type: "INT64"}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name       string
		location   string
		references []domain.TableReference
	}{
		{
			name: "mixed datasets",
			references: []domain.TableReference{
				{ProjectID: "test-project", DatasetID: "eu_source", TableID: "events"},
				{ProjectID: "test-project", DatasetID: "us_source", TableID: "events"},
			},
		},
		{
			name: "explicit location mismatch", location: "US",
			references: []domain.TableReference{{ProjectID: "test-project", DatasetID: "eu_source", TableID: "events"}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := memory.NewJobRepository()
			service := newTestQueryService(
				repository, &countingQueryEngine{}, clock, fixedQueryID("generated"),
				WithQueryAnalyzer(staticQueryAnalyzer{analysis: ports.QueryAnalysis{ReferencedTables: test.references}}),
				WithQueryDestinationCatalog(catalog),
			)
			_, _, err := service.newJob(ctx, QueryInput{
				ProjectID: "test-project", JobID: "must-not-persist", Location: test.location, SQL: "SELECT 1",
			})
			if !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("location error = %v, want invalid", err)
			}
			jobs, listErr := repository.List(ctx, "test-project", "")
			if listErr != nil {
				t.Fatal(listErr)
			}
			if len(jobs) != 0 {
				t.Fatalf("rejected query persisted jobs: %#v", jobs)
			}
		})
	}
}

func TestQueryReferenceAppliesTableExpirationBeforeJobInsertion(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	clock := &mutableCatalogClock{now: now}
	warehouse := &expirationWarehouse{}
	catalog := NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "EU"}); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}}, ExpirationTime: &expires,
	}); err != nil {
		t.Fatal(err)
	}
	clock.Set(expires)
	repository := memory.NewJobRepository()
	service := newTestQueryService(
		repository, &countingQueryEngine{}, clock, fixedQueryID("generated"),
		WithQueryAnalyzer(staticQueryAnalyzer{analysis: ports.QueryAnalysis{ReferencedTables: []domain.TableReference{{
			ProjectID: "test-project", DatasetID: "analytics", TableID: "events",
		}}}}),
		WithQueryDestinationCatalog(catalog),
	)
	if _, _, err := service.newJob(ctx, QueryInput{
		ProjectID: "test-project", JobID: "expired-source", SQL: "SELECT id FROM `test-project.analytics.events`",
	}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired query source error = %v, want not found", err)
	}
	jobs, err := repository.List(ctx, "test-project", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expired-source query persisted jobs: %#v", jobs)
	}
	warehouse.mu.Lock()
	drops := len(warehouse.droppedTables)
	warehouse.mu.Unlock()
	if drops != 1 {
		t.Fatalf("expired query source physical drops = %d, want one", drops)
	}
}

func TestAnonymousDestinationIdentityIsGeneratedBeforeJobInsertion(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	clock := fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	service := newTestQueryService(
		memory.NewJobRepository(), &countingQueryEngine{}, clock, fixedQueryID("generated"),
		WithQueryAnalyzer(staticQueryAnalyzer{analysis: ports.QueryAnalysis{ProducesRows: true}}),
		WithQueryMaterializer(&compensatingMaterializer{}), WithQueryDestinationCatalog(failedPublicationCatalog{}),
	)
	job, created, err := service.newJob(ctx, QueryInput{ProjectID: "test-project", JobID: "anonymous-job", SQL: "SELECT 1"})
	if err != nil {
		t.Fatal(err)
	}
	if !created || !job.Configuration.AnonymousDestination || job.Configuration.Destination == nil {
		t.Fatalf("anonymous job configuration = %#v", job.Configuration)
	}
	destination := *job.Configuration.Destination
	if destination.ProjectID != "test-project" ||
		!strings.HasPrefix(destination.DatasetID, "_bqemu_anonymous_") ||
		!strings.HasPrefix(destination.TableID, "_bqemu_query_") ||
		job.Configuration.WriteDisposition != domain.WriteEmpty ||
		job.Configuration.CreateDisposition != domain.CreateIfNeeded {
		t.Fatalf("generated destination = %#v configuration=%#v", destination, job.Configuration)
	}
}

func TestAnonymousMaterializationPublicationFailureIsCompensated(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	materializer := &compensatingMaterializer{}
	service := newTestQueryService(
		memory.NewJobRepository(), &countingQueryEngine{}, fixedClock{now: time.Unix(1, 0)}, fixedQueryID("generated"),
		WithQueryAnalyzer(staticQueryAnalyzer{analysis: ports.QueryAnalysis{ProducesRows: true}}),
		WithQueryMaterializer(materializer), WithQueryDestinationCatalog(failedPublicationCatalog{}),
	)
	job, err := service.RunSync(ctx, QueryInput{ProjectID: "test-project", JobID: "anonymous-publish-fails", SQL: "SELECT 1 AS id"})
	if err != nil {
		t.Fatal(err)
	}
	if job.State != domain.JobDone || job.Error == nil || !job.Configuration.AnonymousDestination {
		t.Fatalf("anonymous publication failure job = %#v", job)
	}
	if got := materializer.drops.Load(); got != 1 {
		t.Fatalf("anonymous compensating drops = %d, want 1", got)
	}
}
