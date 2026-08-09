package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestCatalogMetadataSurvivesRepositoryRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bqemu-state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	catalog := repositories.Catalog()
	// The bundle exposes a catalog context port. Consumers with narrower duties
	// can retain only the composed resource interface they require.
	var _ ports.ProjectRepository = catalog
	var _ ports.DatasetRepository = catalog
	var _ ports.TableRepository = catalog
	var _ ports.ViewRepository = catalog.(ports.ViewRepository)

	project, dataset, table := completeCatalogMetadata()
	if err := catalog.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := catalog.CreateDataset(ctx, dataset); err != nil {
		t.Fatal(err)
	}
	if err := catalog.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	fingerprint := sha256.Sum256([]byte("SELECT id FROM event_log"))
	view := domain.View{
		ProjectID: project.ID, DatasetID: dataset.ID, ID: "event_ids",
		Query: "SELECT id FROM event_log", Schema: []domain.Field{{Name: "id", Type: "INT64", Mode: "NULLABLE"}},
		Dependencies:        []domain.TableReference{{ProjectID: project.ID, DatasetID: dataset.ID, TableID: table.ID}},
		AnalysisFingerprint: fmt.Sprintf("%x", fingerprint), Location: dataset.Location,
		CreatedAt: table.CreatedAt, UpdatedAt: table.UpdatedAt,
	}
	view = domain.CloneView(view)
	if err := catalog.(ports.ViewRepository).CreateView(ctx, view); err != nil {
		t.Fatal(err)
	}
	if err := repositories.Check(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restartedCatalog := restarted.Catalog()

	loadedProject, err := restartedCatalog.GetProject(ctx, project.ID)
	if err != nil {
		t.Fatal(err)
	}
	loadedDataset, err := restartedCatalog.GetDataset(ctx, dataset.ProjectID, dataset.ID)
	if err != nil {
		t.Fatal(err)
	}
	loadedTable, err := restartedCatalog.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		t.Fatal(err)
	}
	loadedView, err := restartedCatalog.(ports.ViewRepository).GetView(ctx, view.ProjectID, view.DatasetID, view.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loadedProject, project) {
		t.Fatalf("project round-trip mismatch:\n got: %#v\nwant: %#v", loadedProject, project)
	}
	if !reflect.DeepEqual(loadedDataset, dataset) {
		t.Fatalf("dataset round-trip mismatch:\n got: %#v\nwant: %#v", loadedDataset, dataset)
	}
	if !reflect.DeepEqual(loadedTable, table) {
		t.Fatalf("table round-trip mismatch:\n got: %#v\nwant: %#v", loadedTable, table)
	}
	if !reflect.DeepEqual(loadedView, view) {
		t.Fatalf("view round-trip mismatch:\n got: %#v\nwant: %#v", loadedView, view)
	}

	assertDecimalIdentity(t, loadedTable.Schema)
	projects, err := restartedCatalog.ListProjects(ctx)
	if err != nil || len(projects) != 1 || projects[0].ID != project.ID {
		t.Fatalf("restarted projects = %#v, %v", projects, err)
	}
	datasets, err := restartedCatalog.ListDatasets(ctx, project.ID)
	if err != nil || len(datasets) != 1 || !reflect.DeepEqual(datasets[0], dataset) {
		t.Fatalf("restarted datasets = %#v, %v", datasets, err)
	}
	tables, err := restartedCatalog.ListTables(ctx, project.ID, dataset.ID)
	if err != nil || len(tables) != 1 || !reflect.DeepEqual(tables[0], table) {
		t.Fatalf("restarted tables = %#v, %v", tables, err)
	}
}

func TestRangePartitioningSurvivesRepositoryRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bqemu-state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	project, dataset, _ := completeCatalogMetadata()
	table := domain.Table{
		ProjectID: project.ID, DatasetID: dataset.ID, ID: "range_partitioned",
		Type: "TABLE", Location: dataset.Location,
		Schema: []domain.Field{{Name: "bucket", Type: "INT64"}},
		RangePartitioning: &domain.RangePartitioning{
			Field: "bucket", Range: domain.Range{Start: -10, End: 100, Interval: 5},
		},
	}
	catalog := repositories.Catalog()
	if err := catalog.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := catalog.CreateDataset(ctx, dataset); err != nil {
		t.Fatal(err)
	}
	if err := catalog.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	loaded, err := restarted.Catalog().GetTable(ctx, table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.RangePartitioning, table.RangePartitioning) || loaded.TimePartitioning != nil {
		t.Fatalf("range partition round-trip mismatch: got=%#v want=%#v", loaded, table)
	}
}

func TestCatalogUpdatesAndCascadesAreDurable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bqemu-state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	catalog := repositories.Catalog()
	project, dataset, table := completeCatalogMetadata()
	if err := catalog.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := catalog.CreateDataset(ctx, dataset); err != nil {
		t.Fatal(err)
	}
	if err := catalog.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}

	dataset.Labels = map[string]string{}
	dataset.Description = "updated dataset"
	dataset.DefaultTableExpirationMs = nil
	dataset.UpdatedAt = dataset.UpdatedAt.Add(time.Hour)
	if err := catalog.UpdateDataset(ctx, dataset); err != nil {
		t.Fatal(err)
	}
	precision, explicitZero := int64(38), int64(0)
	table.Schema = []domain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{
			Name: "payload", Type: "STRUCT", Fields: []domain.Field{{
				Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &explicitZero,
				RoundingMode: domain.RoundingModeHalfAwayFromZero,
			}},
		},
	}
	table.TimePartitioning = nil
	table.Labels = nil
	table.ClusteringFields = nil
	table.UpdatedAt = table.UpdatedAt.Add(2 * time.Hour)
	if err := catalog.UpdateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	loadedDataset, err := restarted.Catalog().GetDataset(ctx, dataset.ProjectID, dataset.ID)
	if err != nil || !reflect.DeepEqual(loadedDataset, dataset) {
		t.Fatalf("updated dataset = %#v, %v; want %#v", loadedDataset, err, dataset)
	}
	loadedTable, err := restarted.Catalog().GetTable(ctx, table.ProjectID, table.DatasetID, table.ID)
	if err != nil || !reflect.DeepEqual(loadedTable, table) {
		t.Fatalf("updated table = %#v, %v; want %#v", loadedTable, err, table)
	}
	if err := restarted.Catalog().DeleteDataset(ctx, dataset.ProjectID, dataset.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Catalog().GetTable(ctx, table.ProjectID, table.DatasetID, table.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cascaded table lookup error = %v", err)
	}
}

func TestUnknownGooseMigrationStopsOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bqemu-state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO bqemu_goose_db_version (version_id, is_applied)
VALUES (?, 1)`, 9_999); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(ctx, path)
	if err == nil || !strings.Contains(err.Error(), "unsupported migration version") {
		t.Fatalf("Open() error = %v, want unknown Goose migration rejection", err)
	}
}

func TestLegacyMigrationLedgerImportsIntoGooseHistory(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE bqemu_goose_db_version`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE bqemu_schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL
) STRICT`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	for version := 1; version <= 5; version++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO bqemu_schema_migrations
    (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`,
			version, "legacy", strings.Repeat("0", 64), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	var legacyCount, maximumVersion int
	if err := restarted.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema
WHERE type = 'table' AND name = 'bqemu_schema_migrations'`).Scan(&legacyCount); err != nil {
		t.Fatal(err)
	}
	if err := restarted.db.QueryRowContext(ctx, `SELECT max(version_id)
FROM bqemu_goose_db_version WHERE is_applied = 1`).Scan(&maximumVersion); err != nil {
		t.Fatal(err)
	}
	if legacyCount != 0 || maximumVersion != 8 {
		t.Fatalf("legacy import retained ledger=%d with Goose version=%d", legacyCount, maximumVersion)
	}
}

func TestMissingSchemaObjectStopsOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bqemu-state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER bqemu_table_fields_parent_insert`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(ctx, path)
	if err == nil || !strings.Contains(err.Error(), "schema object bqemu_table_fields_parent_insert") {
		t.Fatalf("Open() error = %v, want missing schema rejection", err)
	}
}

func TestEmbeddedGooseMigrationsAreVersionedSQLResources(t *testing.T) {
	paths, err := fs.Glob(embeddedMigrations, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"migrations/00001_catalog_baseline.sql",
		"migrations/00002_job_metadata.sql",
		"migrations/00003_storage_read_sessions.sql",
		"migrations/00004_storage_write_ledger.sql",
		"migrations/00005_load_mutation_journal.sql",
		"migrations/00006_drop_legacy_migration_ledger.sql",
		"migrations/00007_logical_views.sql",
		"migrations/00008_runtime_pair_generation.sql",
	}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("migration resource paths = %#v, want %#v", paths, want)
	}
	for _, path := range paths {
		contents, err := embeddedMigrations.ReadFile(path)
		if err != nil || !strings.HasPrefix(string(contents), "-- +goose Up\n") {
			t.Fatalf("migration resource %s = %q, %v", path, contents, err)
		}
	}
}

func completeCatalogMetadata() (domain.Project, domain.Dataset, domain.Table) {
	created := time.Date(2026, 8, 8, 1, 2, 3, 456789000, time.UTC)
	updated := created.Add(37 * time.Minute)
	tableExpiration := int64(86_400_000)
	partitionExpiration := int64(7_200_000)
	expires := created.Add(48 * time.Hour)
	precision38, precision20 := int64(38), int64(20)
	scale4 := int64(4)

	project := domain.Project{
		ID: "test-project", FriendlyName: "Test project", Description: "project metadata",
		CreatedAt: created, UpdatedAt: updated,
	}
	dataset := domain.Dataset{
		ProjectID: project.ID, ID: "analytics", FriendlyName: "Analytics",
		Description: "dataset metadata", Location: "asia-northeast3",
		Labels:                   map[string]string{"environment": "test", "owner": "data"},
		DefaultTableExpirationMs: &tableExpiration, DefaultPartitionExpirationMs: &partitionExpiration,
		CreatedAt: created, UpdatedAt: updated, Hidden: true,
	}
	table := domain.Table{
		ProjectID: project.ID, DatasetID: dataset.ID, ID: "events",
		FriendlyName: "Events", Description: "table metadata",
		Labels: map[string]string{"kind": "fixture"}, Type: "TABLE", Location: dataset.Location,
		ExpirationTime:   &expires,
		TimePartitioning: &domain.TimePartitioning{Type: "DAY", Field: "event_time", ExpirationMs: 3_600_000},
		ClusteringFields: []string{"bucket", "event_time"},
		CreatedAt:        created, UpdatedAt: updated,
		Schema: []domain.Field{
			{Name: "event_time", Type: "TIMESTAMP", Mode: "REQUIRED", Description: "event timestamp"},
			{Name: "bucket", Type: "INT64"},
			{
				Name: "payload", Type: "STRUCT", Description: "nested payload", Fields: []domain.Field{
					{Name: "category", Type: "STRING"},
					{Name: "numeric_default", Type: "NUMERIC"},
					{
						Name: "bignumeric_explicit_rounding", Type: "BIGNUMERIC",
						RoundingMode: domain.RoundingModeUnspecified,
					},
					{
						Name: "bignumeric_precision_only", Type: "BIGNUMERIC", Precision: &precision38,
						RoundingMode: domain.RoundingModeHalfAwayFromZero,
					},
					{
						Name: "numeric_parameterized", Type: "NUMERIC", Precision: &precision20, Scale: &scale4,
						RoundingMode: domain.RoundingModeHalfEven,
					},
					{
						Name: "items", Type: "RECORD", Mode: "REPEATED", Fields: []domain.Field{
							{Name: "enabled", Type: "BOOL", Mode: "REQUIRED"},
						},
					},
				},
			},
		},
	}
	return project, dataset, table
}

func assertDecimalIdentity(t *testing.T, fields []domain.Field) {
	t.Helper()
	payload := fields[2]
	numericDefault := payload.Fields[1]
	if numericDefault.Precision != nil || numericDefault.Scale != nil || numericDefault.RoundingMode != "" {
		t.Fatalf("omitted NUMERIC parameters changed: %#v", numericDefault)
	}
	bigExplicitRounding := payload.Fields[2]
	if bigExplicitRounding.Precision != nil || bigExplicitRounding.Scale != nil ||
		bigExplicitRounding.RoundingMode != domain.RoundingModeUnspecified {
		t.Fatalf("explicit BIGNUMERIC rounding identity changed: %#v", bigExplicitRounding)
	}
	bigPrecisionOnly := payload.Fields[3]
	if bigPrecisionOnly.Precision == nil || *bigPrecisionOnly.Precision != 38 || bigPrecisionOnly.Scale != nil ||
		bigPrecisionOnly.RoundingMode != domain.RoundingModeHalfAwayFromZero {
		t.Fatalf("precision-only BIGNUMERIC identity changed: %#v", bigPrecisionOnly)
	}
	numericParameterized := payload.Fields[4]
	if numericParameterized.Precision == nil || *numericParameterized.Precision != 20 ||
		numericParameterized.Scale == nil || *numericParameterized.Scale != 4 ||
		numericParameterized.RoundingMode != domain.RoundingModeHalfEven {
		t.Fatalf("parameterized NUMERIC identity changed: %#v", numericParameterized)
	}
}
