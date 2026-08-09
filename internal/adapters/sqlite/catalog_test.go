package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestCatalogRepositoryContract(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.sqlite"))

	if _, err := store.GetProject(ctx, "missing-project"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("GetProject missing error = %v, want ErrNotFound", err)
	}
	if err := store.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "missing_parent"}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("CreateDataset missing parent error = %v, want ErrNotFound", err)
	}

	now := time.Date(2026, 8, 8, 12, 34, 56, 789, time.UTC)
	project := domain.Project{
		ID: "test-project", FriendlyName: "Test", Description: "catalog contract", CreatedAt: now, UpdatedAt: now,
	}
	if err := store.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateProject(ctx, project); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate CreateProject error = %v, want ErrConflict", err)
	}
	if err := store.CreateProject(ctx, domain.Project{ID: "another-project"}); err != nil {
		t.Fatal(err)
	}
	projects, err := store.ListProjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{projects[0].ID, projects[1].ID}; !reflect.DeepEqual(got, []string{"another-project", "test-project"}) {
		t.Fatalf("project order = %v", got)
	}

	tableExpiration := int64(86_400_000)
	partitionExpiration := int64(3_600_000)
	dataset := domain.Dataset{
		ProjectID: "test-project", ID: "analytics", FriendlyName: "Analytics", Description: "source of truth",
		Location: "EU", Labels: map[string]string{"owner": "data"},
		DefaultTableExpirationMs: &tableExpiration, DefaultPartitionExpirationMs: &partitionExpiration,
		CreatedAt: now, UpdatedAt: now.Add(time.Minute), Hidden: true,
	}
	if err := store.CreateDataset(ctx, dataset); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDataset(ctx, dataset); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate CreateDataset error = %v, want ErrConflict", err)
	}
	if err := store.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "aaa"}); err != nil {
		t.Fatal(err)
	}
	datasets, err := store.ListDatasets(ctx, "test-project")
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{datasets[0].ID, datasets[1].ID}; !reflect.DeepEqual(got, []string{"aaa", "analytics"}) {
		t.Fatalf("dataset order = %v", got)
	}
	loadedDataset, err := store.GetDataset(ctx, dataset.ProjectID, dataset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loadedDataset, dataset) {
		t.Fatalf("dataset round trip mismatch:\n got: %#v\nwant: %#v", loadedDataset, dataset)
	}
	loadedDataset.Labels["owner"] = "mutated"
	reloadedDataset, err := store.GetDataset(ctx, dataset.ProjectID, dataset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloadedDataset.Labels["owner"] != "data" {
		t.Fatal("returned dataset mutated persisted labels")
	}

	expiresAt := now.Add(24 * time.Hour)
	table := domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		FriendlyName: "Events", Description: "nested rows", Labels: map[string]string{"tier": "test"},
		Type: "TABLE", Location: "EU", ExpirationTime: &expiresAt,
		Schema: []domain.Field{
			{Name: "event_id", Type: "INT64", Mode: "REQUIRED", Description: "identity"},
			{Name: "payload", Type: "RECORD", Mode: "NULLABLE", Fields: []domain.Field{
				{Name: "tags", Type: "STRING", Mode: "REPEATED"},
				{Name: "attributes", Type: "RECORD", Mode: "REPEATED", Fields: []domain.Field{
					{Name: "name", Type: "STRING", Mode: "NULLABLE"},
				}},
			}},
		},
		TimePartitioning:  &domain.TimePartitioning{Type: "DAY", Field: "event_date", ExpirationMs: 7200},
		RangePartitioning: &domain.RangePartitioning{Field: "bucket", Range: domain.Range{Start: 0, End: 100, Interval: 10}},
		ClusteringFields:  []string{"event_id", "payload"}, CreatedAt: now, UpdatedAt: now.Add(2 * time.Minute),
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTable(ctx, table); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate CreateTable error = %v, want ErrConflict", err)
	}
	if err := store.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "missing", ID: "events"}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("CreateTable missing parent error = %v, want ErrNotFound", err)
	}
	loadedTable, err := store.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectedTable := normalizeFieldSlices(table)
	if !reflect.DeepEqual(loadedTable, expectedTable) {
		t.Fatalf("table round trip mismatch:\n got: %#v\nwant: %#v", loadedTable, expectedTable)
	}
	loadedTable.Schema[1].Fields[0].Name = "mutated"
	loadedTable.Labels["tier"] = "mutated"
	reloadedTable, err := store.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloadedTable.Schema[1].Fields[0].Name != "tags" || reloadedTable.Labels["tier"] != "test" {
		t.Fatal("returned table mutated persisted metadata")
	}

	table.Description = "updated"
	table.Schema = append(table.Schema, domain.Field{Name: "amount", Type: "NUMERIC", Mode: "NULLABLE"})
	table.Labels = map[string]string{}
	table.ClusteringFields = nil
	table.UpdatedAt = now.Add(3 * time.Minute)
	if err := store.UpdateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	updatedTable, err := store.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectedTable = normalizeFieldSlices(table)
	if !reflect.DeepEqual(updatedTable, expectedTable) {
		t.Fatalf("updated table mismatch:\n got: %#v\nwant: %#v", updatedTable, expectedTable)
	}
	if err := store.UpdateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "missing"}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("UpdateTable missing error = %v, want ErrNotFound", err)
	}

	if err := store.DeleteDataset(ctx, "test-project", "analytics"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetTable(ctx, "test-project", "analytics", "events"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("table after dataset cascade error = %v, want ErrNotFound", err)
	}
	if err := store.DeleteDataset(ctx, "test-project", "analytics"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("second DeleteDataset error = %v, want ErrNotFound", err)
	}
	if err := store.DeleteProject(ctx, "test-project"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListDatasets(ctx, "test-project"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("ListDatasets after project deletion error = %v, want ErrNotFound", err)
	}
}

func TestCatalogRepositoryPersistsAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store := openTestStore(t, path)
	now := time.Date(2026, 8, 8, 1, 2, 3, 4, time.UTC)
	precision, scale := int64(38), int64(18)
	project := domain.Project{ID: "test-project", CreatedAt: now, UpdatedAt: now}
	dataset := domain.Dataset{ProjectID: project.ID, ID: "analytics", Location: "US", Labels: map[string]string{}, CreatedAt: now, UpdatedAt: now}
	table := domain.Table{
		ProjectID: project.ID, DatasetID: dataset.ID, ID: "events", Type: "TABLE", Location: "US",
		Schema: []domain.Field{{Name: "items", Type: "RECORD", Mode: "REPEATED", Fields: []domain.Field{
			{Name: "amount", Type: "BIGNUMERIC", Mode: "NULLABLE", Precision: &precision, Scale: &scale},
		}}},
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDataset(ctx, dataset); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openTestStore(t, path)
	loaded, err := store.GetTable(ctx, table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectedTable := normalizeFieldSlices(table)
	if !reflect.DeepEqual(loaded, expectedTable) {
		t.Fatalf("table after restart mismatch:\n got: %#v\nwant: %#v", loaded, expectedTable)
	}
}

func TestOpenConfiguresPragmasAndReservedDecimalColumns(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.sqlite"))
	var foreignKeys, busyTimeout int
	var journalMode string
	if err := store.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if foreignKeys != 1 || busyTimeout != 5000 || !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("pragmas: foreign_keys=%d busy_timeout=%d journal_mode=%s", foreignKeys, busyTimeout, journalMode)
	}

	rows, err := store.db.Query("PRAGMA table_info(table_fields)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if !columns["precision"] || !columns["scale"] {
		t.Fatalf("table_fields columns = %v, want precision and scale", columns)
	}
}

func TestOpenRejectsChangedAppliedMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store := openTestStore(t, path)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE schema_migrations SET checksum = 'changed' WHERE version = 1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(ctx, DefaultConfig(path))
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Open error = %v, want checksum mismatch", err)
	}
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(context.Background(), DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func normalizeFieldSlices(table domain.Table) domain.Table {
	var normalize func([]domain.Field) []domain.Field
	normalize = func(fields []domain.Field) []domain.Field {
		result := make([]domain.Field, len(fields))
		for index, field := range fields {
			result[index] = field
			result[index].Fields = normalize(field.Fields)
		}
		return result
	}
	table.Schema = normalize(table.Schema)
	return table
}
