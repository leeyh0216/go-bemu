package application

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	stateadapter "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/state"
)

func TestDurableDDLAlterTypeAndDropColumn(t *testing.T) {
	ctx := context.Background()
	store, warehouse, service := openDurableDDLFixture(t, filepath.Join(t.TempDir(), "state.sqlite"), filepath.Join(t.TempDir(), "warehouse.duckdb"))
	defer store.Close()
	defer warehouse.Close()
	seedDurableDDLTable(t, ctx, service, []domain.Field{{Name: "value", Type: "STRING"}, {Name: "note", Type: "STRING"}})
	if _, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "INSERT INTO `test-project.analytics.events` VALUES ('42', 'keep')"}); err != nil {
		t.Fatal(err)
	}

	err := service.ExecuteDDL(ctx, DDLCommand{
		Kind: "ALTER_COLUMN_TYPE", Table: ddlTestReference(), Name: "value",
		Field: domain.Field{Name: "value", Type: "INT64", Mode: "NULLABLE"},
	})
	if err != nil {
		t.Fatal(err)
	}
	table, err := service.GetTable(ctx, "test-project", "analytics", "events")
	if err != nil {
		t.Fatal(err)
	}
	if table.Schema[0].Type != "INT64" {
		t.Fatalf("canonical type = %s, want INT64", table.Schema[0].Type)
	}
	result, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "SELECT value FROM `test-project.analytics.events`"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(42) {
		t.Fatalf("converted rows = %#v", result.Rows)
	}

	if err := service.ExecuteDDL(ctx, DDLCommand{Kind: "DROP_COLUMN", Table: ddlTestReference(), Name: "note"}); err != nil {
		t.Fatal(err)
	}
	table, err = service.GetTable(ctx, "test-project", "analytics", "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Schema) != 1 || table.Schema[0].Name != "value" {
		t.Fatalf("canonical schema after drop = %#v", table.Schema)
	}
	if pending, err := store.ListPending(ctx, state.MaxPendingList); err != nil || len(pending) != 0 {
		t.Fatalf("pending mutations = %#v, error = %v", pending, err)
	}
	if err := service.RecoverCatalogState(ctx); err != nil {
		t.Fatalf("post-DDL catalog validation failed: %v", err)
	}
}

func TestDurableDDLConversionFailureRollsBackWithoutDrift(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()
	store, warehouse, service := openDurableDDLFixture(t, filepath.Join(temp, "state.sqlite"), filepath.Join(temp, "warehouse.duckdb"))
	defer store.Close()
	defer warehouse.Close()
	seedDurableDDLTable(t, ctx, service, []domain.Field{{Name: "value", Type: "STRING"}})
	if _, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "INSERT INTO `test-project.analytics.events` VALUES ('not-an-integer')"}); err != nil {
		t.Fatal(err)
	}

	err := service.ExecuteDDL(ctx, DDLCommand{
		Kind: "ALTER_COLUMN_TYPE", Table: ddlTestReference(), Name: "value",
		Field: domain.Field{Name: "value", Type: "INT64", Mode: "NULLABLE"},
	})
	if err == nil {
		t.Fatal("incompatible type conversion succeeded")
	}
	table, getErr := service.GetTable(ctx, "test-project", "analytics", "events")
	if getErr != nil {
		t.Fatal(getErr)
	}
	if table.Schema[0].Type != "STRING" {
		t.Fatalf("canonical schema drifted to %#v", table.Schema)
	}
	matches, matchErr := warehouse.TableSchemaMatches(ctx, table)
	if matchErr != nil || !matches {
		t.Fatalf("physical schema was not rolled back: matches=%v error=%v", matches, matchErr)
	}
	result, queryErr := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "SELECT value FROM `test-project.analytics.events`"})
	if queryErr != nil {
		t.Fatal(queryErr)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "not-an-integer" {
		t.Fatalf("physical rows drifted = %#v", result.Rows)
	}
	if pending, listErr := store.ListPending(ctx, state.MaxPendingList); listErr != nil || len(pending) != 0 {
		t.Fatalf("failed mutation remained pending = %#v, error=%v", pending, listErr)
	}
	if recoveryErr := service.RecoverCatalogState(ctx); recoveryErr != nil {
		t.Fatalf("consistent failed mutation blocked recovery: %v", recoveryErr)
	}
}

func TestDDLRecoveryAcrossPrepareAndPhysicalCrashPoints(t *testing.T) {
	for _, test := range []struct {
		name          string
		applyPhysical bool
	}{
		{name: "after_prepare", applyPhysical: false},
		{name: "after_physical_apply", applyPhysical: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			temp := t.TempDir()
			statePath, warehousePath := filepath.Join(temp, "state.sqlite"), filepath.Join(temp, "warehouse.duckdb")
			store, warehouse, service := openDurableDDLFixture(t, statePath, warehousePath)
			seedDurableDDLTable(t, ctx, service, []domain.Field{{Name: "value", Type: "STRING"}})
			if _, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "INSERT INTO `test-project.analytics.events` VALUES ('7')"}); err != nil {
				t.Fatal(err)
			}
			record, plan := prepareDDLTestMutation(t, ctx, store, warehouse, service, "crash-"+test.name)
			if test.applyPhysical {
				if err := warehouse.ApplyTableSchemaChange(ctx, plan); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := warehouse.Close(); err != nil {
				t.Fatal(err)
			}

			store, warehouse, service = openDurableDDLFixture(t, statePath, warehousePath)
			defer store.Close()
			defer warehouse.Close()
			if err := service.RecoverCatalogState(ctx); err != nil {
				t.Fatal(err)
			}
			table, err := service.GetTable(ctx, "test-project", "analytics", "events")
			if err != nil {
				t.Fatal(err)
			}
			if table.Schema[0].Type != "INT64" {
				t.Fatalf("recovered canonical type = %s", table.Schema[0].Type)
			}
			persisted, err := store.Get(ctx, record.ID)
			if err != nil {
				t.Fatal(err)
			}
			if persisted.State != state.MutationApplied {
				t.Fatalf("recovered mutation state = %s", persisted.State)
			}
			result, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "SELECT value FROM `test-project.analytics.events`"})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 || result.Rows[0][0] != int64(7) {
				t.Fatalf("recovered rows = %#v", result.Rows)
			}
		})
	}
}

func TestDDLRecoveryRejectsPhysicalSchemaOutsideBothJournalSides(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()
	statePath, warehousePath := filepath.Join(temp, "state.sqlite"), filepath.Join(temp, "warehouse.duckdb")
	store, warehouse, service := openDurableDDLFixture(t, statePath, warehousePath)
	seedDurableDDLTable(t, ctx, service, []domain.Field{{Name: "value", Type: "STRING"}})
	_, _ = prepareDDLTestMutation(t, ctx, store, warehouse, service, "drift-mutation")
	before, err := store.GetTable(ctx, "test-project", "analytics", "events")
	if err != nil {
		t.Fatal(err)
	}
	third := before
	third.Schema = []domain.Field{{Name: "value", Type: "FLOAT64"}}
	third.UpdatedAt = before.UpdatedAt.Add(2 * time.Second)
	thirdPlan, err := warehouse.PlanTableChange(before, third)
	if err != nil {
		t.Fatal(err)
	}
	if err := warehouse.ApplyTableSchemaChange(ctx, thirdPlan); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.Close(); err != nil {
		t.Fatal(err)
	}

	store, warehouse, service = openDurableDDLFixture(t, statePath, warehousePath)
	defer store.Close()
	defer warehouse.Close()
	err = service.RecoverCatalogState(ctx)
	if err == nil || !strings.Contains(err.Error(), "matches neither side") {
		t.Fatalf("drift recovery error = %v", err)
	}
	canonical, getErr := store.GetTable(ctx, "test-project", "analytics", "events")
	if getErr != nil {
		t.Fatal(getErr)
	}
	if canonical.Schema[0].Type != "STRING" {
		t.Fatalf("drift changed canonical metadata = %#v", canonical.Schema)
	}
}

func TestCatalogRecoveryRejectsOneSidedStoreRestore(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()
	statePath, warehousePath := filepath.Join(temp, "state.sqlite"), filepath.Join(temp, "warehouse.duckdb")
	store, warehouse, service := openDurableDDLFixture(t, statePath, warehousePath)
	seedDurableDDLTable(t, ctx, service, []domain.Field{{Name: "value", Type: "STRING"}})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.Close(); err != nil {
		t.Fatal(err)
	}

	t.Run("canonical_only", func(t *testing.T) {
		store, emptyWarehouse, service := openDurableDDLFixture(t, statePath, filepath.Join(temp, "empty-warehouse.duckdb"))
		defer store.Close()
		defer emptyWarehouse.Close()
		err := service.RecoverCatalogState(ctx)
		if err == nil || !strings.Contains(err.Error(), "missing dataset storage") {
			t.Fatalf("canonical-only restore error = %v", err)
		}
	})
	t.Run("physical_only", func(t *testing.T) {
		emptyStore, warehouse, service := openDurableDDLFixture(t, filepath.Join(temp, "empty-state.sqlite"), warehousePath)
		defer emptyStore.Close()
		defer warehouse.Close()
		err := service.RecoverCatalogState(ctx)
		if err == nil || !strings.Contains(err.Error(), "unexpected dataset storage") {
			t.Fatalf("physical-only restore error = %v", err)
		}
	})
}

func openDurableDDLFixture(t *testing.T, statePath, warehousePath string) (*stateadapter.Store, *duckdb.Warehouse, *CatalogService) {
	t.Helper()
	ctx := context.Background()
	store, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(statePath))
	if err != nil {
		t.Fatal(err)
	}
	warehouse, err := duckdb.New(warehousePath)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	service := NewCatalogService(store, warehouse, fixedClock{now: time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC)})
	return store, warehouse, service
}

func seedDurableDDLTable(t *testing.T, ctx context.Context, service *CatalogService, schema []domain.Field) {
	t.Helper()
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil && !errors.Is(err, domain.ErrConflict) {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil && !errors.Is(err, domain.ErrConflict) {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "events", Schema: schema}); err != nil {
		t.Fatal(err)
	}
}

func prepareDDLTestMutation(
	t *testing.T,
	ctx context.Context,
	store *stateadapter.Store,
	warehouse *duckdb.Warehouse,
	service *CatalogService,
	id string,
) (state.Mutation, ports.TableSchemaChangePlan) {
	t.Helper()
	before, err := service.GetTable(ctx, "test-project", "analytics", "events")
	if err != nil {
		t.Fatal(err)
	}
	after := before
	after.Schema = []domain.Field{{Name: "value", Type: "INT64"}}
	after.UpdatedAt = before.UpdatedAt.Add(time.Second)
	plan, err := warehouse.PlanTableChange(before, after)
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Begin(ctx, state.BeginMutation{
		ID: id, ResourceKey: state.TableResourceKey(before), Kind: state.MutationKindTableSchema,
		ExpectedCanonicalRevision: state.TableRevision(before),
		BeforePhysicalFingerprint: plan.BeforePhysicalFingerprint,
		AfterPhysicalFingerprint:  plan.AfterPhysicalFingerprint,
		TableChange:               state.TableChange{Before: before, After: after},
		PreparedAt:                before.UpdatedAt.Add(time.Nanosecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	return record, plan
}

func ddlTestReference() domain.TableReference {
	return domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
}
