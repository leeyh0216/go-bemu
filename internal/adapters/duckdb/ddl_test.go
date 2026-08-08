package duckdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
)

func TestWarehouseAppliesEveryPlannedDDLMutationAndTruncation(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "analytics"); err != nil {
		t.Fatal(err)
	}
	target := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	schema := []domain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "note", Type: "STRING"},
		{Name: "convertible", Type: "STRING"},
	}
	applyDDLMutation(t, ctx, warehouse, mustDDLMutation(t, engine.TableMutationDescriptor{
		Kind: engine.TableMutationCreate, Target: target, AfterSchema: schema,
		CorrelationID: "create", Generation: 1,
	}))
	if _, err := warehouse.db.ExecContext(ctx, "INSERT INTO "+quotedDDLTable(target)+" VALUES (1, 'not-an-integer', '7')"); err != nil {
		t.Fatal(err)
	}

	score := domain.Field{Name: "score", Type: "NUMERIC", Precision: int64Pointer(10), Scale: int64Pointer(2)}
	afterAdd := append(domain.CloneFields(schema), score)
	applyDDLMutation(t, ctx, warehouse, mustDDLMutation(t, engine.TableMutationDescriptor{
		Kind: engine.TableMutationAddColumn, Target: target, BeforeSchema: schema, AfterSchema: afterAdd,
		FieldChanges:  []engine.FieldChangeDescriptor{{Path: []string{"score"}, After: score}},
		CorrelationID: "add", ExpectedGeneration: 1, Generation: 2,
	}))

	afterRename := domain.CloneFields(afterAdd)
	renameBefore := afterRename[1]
	afterRename[1].Name = "message"
	applyDDLMutation(t, ctx, warehouse, mustDDLMutation(t, engine.TableMutationDescriptor{
		Kind: engine.TableMutationRenameColumn, Target: target, BeforeSchema: afterAdd, AfterSchema: afterRename,
		FieldChanges:  []engine.FieldChangeDescriptor{{Path: []string{"note"}, Before: renameBefore, After: afterRename[1]}},
		CorrelationID: "rename", ExpectedGeneration: 2, Generation: 3,
	}))

	badType := domain.CloneFields(afterRename)
	badBefore := badType[1]
	badType[1].Type = "INT64"
	badMutation := mustDDLMutation(t, engine.TableMutationDescriptor{
		Kind: engine.TableMutationChangeColumnType, Target: target, BeforeSchema: afterRename, AfterSchema: badType,
		FieldChanges:  []engine.FieldChangeDescriptor{{Path: []string{"message"}, Before: badBefore, After: badType[1]}},
		CorrelationID: "bad-type", ExpectedGeneration: 3, Generation: 4,
	})
	badPlan, err := warehouse.PlanTableMutation(ctx, badMutation)
	if err != nil {
		t.Fatal(err)
	}
	if err := warehouse.ApplyTableMutation(ctx, badPlan, badMutation); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("incompatible SET DATA TYPE error = %v", err)
	}
	var preserved string
	if err := warehouse.db.QueryRowContext(ctx, "SELECT message FROM "+quotedDDLTable(target)).Scan(&preserved); err != nil || preserved != "not-an-integer" {
		t.Fatalf("failed type change was not rolled back: value=%q error=%v", preserved, err)
	}

	afterType := domain.CloneFields(afterRename)
	typeBefore := afterType[2]
	afterType[2].Type = "INT64"
	applyDDLMutation(t, ctx, warehouse, mustDDLMutation(t, engine.TableMutationDescriptor{
		Kind: engine.TableMutationChangeColumnType, Target: target, BeforeSchema: afterRename, AfterSchema: afterType,
		FieldChanges:  []engine.FieldChangeDescriptor{{Path: []string{"convertible"}, Before: typeBefore, After: afterType[2]}},
		CorrelationID: "type", ExpectedGeneration: 3, Generation: 4,
	}))

	afterDrop := append(domain.CloneFields(afterType[:3]), afterType[4:]...)
	applyDDLMutation(t, ctx, warehouse, mustDDLMutation(t, engine.TableMutationDescriptor{
		Kind: engine.TableMutationDropColumn, Target: target, BeforeSchema: afterType, AfterSchema: afterDrop,
		FieldChanges:  []engine.FieldChangeDescriptor{{Path: []string{"score"}, Before: afterType[3]}},
		CorrelationID: "drop-column", ExpectedGeneration: 4, Generation: 5,
	}))

	replacement, err := engine.NewDataReplacement(engine.DataReplacementDescriptor{
		Scope: engine.DataReplacementTable, Target: target, Schema: afterDrop,
		CorrelationID: "truncate", ExpectedGeneration: 5, Generation: 6,
		SourceFingerprint: "sha256:" + strings.Repeat("a", 64),
		ResultFingerprint: "sha256:" + strings.Repeat("b", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	truncatePlan, err := warehouse.PlanTableTruncation(ctx, replacement)
	if err != nil {
		t.Fatal(err)
	}
	if err := warehouse.ApplyTableTruncation(ctx, truncatePlan, replacement); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := warehouse.db.QueryRowContext(ctx, "SELECT count(*) FROM "+quotedDDLTable(target)).Scan(&count); err != nil || count != 0 {
		t.Fatalf("TRUNCATE row count = %d, error = %v", count, err)
	}

	applyDDLMutation(t, ctx, warehouse, mustDDLMutation(t, engine.TableMutationDescriptor{
		Kind: engine.TableMutationDrop, Target: target, BeforeSchema: afterDrop,
		CorrelationID: "drop-table", ExpectedGeneration: 6, Generation: 7,
	}))
	var exists int
	if err := warehouse.db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?`, physicalSchema(target.ProjectID, target.DatasetID), target.TableID).Scan(&exists); err != nil || exists != 0 {
		t.Fatalf("DROP TABLE exists = %d, error = %v", exists, err)
	}
}

func applyDDLMutation(t *testing.T, ctx context.Context, warehouse *Warehouse, mutation engine.TableMutation) {
	t.Helper()
	plan, err := warehouse.PlanTableMutation(ctx, mutation)
	if err != nil {
		t.Fatalf("PlanTableMutation(%s): %v", mutation.Kind(), err)
	}
	if err := warehouse.ApplyTableMutation(ctx, plan, mutation); err != nil {
		t.Fatalf("ApplyTableMutation(%s): %v", mutation.Kind(), err)
	}
}

func mustDDLMutation(t *testing.T, descriptor engine.TableMutationDescriptor) engine.TableMutation {
	t.Helper()
	mutation, err := engine.NewTableMutation(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return mutation
}
