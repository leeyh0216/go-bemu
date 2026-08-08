package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type fakeSchemaAdapterPlanner struct {
	err   error
	calls int
}

func (adapter *fakeSchemaAdapterPlanner) ValidateSchemaIntent(context.Context, SchemaIntent) error {
	adapter.calls++
	return adapter.err
}

func TestSchemaIntentAndPlanAreImmutable(t *testing.T) {
	precision, scale := int64(20), int64(2)
	after := []domain.Field{
		{Name: "id", Type: "INT64"},
		{Name: "payload", Type: "STRUCT", Fields: []domain.Field{{
			Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale,
		}}},
	}
	intent, err := NewSchemaIntent(SchemaIntentDescriptor{
		Operation:   SchemaOperationCreate,
		Target:      domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"},
		AfterSchema: after,
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities := schemaTestCapabilities(t)
	planner, err := NewSchemaPlanner(capabilities, &fakeSchemaAdapterPlanner{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planner.Plan(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}

	precision = 1
	after[1].Fields[0].Name = "changed"
	descriptor := capabilities.Descriptor()
	descriptor.Decimal.MaxPrecision = 1
	detachedIntent := plan.Intent()
	detachedIntent.afterSchema[1].Fields[0].Name = "also_changed"

	planned := plan.Intent().AfterSchema()
	if planned[1].Fields[0].Name != "amount" || *planned[1].Fields[0].Precision != 20 {
		t.Fatalf("planned schema changed through an external alias: %#v", planned)
	}
	if plan.EngineIdentity() != capabilities.Identity() || plan.Fingerprint() == "" || plan.LogicalFingerprint() == "" {
		t.Fatalf("invalid immutable schema plan: %#v", plan)
	}
	if rendered := fmt.Sprintf("%#v", plan); strings.Contains(rendered, "DECIMAL(") ||
		strings.Contains(rendered, "CREATE TABLE") || strings.Contains(rendered, "duckdb_sql") {
		t.Fatalf("schema plan contains a physical representation: %s", rendered)
	}
}

func TestSchemaIntentRequiresExactTypedAdditions(t *testing.T) {
	target := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	before := []domain.Field{{Name: "payload", Type: "STRUCT", Fields: []domain.Field{{Name: "name", Type: "STRING"}}}}
	after := domain.CloneFields(before)
	addition := domain.SchemaAddition{
		Path:  []string{"payload", "score"},
		Field: domain.Field{Name: "score", Type: "FLOAT64"},
	}
	after[0].Fields = append(after[0].Fields, addition.Field)
	intent, err := NewSchemaIntent(SchemaIntentDescriptor{
		Operation: SchemaOperationAddColumns, Target: target,
		BeforeSchema: before, AfterSchema: after, Additions: []domain.SchemaAddition{addition},
	})
	if err != nil {
		t.Fatal(err)
	}
	addition.Path[0] = "changed"
	if got := strings.Join(intent.Additions()[0].Path, "."); got != "payload.score" {
		t.Fatalf("addition path = %q", got)
	}

	_, err = NewSchemaIntent(SchemaIntentDescriptor{
		Operation: SchemaOperationAddColumns, Target: target,
		BeforeSchema: before, AfterSchema: after,
		Additions: []domain.SchemaAddition{{Path: []string{"payload", "other"}, Field: domain.Field{Name: "other", Type: "STRING"}}},
	})
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("mismatched additions error = %v", err)
	}
}

func TestSchemaPlannerRejectsCapabilityAndBindingDrift(t *testing.T) {
	capabilities := schemaTestCapabilities(t)
	adapter := &fakeSchemaAdapterPlanner{}
	planner, err := NewSchemaPlanner(capabilities, adapter)
	if err != nil {
		t.Fatal(err)
	}
	intent := testCreateSchemaIntent(t, "events", []domain.Field{{Name: "id", Type: "INT64"}})
	plan, err := planner.Plan(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := planner.ValidateBinding(plan, intent); err != nil {
		t.Fatal(err)
	}

	changed := testCreateSchemaIntent(t, "other", []domain.Field{{Name: "id", Type: "INT64"}})
	if err := planner.ValidateBinding(plan, changed); !planningErrorHasCode(err, PlanningCodeMutationMismatch) {
		t.Fatalf("changed intent error = %v", err)
	}
	sibling, err := NewSchemaPlanner(capabilities, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := sibling.ValidateBinding(plan, intent); !planningErrorHasCode(err, PlanningCodePlannerMismatch) {
		t.Fatalf("sibling planner error = %v", err)
	}

	driftedDescriptor := capabilities.Descriptor()
	driftedDescriptor.Composite.MaxStructDepth++
	drifted, err := NewCapabilities(driftedDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	driftedPlanner, err := NewSchemaPlanner(drifted, adapter)
	if err != nil {
		t.Fatal(err)
	}
	driftedPlanner.issuer = planner.issuer
	if err := driftedPlanner.ValidateBinding(plan, intent); !planningErrorHasCode(err, PlanningCodeCapabilityDrift) {
		t.Fatalf("capability drift error = %v", err)
	}
}

func TestSchemaPlannerFailsBeforeAdapterForUnsupportedLogicalBounds(t *testing.T) {
	descriptor := schemaTestCapabilities(t).Descriptor()
	descriptor.Decimal.MaxPrecision = 10
	descriptor.Decimal.MaxScale = 4
	capabilities, err := NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &fakeSchemaAdapterPlanner{}
	planner, err := NewSchemaPlanner(capabilities, adapter)
	if err != nil {
		t.Fatal(err)
	}
	precision, scale := int64(11), int64(2)
	intent := testCreateSchemaIntent(t, "events", []domain.Field{{
		Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale,
	}})
	_, err = planner.Plan(context.Background(), intent)
	if !errors.Is(err, domain.ErrUnsupported) || adapter.calls != 0 {
		t.Fatalf("plan error=%v adapter_calls=%d", err, adapter.calls)
	}
}

func TestSchemaPlannerStripsRawAdapterCause(t *testing.T) {
	const secret = "duckdb_sql_secret_marker"
	planner, err := NewSchemaPlanner(schemaTestCapabilities(t), &fakeSchemaAdapterPlanner{err: errors.New(secret)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Plan(context.Background(), testCreateSchemaIntent(t, "events", []domain.Field{{Name: "id", Type: "INT64"}}))
	if !errors.Is(err, domain.ErrUnsupported) || strings.Contains(fmt.Sprint(err), secret) {
		t.Fatalf("adapter planning error leaked raw cause: %v", err)
	}
}

func TestSchemaPlannerEnforcesAddColumnDepthAndAtomicity(t *testing.T) {
	descriptor := schemaTestCapabilities(t).Descriptor()
	descriptor.DDL[DDLAddColumn] = DDLCapability{
		Guarantee: DDLGuaranteeAtomicPhysicalStatement, MaxFieldPathDepth: 1,
	}
	capabilities, err := NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := NewSchemaPlanner(capabilities, &fakeSchemaAdapterPlanner{})
	if err != nil {
		t.Fatal(err)
	}
	target := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	before := []domain.Field{{Name: "payload", Type: "STRUCT", Fields: []domain.Field{{Name: "name", Type: "STRING"}}}}
	after := domain.CloneFields(before)
	field := domain.Field{Name: "score", Type: "FLOAT64"}
	after[0].Fields = append(after[0].Fields, field)
	intent, err := NewSchemaIntent(SchemaIntentDescriptor{
		Operation: SchemaOperationAddColumns, Target: target, BeforeSchema: before, AfterSchema: after,
		Additions: []domain.SchemaAddition{{Path: []string{"payload", "score"}, Field: field}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Plan(context.Background(), intent)
	if !errors.Is(err, domain.ErrUnsupported) {
		t.Fatalf("weak add-column capability error = %v", err)
	}
}

func TestPlanningErrorExposesOnlyCatalogedDomainClassifications(t *testing.T) {
	tests := []struct {
		code PlanningErrorCode
		want error
	}{
		{code: PlanningCodeInvalidDescriptor, want: domain.ErrInvalid},
		{code: PlanningCodeUnsupported, want: domain.ErrUnsupported},
		{code: PlanningCodeEngineMismatch, want: domain.ErrPrecondition},
		{code: PlanningCodeMutationMismatch, want: domain.ErrPrecondition},
		{code: PlanningCodeCapabilityDrift, want: domain.ErrPrecondition},
		{code: PlanningCodePlannerMismatch, want: domain.ErrPrecondition},
		{code: PlanningCodePhysicalStateDrift, want: domain.ErrPrecondition},
	}
	for _, testCase := range tests {
		t.Run(string(testCase.code), func(t *testing.T) {
			const secret = "adapter_secret_marker"
			err := newPlanningError(testCase.code, "test", "test.attribute", "stable detail", errors.New(secret))
			if !errors.Is(err, testCase.want) || strings.Contains(fmt.Sprint(err), secret) {
				t.Fatalf("planning error = %v, want classification %v", err, testCase.want)
			}
		})
	}
}

func testCreateSchemaIntent(t *testing.T, tableID string, schema []domain.Field) SchemaIntent {
	t.Helper()
	intent, err := NewSchemaIntent(SchemaIntentDescriptor{
		Operation:   SchemaOperationCreate,
		Target:      domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: tableID},
		AfterSchema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func schemaTestCapabilities(t *testing.T) Capabilities {
	t.Helper()
	return mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4"))
}

func planningErrorHasCode(err error, code PlanningErrorCode) bool {
	var planningErr *PlanningError
	return errors.As(err, &planningErr) && planningErr.Code() == code
}
