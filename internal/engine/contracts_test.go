package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestIdentityRejectsWhitespaceAndControlCharacters(t *testing.T) {
	identity, err := NewIdentity("duckdb", "1.4.4+local")
	if err != nil {
		t.Fatal(err)
	}
	if identity.ID() != "duckdb" || identity.Version() != "1.4.4+local" {
		t.Fatalf("identity = %q@%q", identity.ID(), identity.Version())
	}

	for _, input := range []struct {
		id      string
		version string
	}{
		{id: " duckdb", version: "1.4.4"},
		{id: "duckdb ", version: "1.4.4"},
		{id: "duckdb", version: " 1.4.4"},
		{id: "duckdb", version: "1.4.4 "},
		{id: "duckdb", version: "1.4\t4"},
		{id: "duckdb", version: "1.4\n4"},
		{id: "duckdb", version: "1.4\x004"},
	} {
		if _, err := NewIdentity(input.id, input.version); err == nil {
			t.Fatalf("NewIdentity(%q, %q) succeeded", input.id, input.version)
		}
	}
}

func TestCapabilitiesAreDeepCopiedAndFailClosed(t *testing.T) {
	descriptor := testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	capabilities := mustCapabilities(t, descriptor)
	fingerprint := capabilities.Fingerprint()

	delete(descriptor.DDL, DDLChangeColumnType)
	descriptor.Transactions[TransactionScopeSingleTable] = false
	if _, supported := capabilities.DDL(DDLChangeColumnType); !supported ||
		!capabilities.SupportsTransaction(TransactionScopeSingleTable) ||
		capabilities.Fingerprint() != fingerprint {
		t.Fatal("capabilities retained mutable constructor maps")
	}

	detached := capabilities.Descriptor()
	delete(detached.DDL, DDLChangeColumnType)
	detached.Inspection[InspectionTableShape] = false
	if _, supported := capabilities.DDL(DDLChangeColumnType); !supported ||
		!capabilities.SupportsInspection(InspectionTableShape) {
		t.Fatal("capability descriptor exposed internal maps")
	}

	for name, mutate := range map[string]func(*CapabilitiesDescriptor){
		"zero identity": func(value *CapabilitiesDescriptor) { value.Identity = Identity{} },
		"invalid decimal": func(value *CapabilitiesDescriptor) {
			value.Decimal = DecimalCapabilities{Supported: true, MaxPrecision: 10, MaxScale: 11}
		},
		"invalid composite": func(value *CapabilitiesDescriptor) {
			value.Composite.MaxStructDepth = -1
		},
		"zero ddl guarantee": func(value *CapabilitiesDescriptor) {
			value.DDL[DDLAddColumn] = DDLCapability{MaxFieldPathDepth: 1}
		},
		"zero alter depth": func(value *CapabilitiesDescriptor) {
			value.DDL[DDLAddColumn] = DDLCapability{Guarantee: DDLGuaranteeAtomicPhysicalTable}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := cloneCapabilitiesDescriptor(testCapabilitiesDescriptor(t, "duckdb", "1.4.4"))
			mutate(&candidate)
			_, err := NewCapabilities(candidate)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}
}

func TestTableMutationOwnsSchemaAndCanonicalizesFieldPath(t *testing.T) {
	target := testTarget()
	before := []domain.Field{{
		Name: "Payload", Type: "STRUCT", Fields: []domain.Field{{Name: "Amount", Type: "NUMERIC"}},
	}}
	after := domain.CloneFields(before)
	after[0].Fields[0].Precision = int64Pointer(37)
	after[0].Fields[0].Scale = int64Pointer(8)
	path := []string{"payload", "AMOUNT"}
	mutation := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationChangeColumnType, Target: target, BeforeSchema: before, AfterSchema: after,
		FieldChanges: []FieldChangeDescriptor{{
			Path: path, Before: before[0].Fields[0], After: after[0].Fields[0],
		}},
		CorrelationID: "schema-change-2", ExpectedGeneration: 1, Generation: 2,
	})
	fingerprint := mutation.LogicalFingerprint()

	path[0] = "changed"
	before[0].Fields[0].Name = "changed"
	after[0].Fields[0].Precision = int64Pointer(1)
	change := mutation.FieldChanges()[0]
	if got := change.Path(); len(got) != 2 || got[0] != "Payload" || got[1] != "Amount" {
		t.Fatalf("canonical path = %#v", got)
	}
	detached := mutation.AfterSchema()
	*detached[0].Fields[0].Precision = 1
	if mutation.LogicalFingerprint() != fingerprint || *mutation.AfterSchema()[0].Fields[0].Precision != 37 {
		t.Fatal("mutation retained mutable schema input or exposed mutable output")
	}

	caseVariant := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationChangeColumnType, Target: target, BeforeSchema: mutation.BeforeSchema(),
		AfterSchema: mutation.AfterSchema(), FieldChanges: fieldChangeDescriptors(mutation),
		CorrelationID: "schema-change-2", ExpectedGeneration: 1, Generation: 2,
	})
	if caseVariant.LogicalFingerprint() != fingerprint {
		t.Fatal("canonical field path produced a different logical fingerprint")
	}
	if mutation.GenerationMarkerFingerprint() != caseVariant.GenerationMarkerFingerprint() {
		t.Fatal("equivalent mutations produced different generation markers")
	}

	nextGeneration := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationChangeColumnType, Target: target, BeforeSchema: mutation.BeforeSchema(),
		AfterSchema: mutation.AfterSchema(), FieldChanges: fieldChangeDescriptors(mutation),
		CorrelationID: "schema-change-2", ExpectedGeneration: 2, Generation: 3,
	})
	if nextGeneration.LogicalFingerprint() == fingerprint ||
		nextGeneration.GenerationMarkerFingerprint() == mutation.GenerationMarkerFingerprint() {
		t.Fatal("logical mutation fingerprint omitted its physical generation")
	}
}

func TestTableMutationRequiresOneExactTypedDelta(t *testing.T) {
	target := testTarget()
	before := []domain.Field{{Name: "id", Type: "INT64"}, {Name: "payload", Type: "STRING"}}
	hiddenTypeChange := []domain.Field{
		{Name: "id", Type: "STRING"}, {Name: "payload", Type: "STRING"}, {Name: "created_at", Type: "TIMESTAMP"},
	}
	_, err := NewTableMutation(TableMutationDescriptor{
		Kind: TableMutationAddColumn, Target: target, BeforeSchema: before, AfterSchema: hiddenTypeChange,
		FieldChanges:  []FieldChangeDescriptor{{Path: []string{"created_at"}, After: hiddenTypeChange[2]}},
		CorrelationID: "add-2", ExpectedGeneration: 1, Generation: 2,
	})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)

	_, err = NewTableMutation(TableMutationDescriptor{
		Kind: TableMutationDropColumn, Target: target, BeforeSchema: before,
		AfterSchema: []domain.Field{{Name: "id", Type: "INT64"}},
		FieldChanges: []FieldChangeDescriptor{
			{Path: []string{"payload"}, Before: before[1]},
			{Path: []string{"id"}, Before: before[0]},
		},
		CorrelationID: "drop-2", ExpectedGeneration: 1, Generation: 2,
	})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)

	for name, descriptor := range map[string]TableMutationDescriptor{
		"zero": {},
		"missing correlation": {
			Kind: TableMutationCreate, Target: target, AfterSchema: []domain.Field{{Name: "id", Type: "INT64"}}, Generation: 1,
		},
		"generation did not advance": {
			Kind: TableMutationCreate, Target: target, AfterSchema: []domain.Field{{Name: "id", Type: "INT64"}},
			CorrelationID: "create", ExpectedGeneration: 1, Generation: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewTableMutation(descriptor)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}
}

func TestPlannerAppliesBoundsOnlyToDesiredSchema(t *testing.T) {
	descriptor := testCapabilitiesDescriptor(t, "small-decimal", "1")
	descriptor.Decimal = DecimalCapabilities{Supported: true, MaxPrecision: 20, MaxScale: 9}
	capabilities := mustCapabilities(t, descriptor)
	planner := mustPlanner(t, capabilities, &fakeAdapterPlanner{})

	precision38, scale9 := int64(38), int64(9)
	wideCreate := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationCreate, Target: testTarget(),
		AfterSchema:   []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: &precision38, Scale: &scale9}},
		CorrelationID: "create-wide", Generation: 1,
	})
	_, err := planner.PlanTableChange(context.Background(), wideCreate, testRequirements())
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "logical.decimal.precision")

	precision20 := int64(20)
	repair := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationChangeColumnType, Target: testTarget(),
		BeforeSchema: []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: &precision38, Scale: &scale9}},
		AfterSchema:  []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: &precision20, Scale: &scale9}},
		FieldChanges: []FieldChangeDescriptor{{
			Path:   []string{"amount"},
			Before: domain.Field{Name: "amount", Type: "NUMERIC", Precision: &precision38, Scale: &scale9},
			After:  domain.Field{Name: "amount", Type: "NUMERIC", Precision: &precision20, Scale: &scale9},
		}},
		CorrelationID: "repair-wide", ExpectedGeneration: 1, Generation: 2,
	})
	if _, err := planner.PlanTableChange(context.Background(), repair, testRequirements()); err != nil {
		t.Fatalf("repair mutation rejected because of legacy before schema: %v", err)
	}

	legacyGeographyDrop := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationDrop, Target: testTarget(),
		BeforeSchema:  []domain.Field{{Name: "location", Type: "GEOGRAPHY"}},
		CorrelationID: "drop-legacy", ExpectedGeneration: 2, Generation: 3,
	})
	if _, err := planner.PlanTableChange(context.Background(), legacyGeographyDrop, testRequirements()); err != nil {
		t.Fatalf("legacy unsupported table could not be dropped: %v", err)
	}
}

func TestProductPolicyErrorsStayDistinctFromMalformedSchema(t *testing.T) {
	precision39 := int64(39)
	for name, field := range map[string]domain.Field{
		"wide bignumeric": {Name: "amount", Type: "BIGNUMERIC", Precision: &precision39},
		"geography":       {Name: "location", Type: "GEOGRAPHY"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewTableMutation(TableMutationDescriptor{
				Kind: TableMutationCreate, Target: testTarget(), AfterSchema: []domain.Field{field},
				CorrelationID: "unsupported-create", Generation: 1,
			})
			assertPlanningAttribute(t, err, PlanningCodeUnsupported, "logical.schema.policy")
		})
	}

	_, err := NewTableMutation(TableMutationDescriptor{
		Kind: TableMutationCreate, Target: testTarget(),
		AfterSchema:   []domain.Field{{Name: "broken", Type: "NOT_A_TYPE"}},
		CorrelationID: "malformed-create", Generation: 1,
	})
	assertPlanningAttribute(t, err, PlanningCodeInvalidDescriptor, "logical.schema")
}

func TestPlannerEnforcesAlterScopeAndDDLGuarantee(t *testing.T) {
	mutation := nestedTypeChangeMutation(t)
	descriptor := testCapabilitiesDescriptor(t, "top-level", "1")
	descriptor.DDL[DDLChangeColumnType] = DDLCapability{
		Guarantee: DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 1,
	}
	planner := mustPlanner(t, mustCapabilities(t, descriptor), &fakeAdapterPlanner{})
	_, err := planner.PlanTableChange(context.Background(), mutation, testRequirements())
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "ddl.change-column-type.field-path-depth")

	descriptor = testCapabilitiesDescriptor(t, "statement-only", "1")
	descriptor.DDL[DDLChangeColumnType] = DDLCapability{
		Guarantee: DDLGuaranteeAtomicPhysicalStatement, MaxFieldPathDepth: 15,
	}
	planner = mustPlanner(t, mustCapabilities(t, descriptor), &fakeAdapterPlanner{})
	requirements := testRequirements()
	requirements.DDLGuarantee = DDLGuaranteeAtomicPhysicalTable
	_, err = planner.PlanTableChange(context.Background(), testTypeChangeMutation(t), requirements)
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "ddl.change-column-type.guarantee")

	descriptor = testCapabilitiesDescriptor(t, "table-atomic", "1")
	planner = mustPlanner(t, mustCapabilities(t, descriptor), &fakeAdapterPlanner{})
	if _, err := planner.PlanTableChange(context.Background(), testTypeChangeMutation(t), testRequirements()); err != nil {
		t.Fatalf("stronger DDL guarantee did not satisfy statement requirement: %v", err)
	}
}

func TestTablePlanCanonicalizesRequirementsAndBindsProof(t *testing.T) {
	descriptor := testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	descriptor.Transactions[TransactionScopeMultiTable] = true
	capabilities := mustCapabilities(t, descriptor)
	planner := mustPlanner(t, capabilities, &fakeAdapterPlanner{})
	mutation := testTypeChangeMutation(t)

	firstRequirements := PlanRequirements{
		Transactions:       []TransactionScope{TransactionScopeMultiTable, TransactionScopeSingleTable},
		AtomicReplacements: []AtomicReplacementScope{AtomicReplacementTable, AtomicReplacementPartition},
		Inspection:         []InspectionScope{InspectionTables, InspectionDatasets},
		DDLGuarantee:       DDLGuaranteeAtomicPhysicalStatement,
	}
	secondRequirements := PlanRequirements{
		Transactions:       []TransactionScope{TransactionScopeSingleTable, TransactionScopeMultiTable},
		AtomicReplacements: []AtomicReplacementScope{AtomicReplacementPartition, AtomicReplacementTable},
		Inspection:         []InspectionScope{InspectionDatasets, InspectionTables},
		DDLGuarantee:       DDLGuaranteeAtomicPhysicalStatement,
	}
	first, err := planner.PlanTableChange(context.Background(), mutation, firstRequirements)
	if err != nil {
		t.Fatal(err)
	}
	second, err := planner.PlanTableChange(context.Background(), mutation, secondRequirements)
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint() != second.Fingerprint() {
		t.Fatal("plan fingerprint depends on requirement set order")
	}
	firstRequirements.Transactions[0] = TransactionScopeSingleTable
	detached := first.Requirements()
	detached.Inspection[0] = InspectionTableShape
	if got := first.Requirements(); got.Transactions[0] != TransactionScopeMultiTable || got.Inspection[0] != InspectionDatasets {
		t.Fatal("table plan retained or exposed mutable requirement slices")
	}

	if err := planner.ValidateApplyStart(first, mutation, first.Proof().Before()); err != nil {
		t.Fatalf("valid apply start rejected: %v", err)
	}
	if err := planner.ValidateApplyResult(first, first.Proof().After()); err != nil {
		t.Fatalf("valid apply result rejected: %v", err)
	}
}

func TestTablePlanRejectsPlannerAndPhysicalStateDrift(t *testing.T) {
	capabilities := mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4"))
	planner := mustPlanner(t, capabilities, &fakeAdapterPlanner{})
	mutation := testTypeChangeMutation(t)
	plan, err := planner.PlanTableChange(context.Background(), mutation, testRequirements())
	if err != nil {
		t.Fatal(err)
	}

	driftedBefore := mustPhysicalState(t, PhysicalTableStateDescriptor{
		Target: mutation.Target(), Exists: true, Generation: mutation.ExpectedGeneration(),
		LogicalShapeFingerprint:  mutation.BeforeShapeFingerprint(),
		PhysicalShapeFingerprint: plan.Proof().Before().PhysicalShapeFingerprint(),
		MarkerFingerprint:        physicalFingerprint("different-previous-marker"),
		Provenance:               PhysicalStateManaged,
	})
	assertPlanningCode(t, planner.ValidateApplyStart(plan, mutation, driftedBefore), PlanningCodePhysicalStateDrift)

	driftedAfter := mustPhysicalState(t, PhysicalTableStateDescriptor{
		Target: mutation.Target(), Exists: true, Generation: mutation.Generation(),
		LogicalShapeFingerprint:  mutation.AfterShapeFingerprint(),
		PhysicalShapeFingerprint: plan.Proof().After().PhysicalShapeFingerprint(),
		MarkerFingerprint:        physicalFingerprint("different-after-marker"),
		Provenance:               PhysicalStateManaged,
	})
	assertPlanningCode(t, planner.ValidateApplyResult(plan, driftedAfter), PlanningCodePhysicalStateDrift)

	otherCapabilities := mustCapabilities(t, testCapabilitiesDescriptor(t, "sqlite", "3.51.0"))
	otherPlanner := mustPlanner(t, otherCapabilities, &fakeAdapterPlanner{})
	assertPlanningCode(t, otherPlanner.ValidateApplyStart(plan, mutation, plan.Proof().Before()), PlanningCodeEngineMismatch)

	driftedDescriptor := capabilities.Descriptor()
	driftedDescriptor.Composite.MaxListDepth++
	driftedPlanner := mustPlanner(t, mustCapabilities(t, driftedDescriptor), &fakeAdapterPlanner{})
	assertPlanningCode(t, driftedPlanner.ValidateApplyStart(plan, mutation, plan.Proof().Before()), PlanningCodeCapabilityDrift)

	for range 64 {
		sibling := mustPlanner(t, capabilities, &fakeAdapterPlanner{})
		assertPlanningCode(t, sibling.ValidateApplyStart(plan, mutation, plan.Proof().Before()), PlanningCodePlannerMismatch)
	}
}

func TestPhysicalStatesRepresentUnmanagedObjectsAndDropTombstones(t *testing.T) {
	mutation := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationCreate, Target: testTarget(),
		AfterSchema:   []domain.Field{{Name: "id", Type: "INT64"}},
		CorrelationID: "create-1", Generation: 1,
	})
	unmanaged := mustPhysicalState(t, PhysicalTableStateDescriptor{
		Target: mutation.Target(), Exists: true,
		LogicalShapeFingerprint:  mutation.AfterShapeFingerprint(),
		PhysicalShapeFingerprint: physicalFingerprint("unmanaged-shape"),
		Provenance:               PhysicalStateUnmanaged,
	})
	after := managedState(t, mutation.Target(), mutation.AfterSchema(), 1, mutation.GenerationMarkerFingerprint())
	proof := mustPlanProof(t, unmanaged, after, PlanStrategyCreateTable)
	adapter := &fakeAdapterPlanner{table: func(context.Context, TableMutation) (PlanProof, error) { return proof, nil }}
	planner := mustPlanner(t, mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4")), adapter)
	_, err := planner.PlanTableChange(context.Background(), mutation, testRequirements())
	assertPlanningCode(t, err, PlanningCodePhysicalStateDrift)
	adapter.table = nil

	drop := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationDrop, Target: testTarget(), BeforeSchema: []domain.Field{{Name: "id", Type: "INT64"}},
		CorrelationID: "drop-2", ExpectedGeneration: 1, Generation: 2,
	})
	dropPlan, err := planner.PlanTableChange(context.Background(), drop, testRequirements())
	if err != nil {
		t.Fatal(err)
	}
	tombstone := dropPlan.Proof().After()
	if tombstone.Provenance() != PhysicalStateTombstone || tombstone.Exists() || tombstone.Generation() != 2 ||
		tombstone.LogicalShapeFingerprint() != "" || tombstone.PhysicalShapeFingerprint() != "" ||
		tombstone.MarkerFingerprint() != drop.GenerationMarkerFingerprint() {
		t.Fatalf("drop result is not an inspectable tombstone: %#v", tombstone)
	}
}

func TestPhysicalStateAndPlanProofDescriptorsFailClosed(t *testing.T) {
	target := testTarget()
	shape := physicalFingerprint("shape")
	marker := physicalFingerprint("marker")
	for name, descriptor := range map[string]PhysicalTableStateDescriptor{
		"zero": {},
		"virgin exists": {
			Target: target, Exists: true, Provenance: PhysicalStateVirgin,
		},
		"unmanaged marker": {
			Target: target, Exists: true, LogicalShapeFingerprint: shape,
			PhysicalShapeFingerprint: shape, MarkerFingerprint: marker, Provenance: PhysicalStateUnmanaged,
		},
		"managed without marker": {
			Target: target, Exists: true, Generation: 1,
			LogicalShapeFingerprint: shape, PhysicalShapeFingerprint: shape, Provenance: PhysicalStateManaged,
		},
		"tombstone with physical shape": {
			Target: target, Generation: 1, PhysicalShapeFingerprint: shape,
			MarkerFingerprint: marker, Provenance: PhysicalStateTombstone,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewPhysicalTableState(descriptor)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}

	before := mustPhysicalState(t, PhysicalTableStateDescriptor{Target: target, Provenance: PhysicalStateVirgin})
	after := managedState(t, target, []domain.Field{{Name: "id", Type: "INT64"}}, 1, marker)
	_, err := NewPlanProof(PlanProofDescriptor{Before: before, After: after})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
	_, err = NewPlanProof(PlanProofDescriptor{Before: after, After: after, Strategy: PlanStrategyAlterInPlace})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
}

func TestPlanningErrorsRetainAdapterCauses(t *testing.T) {
	const secret = "PRINTABLE_SECRET_MARKER"
	descriptor := testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	descriptor.Transactions[TransactionScope(secret)] = true
	_, err := NewCapabilities(descriptor)
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
	if strings.Contains(err.Error(), secret) {
		t.Fatal("capability error echoed an unknown descriptor value")
	}

	capabilities := mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4"))
	adapterError := fmt.Errorf("%s: adapter failure", secret)
	adapter := &fakeAdapterPlanner{table: func(context.Context, TableMutation) (PlanProof, error) {
		return PlanProof{}, adapterError
	}}
	planner := mustPlanner(t, capabilities, adapter)
	_, err = planner.PlanTableChange(context.Background(), testTypeChangeMutation(t), testRequirements())
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "adapter.planning")
	if !strings.Contains(err.Error(), secret) || !errors.Is(err, adapterError) {
		t.Fatal("adapter cause was omitted from the planning error")
	}

	wrappedCancellation := fmt.Errorf("%s: %w", secret, context.Canceled)
	adapter.table = func(context.Context, TableMutation) (PlanProof, error) { return PlanProof{}, wrappedCancellation }
	_, err = planner.PlanTableChange(context.Background(), testTypeChangeMutation(t), testRequirements())
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "adapter.planning")
	if !strings.Contains(err.Error(), secret) || !errors.Is(err, context.Canceled) {
		t.Fatal("wrapped adapter cancellation was omitted from the planning error")
	}

	adapter.table = func(context.Context, TableMutation) (PlanProof, error) { return PlanProof{}, context.Canceled }
	_, err = planner.PlanTableChange(context.Background(), testTypeChangeMutation(t), testRequirements())
	if err != context.Canceled {
		t.Fatalf("direct cancellation = %v, want context.Canceled", err)
	}
}

func TestPlanRequirementsRejectUnknownDuplicateAndZeroValues(t *testing.T) {
	planner := mustPlanner(t, mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4")), &fakeAdapterPlanner{})
	mutation := testTypeChangeMutation(t)
	for name, requirements := range map[string]PlanRequirements{
		"zero guarantee": {},
		"unknown transaction": {
			Transactions: []TransactionScope{"cluster"}, DDLGuarantee: DDLGuaranteeAtomicPhysicalStatement,
		},
		"duplicate transaction": {
			Transactions: []TransactionScope{TransactionScopeSingleTable, TransactionScopeSingleTable},
			DDLGuarantee: DDLGuaranteeAtomicPhysicalStatement,
		},
		"unknown replacement": {
			AtomicReplacements: []AtomicReplacementScope{"row"}, DDLGuarantee: DDLGuaranteeAtomicPhysicalStatement,
		},
		"unknown inspection": {
			Inspection: []InspectionScope{"sql"}, DDLGuarantee: DDLGuaranteeAtomicPhysicalStatement,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := planner.PlanTableChange(context.Background(), mutation, requirements)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}
	_, err := NewPlanner(Capabilities{}, &fakeAdapterPlanner{})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
	_, err = NewPlanner(planner.Capabilities(), nil)
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
}

type fakeAdapterPlanner struct {
	table       func(context.Context, TableMutation) (PlanProof, error)
	replacement func(context.Context, DataReplacement) (PlanProof, error)
}

func (adapter *fakeAdapterPlanner) PlanTableMutation(ctx context.Context, mutation TableMutation) (PlanProof, error) {
	if adapter.table != nil {
		return adapter.table(ctx, mutation)
	}
	return proofForMutation(mutation)
}

func (adapter *fakeAdapterPlanner) PlanDataReplacement(
	ctx context.Context,
	replacement DataReplacement,
) (PlanProof, error) {
	if adapter.replacement != nil {
		return adapter.replacement(ctx, replacement)
	}
	return proofForReplacement(replacement)
}

func testCapabilitiesDescriptor(t *testing.T, id, version string) CapabilitiesDescriptor {
	t.Helper()
	identity, err := NewIdentity(id, version)
	if err != nil {
		t.Fatal(err)
	}
	return CapabilitiesDescriptor{
		Identity:  identity,
		Decimal:   DecimalCapabilities{Supported: true, MaxPrecision: 38, MaxScale: 38},
		Composite: CompositeCapabilities{MaxStructDepth: 15, MaxListDepth: 15},
		Transactions: map[TransactionScope]bool{
			TransactionScopeSingleTable: true,
		},
		AtomicReplacements: map[AtomicReplacementScope]bool{
			AtomicReplacementTable: true, AtomicReplacementPartition: true,
		},
		Inspection: map[InspectionScope]bool{
			InspectionDatasets: true, InspectionTables: true, InspectionTableShape: true,
		},
		DDL: map[DDLOperation]DDLCapability{
			DDLCreateTable: {Guarantee: DDLGuaranteeAtomicPhysicalTable},
			DDLDropTable:   {Guarantee: DDLGuaranteeAtomicPhysicalTable},
			DDLAddColumn: {
				Guarantee: DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 15,
			},
			DDLDropColumn: {
				Guarantee: DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 15,
			},
			DDLRenameColumn: {
				Guarantee: DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 15,
			},
			DDLChangeColumnType: {
				Guarantee: DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 15,
			},
		},
	}
}

func cloneCapabilitiesDescriptor(input CapabilitiesDescriptor) CapabilitiesDescriptor {
	result := CapabilitiesDescriptor{
		Identity: input.Identity, Decimal: input.Decimal, Composite: input.Composite,
		Transactions:       make(map[TransactionScope]bool, len(input.Transactions)),
		AtomicReplacements: make(map[AtomicReplacementScope]bool, len(input.AtomicReplacements)),
		Inspection:         make(map[InspectionScope]bool, len(input.Inspection)),
		DDL:                make(map[DDLOperation]DDLCapability, len(input.DDL)),
	}
	for key, value := range input.Transactions {
		result.Transactions[key] = value
	}
	for key, value := range input.AtomicReplacements {
		result.AtomicReplacements[key] = value
	}
	for key, value := range input.Inspection {
		result.Inspection[key] = value
	}
	for key, value := range input.DDL {
		result.DDL[key] = value
	}
	return result
}

func testRequirements() PlanRequirements {
	return PlanRequirements{
		Transactions: []TransactionScope{TransactionScopeSingleTable},
		Inspection:   []InspectionScope{InspectionTableShape},
		DDLGuarantee: DDLGuaranteeAtomicPhysicalStatement,
	}
}

func testTarget() domain.TableReference {
	return domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
}

func testTypeChangeMutation(t *testing.T) TableMutation {
	t.Helper()
	before := []domain.Field{{Name: "amount", Type: "NUMERIC"}}
	after := []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: int64Pointer(37), Scale: int64Pointer(8)}}
	return mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationChangeColumnType, Target: testTarget(), BeforeSchema: before, AfterSchema: after,
		FieldChanges:  []FieldChangeDescriptor{{Path: []string{"amount"}, Before: before[0], After: after[0]}},
		CorrelationID: "schema-change-2", ExpectedGeneration: 1, Generation: 2,
	})
}

func nestedTypeChangeMutation(t *testing.T) TableMutation {
	t.Helper()
	before := []domain.Field{{
		Name: "payload", Type: "STRUCT", Fields: []domain.Field{{Name: "amount", Type: "NUMERIC"}},
	}}
	after := domain.CloneFields(before)
	after[0].Fields[0].Precision = int64Pointer(37)
	after[0].Fields[0].Scale = int64Pointer(8)
	return mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationChangeColumnType, Target: testTarget(), BeforeSchema: before, AfterSchema: after,
		FieldChanges: []FieldChangeDescriptor{{
			Path: []string{"payload", "amount"}, Before: before[0].Fields[0], After: after[0].Fields[0],
		}},
		CorrelationID: "nested-change-2", ExpectedGeneration: 1, Generation: 2,
	})
}

func proofForMutation(mutation TableMutation) (PlanProof, error) {
	var before, after PhysicalTableState
	var err error
	switch mutation.Kind() {
	case TableMutationCreate:
		if mutation.ExpectedGeneration() == 0 {
			before, err = NewPhysicalTableState(PhysicalTableStateDescriptor{
				Target: mutation.Target(), Provenance: PhysicalStateVirgin,
			})
		} else {
			before, err = NewPhysicalTableState(PhysicalTableStateDescriptor{
				Target: mutation.Target(), Generation: mutation.ExpectedGeneration(),
				MarkerFingerprint: physicalFingerprint("previous-marker", fmt.Sprint(mutation.ExpectedGeneration())),
				Provenance:        PhysicalStateTombstone,
			})
		}
		if err != nil {
			return PlanProof{}, err
		}
		after, err = newManagedState(
			mutation.Target(), mutation.AfterSchema(), mutation.Generation(), mutation.GenerationMarkerFingerprint(),
		)
		if err != nil {
			return PlanProof{}, err
		}
		return NewPlanProof(PlanProofDescriptor{Before: before, After: after, Strategy: PlanStrategyCreateTable})
	case TableMutationDrop:
		before, err = newManagedState(
			mutation.Target(), mutation.BeforeSchema(), mutation.ExpectedGeneration(),
			physicalFingerprint("previous-marker", fmt.Sprint(mutation.ExpectedGeneration())),
		)
		if err != nil {
			return PlanProof{}, err
		}
		after, err = NewPhysicalTableState(PhysicalTableStateDescriptor{
			Target: mutation.Target(), Generation: mutation.Generation(),
			MarkerFingerprint: mutation.GenerationMarkerFingerprint(), Provenance: PhysicalStateTombstone,
		})
		if err != nil {
			return PlanProof{}, err
		}
		return NewPlanProof(PlanProofDescriptor{Before: before, After: after, Strategy: PlanStrategyDropTable})
	default:
		before, err = newManagedState(
			mutation.Target(), mutation.BeforeSchema(), mutation.ExpectedGeneration(),
			physicalFingerprint("previous-marker", fmt.Sprint(mutation.ExpectedGeneration())),
		)
		if err != nil {
			return PlanProof{}, err
		}
		after, err = newManagedState(
			mutation.Target(), mutation.AfterSchema(), mutation.Generation(), mutation.GenerationMarkerFingerprint(),
		)
		if err != nil {
			return PlanProof{}, err
		}
		return NewPlanProof(PlanProofDescriptor{Before: before, After: after, Strategy: PlanStrategyAlterInPlace})
	}
}

func fieldChangeDescriptors(mutation TableMutation) []FieldChangeDescriptor {
	changes := mutation.FieldChanges()
	result := make([]FieldChangeDescriptor, len(changes))
	for index, change := range changes {
		result[index] = FieldChangeDescriptor{Path: change.Path(), Before: change.Before(), After: change.After()}
	}
	return result
}

func mustCapabilities(t *testing.T, descriptor CapabilitiesDescriptor) Capabilities {
	t.Helper()
	capabilities, err := NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return capabilities
}

func mustPlanner(t *testing.T, capabilities Capabilities, adapter AdapterPlanner) *Planner {
	t.Helper()
	planner, err := NewPlanner(capabilities, adapter)
	if err != nil {
		t.Fatal(err)
	}
	return planner
}

func mustTableMutation(t *testing.T, descriptor TableMutationDescriptor) TableMutation {
	t.Helper()
	mutation, err := NewTableMutation(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return mutation
}

func mustPhysicalState(t *testing.T, descriptor PhysicalTableStateDescriptor) PhysicalTableState {
	t.Helper()
	state, err := NewPhysicalTableState(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func managedState(
	t *testing.T,
	target domain.TableReference,
	schema []domain.Field,
	generation uint64,
	marker string,
) PhysicalTableState {
	t.Helper()
	state, err := newManagedState(target, schema, generation, marker)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func newManagedState(
	target domain.TableReference,
	schema []domain.Field,
	generation uint64,
	marker string,
) (PhysicalTableState, error) {
	shape := logicalShapeFingerprint(schema)
	return NewPhysicalTableState(PhysicalTableStateDescriptor{
		Target: target, Exists: true, Generation: generation,
		LogicalShapeFingerprint: shape, PhysicalShapeFingerprint: physicalFingerprint("physical-shape", shape),
		MarkerFingerprint: marker, Provenance: PhysicalStateManaged,
	})
}

func mustPlanProof(t *testing.T, before, after PhysicalTableState, strategy PlanStrategy) PlanProof {
	t.Helper()
	proof, err := NewPlanProof(PlanProofDescriptor{Before: before, After: after, Strategy: strategy})
	if err != nil {
		t.Fatal(err)
	}
	return proof
}

func int64Pointer(value int64) *int64 { return &value }

func assertPlanningCode(t *testing.T, err error, code PlanningErrorCode) {
	t.Helper()
	var planningErr *PlanningError
	if !errors.As(err, &planningErr) {
		t.Fatalf("error = %v, want *PlanningError", err)
	}
	if planningErr.Code() != code {
		t.Fatalf("planning error code = %q, want %q", planningErr.Code(), code)
	}
}

func assertPlanningAttribute(t *testing.T, err error, code PlanningErrorCode, attribute string) {
	t.Helper()
	assertPlanningCode(t, err, code)
	var planningErr *PlanningError
	if !errors.As(err, &planningErr) || planningErr.Attribute() != attribute {
		t.Fatalf("planning error attribute = %q, want %q", planningErr.Attribute(), attribute)
	}
}
