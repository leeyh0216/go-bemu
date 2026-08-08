package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestCapabilitiesOwnsDeepCopies(t *testing.T) {
	descriptor := testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	capabilities, err := NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	originalFingerprint := capabilities.Fingerprint()
	descriptor.Transactions[TransactionScopeSingleTable] = false
	descriptor.AtomicReplacements[AtomicReplacementTable] = false
	descriptor.Inspection[InspectionTableShape] = false
	descriptor.DDL[DDLChangeColumnType] = DDLCapability{Atomicity: "corrupted", MaxFieldPathDepth: 15}
	if !capabilities.SupportsTransaction(TransactionScopeSingleTable) ||
		!capabilities.SupportsAtomicReplacement(AtomicReplacementTable) ||
		!capabilities.SupportsInspection(InspectionTableShape) {
		t.Fatal("mutable descriptor changed immutable capabilities")
	}
	if got, ok := capabilities.DDL(DDLChangeColumnType); !ok || got.Atomicity != DDLAtomicityTable || got.MaxFieldPathDepth != 15 {
		t.Fatalf("DDL capability = %v, %t", got, ok)
	}
	if capabilities.Fingerprint() != originalFingerprint || !fingerprintLooksValid(originalFingerprint) {
		t.Fatalf("capability fingerprint changed or is invalid: %q", capabilities.Fingerprint())
	}

	detached := capabilities.Descriptor()
	detached.DDL[DDLChangeColumnType] = DDLCapability{Atomicity: DDLAtomicityStatement, MaxFieldPathDepth: 1}
	delete(detached.Transactions, TransactionScopeSingleTable)
	if got, _ := capabilities.DDL(DDLChangeColumnType); got.Atomicity != DDLAtomicityTable || got.MaxFieldPathDepth != 15 ||
		!capabilities.SupportsTransaction(TransactionScopeSingleTable) {
		t.Fatal("detached descriptor mutated capabilities")
	}
}

func TestCapabilitiesRejectZeroAndInvalidDescriptors(t *testing.T) {
	valid := testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	tests := []struct {
		name       string
		descriptor CapabilitiesDescriptor
	}{
		{name: "zero", descriptor: CapabilitiesDescriptor{}},
		{name: "zero decimal", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) { value.Decimal.MaxPrecision = 0 })},
		{name: "scale above precision", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) { value.Decimal.MaxScale = 39 })},
		{name: "negative struct depth", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) { value.Composite.MaxStructDepth = -1 })},
		{name: "unknown transaction", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) { value.Transactions["cluster"] = true })},
		{name: "unknown replacement", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) { value.AtomicReplacements["row"] = true })},
		{name: "unknown inspection", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) { value.Inspection["sql"] = true })},
		{name: "unknown DDL", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) {
			value.DDL["execute-sql"] = DDLCapability{Atomicity: DDLAtomicityTable}
		})},
		{name: "invalid DDL atomicity", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) {
			value.DDL[DDLAddColumn] = DDLCapability{Atomicity: "database", MaxFieldPathDepth: 1}
		})},
		{name: "missing ALTER scope", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) {
			value.DDL[DDLAddColumn] = DDLCapability{Atomicity: DDLAtomicityTable}
		})},
		{name: "table DDL has field scope", descriptor: mutateCapabilities(valid, func(value *CapabilitiesDescriptor) {
			value.DDL[DDLCreateTable] = DDLCapability{Atomicity: DDLAtomicityTable, MaxFieldPathDepth: 1}
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewCapabilities(test.descriptor)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}
}

func TestIdentityRejectsWhitespaceAndControlCharacters(t *testing.T) {
	for _, version := range []string{"", " 1.4.4", "1.4.4 ", "1.4\t4", "1/4", "v1:secret"} {
		t.Run(version, func(t *testing.T) {
			_, err := NewIdentity("duckdb", version)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}
}

func TestTableMutationAndPlanAreDeepCopiedAndEngineBound(t *testing.T) {
	precision, scale := int64(38), int64(18)
	before := []domain.Field{
		{Name: "id", Type: "INT64"},
		{Name: "payload", Type: "STRUCT", Fields: []domain.Field{{
			Name: "amounts", Type: "BIGNUMERIC", Mode: "REPEATED", Precision: &precision, Scale: &scale,
		}}},
	}
	after := domain.CloneFields(before)
	after[1].Fields[0].Scale = pointerInt64(17)
	changes := []FieldChangeDescriptor{{
		Path: []string{"payload", "amounts"}, Before: before[1].Fields[0], After: after[1].Fields[0],
	}}
	mutation, err := NewTableMutation(TableMutationDescriptor{
		Kind:         TableMutationChangeColumnType,
		Target:       domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"},
		BeforeSchema: before, AfterSchema: after,
		FieldChanges: changes,
	})
	if err != nil {
		t.Fatal(err)
	}
	logicalFingerprint := mutation.LogicalFingerprint()
	before[1].Fields[0].Name = "corrupted"
	*after[1].Fields[0].Scale = 1
	changes[0].Path[1] = "corrupted"
	*changes[0].After.Scale = 2
	if got := mutation.BeforeSchema()[1].Fields[0].Name; got != "amounts" {
		t.Fatalf("mutation retained caller schema alias: %q", got)
	}
	if got := *mutation.AfterSchema()[1].Fields[0].Scale; got != 17 {
		t.Fatalf("mutation retained caller decimal pointer: %d", got)
	}
	if mutation.LogicalFingerprint() != logicalFingerprint || !fingerprintLooksValid(logicalFingerprint) {
		t.Fatal("logical fingerprint changed after caller mutation")
	}
	detachedAfter := mutation.AfterSchema()
	detachedAfter[1].Fields[0].Name = "also_corrupted"
	if mutation.AfterSchema()[1].Fields[0].Name != "amounts" {
		t.Fatal("schema getter did not return a deep copy")
	}
	detachedChanges := mutation.FieldChanges()
	detachedChanges[0].path[0] = "ignored"
	*detachedChanges[0].after.Scale = 3
	if mutation.FieldChanges()[0].Path()[0] != "payload" || *mutation.FieldChanges()[0].After().Scale != 17 {
		t.Fatal("field-change getter did not return a deep copy")
	}

	capabilities, err := NewCapabilities(testCapabilitiesDescriptor(t, "duckdb", "1.4.4"))
	if err != nil {
		t.Fatal(err)
	}
	requirements := PlanRequirements{
		Transactions: []TransactionScope{TransactionScopeSingleTable},
		Inspection:   []InspectionScope{InspectionTableShape},
	}
	validated := 0
	planner := mustPlanner(t, capabilities, TableMutationValidatorFunc(func(_ context.Context, got TableMutation) error {
		validated++
		if got.LogicalFingerprint() != mutation.LogicalFingerprint() {
			t.Fatal("adapter validator received another mutation")
		}
		return nil
	}))
	plan, err := planner.PlanTableChange(context.Background(), mutation, requirements)
	if err != nil {
		t.Fatal(err)
	}
	requirements.Transactions[0] = TransactionScopeMultiTable
	detachedRequirements := plan.Requirements()
	detachedRequirements.Inspection[0] = InspectionDatasets
	if plan.Requirements().Transactions[0] != TransactionScopeSingleTable ||
		plan.Requirements().Inspection[0] != InspectionTableShape {
		t.Fatal("table plan retained a mutable requirement slice")
	}
	if !fingerprintLooksValid(plan.Fingerprint()) || plan.LogicalFingerprint() != logicalFingerprint {
		t.Fatalf("invalid plan fingerprints: plan=%q logical=%q", plan.Fingerprint(), plan.LogicalFingerprint())
	}
	if validated != 1 {
		t.Fatalf("adapter validator calls = %d, want 1", validated)
	}
	if err := planner.ValidateBinding(plan, mutation); err != nil {
		t.Fatalf("validate original binding: %v", err)
	}

	otherEngine, err := NewCapabilities(testCapabilitiesDescriptor(t, "sqlite", "3.51.0"))
	if err != nil {
		t.Fatal(err)
	}
	otherPlanner := mustPlanner(t, otherEngine, acceptingMutationValidator())
	assertPlanningCode(t, otherPlanner.ValidateBinding(plan, mutation), PlanningCodeEngineMismatch)

	driftedDescriptor := capabilities.Descriptor()
	driftedDescriptor.Composite.MaxListDepth++
	driftedCapabilities, err := NewCapabilities(driftedDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	driftedPlanner := mustPlanner(t, driftedCapabilities, acceptingMutationValidator())
	assertPlanningCode(t, driftedPlanner.ValidateBinding(plan, mutation), PlanningCodeCapabilityDrift)
	siblingPlanner := mustPlanner(t, capabilities, acceptingMutationValidator())
	assertPlanningCode(t, siblingPlanner.ValidateBinding(plan, mutation), PlanningCodePlannerMismatch)

	staleAfter := mutation.AfterSchema()
	staleBeforeField := mutation.BeforeSchema()[1].Fields[0]
	staleAfter[1].Fields[0].Scale = pointerInt64(16)
	staleMutation, err := NewTableMutation(TableMutationDescriptor{
		Kind: mutation.Kind(), Target: mutation.Target(), BeforeSchema: mutation.BeforeSchema(), AfterSchema: staleAfter,
		FieldChanges: []FieldChangeDescriptor{{
			Path: []string{"payload", "amounts"}, Before: staleBeforeField, After: staleAfter[1].Fields[0],
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertPlanningCode(t, planner.ValidateBinding(plan, staleMutation), PlanningCodeMutationMismatch)
}

func TestTablePlanRejectsUnsupportedCapabilitiesBeforeCreation(t *testing.T) {
	descriptor := testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	delete(descriptor.DDL, DDLChangeColumnType)
	capabilities, err := NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	mutation := testTypeChangeMutation(t)
	planner := mustPlanner(t, capabilities, acceptingMutationValidator())
	_, err = planner.PlanTableChange(context.Background(), mutation, PlanRequirements{})
	assertPlanningCode(t, err, PlanningCodeUnsupported)
	var planningErr *PlanningError
	if !errors.As(err, &planningErr) || planningErr.Attribute() != "ddl.change-column-type" {
		t.Fatalf("planning diagnostic = %#v", planningErr)
	}

	descriptor = testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	descriptor.AtomicReplacements = map[AtomicReplacementScope]bool{}
	capabilities, err = NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := NewTableMutation(TableMutationDescriptor{
		Kind: TableMutationReplace, Target: mutation.Target(),
		BeforeSchema: mutation.BeforeSchema(), AfterSchema: mutation.AfterSchema(),
	})
	if err != nil {
		t.Fatal(err)
	}
	planner = mustPlanner(t, capabilities, acceptingMutationValidator())
	_, err = planner.PlanTableChange(context.Background(), replacement, PlanRequirements{})
	assertPlanningCode(t, err, PlanningCodeUnsupported)
}

func TestTableMutationKindMustExactlyMatchTypedFieldDelta(t *testing.T) {
	target := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	before := []domain.Field{{Name: "id", Type: "INT64"}, {Name: "payload", Type: "STRING"}}
	afterWithHiddenTypeChange := []domain.Field{
		{Name: "id", Type: "STRING"}, {Name: "payload", Type: "STRING"}, {Name: "created_at", Type: "TIMESTAMP"},
	}
	_, err := NewTableMutation(TableMutationDescriptor{
		Kind: TableMutationAddColumn, Target: target, BeforeSchema: before, AfterSchema: afterWithHiddenTypeChange,
		FieldChanges: []FieldChangeDescriptor{{
			Path: []string{"created_at"}, After: afterWithHiddenTypeChange[2],
		}},
	})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)

	afterWithHiddenDrop := []domain.Field{{Name: "id", Type: "INT64"}, {Name: "created_at", Type: "TIMESTAMP"}}
	_, err = NewTableMutation(TableMutationDescriptor{
		Kind: TableMutationAddColumn, Target: target, BeforeSchema: before, AfterSchema: afterWithHiddenDrop,
		FieldChanges: []FieldChangeDescriptor{{
			Path: []string{"created_at"}, After: afterWithHiddenDrop[1],
		}},
	})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)

	_, err = NewTableMutation(TableMutationDescriptor{
		Kind: TableMutationDropColumn, Target: target, BeforeSchema: before,
		AfterSchema: []domain.Field{{Name: "id", Type: "INT64"}},
		FieldChanges: []FieldChangeDescriptor{
			{Path: []string{"payload"}, Before: before[1]},
			{Path: []string{"id"}, Before: before[0]},
		},
	})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
}

func TestLogicalAndPlanFingerprintsCanonicalizeSetAndPathInputs(t *testing.T) {
	target := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	before := []domain.Field{{Name: "Payload", Type: "STRUCT", Fields: []domain.Field{{Name: "Amount", Type: "NUMERIC"}}}}
	after := domain.CloneFields(before)
	after[0].Fields[0].Precision = pointerInt64(37)
	after[0].Fields[0].Scale = pointerInt64(8)
	mutation := func(path []string) TableMutation {
		return mustTableMutation(t, TableMutationDescriptor{
			Kind: TableMutationChangeColumnType, Target: target, BeforeSchema: before, AfterSchema: after,
			FieldChanges: []FieldChangeDescriptor{{
				Path: path, Before: before[0].Fields[0], After: after[0].Fields[0],
			}},
		})
	}
	canonical := mutation([]string{"Payload", "Amount"})
	caseVariant := mutation([]string{"payload", "AMOUNT"})
	if canonical.LogicalFingerprint() != caseVariant.LogicalFingerprint() ||
		caseVariant.FieldChanges()[0].Path()[0] != "Payload" || caseVariant.FieldChanges()[0].Path()[1] != "Amount" {
		t.Fatal("logical fingerprint or stored path depends on caller path casing")
	}

	capabilityDescriptor := testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	capabilityDescriptor.Transactions[TransactionScopeMultiTable] = true
	planner := mustPlanner(t, mustCapabilities(t, capabilityDescriptor), acceptingMutationValidator())
	first, err := planner.PlanTableChange(context.Background(), canonical, PlanRequirements{
		Transactions:       []TransactionScope{TransactionScopeMultiTable, TransactionScopeSingleTable},
		AtomicReplacements: []AtomicReplacementScope{AtomicReplacementTable, AtomicReplacementPartition},
		Inspection:         []InspectionScope{InspectionTables, InspectionDatasets},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := planner.PlanTableChange(context.Background(), caseVariant, PlanRequirements{
		Transactions:       []TransactionScope{TransactionScopeSingleTable, TransactionScopeMultiTable},
		AtomicReplacements: []AtomicReplacementScope{AtomicReplacementPartition, AtomicReplacementTable},
		Inspection:         []InspectionScope{InspectionDatasets, InspectionTables},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint() != second.Fingerprint() {
		t.Fatal("plan fingerprint depends on requirement set order")
	}
}

func TestPlanningErrorsDoNotEchoUnknownDescriptorValues(t *testing.T) {
	const secret = "PRINTABLE_SECRET_MARKER"
	descriptor := testCapabilitiesDescriptor(t, "duckdb", "1.4.4")
	descriptor.Transactions[TransactionScope(secret)] = true
	_, err := NewCapabilities(descriptor)
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
	if strings.Contains(err.Error(), secret) {
		t.Fatal("capability planning error echoed an unknown descriptor value")
	}

	capabilities := mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4"))
	planner := mustPlanner(t, capabilities, acceptingMutationValidator())
	_, err = planner.PlanTableChange(context.Background(), testTypeChangeMutation(t), PlanRequirements{
		Inspection: []InspectionScope{InspectionScope(secret)},
	})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
	if strings.Contains(err.Error(), secret) {
		t.Fatal("requirement planning error echoed an unknown descriptor value")
	}
}

func TestPlannerEnforcesRecursiveLogicalBoundsAndAlterScope(t *testing.T) {
	target := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	precision, scale := int64(38), int64(9)
	decimalCreate := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationCreate, Target: target,
		AfterSchema: []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: &precision, Scale: &scale}},
	})
	descriptor := testCapabilitiesDescriptor(t, "small-decimal", "1")
	descriptor.Decimal.MaxPrecision = 20
	descriptor.Decimal.MaxScale = 9
	adapterValidationCalls := 0
	planner := mustPlanner(t, mustCapabilities(t, descriptor), TableMutationValidatorFunc(func(context.Context, TableMutation) error {
		adapterValidationCalls++
		return nil
	}))
	_, err := planner.PlanTableChange(context.Background(), decimalCreate, PlanRequirements{})
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "logical.decimal.precision")
	if adapterValidationCalls != 0 {
		t.Fatal("adapter validator ran after portable capability rejection")
	}

	descriptor = testCapabilitiesDescriptor(t, "no-decimal", "1")
	descriptor.Decimal = DecimalCapabilities{}
	planner = mustPlanner(t, mustCapabilities(t, descriptor), acceptingMutationValidator())
	_, err = planner.PlanTableChange(context.Background(), decimalCreate, PlanRequirements{})
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "logical.decimal")

	compositeCreate := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationCreate, Target: target,
		AfterSchema: []domain.Field{{Name: "outer", Type: "STRUCT", Fields: []domain.Field{{
			Name: "inner", Type: "STRUCT", Fields: []domain.Field{{Name: "values", Type: "STRING", Mode: "REPEATED"}},
		}}}},
	})
	descriptor = testCapabilitiesDescriptor(t, "shallow", "1")
	descriptor.Composite = CompositeCapabilities{MaxStructDepth: 1, MaxListDepth: 1}
	planner = mustPlanner(t, mustCapabilities(t, descriptor), acceptingMutationValidator())
	_, err = planner.PlanTableChange(context.Background(), compositeCreate, PlanRequirements{})
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "logical.struct.depth")
	descriptor.Composite = CompositeCapabilities{MaxStructDepth: 2, MaxListDepth: 0}
	planner = mustPlanner(t, mustCapabilities(t, descriptor), acceptingMutationValidator())
	_, err = planner.PlanTableChange(context.Background(), compositeCreate, PlanRequirements{})
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "logical.list.depth")

	nestedBefore := []domain.Field{{Name: "payload", Type: "STRUCT", Fields: []domain.Field{{Name: "amount", Type: "NUMERIC"}}}}
	nestedAfter := domain.CloneFields(nestedBefore)
	nestedAfter[0].Fields[0].Precision = pointerInt64(37)
	nestedAfter[0].Fields[0].Scale = pointerInt64(8)
	nestedChange := mustTableMutation(t, TableMutationDescriptor{
		Kind: TableMutationChangeColumnType, Target: target, BeforeSchema: nestedBefore, AfterSchema: nestedAfter,
		FieldChanges: []FieldChangeDescriptor{{
			Path: []string{"payload", "amount"}, Before: nestedBefore[0].Fields[0], After: nestedAfter[0].Fields[0],
		}},
	})
	descriptor = testCapabilitiesDescriptor(t, "top-level-ddl", "1")
	descriptor.DDL[DDLChangeColumnType] = DDLCapability{Atomicity: DDLAtomicityTable, MaxFieldPathDepth: 1}
	planner = mustPlanner(t, mustCapabilities(t, descriptor), acceptingMutationValidator())
	_, err = planner.PlanTableChange(context.Background(), nestedChange, PlanRequirements{})
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "ddl.change-column-type.field-path-depth")
}

func TestPlannerRequiresAdapterValidationAndPreservesProvenance(t *testing.T) {
	capabilities := mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4"))
	mutation := testTypeChangeMutation(t)
	adapterErr := errors.New("adapter-specific unsupported shape")
	planner := mustPlanner(t, capabilities, TableMutationValidatorFunc(func(context.Context, TableMutation) error {
		return adapterErr
	}))
	_, err := planner.PlanTableChange(context.Background(), mutation, PlanRequirements{})
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "adapter.validation")
	if !errors.Is(err, adapterErr) {
		t.Fatal("adapter validation cause was not retained")
	}

	issuer := mustPlanner(t, capabilities, acceptingMutationValidator())
	plan, err := issuer.PlanTableChange(context.Background(), mutation, PlanRequirements{})
	if err != nil {
		t.Fatal(err)
	}
	for range 32 {
		sibling := mustPlanner(t, capabilities, acceptingMutationValidator())
		assertPlanningCode(t, sibling.ValidateBinding(plan, mutation), PlanningCodePlannerMismatch)
	}
}

func TestTableMutationAndPlanRejectZeroOrAmbiguousDescriptors(t *testing.T) {
	valid := testTypeChangeMutation(t)
	for name, descriptor := range map[string]TableMutationDescriptor{
		"zero":               {},
		"missing target":     {Kind: TableMutationCreate, AfterSchema: valid.AfterSchema()},
		"create with before": {Kind: TableMutationCreate, Target: valid.Target(), BeforeSchema: valid.BeforeSchema(), AfterSchema: valid.AfterSchema()},
		"drop with after":    {Kind: TableMutationDrop, Target: valid.Target(), BeforeSchema: valid.BeforeSchema(), AfterSchema: valid.AfterSchema()},
		"unchanged update": {
			Kind: TableMutationChangeColumnType, Target: valid.Target(),
			BeforeSchema: valid.BeforeSchema(), AfterSchema: valid.BeforeSchema(), FieldChanges: fieldChangeDescriptors(valid),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewTableMutation(descriptor)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}

	capabilities, err := NewCapabilities(testCapabilitiesDescriptor(t, "duckdb", "1.4.4"))
	if err != nil {
		t.Fatal(err)
	}
	planner := mustPlanner(t, capabilities, acceptingMutationValidator())
	for name, requirements := range map[string]PlanRequirements{
		"unknown transaction":   {Transactions: []TransactionScope{"cluster"}},
		"duplicate transaction": {Transactions: []TransactionScope{TransactionScopeSingleTable, TransactionScopeSingleTable}},
		"unknown replacement":   {AtomicReplacements: []AtomicReplacementScope{"row"}},
		"unknown inspection":    {Inspection: []InspectionScope{"sql"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := planner.PlanTableChange(context.Background(), valid, requirements)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}
	_, err = NewPlanner(Capabilities{}, acceptingMutationValidator())
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
	_, err = NewPlanner(capabilities, nil)
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
	_, err = planner.PlanTableChange(context.Background(), TableMutation{}, PlanRequirements{})
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
	assertPlanningCode(t, planner.ValidateBinding(TablePlan{}, valid), PlanningCodeInvalidDescriptor)
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
			DDLCreateTable: {Atomicity: DDLAtomicityStatement}, DDLDropTable: {Atomicity: DDLAtomicityStatement},
			DDLAddColumn:        {Atomicity: DDLAtomicityTable, MaxFieldPathDepth: 15},
			DDLDropColumn:       {Atomicity: DDLAtomicityTable, MaxFieldPathDepth: 15},
			DDLRenameColumn:     {Atomicity: DDLAtomicityTable, MaxFieldPathDepth: 15},
			DDLChangeColumnType: {Atomicity: DDLAtomicityTable, MaxFieldPathDepth: 15},
		},
	}
}

func mustCapabilities(t *testing.T, descriptor CapabilitiesDescriptor) Capabilities {
	t.Helper()
	capabilities, err := NewCapabilities(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return capabilities
}

func mutateCapabilities(input CapabilitiesDescriptor, mutate func(*CapabilitiesDescriptor)) CapabilitiesDescriptor {
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
	mutate(&result)
	return result
}

func testTypeChangeMutation(t *testing.T) TableMutation {
	t.Helper()
	before := []domain.Field{{Name: "amount", Type: "NUMERIC"}}
	precision, scale := int64(37), int64(8)
	after := []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: &precision, Scale: &scale}}
	mutation, err := NewTableMutation(TableMutationDescriptor{
		Kind:         TableMutationChangeColumnType,
		Target:       domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"},
		BeforeSchema: before, AfterSchema: after,
		FieldChanges: []FieldChangeDescriptor{{Path: []string{"amount"}, Before: before[0], After: after[0]}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return mutation
}

func mustTableMutation(t *testing.T, descriptor TableMutationDescriptor) TableMutation {
	t.Helper()
	mutation, err := NewTableMutation(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return mutation
}

func pointerInt64(value int64) *int64 { return &value }

func fieldChangeDescriptors(mutation TableMutation) []FieldChangeDescriptor {
	changes := mutation.FieldChanges()
	result := make([]FieldChangeDescriptor, len(changes))
	for index, change := range changes {
		result[index] = FieldChangeDescriptor{Path: change.Path(), Before: change.Before(), After: change.After()}
	}
	return result
}

func acceptingMutationValidator() TableMutationValidator {
	return TableMutationValidatorFunc(func(context.Context, TableMutation) error { return nil })
}

func mustPlanner(t *testing.T, capabilities Capabilities, validator TableMutationValidator) *Planner {
	t.Helper()
	planner, err := NewPlanner(capabilities, validator)
	if err != nil {
		t.Fatal(err)
	}
	return planner
}

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
