package engine

import (
	"context"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestDataReplacementIsImmutableAndDistinguishesCorrelations(t *testing.T) {
	schema := []domain.Field{{Name: "id", Type: "INT64"}}
	first := mustDataReplacement(t, DataReplacementDescriptor{
		Scope: DataReplacementTable, Target: testTarget(), Schema: schema,
		CorrelationID: "replace-a", ExpectedGeneration: 1, Generation: 2,
		SourceFingerprint: physicalFingerprint("source", "a"), ResultFingerprint: physicalFingerprint("result", "a"),
	})
	second := mustDataReplacement(t, DataReplacementDescriptor{
		Scope: DataReplacementTable, Target: testTarget(), Schema: schema,
		CorrelationID: "replace-b", ExpectedGeneration: 1, Generation: 2,
		SourceFingerprint: physicalFingerprint("source", "a"), ResultFingerprint: physicalFingerprint("result", "a"),
	})
	nextGeneration := mustDataReplacement(t, DataReplacementDescriptor{
		Scope: DataReplacementTable, Target: testTarget(), Schema: schema,
		CorrelationID: "replace-a", ExpectedGeneration: 2, Generation: 3,
		SourceFingerprint: physicalFingerprint("source", "a"), ResultFingerprint: physicalFingerprint("result", "a"),
	})
	differentResult := mustDataReplacement(t, DataReplacementDescriptor{
		Scope: DataReplacementTable, Target: testTarget(), Schema: schema,
		CorrelationID: "replace-a", ExpectedGeneration: 1, Generation: 2,
		SourceFingerprint: physicalFingerprint("source", "a"), ResultFingerprint: physicalFingerprint("result", "b"),
	})
	schema[0].Name = "changed"
	detached := first.Schema()
	detached[0].Name = "changed-again"
	if first.Schema()[0].Name != "id" {
		t.Fatal("data replacement retained or exposed mutable schema")
	}
	if first.LogicalFingerprint() == second.LogicalFingerprint() ||
		first.GenerationMarkerFingerprint() == second.GenerationMarkerFingerprint() {
		t.Fatal("same-schema replacements with different correlations were conflated")
	}
	if first.LogicalFingerprint() == nextGeneration.LogicalFingerprint() ||
		first.GenerationMarkerFingerprint() == nextGeneration.GenerationMarkerFingerprint() {
		t.Fatal("data replacement fingerprint omitted its physical generation")
	}
	if first.LogicalFingerprint() == differentResult.LogicalFingerprint() {
		t.Fatal("data replacement fingerprint omitted its result digest")
	}

	planner := mustPlanner(t, mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4")), &fakeAdapterPlanner{})
	firstPlan, err := planner.PlanDataReplacement(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	secondPlan, err := planner.PlanDataReplacement(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan.Fingerprint() == secondPlan.Fingerprint() {
		t.Fatal("same-schema replacement plans with different correlations were conflated")
	}
	if err := planner.ValidateDataReplacementStart(firstPlan, first, firstPlan.Proof().Before()); err != nil {
		t.Fatalf("valid replacement start rejected: %v", err)
	}
	if err := planner.ValidateDataReplacementResult(firstPlan, firstPlan.Proof().After()); err != nil {
		t.Fatalf("valid replacement result rejected: %v", err)
	}
}

func TestDataReplacementValidatesScopeDigestsAndCapability(t *testing.T) {
	base := DataReplacementDescriptor{
		Target: testTarget(), Schema: []domain.Field{{Name: "id", Type: "INT64"}},
		CorrelationID: "replace-2", ExpectedGeneration: 1, Generation: 2,
		SourceFingerprint: physicalFingerprint("source"), ResultFingerprint: physicalFingerprint("result"),
	}
	for name, mutate := range map[string]func(*DataReplacementDescriptor){
		"zero scope": func(value *DataReplacementDescriptor) {},
		"partition without selection": func(value *DataReplacementDescriptor) {
			value.Scope = DataReplacementPartitions
		},
		"table with selection": func(value *DataReplacementDescriptor) {
			value.Scope = DataReplacementTable
			value.SelectionFingerprint = physicalFingerprint("selection")
		},
		"missing source": func(value *DataReplacementDescriptor) {
			value.Scope = DataReplacementTable
			value.SourceFingerprint = ""
		},
		"missing result": func(value *DataReplacementDescriptor) {
			value.Scope = DataReplacementTable
			value.ResultFingerprint = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			descriptor := base
			mutate(&descriptor)
			_, err := NewDataReplacement(descriptor)
			assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
		})
	}

	partitionReplacement := mustDataReplacement(t, DataReplacementDescriptor{
		Scope: DataReplacementPartitions, Target: base.Target, Schema: base.Schema,
		CorrelationID: base.CorrelationID, ExpectedGeneration: base.ExpectedGeneration, Generation: base.Generation,
		SelectionFingerprint: physicalFingerprint("partition-selection"),
		SourceFingerprint:    base.SourceFingerprint, ResultFingerprint: base.ResultFingerprint,
	})
	descriptor := testCapabilitiesDescriptor(t, "no-partition-replace", "1")
	delete(descriptor.AtomicReplacements, AtomicReplacementPartition)
	adapterCalls := 0
	adapter := &fakeAdapterPlanner{replacement: func(context.Context, DataReplacement) (PlanProof, error) {
		adapterCalls++
		return PlanProof{}, nil
	}}
	planner := mustPlanner(t, mustCapabilities(t, descriptor), adapter)
	_, err := planner.PlanDataReplacement(context.Background(), partitionReplacement)
	assertPlanningAttribute(t, err, PlanningCodeUnsupported, "atomic-replacement.partition")
	if adapterCalls != 0 {
		t.Fatal("adapter ran after portable replacement capability rejection")
	}
}

func TestDataReplacementProofRequiresManagedStateAndExpectedMarker(t *testing.T) {
	replacement := mustDataReplacement(t, DataReplacementDescriptor{
		Scope: DataReplacementTable, Target: testTarget(), Schema: []domain.Field{{Name: "id", Type: "INT64"}},
		CorrelationID: "replace-2", ExpectedGeneration: 1, Generation: 2,
		SourceFingerprint: physicalFingerprint("source"), ResultFingerprint: physicalFingerprint("result"),
	})
	unmanaged := mustPhysicalState(t, PhysicalTableStateDescriptor{
		Target: replacement.Target(), Exists: true,
		LogicalShapeFingerprint:  replacement.ShapeFingerprint(),
		PhysicalShapeFingerprint: physicalFingerprint("unmanaged"), Provenance: PhysicalStateUnmanaged,
	})
	after := managedState(
		t, replacement.Target(), replacement.Schema(), replacement.Generation(), replacement.GenerationMarkerFingerprint(),
	)
	proof := mustPlanProof(t, unmanaged, after, PlanStrategyReplaceTableData)
	adapter := &fakeAdapterPlanner{replacement: func(context.Context, DataReplacement) (PlanProof, error) {
		return proof, nil
	}}
	planner := mustPlanner(t, mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4")), adapter)
	_, err := planner.PlanDataReplacement(context.Background(), replacement)
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)

	before := managedState(
		t, replacement.Target(), replacement.Schema(), replacement.ExpectedGeneration(), physicalFingerprint("previous-marker"),
	)
	wrongAfter := managedState(
		t, replacement.Target(), replacement.Schema(), replacement.Generation(), physicalFingerprint("wrong-current-marker"),
	)
	adapter.replacement = func(context.Context, DataReplacement) (PlanProof, error) {
		return NewPlanProof(PlanProofDescriptor{
			Before: before, After: wrongAfter, Strategy: PlanStrategyReplaceTableData,
		})
	}
	_, err = planner.PlanDataReplacement(context.Background(), replacement)
	assertPlanningCode(t, err, PlanningCodeInvalidDescriptor)
}

func TestDataReplacementApplyDetectsPhysicalMarkerDrift(t *testing.T) {
	replacement := mustDataReplacement(t, DataReplacementDescriptor{
		Scope: DataReplacementPartitions, Target: testTarget(), Schema: []domain.Field{{Name: "id", Type: "INT64"}},
		CorrelationID: "partition-replace-2", ExpectedGeneration: 1, Generation: 2,
		SelectionFingerprint: physicalFingerprint("selection"),
		SourceFingerprint:    physicalFingerprint("source"), ResultFingerprint: physicalFingerprint("result"),
	})
	planner := mustPlanner(t, mustCapabilities(t, testCapabilitiesDescriptor(t, "duckdb", "1.4.4")), &fakeAdapterPlanner{})
	plan, err := planner.PlanDataReplacement(context.Background(), replacement)
	if err != nil {
		t.Fatal(err)
	}
	drifted := managedState(
		t, replacement.Target(), replacement.Schema(), replacement.Generation(), physicalFingerprint("drifted-marker"),
	)
	assertPlanningCode(t, planner.ValidateDataReplacementResult(plan, drifted), PlanningCodePhysicalStateDrift)
}

func proofForReplacement(replacement DataReplacement) (PlanProof, error) {
	before, err := newManagedState(
		replacement.Target(), replacement.Schema(), replacement.ExpectedGeneration(),
		physicalFingerprint("previous-marker", replacement.CorrelationID()),
	)
	if err != nil {
		return PlanProof{}, err
	}
	after, err := newManagedState(
		replacement.Target(), replacement.Schema(), replacement.Generation(), replacement.GenerationMarkerFingerprint(),
	)
	if err != nil {
		return PlanProof{}, err
	}
	strategy := PlanStrategyReplaceTableData
	if replacement.Scope() == DataReplacementPartitions {
		strategy = PlanStrategyReplacePartitions
	}
	return NewPlanProof(PlanProofDescriptor{Before: before, After: after, Strategy: strategy})
}

func mustDataReplacement(t *testing.T, descriptor DataReplacementDescriptor) DataReplacement {
	t.Helper()
	replacement, err := NewDataReplacement(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return replacement
}
