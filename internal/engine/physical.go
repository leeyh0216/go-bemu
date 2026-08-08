package engine

import (
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type PlanStrategy string

const (
	PlanStrategyCreateTable       PlanStrategy = "create-table"
	PlanStrategyDropTable         PlanStrategy = "drop-table"
	PlanStrategyAlterInPlace      PlanStrategy = "alter-in-place"
	PlanStrategyRebuildTable      PlanStrategy = "rebuild-table"
	PlanStrategyReplaceTableData  PlanStrategy = "replace-table-data"
	PlanStrategyReplacePartitions PlanStrategy = "replace-partitions"
)

type PhysicalStateProvenance string

const (
	PhysicalStateVirgin    PhysicalStateProvenance = "virgin"
	PhysicalStateUnmanaged PhysicalStateProvenance = "unmanaged"
	PhysicalStateManaged   PhysicalStateProvenance = "managed"
	PhysicalStateTombstone PhysicalStateProvenance = "tombstone"
)

type PhysicalTableStateDescriptor struct {
	Target                   domain.TableReference
	Exists                   bool
	Generation               uint64
	LogicalShapeFingerprint  string
	PhysicalShapeFingerprint string
	MarkerFingerprint        string
	Provenance               PhysicalStateProvenance
}

// PhysicalTableState is an engine-neutral inspection result. Shape fingerprints
// and the correlation-derived marker describe the applied physical mutation.
// The adapter records Generation and its marker in the same transaction as the
// physical mutation.
type PhysicalTableState struct {
	target                   domain.TableReference
	exists                   bool
	generation               uint64
	logicalShapeFingerprint  string
	physicalShapeFingerprint string
	markerFingerprint        string
	provenance               PhysicalStateProvenance
}

func NewPhysicalTableState(descriptor PhysicalTableStateDescriptor) (PhysicalTableState, error) {
	if err := validateTableReference(descriptor.Target); err != nil {
		return PhysicalTableState{}, err
	}
	switch descriptor.Provenance {
	case PhysicalStateVirgin:
		if descriptor.Generation != 0 || descriptor.Exists || descriptor.LogicalShapeFingerprint != "" ||
			descriptor.PhysicalShapeFingerprint != "" || descriptor.MarkerFingerprint != "" {
			return PhysicalTableState{}, invalidPhysicalState("virgin state must be absent and fingerprint-free")
		}
	case PhysicalStateUnmanaged:
		if descriptor.Generation != 0 || !descriptor.Exists || !fingerprintLooksValid(descriptor.LogicalShapeFingerprint) ||
			!fingerprintLooksValid(descriptor.PhysicalShapeFingerprint) || descriptor.MarkerFingerprint != "" {
			return PhysicalTableState{}, invalidPhysicalState("unmanaged state must expose existing shape without a generation")
		}
	case PhysicalStateManaged:
		if descriptor.Generation == 0 || !descriptor.Exists || !fingerprintLooksValid(descriptor.LogicalShapeFingerprint) ||
			!fingerprintLooksValid(descriptor.PhysicalShapeFingerprint) || !fingerprintLooksValid(descriptor.MarkerFingerprint) {
			return PhysicalTableState{}, invalidPhysicalState("managed state must expose generation and existing shape")
		}
	case PhysicalStateTombstone:
		if descriptor.Generation == 0 || descriptor.Exists || descriptor.LogicalShapeFingerprint != "" ||
			descriptor.PhysicalShapeFingerprint != "" || !fingerprintLooksValid(descriptor.MarkerFingerprint) {
			return PhysicalTableState{}, invalidPhysicalState("tombstone must expose generation and metadata marker fingerprint")
		}
	default:
		return PhysicalTableState{}, invalidPhysicalState("physical state provenance is invalid")
	}
	return PhysicalTableState{
		target: descriptor.Target, exists: descriptor.Exists, generation: descriptor.Generation,
		logicalShapeFingerprint:  descriptor.LogicalShapeFingerprint,
		physicalShapeFingerprint: descriptor.PhysicalShapeFingerprint,
		markerFingerprint:        descriptor.MarkerFingerprint,
		provenance:               descriptor.Provenance,
	}, nil
}

func (state PhysicalTableState) Target() domain.TableReference { return state.target }
func (state PhysicalTableState) Exists() bool                  { return state.exists }
func (state PhysicalTableState) Generation() uint64            { return state.generation }
func (state PhysicalTableState) LogicalShapeFingerprint() string {
	return state.logicalShapeFingerprint
}
func (state PhysicalTableState) PhysicalShapeFingerprint() string {
	return state.physicalShapeFingerprint
}
func (state PhysicalTableState) MarkerFingerprint() string           { return state.markerFingerprint }
func (state PhysicalTableState) Provenance() PhysicalStateProvenance { return state.provenance }

func (state PhysicalTableState) validate() error {
	_, err := NewPhysicalTableState(PhysicalTableStateDescriptor{
		Target: state.target, Exists: state.exists, Generation: state.generation,
		LogicalShapeFingerprint: state.logicalShapeFingerprint, PhysicalShapeFingerprint: state.physicalShapeFingerprint,
		MarkerFingerprint: state.markerFingerprint, Provenance: state.provenance,
	})
	return err
}

func (state PhysicalTableState) same(other PhysicalTableState) bool {
	return state.target == other.target && state.exists == other.exists && state.generation == other.generation &&
		state.logicalShapeFingerprint == other.logicalShapeFingerprint &&
		state.physicalShapeFingerprint == other.physicalShapeFingerprint &&
		state.markerFingerprint == other.markerFingerprint && state.provenance == other.provenance
}

type PlanProofDescriptor struct {
	Before   PhysicalTableState
	After    PhysicalTableState
	Strategy PlanStrategy
}

// PlanProof is produced by an adapter inspection/planning pass and sealed into
// a runtime-issued plan. Apply must inspect inside its transaction and validate
// both Before and After through the issuing Planner.
type PlanProof struct {
	before   PhysicalTableState
	after    PhysicalTableState
	strategy PlanStrategy
}

func NewPlanProof(descriptor PlanProofDescriptor) (PlanProof, error) {
	if err := descriptor.Before.validate(); err != nil {
		return PlanProof{}, err
	}
	if err := descriptor.After.validate(); err != nil {
		return PlanProof{}, err
	}
	if descriptor.Before.target != descriptor.After.target {
		return PlanProof{}, invalidPlanProof("before and after targets differ")
	}
	if descriptor.After.generation <= descriptor.Before.generation {
		return PlanProof{}, invalidPlanProof("after generation must advance")
	}
	if !validPlanStrategy(descriptor.Strategy) {
		return PlanProof{}, invalidPlanProof("plan strategy is invalid")
	}
	return PlanProof{before: descriptor.Before, after: descriptor.After, strategy: descriptor.Strategy}, nil
}

func (proof PlanProof) Before() PhysicalTableState { return proof.before }
func (proof PlanProof) After() PhysicalTableState  { return proof.after }
func (proof PlanProof) Strategy() PlanStrategy     { return proof.strategy }

func (proof PlanProof) validate() error {
	_, err := NewPlanProof(PlanProofDescriptor{Before: proof.before, After: proof.after, Strategy: proof.strategy})
	return err
}

func validPlanStrategy(strategy PlanStrategy) bool {
	switch strategy {
	case PlanStrategyCreateTable, PlanStrategyDropTable, PlanStrategyAlterInPlace, PlanStrategyRebuildTable,
		PlanStrategyReplaceTableData, PlanStrategyReplacePartitions:
		return true
	default:
		return false
	}
}

func invalidPhysicalState(detail string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, "physical-inspection", "physical.state", detail, nil,
	)
}

func invalidPlanProof(detail string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, "plan-proof", "plan.proof", detail, nil,
	)
}

func physicalStateFingerprintDocument(state PhysicalTableState) any {
	return struct {
		ProjectID                string                  `json:"projectId"`
		DatasetID                string                  `json:"datasetId"`
		TableID                  string                  `json:"tableId"`
		Exists                   bool                    `json:"exists"`
		Generation               uint64                  `json:"generation"`
		LogicalShapeFingerprint  string                  `json:"logicalShapeFingerprint"`
		PhysicalShapeFingerprint string                  `json:"physicalShapeFingerprint"`
		MarkerFingerprint        string                  `json:"markerFingerprint"`
		Provenance               PhysicalStateProvenance `json:"provenance"`
	}{
		ProjectID: state.target.ProjectID, DatasetID: state.target.DatasetID, TableID: state.target.TableID,
		Exists: state.exists, Generation: state.generation,
		LogicalShapeFingerprint:  state.logicalShapeFingerprint,
		PhysicalShapeFingerprint: state.physicalShapeFingerprint,
		MarkerFingerprint:        state.markerFingerprint,
		Provenance:               state.provenance,
	}
}

// generationMarkerFingerprint is the engine-neutral marker BQEMU expects an
// adapter to record atomically with a physical mutation. Only the digest crosses
// the adapter boundary; the correlation identifier is never stored in a proof.
func generationMarkerFingerprint(target domain.TableReference, correlationID string, generation uint64) string {
	return fingerprintJSON(struct {
		ProjectID     string `json:"projectId"`
		DatasetID     string `json:"datasetId"`
		TableID       string `json:"tableId"`
		CorrelationID string `json:"correlationId"`
		Generation    uint64 `json:"generation"`
	}{
		ProjectID: target.ProjectID, DatasetID: target.DatasetID, TableID: target.TableID,
		CorrelationID: correlationID, Generation: generation,
	})
}

func planProofFingerprintDocument(proof PlanProof) any {
	return struct {
		Before   any          `json:"before"`
		After    any          `json:"after"`
		Strategy PlanStrategy `json:"strategy"`
	}{
		Before:   physicalStateFingerprintDocument(proof.before),
		After:    physicalStateFingerprintDocument(proof.after),
		Strategy: proof.strategy,
	}
}

func physicalFingerprint(label string, values ...string) string {
	return fingerprintJSON(struct {
		Label  string   `json:"label"`
		Values []string `json:"values"`
	}{Label: label, Values: values})
}

func unexpectedPhysicalState(phase string) error {
	return newPlanningError(
		PlanningCodePhysicalStateDrift, "validate-table-plan", "physical."+phase,
		fmt.Sprintf("physical table state differs from the sealed %s proof", phase), nil,
	)
}
