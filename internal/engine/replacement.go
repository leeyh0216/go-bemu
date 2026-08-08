package engine

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type DataReplacementScope string

const (
	DataReplacementTable      DataReplacementScope = "table"
	DataReplacementPartitions DataReplacementScope = "partitions"
)

type DataReplacementDescriptor struct {
	Scope                DataReplacementScope
	Target               domain.TableReference
	Schema               []domain.Field
	CorrelationID        string
	ExpectedGeneration   uint64
	Generation           uint64
	SelectionFingerprint string
	SourceFingerprint    string
	ResultFingerprint    string
}

// DataReplacement distinguishes same-schema data generations without treating
// data replacement as schema DDL. Partition selection is digest-only.
type DataReplacement struct {
	scope                DataReplacementScope
	target               domain.TableReference
	schema               []domain.Field
	correlationID        string
	expectedGeneration   uint64
	generation           uint64
	selectionFingerprint string
	sourceFingerprint    string
	resultFingerprint    string
	logicalFingerprint   string
}

func NewDataReplacement(descriptor DataReplacementDescriptor) (DataReplacement, error) {
	if descriptor.Scope != DataReplacementTable && descriptor.Scope != DataReplacementPartitions {
		return DataReplacement{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "data-replacement", "replacement.scope", "replacement scope is invalid", nil,
		)
	}
	if err := validateTableReference(descriptor.Target); err != nil {
		return DataReplacement{}, err
	}
	if err := validateCorrelation(
		descriptor.CorrelationID, descriptor.ExpectedGeneration, descriptor.Generation, "data-replacement",
	); err != nil {
		return DataReplacement{}, err
	}
	schema := domain.CloneFields(descriptor.Schema)
	if err := validateLogicalTableSchema(descriptor.Target, schema); err != nil {
		return DataReplacement{}, classifyLogicalSchemaError("data-replacement", err)
	}
	if descriptor.Scope == DataReplacementPartitions {
		if !fingerprintLooksValid(descriptor.SelectionFingerprint) {
			return DataReplacement{}, newPlanningError(
				PlanningCodeInvalidDescriptor, "data-replacement", "replacement.selection-fingerprint",
				"partition selection fingerprint is invalid", nil,
			)
		}
	} else if descriptor.SelectionFingerprint != "" {
		return DataReplacement{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "data-replacement", "replacement.selection-fingerprint",
			"table replacement cannot carry partition selection", nil,
		)
	}
	if !fingerprintLooksValid(descriptor.SourceFingerprint) || !fingerprintLooksValid(descriptor.ResultFingerprint) {
		return DataReplacement{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "data-replacement", "replacement.data-fingerprint",
			"source and result fingerprints are required", nil,
		)
	}
	replacement := DataReplacement{
		scope: descriptor.Scope, target: descriptor.Target, schema: schema,
		correlationID: descriptor.CorrelationID, expectedGeneration: descriptor.ExpectedGeneration,
		generation: descriptor.Generation, selectionFingerprint: descriptor.SelectionFingerprint,
		sourceFingerprint: descriptor.SourceFingerprint, resultFingerprint: descriptor.ResultFingerprint,
	}
	replacement.logicalFingerprint = dataReplacementFingerprint(replacement)
	return replacement, nil
}

func (replacement DataReplacement) Scope() DataReplacementScope   { return replacement.scope }
func (replacement DataReplacement) Target() domain.TableReference { return replacement.target }
func (replacement DataReplacement) Schema() []domain.Field {
	return domain.CloneFields(replacement.schema)
}
func (replacement DataReplacement) CorrelationID() string      { return replacement.correlationID }
func (replacement DataReplacement) ExpectedGeneration() uint64 { return replacement.expectedGeneration }
func (replacement DataReplacement) Generation() uint64         { return replacement.generation }
func (replacement DataReplacement) GenerationMarkerFingerprint() string {
	return generationMarkerFingerprint(replacement.target, replacement.correlationID, replacement.generation)
}
func (replacement DataReplacement) SelectionFingerprint() string {
	return replacement.selectionFingerprint
}
func (replacement DataReplacement) SourceFingerprint() string  { return replacement.sourceFingerprint }
func (replacement DataReplacement) ResultFingerprint() string  { return replacement.resultFingerprint }
func (replacement DataReplacement) LogicalFingerprint() string { return replacement.logicalFingerprint }
func (replacement DataReplacement) ShapeFingerprint() string {
	return logicalShapeFingerprint(replacement.schema)
}

func (replacement DataReplacement) validate() error {
	if replacement.logicalFingerprint == "" || replacement.logicalFingerprint != dataReplacementFingerprint(replacement) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "data-replacement", "replacement.fingerprint",
			"zero or inconsistent data replacement", nil,
		)
	}
	return nil
}

type DataReplacementPlan struct {
	engineIdentity        Identity
	capabilityFingerprint string
	replacement           DataReplacement
	proof                 PlanProof
	planFingerprint       string
	issuer                *planIssuer
}

func (plan DataReplacementPlan) EngineIdentity() Identity     { return plan.engineIdentity }
func (plan DataReplacementPlan) Replacement() DataReplacement { return plan.replacement }
func (plan DataReplacementPlan) Proof() PlanProof             { return plan.proof }
func (plan DataReplacementPlan) LogicalFingerprint() string {
	return plan.replacement.logicalFingerprint
}
func (plan DataReplacementPlan) Fingerprint() string { return plan.planFingerprint }

func (planner *Planner) PlanDataReplacement(
	ctx context.Context,
	replacement DataReplacement,
) (DataReplacementPlan, error) {
	if err := planner.validateRuntime(); err != nil {
		return DataReplacementPlan{}, err
	}
	if err := replacement.validate(); err != nil {
		return DataReplacementPlan{}, err
	}
	if err := validateLogicalFieldsForOperation(planner.capabilities, "data-replacement", replacement.schema, 0, 0); err != nil {
		return DataReplacementPlan{}, err
	}
	requiredScope := AtomicReplacementTable
	if replacement.scope == DataReplacementPartitions {
		requiredScope = AtomicReplacementPartition
	}
	if !planner.capabilities.SupportsAtomicReplacement(requiredScope) {
		return DataReplacementPlan{}, unsupportedCapability("data-replacement", "atomic-replacement."+string(requiredScope))
	}
	proof, err := planner.adapter.PlanDataReplacement(ctx, replacement)
	if err != nil {
		return DataReplacementPlan{}, adapterPlanningError("data-replacement", err)
	}
	if err := validateReplacementProof(replacement, proof); err != nil {
		return DataReplacementPlan{}, err
	}
	plan := DataReplacementPlan{
		engineIdentity: planner.capabilities.identity, capabilityFingerprint: planner.capabilities.fingerprint,
		replacement: replacement, proof: proof, issuer: planner.issuer,
	}
	plan.planFingerprint = dataReplacementPlanFingerprint(plan)
	return plan, nil
}

func (planner *Planner) ValidateDataReplacementStart(
	plan DataReplacementPlan,
	replacement DataReplacement,
	current PhysicalTableState,
) error {
	if err := planner.validateDataReplacementBinding(plan, replacement); err != nil {
		return err
	}
	if !plan.proof.before.same(current) {
		return unexpectedPhysicalState("before")
	}
	return nil
}

func (planner *Planner) ValidateDataReplacementResult(plan DataReplacementPlan, current PhysicalTableState) error {
	if plan.planFingerprint == "" || plan.planFingerprint != dataReplacementPlanFingerprint(plan) ||
		planner == nil || plan.issuer != planner.issuer {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "validate-data-replacement", "plan.binding", "invalid replacement plan binding", nil,
		)
	}
	if !plan.proof.after.same(current) {
		return unexpectedPhysicalState("after")
	}
	return nil
}

func (planner *Planner) validateDataReplacementBinding(plan DataReplacementPlan, replacement DataReplacement) error {
	if plan.planFingerprint == "" || plan.planFingerprint != dataReplacementPlanFingerprint(plan) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "validate-data-replacement", "plan.fingerprint", "invalid replacement plan", nil,
		)
	}
	if err := planner.validateRuntime(); err != nil {
		return err
	}
	if err := replacement.validate(); err != nil {
		return err
	}
	if plan.engineIdentity != planner.capabilities.identity {
		return newPlanningError(PlanningCodeEngineMismatch, "validate-data-replacement", "engine.identity", "plan belongs to another engine", nil)
	}
	if plan.capabilityFingerprint != planner.capabilities.fingerprint {
		return newPlanningError(PlanningCodeCapabilityDrift, "validate-data-replacement", "capability.fingerprint", "capabilities changed", nil)
	}
	if plan.replacement.logicalFingerprint != replacement.logicalFingerprint {
		return newPlanningError(PlanningCodeMutationMismatch, "validate-data-replacement", "replacement.fingerprint", "replacement changed", nil)
	}
	if plan.issuer != planner.issuer {
		return newPlanningError(PlanningCodePlannerMismatch, "validate-data-replacement", "planner.provenance", "another planner issued the plan", nil)
	}
	return nil
}

func validateReplacementProof(replacement DataReplacement, proof PlanProof) error {
	if err := proof.validate(); err != nil {
		return err
	}
	if proof.before.target != replacement.target || proof.after.target != replacement.target || !proof.before.exists || !proof.after.exists ||
		proof.before.provenance != PhysicalStateManaged || proof.after.provenance != PhysicalStateManaged {
		return invalidPlanProof("replacement proof target or existence is invalid")
	}
	shape := replacement.ShapeFingerprint()
	if proof.before.logicalShapeFingerprint != shape || proof.after.logicalShapeFingerprint != shape {
		return invalidPlanProof("replacement proof logical shape differs")
	}
	if proof.before.generation != replacement.expectedGeneration || proof.after.generation != replacement.generation {
		return invalidPlanProof("replacement proof generation differs")
	}
	expectedStrategy := PlanStrategyReplaceTableData
	if replacement.scope == DataReplacementPartitions {
		expectedStrategy = PlanStrategyReplacePartitions
	}
	if proof.strategy != expectedStrategy {
		return invalidPlanProof("replacement proof strategy differs")
	}
	if proof.after.markerFingerprint != replacement.GenerationMarkerFingerprint() {
		return invalidPlanProof("replacement proof marker differs")
	}
	return nil
}

func dataReplacementFingerprint(replacement DataReplacement) string {
	document := struct {
		Scope                DataReplacementScope `json:"scope"`
		ProjectID            string               `json:"projectId"`
		DatasetID            string               `json:"datasetId"`
		TableID              string               `json:"tableId"`
		Schema               []domain.Field       `json:"schema"`
		CorrelationID        string               `json:"correlationId"`
		Generation           uint64               `json:"generation"`
		SelectionFingerprint string               `json:"selectionFingerprint"`
		SourceFingerprint    string               `json:"sourceFingerprint"`
		ResultFingerprint    string               `json:"resultFingerprint"`
		ExpectedGeneration   uint64               `json:"expectedGeneration"`
	}{
		Scope: replacement.scope, ProjectID: replacement.target.ProjectID,
		DatasetID: replacement.target.DatasetID, TableID: replacement.target.TableID,
		Schema: replacement.schema, CorrelationID: replacement.correlationID,
		ExpectedGeneration: replacement.expectedGeneration, Generation: replacement.generation,
		SelectionFingerprint: replacement.selectionFingerprint,
		SourceFingerprint:    replacement.sourceFingerprint, ResultFingerprint: replacement.resultFingerprint,
	}
	return fingerprintJSON(document)
}

func dataReplacementPlanFingerprint(plan DataReplacementPlan) string {
	document := struct {
		EngineIdentity        string `json:"engineIdentity"`
		CapabilityFingerprint string `json:"capabilityFingerprint"`
		LogicalFingerprint    string `json:"logicalFingerprint"`
		Proof                 any    `json:"proof"`
	}{
		EngineIdentity: plan.engineIdentity.key(), CapabilityFingerprint: plan.capabilityFingerprint,
		LogicalFingerprint: plan.replacement.logicalFingerprint, Proof: planProofFingerprintDocument(plan.proof),
	}
	return fingerprintJSON(document)
}
