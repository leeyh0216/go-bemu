package engine

import (
	"context"
	"reflect"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type SchemaOperation string

const (
	SchemaOperationValidate   SchemaOperation = "validate"
	SchemaOperationCreate     SchemaOperation = "create-table"
	SchemaOperationAddColumns SchemaOperation = "add-columns"
)

type SchemaIntentDescriptor struct {
	Operation    SchemaOperation
	Target       domain.TableReference
	BeforeSchema []domain.Field
	AfterSchema  []domain.Field
	Additions    []domain.SchemaAddition
}

// SchemaIntent is a complete logical input to schema representability
// planning. It contains no executable SQL or physical engine type names.
type SchemaIntent struct {
	operation    SchemaOperation
	target       domain.TableReference
	beforeSchema []domain.Field
	afterSchema  []domain.Field
	additions    []domain.SchemaAddition
	fingerprint  string
}

func NewSchemaIntent(descriptor SchemaIntentDescriptor) (SchemaIntent, error) {
	if !validSchemaOperation(descriptor.Operation) {
		return SchemaIntent{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "schema", "schema.operation", "schema operation is invalid", nil,
		)
	}
	if err := validateTableReference(descriptor.Target); err != nil {
		return SchemaIntent{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "schema", "schema.target", "table reference is invalid", nil,
		)
	}
	before := domain.CloneFields(descriptor.BeforeSchema)
	after := domain.CloneFields(descriptor.AfterSchema)
	additions := cloneSchemaAdditions(descriptor.Additions)
	if err := validateSchemaIntentTransition(descriptor.Operation, descriptor.Target, before, after, additions); err != nil {
		return SchemaIntent{}, err
	}
	intent := SchemaIntent{
		operation: descriptor.Operation, target: descriptor.Target,
		beforeSchema: before, afterSchema: after, additions: additions,
	}
	intent.fingerprint = schemaIntentFingerprint(intent)
	return intent, nil
}

func (intent SchemaIntent) Operation() SchemaOperation    { return intent.operation }
func (intent SchemaIntent) Target() domain.TableReference { return intent.target }
func (intent SchemaIntent) BeforeSchema() []domain.Field {
	return domain.CloneFields(intent.beforeSchema)
}
func (intent SchemaIntent) AfterSchema() []domain.Field {
	return domain.CloneFields(intent.afterSchema)
}
func (intent SchemaIntent) Additions() []domain.SchemaAddition {
	return cloneSchemaAdditions(intent.additions)
}
func (intent SchemaIntent) Fingerprint() string { return intent.fingerprint }

func (intent SchemaIntent) validate() error {
	if !validSchemaOperation(intent.operation) || intent.fingerprint == "" ||
		intent.fingerprint != schemaIntentFingerprint(intent) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "schema", "schema.fingerprint", "zero or inconsistent schema intent", nil,
		)
	}
	return nil
}

// SchemaAdapterPlanner performs the remaining pure, engine-specific
// representability check. It must not inspect or mutate physical state.
type SchemaAdapterPlanner interface {
	ValidateSchemaIntent(context.Context, SchemaIntent) error
}

type schemaPlanIssuer struct{ marker byte }

// SchemaPlanner issues short-lived schema authorization plans bound to one
// engine runtime and capability snapshot. These are not physical mutation
// receipts and must not be persisted.
type SchemaPlanner struct {
	capabilities Capabilities
	adapter      SchemaAdapterPlanner
	issuer       *schemaPlanIssuer
}

type SchemaPlan struct {
	engineIdentity        Identity
	capabilityFingerprint string
	intent                SchemaIntent
	fingerprint           string
	issuer                *schemaPlanIssuer
}

func NewSchemaPlanner(capabilities Capabilities, adapter SchemaAdapterPlanner) (*SchemaPlanner, error) {
	if err := capabilities.validate(); err != nil {
		return nil, err
	}
	if interfaceIsNil(adapter) {
		return nil, newPlanningError(
			PlanningCodeInvalidDescriptor, "schema", "adapter.schema-planner", "schema adapter planner is required", nil,
		)
	}
	return &SchemaPlanner{
		capabilities: capabilities, adapter: adapter, issuer: &schemaPlanIssuer{marker: 1},
	}, nil
}

func (planner *SchemaPlanner) Plan(ctx context.Context, intent SchemaIntent) (SchemaPlan, error) {
	if planner == nil || planner.issuer == nil || interfaceIsNil(planner.adapter) {
		return SchemaPlan{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "schema", "schema.planner", "schema planner is invalid", nil,
		)
	}
	if err := intent.validate(); err != nil {
		return SchemaPlan{}, err
	}
	if err := validateLogicalFieldsForOperation(
		planner.capabilities, string(intent.operation), intent.afterSchema, 0, 0,
	); err != nil {
		return SchemaPlan{}, err
	}
	if err := validateSchemaOperationCapabilities(planner.capabilities, intent); err != nil {
		return SchemaPlan{}, err
	}
	if err := planner.adapter.ValidateSchemaIntent(ctx, intent); err != nil {
		return SchemaPlan{}, adapterPlanningError(string(intent.operation), err)
	}
	plan := SchemaPlan{
		engineIdentity:        planner.capabilities.identity,
		capabilityFingerprint: planner.capabilities.fingerprint,
		intent:                intent,
		issuer:                planner.issuer,
	}
	plan.fingerprint = schemaPlanFingerprint(plan)
	return plan, nil
}

// ValidateBinding verifies a plan immediately before an adapter builds or
// executes physical work. The supplied intent must be reconstructed from the
// actual execution arguments.
func (planner *SchemaPlanner) ValidateBinding(plan SchemaPlan, intent SchemaIntent) error {
	if planner == nil || planner.issuer == nil || interfaceIsNil(planner.adapter) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "schema", "schema.planner", "schema planner is invalid", nil,
		)
	}
	if err := intent.validate(); err != nil {
		return err
	}
	if plan.fingerprint == "" || plan.fingerprint != schemaPlanFingerprint(plan) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "schema", "schema-plan.fingerprint", "schema plan is invalid", nil,
		)
	}
	if plan.engineIdentity != planner.capabilities.identity {
		return newPlanningError(
			PlanningCodeEngineMismatch, "schema", "engine.identity", "schema plan belongs to another engine", nil,
		)
	}
	if plan.capabilityFingerprint != planner.capabilities.fingerprint {
		return newPlanningError(
			PlanningCodeCapabilityDrift, "schema", "capability.fingerprint", "schema planning capabilities changed", nil,
		)
	}
	if plan.intent.fingerprint != intent.fingerprint {
		return newPlanningError(
			PlanningCodeMutationMismatch, "schema", "schema.fingerprint", "schema intent changed after planning", nil,
		)
	}
	if plan.issuer != planner.issuer {
		return newPlanningError(
			PlanningCodePlannerMismatch, "schema", "planner.provenance", "another planner issued the schema plan", nil,
		)
	}
	return nil
}

func (plan SchemaPlan) EngineIdentity() Identity { return plan.engineIdentity }
func (plan SchemaPlan) Intent() SchemaIntent     { return cloneSchemaIntent(plan.intent) }
func (plan SchemaPlan) CapabilityFingerprint() string {
	return plan.capabilityFingerprint
}
func (plan SchemaPlan) LogicalFingerprint() string { return plan.intent.fingerprint }
func (plan SchemaPlan) Fingerprint() string        { return plan.fingerprint }

func validateSchemaIntentTransition(
	operation SchemaOperation,
	target domain.TableReference,
	before, after []domain.Field,
	additions []domain.SchemaAddition,
) error {
	if err := validateLogicalTableSchema(target, after); err != nil {
		return classifyLogicalSchemaError(string(operation), err)
	}
	switch operation {
	case SchemaOperationValidate:
		if len(before) != 0 || len(additions) != 0 {
			return invalidSchemaIntent("validation cannot contain a prior schema or additions")
		}
	case SchemaOperationCreate:
		if len(before) != 0 || len(additions) != 0 {
			return invalidSchemaIntent("create requires an absent prior schema and no additions")
		}
	case SchemaOperationAddColumns:
		if err := validateLogicalTableSchema(target, before); err != nil {
			return classifyLogicalSchemaError(string(operation), err)
		}
		expected, err := domain.ValidateSchemaEvolution(before, after)
		if err != nil {
			return classifyLogicalSchemaError(string(operation), err)
		}
		if len(expected) == 0 || !reflect.DeepEqual(expected, additions) {
			return invalidSchemaIntent("add-column intent does not match the exact logical schema transition")
		}
	}
	return nil
}

func validateSchemaOperationCapabilities(capabilities Capabilities, intent SchemaIntent) error {
	var operation DDLOperation
	var required DDLGuarantee
	switch intent.operation {
	case SchemaOperationValidate:
		return nil
	case SchemaOperationCreate:
		operation, required = DDLCreateTable, DDLGuaranteeAtomicPhysicalStatement
	case SchemaOperationAddColumns:
		operation, required = DDLAddColumn, DDLGuaranteeAtomicPhysicalTable
	}
	capability, supported := capabilities.DDL(operation)
	if !supported || !ddlGuaranteeSatisfies(capability.Guarantee, required) {
		return unsupportedCapability(string(intent.operation), "ddl."+string(operation)+".guarantee")
	}
	if operation == DDLAddColumn {
		for _, addition := range intent.additions {
			if len(addition.Path) > capability.MaxFieldPathDepth {
				return unsupportedCapability(string(intent.operation), "ddl.add-column.field-path-depth")
			}
		}
	}
	return nil
}

func validSchemaOperation(operation SchemaOperation) bool {
	switch operation {
	case SchemaOperationValidate, SchemaOperationCreate, SchemaOperationAddColumns:
		return true
	default:
		return false
	}
}

func cloneSchemaAdditions(input []domain.SchemaAddition) []domain.SchemaAddition {
	result := make([]domain.SchemaAddition, len(input))
	for index, addition := range input {
		result[index] = domain.SchemaAddition{
			Path:  append([]string(nil), addition.Path...),
			Field: cloneField(addition.Field),
		}
	}
	return result
}

func cloneSchemaIntent(input SchemaIntent) SchemaIntent {
	return SchemaIntent{
		operation: input.operation, target: input.target,
		beforeSchema: domain.CloneFields(input.beforeSchema),
		afterSchema:  domain.CloneFields(input.afterSchema),
		additions:    cloneSchemaAdditions(input.additions), fingerprint: input.fingerprint,
	}
}

func schemaIntentFingerprint(intent SchemaIntent) string {
	return fingerprintJSON(struct {
		Operation    SchemaOperation         `json:"operation"`
		ProjectID    string                  `json:"projectId"`
		DatasetID    string                  `json:"datasetId"`
		TableID      string                  `json:"tableId"`
		BeforeSchema []domain.Field          `json:"beforeSchema"`
		AfterSchema  []domain.Field          `json:"afterSchema"`
		Additions    []domain.SchemaAddition `json:"additions"`
	}{
		Operation: intent.operation, ProjectID: intent.target.ProjectID,
		DatasetID: intent.target.DatasetID, TableID: intent.target.TableID,
		BeforeSchema: intent.beforeSchema, AfterSchema: intent.afterSchema, Additions: intent.additions,
	})
}

func schemaPlanFingerprint(plan SchemaPlan) string {
	return fingerprintJSON(struct {
		EngineIdentity        string `json:"engineIdentity"`
		CapabilityFingerprint string `json:"capabilityFingerprint"`
		LogicalFingerprint    string `json:"logicalFingerprint"`
	}{
		EngineIdentity:        plan.engineIdentity.key(),
		CapabilityFingerprint: plan.capabilityFingerprint,
		LogicalFingerprint:    plan.intent.fingerprint,
	})
}

func invalidSchemaIntent(detail string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, "schema", "schema.transition", detail, nil,
	)
}
