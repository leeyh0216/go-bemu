package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type TableMutationKind string

const (
	TableMutationCreate           TableMutationKind = "create"
	TableMutationDrop             TableMutationKind = "drop"
	TableMutationAddColumn        TableMutationKind = "add-column"
	TableMutationDropColumn       TableMutationKind = "drop-column"
	TableMutationRenameColumn     TableMutationKind = "rename-column"
	TableMutationChangeColumnType TableMutationKind = "change-column-type"
)

type TableMutationDescriptor struct {
	Kind               TableMutationKind
	Target             domain.TableReference
	BeforeSchema       []domain.Field
	AfterSchema        []domain.Field
	FieldChanges       []FieldChangeDescriptor
	CorrelationID      string
	ExpectedGeneration uint64
	Generation         uint64
}

// FieldChangeDescriptor identifies the one logical field delta represented by
// an ALTER-style TableMutation. Path uses canonical field names from the
// before schema, except add-column where it names the field in the after
// schema. Nested paths make the mutation scope explicit.
type FieldChangeDescriptor struct {
	Path   []string
	Before domain.Field
	After  domain.Field
}

type FieldChange struct {
	path   []string
	before domain.Field
	after  domain.Field
}

func (change FieldChange) Path() []string { return append([]string(nil), change.path...) }

func (change FieldChange) Before() domain.Field { return cloneField(change.before) }

func (change FieldChange) After() domain.Field { return cloneField(change.after) }

// TableMutation is the canonical logical change supplied by BQEMU. It contains
// no engine SQL or physical type spelling and owns recursive schema copies.
type TableMutation struct {
	kind               TableMutationKind
	target             domain.TableReference
	beforeSchema       []domain.Field
	afterSchema        []domain.Field
	fieldChanges       []FieldChange
	correlationID      string
	expectedGeneration uint64
	generation         uint64
	logicalFingerprint string
}

func NewTableMutation(descriptor TableMutationDescriptor) (TableMutation, error) {
	if !validTableMutationKind(descriptor.Kind) {
		return TableMutation{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "table-mutation", "mutation.kind", "table mutation kind is missing or invalid", nil,
		)
	}
	if err := validateTableReference(descriptor.Target); err != nil {
		return TableMutation{}, err
	}
	if err := validateCorrelation(descriptor.CorrelationID, descriptor.ExpectedGeneration, descriptor.Generation, "table-mutation"); err != nil {
		return TableMutation{}, err
	}
	before := domain.CloneFields(descriptor.BeforeSchema)
	after := domain.CloneFields(descriptor.AfterSchema)
	changes := cloneFieldChangeDescriptors(descriptor.FieldChanges)
	if err := canonicalizeFieldChangePaths(descriptor.Kind, before, after, changes); err != nil {
		return TableMutation{}, err
	}
	if err := validateMutationSchemas(descriptor.Kind, descriptor.Target, before, after, changes); err != nil {
		return TableMutation{}, err
	}
	mutation := TableMutation{
		kind: descriptor.Kind, target: descriptor.Target, beforeSchema: before, afterSchema: after,
		fieldChanges: changes, correlationID: descriptor.CorrelationID,
		expectedGeneration: descriptor.ExpectedGeneration, generation: descriptor.Generation,
	}
	mutation.logicalFingerprint = mutationFingerprint(mutation)
	return mutation, nil
}

func (mutation TableMutation) Kind() TableMutationKind { return mutation.kind }

func (mutation TableMutation) Target() domain.TableReference { return mutation.target }

func (mutation TableMutation) BeforeSchema() []domain.Field {
	return domain.CloneFields(mutation.beforeSchema)
}

func (mutation TableMutation) AfterSchema() []domain.Field {
	return domain.CloneFields(mutation.afterSchema)
}

func (mutation TableMutation) FieldChanges() []FieldChange {
	return cloneFieldChanges(mutation.fieldChanges)
}

func (mutation TableMutation) CorrelationID() string { return mutation.correlationID }

func (mutation TableMutation) ExpectedGeneration() uint64 { return mutation.expectedGeneration }

func (mutation TableMutation) Generation() uint64 { return mutation.generation }

// GenerationMarkerFingerprint is the marker an adapter must write in the same
// transaction as this mutation's physical change.
func (mutation TableMutation) GenerationMarkerFingerprint() string {
	return generationMarkerFingerprint(mutation.target, mutation.correlationID, mutation.generation)
}

func (mutation TableMutation) BeforeShapeFingerprint() string {
	return logicalShapeFingerprint(mutation.beforeSchema)
}

func (mutation TableMutation) AfterShapeFingerprint() string {
	return logicalShapeFingerprint(mutation.afterSchema)
}

func (mutation TableMutation) LogicalFingerprint() string { return mutation.logicalFingerprint }

func (mutation TableMutation) validate() error {
	if !validTableMutationKind(mutation.kind) || mutation.logicalFingerprint == "" ||
		mutation.logicalFingerprint != mutationFingerprint(mutation) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "table-mutation", "mutation.fingerprint", "zero or inconsistent table mutation", nil,
		)
	}
	return nil
}

type PlanRequirements struct {
	Transactions       []TransactionScope
	AtomicReplacements []AtomicReplacementScope
	Inspection         []InspectionScope
	DDLGuarantee       DDLGuarantee
}

// TablePlan is an immutable, engine-bound authorization to apply one logical
// table mutation. Engine adapters keep executable physical plans private.
type TablePlan struct {
	engineIdentity        Identity
	capabilityFingerprint string
	mutation              TableMutation
	requirements          PlanRequirements
	proof                 PlanProof
	planFingerprint       string
	issuer                *planIssuer
}

// AdapterPlanner inspects physical state and produces a typed, side-effect-free
// proof for schema changes and data replacement. Executable SQL remains private
// to the adapter.
type AdapterPlanner interface {
	PlanTableMutation(context.Context, TableMutation) (PlanProof, error)
	PlanDataReplacement(context.Context, DataReplacement) (PlanProof, error)
}

type planIssuer struct{ marker byte }

// Planner is the only TablePlan issuer. A runtime binds it to one immutable
// capability snapshot and one adapter planner. The in-memory issuer seal is
// deliberately absent from fingerprints because plans must never be persisted.
type Planner struct {
	capabilities Capabilities
	adapter      AdapterPlanner
	issuer       *planIssuer
}

func NewPlanner(capabilities Capabilities, adapter AdapterPlanner) (*Planner, error) {
	if err := capabilities.validate(); err != nil {
		return nil, err
	}
	if interfaceIsNil(adapter) {
		return nil, newPlanningError(
			PlanningCodeInvalidDescriptor, "planner", "adapter.planner", "adapter planner is required", nil,
		)
	}
	return &Planner{capabilities: capabilities, adapter: adapter, issuer: &planIssuer{marker: 1}}, nil
}

func (planner *Planner) Capabilities() Capabilities {
	if planner == nil {
		return Capabilities{}
	}
	return planner.capabilities
}

func (planner *Planner) PlanTableChange(
	ctx context.Context,
	mutation TableMutation,
	requirements PlanRequirements,
) (TablePlan, error) {
	if err := planner.validateRuntime(); err != nil {
		return TablePlan{}, err
	}
	if err := mutation.validate(); err != nil {
		return TablePlan{}, err
	}
	requirements, err := normalizeRequirements(requirements)
	if err != nil {
		return TablePlan{}, err
	}
	if err := validateDesiredLogicalSchema(planner.capabilities, mutation); err != nil {
		return TablePlan{}, err
	}
	if err := validateMutationCapability(planner.capabilities, mutation); err != nil {
		return TablePlan{}, err
	}
	if err := validateRequirements(planner.capabilities, mutation.kind, requirements); err != nil {
		return TablePlan{}, err
	}
	proof, err := planner.adapter.PlanTableMutation(ctx, mutation)
	if err != nil {
		return TablePlan{}, adapterPlanningError(string(mutation.kind), err)
	}
	if err := validateTablePlanProof(planner.capabilities, mutation, proof); err != nil {
		return TablePlan{}, err
	}
	plan := TablePlan{
		engineIdentity: planner.capabilities.identity, capabilityFingerprint: planner.capabilities.fingerprint,
		mutation: mutation, requirements: requirements, proof: proof, issuer: planner.issuer,
	}
	plan.planFingerprint = tablePlanFingerprint(plan)
	return plan, nil
}

func (plan TablePlan) EngineIdentity() Identity { return plan.engineIdentity }

func (plan TablePlan) Mutation() TableMutation { return plan.mutation }

func (plan TablePlan) Requirements() PlanRequirements { return cloneRequirements(plan.requirements) }

func (plan TablePlan) Proof() PlanProof { return plan.proof }

func (plan TablePlan) LogicalFingerprint() string { return plan.mutation.logicalFingerprint }

func (plan TablePlan) Fingerprint() string { return plan.planFingerprint }

// ValidateApplyStart must run against an inspection taken inside the engine
// transaction before the adapter mutates data or generation markers.
func (planner *Planner) ValidateApplyStart(plan TablePlan, mutation TableMutation, current PhysicalTableState) error {
	if err := planner.validateTableBinding(plan, mutation); err != nil {
		return err
	}
	if !plan.proof.before.same(current) {
		return unexpectedPhysicalState("before")
	}
	return nil
}

// ValidateApplyResult must run before commit. DROP results are tombstones stored
// in engine-owned marker metadata, so they remain inspectable after the table is
// absent.
func (planner *Planner) ValidateApplyResult(plan TablePlan, current PhysicalTableState) error {
	if plan.planFingerprint == "" || plan.planFingerprint != tablePlanFingerprint(plan) ||
		planner == nil || plan.issuer != planner.issuer {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "validate-table-plan", "plan.binding", "invalid table plan binding", nil,
		)
	}
	if !plan.proof.after.same(current) {
		return unexpectedPhysicalState("after")
	}
	return nil
}

func (planner *Planner) validateTableBinding(plan TablePlan, mutation TableMutation) error {
	if plan.planFingerprint == "" || plan.planFingerprint != tablePlanFingerprint(plan) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "validate-table-plan", "plan.fingerprint", "zero or inconsistent table plan", nil,
		)
	}
	if err := planner.validateRuntime(); err != nil {
		return err
	}
	if err := mutation.validate(); err != nil {
		return err
	}
	if plan.engineIdentity != planner.capabilities.identity {
		return newPlanningError(
			PlanningCodeEngineMismatch, "validate-table-plan", "engine.identity", "plan belongs to another engine identity", nil,
		)
	}
	if plan.capabilityFingerprint != planner.capabilities.fingerprint {
		return newPlanningError(
			PlanningCodeCapabilityDrift, "validate-table-plan", "capability.fingerprint", "engine capabilities changed after planning", nil,
		)
	}
	if plan.mutation.logicalFingerprint != mutation.logicalFingerprint {
		return newPlanningError(
			PlanningCodeMutationMismatch, "validate-table-plan", "mutation.fingerprint", "logical mutation changed after planning", nil,
		)
	}
	if plan.issuer != planner.issuer {
		return newPlanningError(
			PlanningCodePlannerMismatch, "validate-table-plan", "planner.provenance", "plan was not issued by this runtime planner", nil,
		)
	}
	return nil
}

func (planner *Planner) validateRuntime() error {
	if planner == nil || planner.issuer == nil || interfaceIsNil(planner.adapter) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "table-plan", "planner", "zero or invalid planner", nil,
		)
	}
	return nil
}

func adapterPlanningError(operation string, err error) error {
	var planningErr *PlanningError
	if errors.As(err, &planningErr) {
		return planningErr
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return err
	}
	return newPlanningError(
		PlanningCodeUnsupported, operation, "adapter.planning", "engine adapter rejected the logical plan", nil,
	)
}

func interfaceIsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func validateMutationCapability(capabilities Capabilities, mutation TableMutation) error {
	operation := mutationDDLOperation(mutation.kind)
	ddl, supported := capabilities.DDL(operation)
	if !supported {
		return unsupportedPlanCapability(mutation.kind, "ddl."+string(operation))
	}
	if ddlUsesFieldPath(operation) && len(mutation.fieldChanges[0].path) > ddl.MaxFieldPathDepth {
		return unsupportedPlanCapability(mutation.kind, "ddl."+string(operation)+".field-path-depth")
	}
	return nil
}

func validateTablePlanProof(
	capabilities Capabilities,
	mutation TableMutation,
	proof PlanProof,
) error {
	if err := proof.validate(); err != nil {
		return err
	}
	if proof.before.target != mutation.target || proof.after.target != mutation.target {
		return invalidPlanProof("table mutation proof target differs")
	}
	if proof.before.provenance == PhysicalStateUnmanaged {
		return newPlanningError(
			PlanningCodePhysicalStateDrift, string(mutation.kind), "physical.provenance",
			"unmanaged physical table requires explicit reconciliation", nil,
		)
	}
	if proof.before.generation != mutation.expectedGeneration || proof.after.generation != mutation.generation {
		return invalidPlanProof("table mutation proof generation differs")
	}
	switch mutation.kind {
	case TableMutationCreate:
		if proof.before.exists || proof.after.provenance != PhysicalStateManaged || !proof.after.exists ||
			proof.after.logicalShapeFingerprint != mutation.AfterShapeFingerprint() || proof.strategy != PlanStrategyCreateTable {
			return invalidPlanProof("create proof does not describe absent to managed table")
		}
	case TableMutationDrop:
		if proof.before.provenance != PhysicalStateManaged || !proof.before.exists ||
			proof.before.logicalShapeFingerprint != mutation.BeforeShapeFingerprint() ||
			proof.after.provenance != PhysicalStateTombstone || proof.after.exists ||
			proof.strategy != PlanStrategyDropTable {
			return invalidPlanProof("drop proof does not describe managed table to tombstone")
		}
	default:
		if proof.before.provenance != PhysicalStateManaged || !proof.before.exists ||
			proof.before.logicalShapeFingerprint != mutation.BeforeShapeFingerprint() ||
			proof.after.provenance != PhysicalStateManaged || !proof.after.exists ||
			proof.after.logicalShapeFingerprint != mutation.AfterShapeFingerprint() {
			return invalidPlanProof("ALTER proof shape or managed provenance differs")
		}
		if proof.strategy != PlanStrategyAlterInPlace && proof.strategy != PlanStrategyRebuildTable {
			return invalidPlanProof("ALTER proof strategy is invalid")
		}
		if proof.strategy == PlanStrategyRebuildTable && !capabilities.SupportsAtomicReplacement(AtomicReplacementTable) {
			return unsupportedPlanCapability(mutation.kind, "atomic-replacement.table")
		}
	}
	if proof.after.markerFingerprint != mutation.GenerationMarkerFingerprint() {
		return invalidPlanProof("table mutation proof marker differs")
	}
	return nil
}

func validateRequirements(capabilities Capabilities, kind TableMutationKind, requirements PlanRequirements) error {
	for _, scope := range requirements.Transactions {
		if !capabilities.SupportsTransaction(scope) {
			return unsupportedPlanCapability(kind, "transaction."+string(scope))
		}
	}
	for _, scope := range requirements.AtomicReplacements {
		if !capabilities.SupportsAtomicReplacement(scope) {
			return unsupportedPlanCapability(kind, "atomic-replacement."+string(scope))
		}
	}
	for _, scope := range requirements.Inspection {
		if !capabilities.SupportsInspection(scope) {
			return unsupportedPlanCapability(kind, "inspection."+string(scope))
		}
	}
	operation := mutationDDLOperation(kind)
	ddl, supported := capabilities.DDL(operation)
	if !supported || !ddlGuaranteeSatisfies(ddl.Guarantee, requirements.DDLGuarantee) {
		return unsupportedPlanCapability(kind, "ddl."+string(operation)+".guarantee")
	}
	return nil
}

func unsupportedPlanCapability(kind TableMutationKind, attribute string) error {
	return unsupportedCapability(string(kind), attribute)
}

func unsupportedCapability(operation, attribute string) error {
	return newPlanningError(
		PlanningCodeUnsupported, operation, attribute, "required logical capability is not supported", nil,
	)
}

func normalizeRequirements(input PlanRequirements) (PlanRequirements, error) {
	result := cloneRequirements(input)
	if !validDDLGuarantee(result.DDLGuarantee) {
		return PlanRequirements{}, invalidRequirement("ddl-guarantee", "")
	}
	seenTransactions := make(map[TransactionScope]struct{}, len(result.Transactions))
	for _, scope := range result.Transactions {
		if !validTransactionScope(scope) {
			return PlanRequirements{}, invalidRequirement("transaction", string(scope))
		}
		if _, exists := seenTransactions[scope]; exists {
			return PlanRequirements{}, duplicateRequirement("transaction", string(scope))
		}
		seenTransactions[scope] = struct{}{}
	}
	sort.Slice(result.Transactions, func(left, right int) bool { return result.Transactions[left] < result.Transactions[right] })
	seenReplacement := make(map[AtomicReplacementScope]struct{}, len(result.AtomicReplacements))
	for _, scope := range result.AtomicReplacements {
		if !validAtomicReplacementScope(scope) {
			return PlanRequirements{}, invalidRequirement("atomic-replacement", string(scope))
		}
		if _, exists := seenReplacement[scope]; exists {
			return PlanRequirements{}, duplicateRequirement("atomic-replacement", string(scope))
		}
		seenReplacement[scope] = struct{}{}
	}
	sort.Slice(result.AtomicReplacements, func(left, right int) bool {
		return result.AtomicReplacements[left] < result.AtomicReplacements[right]
	})
	seenInspection := make(map[InspectionScope]struct{}, len(result.Inspection))
	for _, scope := range result.Inspection {
		if !validInspectionScope(scope) {
			return PlanRequirements{}, invalidRequirement("inspection", string(scope))
		}
		if _, exists := seenInspection[scope]; exists {
			return PlanRequirements{}, duplicateRequirement("inspection", string(scope))
		}
		seenInspection[scope] = struct{}{}
	}
	sort.Slice(result.Inspection, func(left, right int) bool { return result.Inspection[left] < result.Inspection[right] })
	return result, nil
}

func invalidRequirement(kind, _ string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, "table-plan", "requirement."+kind,
		"plan requirements contain an unknown value", nil,
	)
}

func duplicateRequirement(kind, _ string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, "table-plan", "requirement."+kind,
		"plan requirements contain a duplicate value", nil,
	)
}

func cloneRequirements(input PlanRequirements) PlanRequirements {
	return PlanRequirements{
		Transactions:       append([]TransactionScope(nil), input.Transactions...),
		AtomicReplacements: append([]AtomicReplacementScope(nil), input.AtomicReplacements...),
		Inspection:         append([]InspectionScope(nil), input.Inspection...),
		DDLGuarantee:       input.DDLGuarantee,
	}
}

func validTableMutationKind(kind TableMutationKind) bool {
	switch kind {
	case TableMutationCreate, TableMutationDrop, TableMutationAddColumn, TableMutationDropColumn,
		TableMutationRenameColumn, TableMutationChangeColumnType:
		return true
	default:
		return false
	}
}

func mutationDDLOperation(kind TableMutationKind) DDLOperation {
	switch kind {
	case TableMutationCreate:
		return DDLCreateTable
	case TableMutationDrop:
		return DDLDropTable
	case TableMutationAddColumn:
		return DDLAddColumn
	case TableMutationDropColumn:
		return DDLDropColumn
	case TableMutationRenameColumn:
		return DDLRenameColumn
	case TableMutationChangeColumnType:
		return DDLChangeColumnType
	default:
		return ""
	}
}

func validateTableReference(reference domain.TableReference) error {
	placeholder := domain.Table{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID, ID: reference.TableID,
		Schema: []domain.Field{{Name: "placeholder", Type: "STRING"}},
	}
	if err := placeholder.Validate(); err != nil {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "table-mutation", "mutation.target", "table reference is invalid", err,
		)
	}
	return nil
}

var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func validateCorrelation(correlationID string, expectedGeneration, generation uint64, operation string) error {
	if !correlationPattern.MatchString(correlationID) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, operation, "correlation.id", "correlation ID is invalid", nil,
		)
	}
	if generation == 0 || generation <= expectedGeneration {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, operation, "correlation.generation", "generation must advance", nil,
		)
	}
	return nil
}

func validateMutationSchemas(
	kind TableMutationKind,
	target domain.TableReference,
	before, after []domain.Field,
	changes []FieldChange,
) error {
	validateBefore := func() error {
		if err := validateLegacySchemaStructure(before, false); err != nil {
			return newPlanningError(
				PlanningCodeInvalidDescriptor, string(kind), "mutation.before-schema", "logical table schema is malformed", nil,
			)
		}
		return nil
	}
	validateAfter := func() error {
		if err := validateLogicalTableSchema(target, after); err != nil {
			return classifyLogicalSchemaError(string(kind), err)
		}
		return nil
	}
	switch kind {
	case TableMutationCreate:
		if len(before) != 0 || len(changes) != 0 {
			return invalidSchemaTransition(kind, "create requires an absent before schema")
		}
		return validateAfter()
	case TableMutationDrop:
		if len(after) != 0 || len(changes) != 0 {
			return invalidSchemaTransition(kind, "drop requires an absent after schema")
		}
		return validateBefore()
	default:
		if err := validateBefore(); err != nil {
			return err
		}
		if err := validateAfter(); err != nil {
			return err
		}
		if len(changes) != 1 {
			return invalidSchemaTransition(kind, "ALTER mutation requires exactly one typed field change")
		}
		return validateAndApplyFieldChange(kind, before, after, changes[0])
	}
}

var logicalFieldNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateLegacySchemaStructure(fields []domain.Field, nested bool) error {
	if len(fields) == 0 && !nested {
		return fmt.Errorf("schema is empty")
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if len(field.Name) > 1024 || !logicalFieldNamePattern.MatchString(field.Name) {
			return fmt.Errorf("field name is invalid")
		}
		key := strings.ToLower(field.Name)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("field is duplicated")
		}
		seen[key] = struct{}{}
		fieldType := strings.ToUpper(field.Type)
		switch fieldType {
		case "GEOGRAPHY", "BOOL", "BOOLEAN", "INT64", "INTEGER", "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC",
			"STRING", "BYTES", "DATE", "DATETIME", "TIME", "TIMESTAMP", "JSON", "RECORD", "STRUCT":
		default:
			return fmt.Errorf("field type is invalid")
		}
		mode := strings.ToUpper(field.Mode)
		if mode != "" && mode != "NULLABLE" && mode != "REQUIRED" && mode != "REPEATED" {
			return fmt.Errorf("field mode is invalid")
		}
		isStruct := fieldType == "RECORD" || fieldType == "STRUCT"
		if isStruct != (len(field.Fields) > 0) {
			return fmt.Errorf("field nesting is invalid")
		}
		if fieldType == "NUMERIC" || fieldType == "BIGNUMERIC" {
			if err := validateLegacyDecimal(field); err != nil {
				return err
			}
		} else if field.Precision != nil || field.Scale != nil || field.RoundingMode != "" {
			return fmt.Errorf("scalar parameters are invalid")
		}
		if len(field.Fields) > 0 {
			if err := validateLegacySchemaStructure(field.Fields, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateLegacyDecimal(field domain.Field) error {
	if field.Precision == nil && field.Scale != nil {
		return fmt.Errorf("scale requires precision")
	}
	if field.Precision != nil {
		precision := *field.Precision
		scale := int64(0)
		if field.Scale != nil {
			scale = *field.Scale
		}
		maximumPrecision, maximumScale := int64(38), int64(9)
		if strings.EqualFold(field.Type, "BIGNUMERIC") {
			maximumPrecision, maximumScale = 76, 38
		}
		if precision < 1 || precision > maximumPrecision || scale < 0 || scale > maximumScale || scale > precision {
			return fmt.Errorf("decimal parameters are malformed")
		}
	}
	switch field.RoundingMode {
	case "", domain.RoundingModeUnspecified, domain.RoundingModeHalfAwayFromZero, domain.RoundingModeHalfEven:
		return nil
	default:
		return fmt.Errorf("rounding mode is invalid")
	}
}

func validateLogicalTableSchema(target domain.TableReference, schema []domain.Field) error {
	return (domain.Table{ProjectID: target.ProjectID, DatasetID: target.DatasetID, ID: target.TableID, Schema: schema}).Validate()
}

func classifyLogicalSchemaError(operation string, err error) error {
	if errors.Is(err, domain.ErrUnsupported) {
		return newPlanningError(
			PlanningCodeUnsupported, operation, "logical.schema.policy", "logical schema is outside the emulator policy", nil,
		)
	}
	return newPlanningError(
		PlanningCodeInvalidDescriptor, operation, "logical.schema", "logical table schema is malformed", nil,
	)
}

func validateAndApplyFieldChange(kind TableMutationKind, before, after []domain.Field, change FieldChange) error {
	if len(change.path) == 0 {
		return invalidFieldChange(kind, "field path is required")
	}
	for _, segment := range change.path {
		if strings.TrimSpace(segment) == "" {
			return invalidFieldChange(kind, "field path contains an empty segment")
		}
	}
	working := domain.CloneFields(before)
	changed, err := applyFieldChange(working, change.path, kind, change)
	if err != nil {
		return invalidFieldChange(kind, err.Error())
	}
	if !reflect.DeepEqual(changed, after) {
		return invalidFieldChange(kind, "typed field change does not exactly produce the after schema")
	}
	return nil
}

func applyFieldChange(fields []domain.Field, path []string, kind TableMutationKind, change FieldChange) ([]domain.Field, error) {
	name := path[0]
	index := fieldIndex(fields, name)
	if len(path) > 1 {
		if index < 0 {
			return nil, fmt.Errorf("parent field is absent")
		}
		if !isStructField(fields[index]) {
			return nil, fmt.Errorf("nested field parent is not STRUCT")
		}
		nested, err := applyFieldChange(fields[index].Fields, path[1:], kind, change)
		if err != nil {
			return nil, err
		}
		fields[index].Fields = nested
		return fields, nil
	}

	switch kind {
	case TableMutationAddColumn:
		if index >= 0 || !isZeroField(change.before) || !strings.EqualFold(change.after.Name, name) {
			return nil, fmt.Errorf("add-column requires an absent path and matching after field")
		}
		if err := change.after.Validate(); err != nil {
			return nil, fmt.Errorf("add-column field is invalid")
		}
		return append(fields, cloneField(change.after)), nil
	case TableMutationDropColumn:
		if index < 0 || !isZeroField(change.after) || !reflect.DeepEqual(fields[index], change.before) {
			return nil, fmt.Errorf("drop-column before field does not match the schema")
		}
		return append(fields[:index:index], fields[index+1:]...), nil
	case TableMutationRenameColumn:
		if index < 0 || !reflect.DeepEqual(fields[index], change.before) || !validRename(change.before, change.after) {
			return nil, fmt.Errorf("rename-column fields do not describe a name-only change")
		}
		if fieldIndex(fields, change.after.Name) >= 0 {
			return nil, fmt.Errorf("rename-column destination already exists")
		}
		fields[index] = cloneField(change.after)
		return fields, nil
	case TableMutationChangeColumnType:
		if index < 0 || !reflect.DeepEqual(fields[index], change.before) || !validTypeChange(change.before, change.after) {
			return nil, fmt.Errorf("change-column-type fields do not describe a type-only change")
		}
		fields[index] = cloneField(change.after)
		return fields, nil
	default:
		return nil, fmt.Errorf("field change is not valid for this mutation")
	}
}

func validRename(before, after domain.Field) bool {
	if before.Name == "" || after.Name == "" || strings.EqualFold(before.Name, after.Name) {
		return false
	}
	before.Name = after.Name
	return reflect.DeepEqual(before, after)
}

func validTypeChange(before, after domain.Field) bool {
	if !strings.EqualFold(before.Name, after.Name) || before.Mode != after.Mode ||
		before.Description != after.Description || !reflect.DeepEqual(before.Fields, after.Fields) {
		return false
	}
	changed := !strings.EqualFold(before.Type, after.Type) ||
		!reflect.DeepEqual(before.Precision, after.Precision) ||
		!reflect.DeepEqual(before.Scale, after.Scale) || before.RoundingMode != after.RoundingMode
	before.Type = after.Type
	before.Precision = domain.CloneOptionalInt64(after.Precision)
	before.Scale = domain.CloneOptionalInt64(after.Scale)
	before.RoundingMode = after.RoundingMode
	return changed && reflect.DeepEqual(before, after)
}

func fieldIndex(fields []domain.Field, name string) int {
	for index := range fields {
		if strings.EqualFold(fields[index].Name, name) {
			return index
		}
	}
	return -1
}

func isStructField(field domain.Field) bool {
	return strings.EqualFold(field.Type, "STRUCT") || strings.EqualFold(field.Type, "RECORD")
}

func isZeroField(field domain.Field) bool { return reflect.DeepEqual(field, domain.Field{}) }

func invalidFieldChange(kind TableMutationKind, detail string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, string(kind), "mutation.field-change", detail, nil,
	)
}

func cloneFieldChangeDescriptors(input []FieldChangeDescriptor) []FieldChange {
	result := make([]FieldChange, len(input))
	for index, change := range input {
		result[index] = FieldChange{
			path: append([]string(nil), change.Path...), before: cloneField(change.Before), after: cloneField(change.After),
		}
	}
	return result
}

func canonicalizeFieldChangePaths(
	kind TableMutationKind,
	before, after []domain.Field,
	changes []FieldChange,
) error {
	if len(changes) == 0 {
		return nil
	}
	schema := before
	if kind == TableMutationAddColumn {
		schema = after
	}
	for index := range changes {
		canonical, ok := canonicalFieldPath(schema, changes[index].path)
		if !ok {
			return invalidFieldChange(kind, "field path is absent from the canonical schema")
		}
		changes[index].path = canonical
	}
	return nil
}

func canonicalFieldPath(fields []domain.Field, path []string) ([]string, bool) {
	if len(path) == 0 {
		return nil, false
	}
	index := fieldIndex(fields, path[0])
	if index < 0 {
		return nil, false
	}
	canonical := []string{fields[index].Name}
	if len(path) == 1 {
		return canonical, true
	}
	nested, ok := canonicalFieldPath(fields[index].Fields, path[1:])
	if !ok {
		return nil, false
	}
	return append(canonical, nested...), true
}

func cloneFieldChanges(input []FieldChange) []FieldChange {
	result := make([]FieldChange, len(input))
	for index, change := range input {
		result[index] = FieldChange{
			path: append([]string(nil), change.path...), before: cloneField(change.before), after: cloneField(change.after),
		}
	}
	return result
}

func cloneField(input domain.Field) domain.Field { return domain.CloneFields([]domain.Field{input})[0] }

func validateDesiredLogicalSchema(capabilities Capabilities, mutation TableMutation) error {
	if mutation.kind == TableMutationDrop {
		return nil
	}
	return validateLogicalFields(capabilities, mutation.kind, mutation.afterSchema, 0, 0)
}

func validateLogicalFields(
	capabilities Capabilities,
	kind TableMutationKind,
	fields []domain.Field,
	structDepth, listDepth int,
) error {
	return validateLogicalFieldsForOperation(capabilities, string(kind), fields, structDepth, listDepth)
}

func validateLogicalFieldsForOperation(
	capabilities Capabilities,
	operation string,
	fields []domain.Field,
	structDepth, listDepth int,
) error {
	for _, field := range fields {
		fieldStructDepth, fieldListDepth := structDepth, listDepth
		if strings.EqualFold(field.Mode, "REPEATED") {
			fieldListDepth++
			if fieldListDepth > capabilities.composite.MaxListDepth {
				return unsupportedCapability(operation, "logical.list.depth")
			}
		}
		if isStructField(field) {
			fieldStructDepth++
			if fieldStructDepth > capabilities.composite.MaxStructDepth {
				return unsupportedCapability(operation, "logical.struct.depth")
			}
		}
		if strings.EqualFold(field.Type, "NUMERIC") || strings.EqualFold(field.Type, "BIGNUMERIC") {
			if !capabilities.decimal.Supported {
				return unsupportedCapability(operation, "logical.decimal")
			}
			parameters, err := field.EffectiveDecimalParameters()
			if err != nil {
				return classifyLogicalSchemaError(operation, err)
			}
			if parameters.Precision > capabilities.decimal.MaxPrecision {
				return unsupportedCapability(operation, "logical.decimal.precision")
			}
			if parameters.Scale > capabilities.decimal.MaxScale {
				return unsupportedCapability(operation, "logical.decimal.scale")
			}
		}
		if err := validateLogicalFieldsForOperation(capabilities, operation, field.Fields, fieldStructDepth, fieldListDepth); err != nil {
			return err
		}
	}
	return nil
}

func invalidSchemaTransition(kind TableMutationKind, detail string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, string(kind), "mutation.schema-transition", detail, nil,
	)
}

type mutationFingerprintDocument struct {
	Kind               TableMutationKind                `json:"kind"`
	ProjectID          string                           `json:"projectId"`
	DatasetID          string                           `json:"datasetId"`
	TableID            string                           `json:"tableId"`
	BeforeSchema       []domain.Field                   `json:"beforeSchema"`
	AfterSchema        []domain.Field                   `json:"afterSchema"`
	FieldChanges       []fieldChangeFingerprintDocument `json:"fieldChanges"`
	CorrelationID      string                           `json:"correlationId"`
	ExpectedGeneration uint64                           `json:"expectedGeneration"`
	Generation         uint64                           `json:"generation"`
}

type fieldChangeFingerprintDocument struct {
	Path   []string     `json:"path"`
	Before domain.Field `json:"before"`
	After  domain.Field `json:"after"`
}

func mutationFingerprint(mutation TableMutation) string {
	document := mutationFingerprintDocument{
		Kind: mutation.kind, ProjectID: mutation.target.ProjectID,
		DatasetID: mutation.target.DatasetID, TableID: mutation.target.TableID,
		BeforeSchema: mutation.beforeSchema, AfterSchema: mutation.afterSchema,
		CorrelationID: mutation.correlationID, ExpectedGeneration: mutation.expectedGeneration,
		Generation: mutation.generation,
	}
	for _, change := range mutation.fieldChanges {
		document.FieldChanges = append(document.FieldChanges, fieldChangeFingerprintDocument{
			Path: change.path, Before: change.before, After: change.after,
		})
	}
	return fingerprintJSON(document)
}

type tablePlanFingerprintDocument struct {
	EngineIdentity        string           `json:"engineIdentity"`
	CapabilityFingerprint string           `json:"capabilityFingerprint"`
	LogicalFingerprint    string           `json:"logicalFingerprint"`
	Requirements          PlanRequirements `json:"requirements"`
	Proof                 any              `json:"proof"`
}

func tablePlanFingerprint(plan TablePlan) string {
	return fingerprintJSON(tablePlanFingerprintDocument{
		EngineIdentity: plan.engineIdentity.key(), CapabilityFingerprint: plan.capabilityFingerprint,
		LogicalFingerprint: plan.mutation.logicalFingerprint, Requirements: plan.requirements,
		Proof: planProofFingerprintDocument(plan.proof),
	})
}

func logicalShapeFingerprint(schema []domain.Field) string {
	if len(schema) == 0 {
		return ""
	}
	return fingerprintJSON(struct {
		Schema []domain.Field `json:"schema"`
	}{Schema: schema})
}

func fingerprintJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal engine fingerprint: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func fingerprintLooksValid(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}
