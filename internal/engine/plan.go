package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
	TableMutationReplace          TableMutationKind = "replace"
)

type TableMutationDescriptor struct {
	Kind         TableMutationKind
	Target       domain.TableReference
	BeforeSchema []domain.Field
	AfterSchema  []domain.Field
	FieldChanges []FieldChangeDescriptor
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
	before := domain.CloneFields(descriptor.BeforeSchema)
	after := domain.CloneFields(descriptor.AfterSchema)
	changes := cloneFieldChangeDescriptors(descriptor.FieldChanges)
	if err := validateMutationSchemas(descriptor.Kind, descriptor.Target, before, after, changes); err != nil {
		return TableMutation{}, err
	}
	mutation := TableMutation{
		kind: descriptor.Kind, target: descriptor.Target, beforeSchema: before, afterSchema: after,
		fieldChanges: changes,
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
}

// TablePlan is an immutable, engine-bound authorization to apply one logical
// table mutation. Engine adapters keep executable physical plans private.
type TablePlan struct {
	engineIdentity        Identity
	capabilityFingerprint string
	mutation              TableMutation
	requirements          PlanRequirements
	planFingerprint       string
	issuer                *planIssuer
}

// TableMutationValidator is implemented by an engine adapter. It verifies all
// adapter-specific execution constraints without performing a physical side
// effect. Returning nil attests that the adapter can execute this mutation.
type TableMutationValidator interface {
	ValidateTableMutation(context.Context, TableMutation) error
}

type TableMutationValidatorFunc func(context.Context, TableMutation) error

func (function TableMutationValidatorFunc) ValidateTableMutation(ctx context.Context, mutation TableMutation) error {
	return function(ctx, mutation)
}

type planIssuer struct{ marker byte }

// Planner is the only TablePlan issuer. A runtime binds it to one immutable
// capability snapshot and one adapter validator. The in-memory issuer seal is
// deliberately absent from fingerprints because plans must never be persisted.
type Planner struct {
	capabilities Capabilities
	validator    TableMutationValidator
	issuer       *planIssuer
}

func NewPlanner(capabilities Capabilities, validator TableMutationValidator) (*Planner, error) {
	if err := capabilities.validate(); err != nil {
		return nil, err
	}
	if interfaceIsNil(validator) {
		return nil, newPlanningError(
			PlanningCodeInvalidDescriptor, "planner", "adapter.validator", "table mutation validator is required", nil,
		)
	}
	return &Planner{capabilities: capabilities, validator: validator, issuer: &planIssuer{marker: 1}}, nil
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
	if planner == nil || planner.issuer == nil || interfaceIsNil(planner.validator) {
		return TablePlan{}, newPlanningError(
			PlanningCodeInvalidDescriptor, "table-plan", "planner", "zero or invalid planner", nil,
		)
	}
	if err := mutation.validate(); err != nil {
		return TablePlan{}, err
	}
	requirements, err := normalizeRequirements(requirements)
	if err != nil {
		return TablePlan{}, err
	}
	if err := validateLogicalSchemas(planner.capabilities, mutation); err != nil {
		return TablePlan{}, err
	}
	if err := validateMutationCapability(planner.capabilities, mutation); err != nil {
		return TablePlan{}, err
	}
	if err := validateRequirements(planner.capabilities, mutation.kind, requirements); err != nil {
		return TablePlan{}, err
	}
	if err := planner.validator.ValidateTableMutation(ctx, mutation); err != nil {
		var planningErr *PlanningError
		if errors.As(err, &planningErr) {
			return TablePlan{}, err
		}
		return TablePlan{}, newPlanningError(
			PlanningCodeUnsupported, string(mutation.kind), "adapter.validation",
			"engine adapter rejected the logical mutation", err,
		)
	}
	plan := TablePlan{
		engineIdentity: planner.capabilities.identity, capabilityFingerprint: planner.capabilities.fingerprint,
		mutation: mutation, requirements: requirements, issuer: planner.issuer,
	}
	plan.planFingerprint = tablePlanFingerprint(plan)
	return plan, nil
}

func (plan TablePlan) EngineIdentity() Identity { return plan.engineIdentity }

func (plan TablePlan) Mutation() TableMutation { return plan.mutation }

func (plan TablePlan) Requirements() PlanRequirements { return cloneRequirements(plan.requirements) }

func (plan TablePlan) LogicalFingerprint() string { return plan.mutation.logicalFingerprint }

func (plan TablePlan) Fingerprint() string { return plan.planFingerprint }

// ValidateBinding prevents a plan from being reused with another planner,
// engine, capability snapshot, or stale logical mutation.
func (planner *Planner) ValidateBinding(plan TablePlan, mutation TableMutation) error {
	if plan.planFingerprint == "" || plan.planFingerprint != tablePlanFingerprint(plan) {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "validate-table-plan", "plan.fingerprint", "zero or inconsistent table plan", nil,
		)
	}
	if planner == nil || planner.issuer == nil {
		return newPlanningError(
			PlanningCodeInvalidDescriptor, "validate-table-plan", "planner", "zero or invalid planner", nil,
		)
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
	if mutation.kind == TableMutationReplace {
		if !capabilities.SupportsAtomicReplacement(AtomicReplacementTable) {
			return unsupportedPlanCapability(mutation.kind, "atomic-replacement.table")
		}
		return nil
	}
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
	return nil
}

func unsupportedPlanCapability(kind TableMutationKind, attribute string) error {
	return newPlanningError(
		PlanningCodeUnsupported, string(kind), attribute, "required logical capability is not supported", nil,
	)
}

func normalizeRequirements(input PlanRequirements) (PlanRequirements, error) {
	result := cloneRequirements(input)
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
	return result, nil
}

func invalidRequirement(kind, value string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, "table-plan", "requirement."+kind,
		fmt.Sprintf("unknown %s requirement %q", kind, value), nil,
	)
}

func duplicateRequirement(kind, value string) error {
	return newPlanningError(
		PlanningCodeInvalidDescriptor, "table-plan", "requirement."+kind,
		fmt.Sprintf("duplicate %s requirement %q", kind, value), nil,
	)
}

func cloneRequirements(input PlanRequirements) PlanRequirements {
	return PlanRequirements{
		Transactions:       append([]TransactionScope(nil), input.Transactions...),
		AtomicReplacements: append([]AtomicReplacementScope(nil), input.AtomicReplacements...),
		Inspection:         append([]InspectionScope(nil), input.Inspection...),
	}
}

func validTableMutationKind(kind TableMutationKind) bool {
	switch kind {
	case TableMutationCreate, TableMutationDrop, TableMutationAddColumn, TableMutationDropColumn,
		TableMutationRenameColumn, TableMutationChangeColumnType, TableMutationReplace:
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

func validateMutationSchemas(
	kind TableMutationKind,
	target domain.TableReference,
	before, after []domain.Field,
	changes []FieldChange,
) error {
	validateSchema := func(attribute string, schema []domain.Field) error {
		table := domain.Table{ProjectID: target.ProjectID, DatasetID: target.DatasetID, ID: target.TableID, Schema: schema}
		if err := table.Validate(); err != nil {
			return newPlanningError(
				PlanningCodeInvalidDescriptor, string(kind), attribute, "logical table schema is invalid", err,
			)
		}
		return nil
	}
	switch kind {
	case TableMutationCreate:
		if len(before) != 0 || len(changes) != 0 {
			return invalidSchemaTransition(kind, "create requires an absent before schema")
		}
		return validateSchema("mutation.after-schema", after)
	case TableMutationDrop:
		if len(after) != 0 || len(changes) != 0 {
			return invalidSchemaTransition(kind, "drop requires an absent after schema")
		}
		return validateSchema("mutation.before-schema", before)
	case TableMutationReplace:
		if len(changes) != 0 {
			return invalidSchemaTransition(kind, "replace must not carry ALTER field changes")
		}
		if err := validateSchema("mutation.before-schema", before); err != nil {
			return err
		}
		if err := validateSchema("mutation.after-schema", after); err != nil {
			return err
		}
		if reflect.DeepEqual(before, after) {
			return invalidSchemaTransition(kind, "before and after schemas must differ")
		}
		return nil
	default:
		if err := validateSchema("mutation.before-schema", before); err != nil {
			return err
		}
		if err := validateSchema("mutation.after-schema", after); err != nil {
			return err
		}
		if len(changes) != 1 {
			return invalidSchemaTransition(kind, "ALTER mutation requires exactly one typed field change")
		}
		return validateAndApplyFieldChange(kind, before, after, changes[0])
	}
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

func validateLogicalSchemas(capabilities Capabilities, mutation TableMutation) error {
	for _, schema := range [][]domain.Field{mutation.beforeSchema, mutation.afterSchema} {
		if err := validateLogicalFields(capabilities, mutation.kind, schema, 0, 0); err != nil {
			return err
		}
	}
	return nil
}

func validateLogicalFields(
	capabilities Capabilities,
	kind TableMutationKind,
	fields []domain.Field,
	structDepth, listDepth int,
) error {
	for _, field := range fields {
		fieldStructDepth, fieldListDepth := structDepth, listDepth
		if strings.EqualFold(field.Mode, "REPEATED") {
			fieldListDepth++
			if fieldListDepth > capabilities.composite.MaxListDepth {
				return unsupportedPlanCapability(kind, "logical.list.depth")
			}
		}
		if isStructField(field) {
			fieldStructDepth++
			if fieldStructDepth > capabilities.composite.MaxStructDepth {
				return unsupportedPlanCapability(kind, "logical.struct.depth")
			}
		}
		if strings.EqualFold(field.Type, "NUMERIC") || strings.EqualFold(field.Type, "BIGNUMERIC") {
			if !capabilities.decimal.Supported {
				return unsupportedPlanCapability(kind, "logical.decimal")
			}
			parameters, err := field.EffectiveDecimalParameters()
			if err != nil {
				return newPlanningError(
					PlanningCodeInvalidDescriptor, string(kind), "logical.decimal", "decimal parameters are invalid", err,
				)
			}
			if parameters.Precision > capabilities.decimal.MaxPrecision {
				return unsupportedPlanCapability(kind, "logical.decimal.precision")
			}
			if parameters.Scale > capabilities.decimal.MaxScale {
				return unsupportedPlanCapability(kind, "logical.decimal.scale")
			}
		}
		if err := validateLogicalFields(capabilities, kind, field.Fields, fieldStructDepth, fieldListDepth); err != nil {
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
	Kind         TableMutationKind                `json:"kind"`
	ProjectID    string                           `json:"projectId"`
	DatasetID    string                           `json:"datasetId"`
	TableID      string                           `json:"tableId"`
	BeforeSchema []domain.Field                   `json:"beforeSchema"`
	AfterSchema  []domain.Field                   `json:"afterSchema"`
	FieldChanges []fieldChangeFingerprintDocument `json:"fieldChanges"`
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
}

func tablePlanFingerprint(plan TablePlan) string {
	return fingerprintJSON(tablePlanFingerprintDocument{
		EngineIdentity: plan.engineIdentity.key(), CapabilityFingerprint: plan.capabilityFingerprint,
		LogicalFingerprint: plan.mutation.logicalFingerprint, Requirements: plan.requirements,
	})
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
