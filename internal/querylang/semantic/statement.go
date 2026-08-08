// Package semantic contains engine-neutral facts produced by GoogleSQL
// analysis. Values in this package own their data and never retain parser or
// analyzer handles.
package semantic

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

// TypeKind uses BigQuery's public type names rather than an execution
// engine's physical types.
type TypeKind string

const (
	TypeBool       TypeKind = "BOOL"
	TypeInt64      TypeKind = "INT64"
	TypeFloat64    TypeKind = "FLOAT64"
	TypeNumeric    TypeKind = "NUMERIC"
	TypeBigNumeric TypeKind = "BIGNUMERIC"
	TypeString     TypeKind = "STRING"
	TypeBytes      TypeKind = "BYTES"
	TypeDate       TypeKind = "DATE"
	TypeDatetime   TypeKind = "DATETIME"
	TypeTime       TypeKind = "TIME"
	TypeTimestamp  TypeKind = "TIMESTAMP"
	TypeJSON       TypeKind = "JSON"
	TypeArray      TypeKind = "ARRAY"
	TypeStruct     TypeKind = "STRUCT"
)

// TypeDescriptor is mutable construction input. NewType recursively copies
// it into an immutable Type.
type TypeDescriptor struct {
	Kind         TypeKind
	Precision    *int64
	Scale        *int64
	RoundingMode domain.RoundingMode
	Element      *TypeDescriptor
	Fields       []FieldDescriptor
}

// FieldDescriptor is a named STRUCT member used only while constructing a
// Type.
type FieldDescriptor struct {
	Name string
	Type TypeDescriptor
}

// Type is an immutable logical GoogleSQL type.
type Type struct {
	kind               TypeKind
	precision          int64
	hasPrecision       bool
	scale              int64
	hasScale           bool
	effectivePrecision int64
	effectiveScale     int64
	roundingMode       domain.RoundingMode
	effectiveRounding  domain.RoundingMode
	element            *Type
	fields             []Field
}

// Field is an immutable STRUCT member.
type Field struct {
	name string
	typ  Type
}

// NewType validates and owns a recursive logical type descriptor.
func NewType(descriptor TypeDescriptor) (Type, error) {
	typ := Type{kind: descriptor.Kind}
	switch descriptor.Kind {
	case TypeBool, TypeInt64, TypeFloat64, TypeString, TypeBytes,
		TypeDate, TypeDatetime, TypeTime, TypeTimestamp, TypeJSON:
		if descriptor.Precision != nil || descriptor.Scale != nil || descriptor.RoundingMode != "" || descriptor.Element != nil || len(descriptor.Fields) != 0 {
			return Type{}, invalidType()
		}
	case TypeNumeric, TypeBigNumeric:
		if descriptor.Element != nil || len(descriptor.Fields) != 0 {
			return Type{}, invalidType()
		}
		canonical := domain.Field{
			Name: "semantic_decimal", Type: string(descriptor.Kind),
			Precision:    domain.CloneOptionalInt64(descriptor.Precision),
			Scale:        domain.CloneOptionalInt64(descriptor.Scale),
			RoundingMode: descriptor.RoundingMode,
		}
		parameters, err := canonical.EffectiveDecimalParameters()
		if err != nil {
			if errors.Is(err, domain.ErrUnsupported) {
				return Type{}, fmt.Errorf("%w: capability=%s semantic decimal precision exceeds the supported boundary", domain.ErrUnsupported, domain.CapabilitySparkDecimal38V1)
			}
			return Type{}, invalidType()
		}
		roundingMode, err := canonical.EffectiveRoundingMode()
		if err != nil {
			return Type{}, invalidType()
		}
		if descriptor.Precision != nil {
			typ.precision, typ.hasPrecision = *descriptor.Precision, true
		}
		if descriptor.Scale != nil {
			typ.scale, typ.hasScale = *descriptor.Scale, true
		}
		typ.effectivePrecision = parameters.Precision
		typ.effectiveScale = parameters.Scale
		typ.roundingMode = descriptor.RoundingMode
		typ.effectiveRounding = roundingMode
	case TypeArray:
		if descriptor.Element == nil || descriptor.Precision != nil || descriptor.Scale != nil || descriptor.RoundingMode != "" || len(descriptor.Fields) != 0 {
			return Type{}, invalidType()
		}
		element, err := NewType(*descriptor.Element)
		if err != nil {
			return Type{}, err
		}
		typ.element = &element
	case TypeStruct:
		if len(descriptor.Fields) == 0 || descriptor.Precision != nil || descriptor.Scale != nil || descriptor.RoundingMode != "" || descriptor.Element != nil {
			return Type{}, invalidType()
		}
		seen := make(map[string]struct{}, len(descriptor.Fields))
		typ.fields = make([]Field, 0, len(descriptor.Fields))
		for _, descriptorField := range descriptor.Fields {
			name := strings.TrimSpace(descriptorField.Name)
			key := strings.ToLower(name)
			if name == "" {
				return Type{}, invalidType()
			}
			if _, exists := seen[key]; exists {
				return Type{}, invalidType()
			}
			seen[key] = struct{}{}
			fieldType, err := NewType(descriptorField.Type)
			if err != nil {
				return Type{}, err
			}
			typ.fields = append(typ.fields, Field{name: name, typ: fieldType})
		}
	default:
		return Type{}, fmt.Errorf("%w: semantic GoogleSQL type is unsupported", domain.ErrUnsupported)
	}
	return typ, nil
}

func invalidType() error {
	return fmt.Errorf("%w: invalid semantic GoogleSQL type", domain.ErrInvalid)
}

func (typ Type) Kind() TypeKind { return typ.kind }

func (typ Type) Precision() (int64, bool) { return typ.precision, typ.hasPrecision }

func (typ Type) Scale() (int64, bool) { return typ.scale, typ.hasScale }

// EffectiveDecimalParameters returns the Spark-compatible shape selected by
// the canonical BQEMU decimal policy.
func (typ Type) EffectiveDecimalParameters() (domain.DecimalParameters, bool) {
	if typ.kind != TypeNumeric && typ.kind != TypeBigNumeric {
		return domain.DecimalParameters{}, false
	}
	return domain.DecimalParameters{Precision: typ.effectivePrecision, Scale: typ.effectiveScale}, true
}

// RoundingMode preserves omission. EffectiveRoundingMode applies BigQuery's
// default without changing that presence information.
func (typ Type) RoundingMode() domain.RoundingMode { return typ.roundingMode }

func (typ Type) EffectiveRoundingMode() (domain.RoundingMode, bool) {
	if typ.kind != TypeNumeric && typ.kind != TypeBigNumeric {
		return "", false
	}
	return typ.effectiveRounding, true
}

func (typ Type) Element() (Type, bool) {
	if typ.element == nil {
		return Type{}, false
	}
	return cloneType(*typ.element), true
}

func (typ Type) Fields() []Field {
	fields := make([]Field, len(typ.fields))
	for index, field := range typ.fields {
		fields[index] = Field{name: field.name, typ: cloneType(field.typ)}
	}
	return fields
}

func (field Field) Name() string { return field.name }

func (field Field) Type() Type { return cloneType(field.typ) }

func cloneType(typ Type) Type {
	clone := typ
	if typ.element != nil {
		element := cloneType(*typ.element)
		clone.element = &element
	}
	clone.fields = typ.Fields()
	return clone
}

// ColumnDescriptor is mutable construction input for a result column.
type ColumnDescriptor struct {
	Name string
	Type Type
}

// Column is an immutable analyzed output column.
type Column struct {
	name string
	typ  Type
}

func (column Column) Name() string { return column.name }

func (column Column) Type() Type { return cloneType(column.typ) }

// RelationBindingKind separates physical catalog tables from local query
// scopes such as CTEs, derived tables, joins, and UNNEST relations.
type RelationBindingKind string

const (
	RelationPhysical RelationBindingKind = "PHYSICAL"
	RelationLocal    RelationBindingKind = "LOCAL"
)

const (
	ErrorRelationBindingInvalidV1   = "query.semantic.relation-binding-invalid-v1"
	ErrorExpressionBindingInvalidV1 = "query.semantic.expression-binding-invalid-v1"
	ErrorSymbolBindingInvalidV1     = "query.semantic.symbol-binding-invalid-v1"
)

// RelationBindingDescriptor is mutable construction input for one AST
// relation occurrence. Key, rather than a path, is the public lookup key so
// callers cannot accidentally lower an unresolved table name.
type RelationBindingDescriptor struct {
	Key       queryast.NodeKey
	Kind      RelationBindingKind
	Reference domain.TableReference
	LocalName string
}

// RelationBinding is an immutable resolution of one AST relation occurrence.
type RelationBinding struct {
	key       queryast.NodeKey
	kind      RelationBindingKind
	reference domain.TableReference
	localName string
}

func (binding RelationBinding) Key() queryast.NodeKey { return binding.key }

func (binding RelationBinding) Kind() RelationBindingKind { return binding.kind }

func (binding RelationBinding) Reference() (domain.TableReference, bool) {
	if binding.kind != RelationPhysical {
		return domain.TableReference{}, false
	}
	return binding.reference, true
}

func (binding RelationBinding) LocalName() (string, bool) {
	if binding.kind != RelationLocal {
		return "", false
	}
	return binding.localName, true
}

// ExpressionTypeDescriptor binds the resolved logical type of one syntax
// expression. A descriptor never owns an external analyzer type handle.
type ExpressionTypeDescriptor struct {
	Key  queryast.NodeKey
	Type Type
}

// SymbolBindingKind describes an analyzer-resolved identifier whose runtime
// meaning cannot be recovered from its spelling alone.
type SymbolBindingKind string

const (
	SymbolColumn         SymbolBindingKind = "COLUMN"
	SymbolScriptVariable SymbolBindingKind = "SCRIPT_VARIABLE"
	SymbolValue          SymbolBindingKind = "VALUE"
)

// SymbolBindingDescriptor is mutable construction input for an identifier
// binding. The owned Statement copies and validates every value.
type SymbolBindingDescriptor struct {
	Key  queryast.NodeKey
	Kind SymbolBindingKind
	Name string
}

// SymbolBinding is the immutable analyzer decision for one identifier node.
type SymbolBinding struct {
	key  queryast.NodeKey
	kind SymbolBindingKind
	name string
}

func (binding SymbolBinding) Key() queryast.NodeKey { return binding.key }

func (binding SymbolBinding) Kind() SymbolBindingKind { return binding.kind }

func (binding SymbolBinding) Name() string { return binding.name }

// StatementDescriptor is mutable construction input for a Statement.
// ResolvedKind is supplied by the official analyzer or the canonical catalog
// binder and must agree with the syntax kind. It deliberately reuses
// ast.StatementKind instead of defining a second enum that could drift.
type StatementDescriptor struct {
	Syntax              queryast.Statement
	ResolvedKind        queryast.StatementKind
	RelationBindings    []RelationBindingDescriptor
	ExpressionTypes     []ExpressionTypeDescriptor
	SymbolBindings      []SymbolBindingDescriptor
	ExpressionsComplete bool
	OutputColumns       []ColumnDescriptor
}

// Statement owns an immutable syntax tree plus all canonical bindings needed
// by an engine visitor. The raw SQL and external parser/analyzer handles are
// never retained.
type Statement struct {
	syntax              queryast.Statement
	relationBindings    map[queryast.NodeKey]RelationBinding
	relationOrder       []queryast.NodeKey
	expressionTypes     map[queryast.NodeKey]Type
	expressionOrder     []queryast.NodeKey
	symbolBindings      map[queryast.NodeKey]SymbolBinding
	expressionsComplete bool
	referencedTables    []domain.TableReference
	mutationTargets     []domain.TableReference
	outputColumns       []Column
	analysisFingerprint string
}

// NewStatement rejects partial or ambiguous relation analysis. Expression
// bindings may be a verified subset only when ExpressionsComplete is false;
// engine visitors can require completeness before lowering expressions.
func NewStatement(descriptor StatementDescriptor) (Statement, error) {
	if descriptor.Syntax == nil || !validStatementKind(descriptor.Syntax.Kind()) {
		return Statement{}, fmt.Errorf("%w: unsupported semantic statement kind", domain.ErrUnsupported)
	}
	if descriptor.ResolvedKind != descriptor.Syntax.Kind() {
		return Statement{}, fmt.Errorf("%w: resolved and syntax statement kinds disagree", domain.ErrPrecondition)
	}

	relations, err := queryast.Relations(descriptor.Syntax)
	if err != nil {
		return Statement{}, relationBindingFailure()
	}
	relationBindings, relationOrder, err := validateRelationBindings(descriptor.Syntax, relations, descriptor.RelationBindings)
	if err != nil {
		return Statement{}, err
	}

	expressions, err := queryast.Expressions(descriptor.Syntax)
	if err != nil {
		return Statement{}, expressionBindingFailure()
	}
	expressionTypes, expressionOrder, err := validateExpressionBindings(
		descriptor.Syntax, expressions, descriptor.ExpressionTypes, descriptor.ExpressionsComplete,
	)
	if err != nil {
		return Statement{}, err
	}
	symbolBindings, err := validateSymbolBindings(descriptor.Syntax, expressions, descriptor.SymbolBindings)
	if err != nil {
		return Statement{}, err
	}

	references := make([]domain.TableReference, 0, len(relationBindings))
	for _, key := range relationOrder {
		binding := relationBindings[key]
		if reference, physical := binding.Reference(); physical {
			references = append(references, reference)
		}
	}
	references, err = canonicalReferences(references)
	if err != nil {
		return Statement{}, relationBindingFailure()
	}
	targets, err := mutationTargets(descriptor.Syntax, relationBindings)
	if err != nil {
		return Statement{}, err
	}
	targets, err = canonicalReferences(targets)
	if err != nil {
		return Statement{}, relationBindingFailure()
	}

	columns := make([]Column, len(descriptor.OutputColumns))
	for index, descriptorColumn := range descriptor.OutputColumns {
		if strings.TrimSpace(descriptorColumn.Name) == "" {
			return Statement{}, fmt.Errorf("%w: invalid semantic output column", domain.ErrInvalid)
		}
		if !validType(descriptorColumn.Type) {
			return Statement{}, invalidType()
		}
		columns[index] = Column{name: descriptorColumn.Name, typ: cloneType(descriptorColumn.Type)}
	}

	statement := Statement{
		syntax: descriptor.Syntax, relationBindings: relationBindings, relationOrder: relationOrder,
		expressionTypes: expressionTypes, expressionOrder: expressionOrder, symbolBindings: symbolBindings,
		expressionsComplete: descriptor.ExpressionsComplete, referencedTables: references,
		mutationTargets: targets, outputColumns: columns,
	}
	statement.analysisFingerprint = fingerprintStatement(statement)
	return statement, nil
}

func validStatementKind(kind queryast.StatementKind) bool {
	switch kind {
	case queryast.StatementScript, queryast.StatementDeclare, queryast.StatementSet,
		queryast.StatementSelect, queryast.StatementInsert, queryast.StatementUpdate,
		queryast.StatementDelete, queryast.StatementMerge, queryast.StatementCreateTable,
		queryast.StatementAlterTable, queryast.StatementDropTable, queryast.StatementTruncateTable:
		return true
	default:
		return false
	}
}

func validType(typ Type) bool {
	switch typ.kind {
	case TypeBool, TypeInt64, TypeFloat64, TypeNumeric, TypeBigNumeric,
		TypeString, TypeBytes, TypeDate, TypeDatetime, TypeTime, TypeTimestamp,
		TypeJSON, TypeArray, TypeStruct:
		return true
	default:
		return false
	}
}

func validateRelationBindings(
	syntax queryast.Statement,
	relations []queryast.Relation,
	descriptors []RelationBindingDescriptor,
) (map[queryast.NodeKey]RelationBinding, []queryast.NodeKey, error) {
	expected := make(map[queryast.NodeKey]queryast.Relation, len(relations))
	order := make([]queryast.NodeKey, 0, len(relations))
	for _, relation := range relations {
		key := relation.NodeKey()
		if key.SourceDigest() != syntax.Source().Digest() {
			return nil, nil, relationBindingFailure()
		}
		if _, duplicate := expected[key]; duplicate {
			return nil, nil, relationBindingFailure()
		}
		expected[key] = relation
		order = append(order, key)
	}
	if len(descriptors) != len(expected) {
		return nil, nil, relationBindingFailure(relations...)
	}

	bindings := make(map[queryast.NodeKey]RelationBinding, len(descriptors))
	for _, descriptor := range descriptors {
		relation, exists := expected[descriptor.Key]
		if !exists || descriptor.Key.SourceDigest() != syntax.Source().Digest() {
			return nil, nil, relationBindingFailure(relations...)
		}
		if _, duplicate := bindings[descriptor.Key]; duplicate {
			return nil, nil, relationBindingFailure()
		}
		binding := RelationBinding{key: descriptor.Key, kind: descriptor.Kind}
		switch descriptor.Kind {
		case RelationPhysical:
			if relation.Kind() != queryast.RelationTable || !completeReference(descriptor.Reference) || strings.TrimSpace(descriptor.LocalName) != "" {
				return nil, nil, relationBindingFailure()
			}
			binding.reference = descriptor.Reference
		case RelationLocal:
			if completeReference(descriptor.Reference) || descriptor.Reference != (domain.TableReference{}) {
				return nil, nil, relationBindingFailure()
			}
			binding.localName = strings.TrimSpace(descriptor.LocalName)
		default:
			return nil, nil, relationBindingFailure()
		}
		bindings[descriptor.Key] = binding
	}
	return bindings, order, nil
}

func validateExpressionBindings(
	syntax queryast.Statement,
	expressions []queryast.Expression,
	descriptors []ExpressionTypeDescriptor,
	complete bool,
) (map[queryast.NodeKey]Type, []queryast.NodeKey, error) {
	expected := make(map[queryast.NodeKey]struct{}, len(expressions))
	order := make([]queryast.NodeKey, 0, len(expressions))
	for _, expression := range expressions {
		key := expression.NodeKey()
		if key.SourceDigest() != syntax.Source().Digest() {
			return nil, nil, expressionBindingFailure()
		}
		if _, duplicate := expected[key]; duplicate {
			return nil, nil, expressionBindingFailure()
		}
		expected[key] = struct{}{}
		order = append(order, key)
	}
	if complete && len(descriptors) != len(expected) {
		return nil, nil, expressionBindingFailure()
	}
	bindings := make(map[queryast.NodeKey]Type, len(descriptors))
	for _, descriptor := range descriptors {
		if _, exists := expected[descriptor.Key]; !exists || descriptor.Key.SourceDigest() != syntax.Source().Digest() || !validType(descriptor.Type) {
			return nil, nil, expressionBindingFailure()
		}
		if _, duplicate := bindings[descriptor.Key]; duplicate {
			return nil, nil, expressionBindingFailure()
		}
		bindings[descriptor.Key] = cloneType(descriptor.Type)
	}
	return bindings, order, nil
}

func validateSymbolBindings(
	syntax queryast.Statement,
	expressions []queryast.Expression,
	descriptors []SymbolBindingDescriptor,
) (map[queryast.NodeKey]SymbolBinding, error) {
	expected := make(map[queryast.NodeKey]*queryast.IdentifierExpression)
	declaredVariables := make(map[string]struct{})
	if script, ok := syntax.(*queryast.ScriptStatement); ok {
		for _, child := range script.Statements() {
			declaration, ok := child.(*queryast.DeclareStatement)
			if !ok {
				continue
			}
			for _, variable := range declaration.Variables() {
				declaredVariables[strings.ToLower(variable.Value())] = struct{}{}
			}
		}
	}
	for _, expression := range expressions {
		identifier, ok := expression.(*queryast.IdentifierExpression)
		if ok {
			expected[identifier.NodeKey()] = identifier
		}
	}
	bindings := make(map[queryast.NodeKey]SymbolBinding, len(descriptors))
	for _, descriptor := range descriptors {
		identifier, exists := expected[descriptor.Key]
		name := strings.TrimSpace(descriptor.Name)
		if !exists || descriptor.Key.SourceDigest() != syntax.Source().Digest() ||
			name == "" || strings.IndexByte(name, 0) >= 0 {
			return nil, symbolBindingFailure()
		}
		switch descriptor.Kind {
		case SymbolColumn, SymbolScriptVariable, SymbolValue:
		default:
			return nil, symbolBindingFailure()
		}
		segments := identifier.Path().Segments()
		if len(segments) != 1 || !strings.EqualFold(segments[0], name) {
			return nil, symbolBindingFailure()
		}
		if _, duplicate := bindings[descriptor.Key]; duplicate {
			return nil, symbolBindingFailure()
		}
		bindings[descriptor.Key] = SymbolBinding{key: descriptor.Key, kind: descriptor.Kind, name: name}
	}
	for key, identifier := range expected {
		segments := identifier.Path().Segments()
		if len(segments) != 1 {
			continue
		}
		if _, declared := declaredVariables[strings.ToLower(segments[0])]; !declared {
			continue
		}
		if _, bound := bindings[key]; !bound {
			return nil, symbolBindingFailure()
		}
	}
	return bindings, nil
}

func relationBindingFailure(relations ...queryast.Relation) error {
	paths := make([][]string, 0, len(relations))
	for _, relation := range relations {
		if table, ok := relation.(*queryast.TableRelation); ok {
			paths = append(paths, table.Path().Segments())
		}
	}
	return fmt.Errorf("%w: code=%s analyzed relation binding is invalid: unresolved_relations=%v", domain.ErrPrecondition, ErrorRelationBindingInvalidV1, paths)
}

func expressionBindingFailure() error {
	return fmt.Errorf("%w: code=%s analyzed expression binding is invalid", domain.ErrPrecondition, ErrorExpressionBindingInvalidV1)
}

func symbolBindingFailure() error {
	return fmt.Errorf("%w: code=%s analyzed symbol binding is invalid", domain.ErrPrecondition, ErrorSymbolBindingInvalidV1)
}

func completeReference(reference domain.TableReference) bool {
	return reference.ProjectID != "" && reference.DatasetID != "" && reference.TableID != ""
}

func mutationTargets(
	statement queryast.Statement,
	bindings map[queryast.NodeKey]RelationBinding,
) ([]domain.TableReference, error) {
	keys, err := mutationTargetKeys(statement)
	if err != nil {
		return nil, relationBindingFailure()
	}
	targets := make([]domain.TableReference, 0, len(keys))
	for _, key := range keys {
		binding, exists := bindings[key]
		if !exists {
			return nil, relationBindingFailure()
		}
		reference, physical := binding.Reference()
		if !physical {
			return nil, relationBindingFailure()
		}
		targets = append(targets, reference)
	}
	return targets, nil
}

func mutationTargetKeys(statement queryast.Statement) ([]queryast.NodeKey, error) {
	switch value := statement.(type) {
	case *queryast.ScriptStatement:
		var keys []queryast.NodeKey
		for _, child := range value.Statements() {
			childKeys, err := mutationTargetKeys(child)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
		return keys, nil
	case *queryast.InsertStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.UpdateStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.DeleteStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.MergeStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.CreateTableStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.DropTableStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.AlterTableStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.TruncateTableStatement:
		return []queryast.NodeKey{value.Target().NodeKey()}, nil
	case *queryast.DeclareStatement, *queryast.SetStatement, *queryast.SelectStatement:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown statement kind")
	}
}

func canonicalReferences(references []domain.TableReference) ([]domain.TableReference, error) {
	unique := make(map[string]domain.TableReference, len(references))
	for _, reference := range references {
		if reference.ProjectID == "" || reference.DatasetID == "" || reference.TableID == "" {
			return nil, fmt.Errorf("%w: incomplete semantic table reference", domain.ErrInvalid)
		}
		key := strings.ToLower(reference.ProjectID + "\x00" + reference.DatasetID + "\x00" + reference.TableID)
		unique[key] = reference
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]domain.TableReference, 0, len(keys))
	for _, key := range keys {
		result = append(result, unique[key])
	}
	return result, nil
}

func fingerprintStatement(statement Statement) string {
	var document strings.Builder
	document.WriteString(statement.syntax.SemanticFingerprint())
	document.WriteByte(0)
	document.WriteString(string(statement.syntax.Kind()))
	for _, key := range statement.relationOrder {
		binding := statement.relationBindings[key]
		document.WriteByte(0)
		document.WriteString(key.Fingerprint())
		document.WriteByte(0)
		document.WriteString(string(binding.kind))
		if binding.kind == RelationPhysical {
			document.WriteByte(0)
			document.WriteString(binding.reference.ProjectID)
			document.WriteByte(0)
			document.WriteString(binding.reference.DatasetID)
			document.WriteByte(0)
			document.WriteString(binding.reference.TableID)
		} else {
			document.WriteByte(0)
			document.WriteString(binding.localName)
		}
	}
	for _, key := range statement.expressionOrder {
		typ, exists := statement.expressionTypes[key]
		if !exists {
			continue
		}
		document.WriteByte(0)
		document.WriteString(key.Fingerprint())
		document.WriteByte(0)
		writeTypeFingerprint(&document, typ)
	}
	for _, key := range statement.expressionOrder {
		if binding, exists := statement.symbolBindings[key]; exists {
			document.WriteByte(0)
			document.WriteString(string(binding.kind))
			document.WriteByte(0)
			document.WriteString(strings.ToLower(binding.name))
		}
	}
	document.WriteByte(0)
	document.WriteString(strconv.FormatBool(statement.expressionsComplete))
	for _, column := range statement.outputColumns {
		document.WriteByte(0)
		document.WriteString(column.name)
		document.WriteByte(0)
		writeTypeFingerprint(&document, column.typ)
	}
	digest := sha256.Sum256([]byte(document.String()))
	return hex.EncodeToString(digest[:])
}

func writeTypeFingerprint(document *strings.Builder, typ Type) {
	document.WriteString(string(typ.kind))
	document.WriteByte(':')
	document.WriteString(strconv.FormatBool(typ.hasPrecision))
	document.WriteByte(':')
	document.WriteString(strconv.FormatInt(typ.precision, 10))
	document.WriteByte(':')
	document.WriteString(strconv.FormatBool(typ.hasScale))
	document.WriteByte(':')
	document.WriteString(strconv.FormatInt(typ.scale, 10))
	document.WriteByte(':')
	document.WriteString(string(typ.roundingMode))
	if typ.element != nil {
		document.WriteString(":element{")
		writeTypeFingerprint(document, *typ.element)
		document.WriteByte('}')
	}
	for _, field := range typ.fields {
		document.WriteString(":field{")
		document.WriteString(field.name)
		document.WriteByte(0)
		writeTypeFingerprint(document, field.typ)
		document.WriteByte('}')
	}
}

// Syntax returns the immutable engine-neutral AST owned by this analyzed
// statement.
func (statement Statement) Syntax() queryast.Statement { return statement.syntax }

// Kind delegates to Syntax so there is one statement-kind source of truth.
func (statement Statement) Kind() queryast.StatementKind { return statement.syntax.Kind() }

func (statement Statement) RelationBinding(key queryast.NodeKey) (RelationBinding, bool) {
	binding, exists := statement.relationBindings[key]
	return binding, exists
}

func (statement Statement) RequireRelationBinding(key queryast.NodeKey) (RelationBinding, error) {
	binding, exists := statement.RelationBinding(key)
	if !exists {
		return RelationBinding{}, relationBindingFailure()
	}
	return binding, nil
}

func (statement Statement) ExpressionType(key queryast.NodeKey) (Type, bool) {
	typ, exists := statement.expressionTypes[key]
	if !exists {
		return Type{}, false
	}
	return cloneType(typ), true
}

func (statement Statement) RequireExpressionType(key queryast.NodeKey) (Type, error) {
	typ, exists := statement.ExpressionType(key)
	if !exists {
		return Type{}, expressionBindingFailure()
	}
	return typ, nil
}

func (statement Statement) SymbolBinding(key queryast.NodeKey) (SymbolBinding, bool) {
	binding, exists := statement.symbolBindings[key]
	return binding, exists
}

// RelationsComplete is always true for a constructed Statement. It is an
// explicit lowering precondition rather than an analyzer implementation
// detail.
func (statement Statement) RelationsComplete() bool { return true }

func (statement Statement) ExpressionsComplete() bool { return statement.expressionsComplete }

// AnalysisFingerprint is a stable attestation over syntax and semantic
// bindings. It contains no source text, literal values, or identifiers.
func (statement Statement) AnalysisFingerprint() string { return statement.analysisFingerprint }

func (statement Statement) ReferencedTables() []domain.TableReference {
	return append([]domain.TableReference(nil), statement.referencedTables...)
}

func (statement Statement) MutationTargets() []domain.TableReference {
	return append([]domain.TableReference(nil), statement.mutationTargets...)
}

func (statement Statement) OutputColumns() []Column {
	columns := make([]Column, len(statement.outputColumns))
	for index, column := range statement.outputColumns {
		columns[index] = Column{name: column.name, typ: cloneType(column.typ)}
	}
	return columns
}
