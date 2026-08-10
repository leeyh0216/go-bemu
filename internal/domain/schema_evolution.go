package domain

// Capability ID CAP-SCHEMA-ADDITIVE-V1 follows BigQuery's additive schema
// rules documented at https://cloud.google.com/bigquery/docs/managing-table-schemas#add_columns
// and https://cloud.google.com/bigquery/docs/nested-repeated#modify_nested_schemas.

import (
	"fmt"
	"strings"
)

const CapabilitySchemaAdditiveV1 = "CAP-SCHEMA-ADDITIVE-V1"
const CapabilitySchemaUpdateV1 = "CAP-SCHEMA-UPDATE-V1"
const CapabilitySchemaTypeConversionV1 = "CAP-SCHEMA-TYPE-CONVERSION-V1"

type SchemaAddition struct {
	Path  []string
	Field Field
}

type SchemaRelaxation struct {
	Path   []string
	Before Field
	After  Field
}

type SchemaEvolutionOptions struct {
	AllowFieldAddition   bool
	AllowFieldRelaxation bool
}

type SchemaEvolution struct {
	Additions   []SchemaAddition
	Relaxations []SchemaRelaxation
}

// ValidateSchemaEvolution accepts only end-position additions. Existing fields
// may update descriptions, but their identity, order, type, mode, and nested
// structure cannot be removed or rewritten. Each field added to an existing
// record must be NULLABLE (including the empty default) or REPEATED. A wholly
// new nullable/repeated RECORD may contain REQUIRED children.
func ValidateSchemaEvolution(current, proposed []Field) ([]SchemaAddition, error) {
	evolution, err := validateSchemaUpdate(current, proposed, SchemaEvolutionOptions{AllowFieldAddition: true}, CapabilitySchemaAdditiveV1)
	if err != nil {
		return nil, err
	}
	return evolution.Additions, nil
}

// ValidateSchemaUpdate accepts only end-position additions and REQUIRED to
// NULLABLE relaxations enabled by the supplied options. It rejects every
// other logical drift, including reorder, rename, type, decimal, and repeated
// mode changes.
func ValidateSchemaUpdate(current, proposed []Field, options SchemaEvolutionOptions) (SchemaEvolution, error) {
	return validateSchemaUpdate(current, proposed, options, CapabilitySchemaUpdateV1)
}

func validateSchemaUpdate(current, proposed []Field, options SchemaEvolutionOptions, capability string) (SchemaEvolution, error) {
	if err := validateFieldList(current, nil); err != nil {
		return SchemaEvolution{}, err
	}
	if err := validateFieldList(proposed, nil); err != nil {
		return SchemaEvolution{}, err
	}
	return validateSchemaLevel(current, proposed, nil, options, capability)
}

func validateSchemaLevel(
	current, proposed []Field,
	parent []string,
	options SchemaEvolutionOptions,
	capability string,
) (SchemaEvolution, error) {
	if len(proposed) < len(current) {
		return SchemaEvolution{}, schemaEvolutionError(capability, parent, "field removal is not supported")
	}
	evolution := SchemaEvolution{}
	for index, existing := range current {
		candidate := proposed[index]
		path := appendPath(parent, existing.Name)
		if candidate.Name != existing.Name {
			return SchemaEvolution{}, schemaEvolutionError(capability, path, "field removal, rename, or reorder is not supported")
		}
		if canonicalFieldType(candidate.Type) != canonicalFieldType(existing.Type) {
			return SchemaEvolution{}, schemaEvolutionError(capability, path, fmt.Sprintf("type change %s -> %s is not supported", existing.Type, candidate.Type))
		}
		if !sameOptionalInt64(candidate.Precision, existing.Precision) || !sameOptionalInt64(candidate.Scale, existing.Scale) {
			return SchemaEvolution{}, schemaEvolutionError(capability, path, "decimal precision or scale change is not supported")
		}
		if candidate.RoundingMode != existing.RoundingMode {
			return SchemaEvolution{}, schemaEvolutionError(capability, path, "decimal rounding mode change is not supported")
		}
		beforeMode, afterMode := canonicalFieldMode(existing.Mode), canonicalFieldMode(candidate.Mode)
		if beforeMode != afterMode {
			if beforeMode != "REQUIRED" || afterMode != "NULLABLE" {
				return SchemaEvolution{}, schemaEvolutionError(capability, path, fmt.Sprintf("mode change %s -> %s is not supported", beforeMode, afterMode))
			}
			if !options.AllowFieldRelaxation {
				return SchemaEvolution{}, schemaEvolutionError(capability, path, "REQUIRED to NULLABLE relaxation was not enabled")
			}
			evolution.Relaxations = append(evolution.Relaxations, SchemaRelaxation{
				Path: append([]string(nil), path...), Before: cloneSchemaField(existing), After: cloneSchemaField(candidate),
			})
		}
		if isRecord(existing) {
			nested, err := validateSchemaLevel(existing.Fields, candidate.Fields, path, options, capability)
			if err != nil {
				return SchemaEvolution{}, err
			}
			evolution.Additions = append(evolution.Additions, nested.Additions...)
			evolution.Relaxations = append(evolution.Relaxations, nested.Relaxations...)
		} else if len(candidate.Fields) != 0 {
			return SchemaEvolution{}, schemaEvolutionError(capability, path, "non-RECORD fields cannot gain nested fields")
		}
	}
	for _, added := range proposed[len(current):] {
		path := appendPath(parent, added.Name)
		if !options.AllowFieldAddition {
			return SchemaEvolution{}, schemaEvolutionError(capability, path, "field addition was not enabled")
		}
		if err := validateAddedField(added, path, capability); err != nil {
			return SchemaEvolution{}, err
		}
		evolution.Additions = append(evolution.Additions, SchemaAddition{Path: path, Field: cloneSchemaField(added)})
	}
	return evolution, nil
}

func sameOptionalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validateAddedField(field Field, path []string, capability string) error {
	mode := canonicalFieldMode(field.Mode)
	if mode != "NULLABLE" && mode != "REPEATED" {
		return schemaEvolutionError(capability, path, "new fields must be NULLABLE or REPEATED")
	}
	return nil
}

func schemaEvolutionError(capability string, path []string, reason string) error {
	location := strings.Join(path, ".")
	if location == "" {
		location = "schema"
	}
	return fmt.Errorf("%w: capability=%s field=%s: %s; fix_hint=preserve existing fields and enable only required additions or relaxations", ErrInvalid, capability, location, reason)
}

func cloneSchemaField(field Field) Field {
	return CloneFields([]Field{field})[0]
}

func appendPath(parent []string, name string) []string {
	path := make([]string, len(parent), len(parent)+1)
	copy(path, parent)
	return append(path, name)
}

func canonicalFieldMode(mode string) string {
	if mode == "" {
		return "NULLABLE"
	}
	return strings.ToUpper(mode)
}

func canonicalFieldType(fieldType string) string {
	switch strings.ToUpper(fieldType) {
	case "BOOL":
		return "BOOLEAN"
	case "INTEGER":
		return "INT64"
	case "FLOAT":
		return "FLOAT64"
	case "STRUCT":
		return "RECORD"
	default:
		return strings.ToUpper(fieldType)
	}
}

func isRecord(field Field) bool {
	return canonicalFieldType(field.Type) == "RECORD"
}
