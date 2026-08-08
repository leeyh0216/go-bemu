package domain

// Capability ID CAP-SCHEMA-ADDITIVE-V1 follows BigQuery's additive schema
// rules documented at https://cloud.google.com/bigquery/docs/managing-table-schemas#add_columns
// and https://cloud.google.com/bigquery/docs/nested-repeated#modify_nested_schemas.

import (
	"fmt"
	"strings"
)

const CapabilitySchemaAdditiveV1 = "CAP-SCHEMA-ADDITIVE-V1"

type SchemaAddition struct {
	Path  []string
	Field Field
}

// ValidateSchemaEvolution accepts only end-position additions. Existing fields
// may update descriptions, but their identity, order, type, mode, and nested
// structure cannot be removed or rewritten. Every added field at every nesting
// level must be NULLABLE (including the empty default) or REPEATED.
func ValidateSchemaEvolution(current, proposed []Field) ([]SchemaAddition, error) {
	if err := validateFieldList(proposed, nil); err != nil {
		return nil, err
	}
	return validateSchemaLevel(current, proposed, nil)
}

func validateSchemaLevel(current, proposed []Field, parent []string) ([]SchemaAddition, error) {
	if len(proposed) < len(current) {
		return nil, schemaEvolutionError(parent, "field removal is not supported")
	}
	additions := make([]SchemaAddition, 0)
	for index, existing := range current {
		candidate := proposed[index]
		path := appendPath(parent, existing.Name)
		if candidate.Name != existing.Name {
			return nil, schemaEvolutionError(path, "field removal, rename, or reorder is not supported")
		}
		if canonicalFieldType(candidate.Type) != canonicalFieldType(existing.Type) {
			return nil, schemaEvolutionError(path, fmt.Sprintf("type change %s -> %s is not supported", existing.Type, candidate.Type))
		}
		if !sameOptionalInt64(candidate.Precision, existing.Precision) || !sameOptionalInt64(candidate.Scale, existing.Scale) {
			return nil, schemaEvolutionError(path, "decimal precision or scale change is not supported")
		}
		if candidate.RoundingMode != existing.RoundingMode {
			return nil, schemaEvolutionError(path, "decimal rounding mode change is not supported")
		}
		if canonicalFieldMode(candidate.Mode) != canonicalFieldMode(existing.Mode) {
			return nil, schemaEvolutionError(path, fmt.Sprintf("mode change %s -> %s is not supported", canonicalFieldMode(existing.Mode), canonicalFieldMode(candidate.Mode)))
		}
		if isRecord(existing) {
			nested, err := validateSchemaLevel(existing.Fields, candidate.Fields, path)
			if err != nil {
				return nil, err
			}
			additions = append(additions, nested...)
		} else if len(candidate.Fields) != 0 {
			return nil, schemaEvolutionError(path, "non-RECORD fields cannot gain nested fields")
		}
	}
	for _, added := range proposed[len(current):] {
		if err := validateAddedField(added, appendPath(parent, added.Name)); err != nil {
			return nil, err
		}
		additions = append(additions, SchemaAddition{Path: appendPath(parent, added.Name), Field: added})
	}
	return additions, nil
}

func sameOptionalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validateAddedField(field Field, path []string) error {
	mode := canonicalFieldMode(field.Mode)
	if mode != "NULLABLE" && mode != "REPEATED" {
		return schemaEvolutionError(path, "new fields must be NULLABLE or REPEATED")
	}
	for _, nested := range field.Fields {
		if err := validateAddedField(nested, appendPath(path, nested.Name)); err != nil {
			return err
		}
	}
	return nil
}

func schemaEvolutionError(path []string, reason string) error {
	location := strings.Join(path, ".")
	if location == "" {
		location = "schema"
	}
	return fmt.Errorf("%w: capability=%s field=%s: %s; fix_hint=preserve existing fields and append only NULLABLE or REPEATED fields", ErrInvalid, CapabilitySchemaAdditiveV1, location, reason)
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
