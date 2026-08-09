package rest

import (
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func validateTableMetadataView(value string) error {
	switch value {
	case "", "BASIC":
		return nil
	case "STORAGE_STATS", "FULL", "TABLE_METADATA_VIEW_UNSPECIFIED":
		return fmt.Errorf("%w: table view %q requires storage statistics", domain.ErrUnsupported, value)
	default:
		return fmt.Errorf("%w: unknown table view %q", domain.ErrInvalid, value)
	}
}

// projectSelectedTableFields selects top-level schema fields in caller order.
// Nested field projection remains a Storage Read capability, not a catalog
// metadata shorthand.
func projectSelectedTableFields(fields []domain.Field, raw string) ([]domain.Field, error) {
	if raw == "" {
		return fields, nil
	}
	byName := make(map[string]domain.Field, len(fields))
	for _, field := range fields {
		byName[strings.ToLower(field.Name)] = field
	}
	parts := strings.Split(raw, ",")
	projected := make([]domain.Field, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		key := strings.ToLower(name)
		field, ok := byName[key]
		if name == "" || !ok {
			return nil, fmt.Errorf("%w: selectedFields contains unknown schema field %q", domain.ErrInvalid, name)
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("%w: selectedFields repeats schema field %q", domain.ErrInvalid, name)
		}
		seen[key] = struct{}{}
		projected = append(projected, field)
	}
	return projected, nil
}
