package rest

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func validateTableMetadataView(value string) error {
	switch value {
	case "", "BASIC":
		return nil
	case "STORAGE_STATS":
		return nil
	case "FULL", "TABLE_METADATA_VIEW_UNSPECIFIED":
		return fmt.Errorf("%w: table view %q requires storage statistics", domain.ErrUnsupported, value)
	default:
		return fmt.Errorf("%w: unknown table view %q", domain.ErrInvalid, value)
	}
}

func (s *Server) applyTableMetadataView(ctx context.Context, table domain.Table, resource *tableResource, view string) error {
	if view != "STORAGE_STATS" {
		return nil
	}
	if s.tableStatistics == nil {
		return fmt.Errorf("%w: table view %q requires storage statistics", domain.ErrUnsupported, view)
	}
	statistics, err := s.tableStatistics.TableStatistics(ctx, domain.TableReference{
		ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID,
	})
	if err != nil {
		return fmt.Errorf("read table storage statistics: %w", err)
	}
	resource.NumRows = strconv.FormatInt(statistics.RowCount, 10)
	resource.NumBytes = strconv.FormatInt(statistics.LogicalBytes, 10)
	resource.NumLongTermBytes = "0"
	resource.NumActiveLogicalBytes = strconv.FormatInt(statistics.LogicalBytes, 10)
	resource.NumLongTermLogicalBytes = "0"
	resource.NumTotalLogicalBytes = strconv.FormatInt(statistics.LogicalBytes, 10)
	resource.NumActivePhysicalBytes = strconv.FormatInt(statistics.PhysicalBytes, 10)
	resource.NumLongTermPhysicalBytes = "0"
	resource.NumTotalPhysicalBytes = strconv.FormatInt(statistics.PhysicalBytes, 10)
	return nil
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
