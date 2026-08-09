package rest

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

// datasetLabelFilter models the documented datasets.list filter grammar:
// labels.<name>[:<value>] terms separated by whitespace are ANDed together.
type datasetLabelFilter struct {
	name     string
	value    string
	hasValue bool
}

func parseDatasetLabelFilters(raw string) ([]datasetLabelFilter, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	terms := strings.Fields(raw)
	filters := make([]datasetLabelFilter, 0, len(terms))
	for _, term := range terms {
		nameAndValue, ok := strings.CutPrefix(term, "labels.")
		if !ok {
			return nil, invalidDatasetFilter(term)
		}
		name, value, hasValue := strings.Cut(nameAndValue, ":")
		if name == "" || !validDatasetLabelFilterPart(name) || (hasValue && (value == "" || !validDatasetLabelFilterPart(value))) {
			return nil, invalidDatasetFilter(term)
		}
		filters = append(filters, datasetLabelFilter{name: name, value: value, hasValue: hasValue})
	}
	return filters, nil
}

func invalidDatasetFilter(term string) error {
	return fmt.Errorf("%w: filter term %q must use labels.<name>[:<value>]", domain.ErrInvalid, term)
}

func validDatasetLabelFilterPart(value string) bool {
	if len(value) > 63 {
		return false
	}
	for _, character := range value {
		if !(unicode.IsLower(character) || unicode.IsDigit(character) || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func filterDatasetsByLabels(datasets []domain.Dataset, filters []datasetLabelFilter) []domain.Dataset {
	if len(filters) == 0 {
		return datasets
	}
	filtered := make([]domain.Dataset, 0, len(datasets))
	for _, dataset := range datasets {
		matches := true
		for _, filter := range filters {
			value, present := dataset.Labels[filter.name]
			if !present || (filter.hasValue && value != filter.value) {
				matches = false
				break
			}
		}
		if matches {
			filtered = append(filtered, dataset)
		}
	}
	return filtered
}

func validateDatasetMetadataView(value string) error {
	switch value {
	case "", "METADATA":
		return nil
	case "ACL", "FULL", "DATASET_VIEW_UNSPECIFIED":
		return fmt.Errorf("%w: datasetView %q requires dataset ACL/IAM metadata", domain.ErrUnsupported, value)
	default:
		return fmt.Errorf("%w: unknown datasetView %q", domain.ErrInvalid, value)
	}
}
