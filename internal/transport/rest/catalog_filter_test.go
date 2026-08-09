package rest

import (
	"errors"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestDatasetLabelFilterGrammarAndANDSemantics(t *testing.T) {
	filters, err := parseDatasetLabelFilters("labels.department:receiving labels.active")
	if err != nil {
		t.Fatal(err)
	}
	filtered := filterDatasetsByLabels([]domain.Dataset{
		{ID: "receiving_active", Labels: map[string]string{"department": "receiving", "active": "true"}},
		{ID: "receiving_inactive", Labels: map[string]string{"department": "receiving"}},
		{ID: "shipping_active", Labels: map[string]string{"department": "shipping", "active": "true"}},
	}, filters)
	if len(filtered) != 1 || filtered[0].ID != "receiving_active" {
		t.Fatalf("filtered datasets = %#v", filtered)
	}
}

func TestDatasetLabelFilterRejectsUnsupportedGrammar(t *testing.T) {
	for _, raw := range []string{"department:receiving", "labels.", "labels.department:", "labels.Department", "labels.department:receiving:extra"} {
		t.Run(raw, func(t *testing.T) {
			_, err := parseDatasetLabelFilters(raw)
			if !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("filter %q error = %v, want invalid", raw, err)
			}
		})
	}
}

func TestDatasetMetadataViewValidation(t *testing.T) {
	for name, want := range map[string]error{
		"": nil, "METADATA": nil, "ACL": domain.ErrUnsupported,
		"FULL": domain.ErrUnsupported, "DATASET_VIEW_UNSPECIFIED": domain.ErrUnsupported,
		"unknown": domain.ErrInvalid,
	} {
		t.Run(name, func(t *testing.T) {
			err := validateDatasetMetadataView(name)
			if want == nil && err != nil {
				t.Fatalf("view %q error = %v", name, err)
			}
			if want != nil && !errors.Is(err, want) {
				t.Fatalf("view %q error = %v, want %v", name, err, want)
			}
		})
	}
}
