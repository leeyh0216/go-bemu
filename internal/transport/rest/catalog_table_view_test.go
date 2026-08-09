package rest

import (
	"errors"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestProjectSelectedTableFields(t *testing.T) {
	fields := []domain.Field{{Name: "id", Type: "INT64"}, {Name: "payload", Type: "STRING"}}
	projected, err := projectSelectedTableFields(fields, "payload, id")
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 2 || projected[0].Name != "payload" || projected[1].Name != "id" {
		t.Fatalf("projected fields = %#v", projected)
	}
	for _, raw := range []string{"unknown", "id,id", "id,"} {
		if _, err := projectSelectedTableFields(fields, raw); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("selectedFields %q error = %v, want invalid", raw, err)
		}
	}
}

func TestTableMetadataViewValidation(t *testing.T) {
	for name, want := range map[string]error{"": nil, "BASIC": nil, "STORAGE_STATS": domain.ErrUnsupported, "FULL": domain.ErrUnsupported, "invalid": domain.ErrInvalid} {
		t.Run(name, func(t *testing.T) {
			err := validateTableMetadataView(name)
			if want == nil && err != nil {
				t.Fatalf("view %q error = %v", name, err)
			}
			if want != nil && !errors.Is(err, want) {
				t.Fatalf("view %q error = %v, want %v", name, err, want)
			}
		})
	}
}
