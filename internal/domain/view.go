package domain

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var analysisFingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// View is a logical GoogleSQL view definition. It is deliberately distinct
// from Table: a view has no table-only physical schema policy such as
// partitioning, clustering, or expiration-driven storage lifecycle.
type View struct {
	ProjectID           string
	DatasetID           string
	ID                  string
	FriendlyName        string
	Description         string
	Labels              map[string]string
	Query               string
	UseLegacySQL        bool
	Schema              []Field
	Dependencies        []TableReference
	AnalysisFingerprint string
	Location            string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (view View) Validate() error {
	if !projectIDPattern.MatchString(view.ProjectID) || len(view.DatasetID) > 1024 || len(view.ID) > 1024 ||
		!resourceIDPattern.MatchString(view.DatasetID) || !resourceIDPattern.MatchString(view.ID) {
		return fmt.Errorf("%w: invalid view reference %s:%s.%s", ErrInvalid, view.ProjectID, view.DatasetID, view.ID)
	}
	if view.UseLegacySQL {
		return fmt.Errorf("%w: legacy SQL views are not supported", ErrUnsupported)
	}
	if strings.TrimSpace(view.Query) == "" {
		return fmt.Errorf("%w: view query is required", ErrInvalid)
	}
	if !analysisFingerprintPattern.MatchString(view.AnalysisFingerprint) {
		return fmt.Errorf("%w: view analysis fingerprint is invalid", ErrInvalid)
	}
	if len(view.Schema) == 0 {
		return fmt.Errorf("%w: view schema requires at least one field", ErrInvalid)
	}
	if err := validateFieldList(view.Schema, nil); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(view.Dependencies))
	for _, dependency := range view.Dependencies {
		if err := validateViewReference(dependency); err != nil {
			return err
		}
		key := strings.ToLower(dependency.ProjectID + "\x00" + dependency.DatasetID + "\x00" + dependency.TableID)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: duplicate view dependency %s:%s.%s", ErrInvalid, dependency.ProjectID, dependency.DatasetID, dependency.TableID)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateViewReference(reference TableReference) error {
	return (Table{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID, ID: reference.TableID,
		Schema: []Field{{Name: "placeholder", Type: "STRING"}},
	}).Validate()
}

func CloneView(view View) View {
	clone := view
	clone.Labels = cloneViewLabels(view.Labels)
	clone.Schema = CloneFields(view.Schema)
	clone.Dependencies = append([]TableReference(nil), view.Dependencies...)
	return clone
}

func cloneViewLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	clone := make(map[string]string, len(labels))
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}
