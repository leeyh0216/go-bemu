package domain

// Official BigQuery resource and schema semantics:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#Dataset
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
//   - https://cloud.google.com/bigquery/docs/schemas

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	projectIDPattern  = regexp.MustCompile(`^[a-z][a-z0-9-]{2,62}$`)
	resourceIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type Project struct {
	ID           string
	FriendlyName string
	Description  string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (p Project) Validate() error {
	if !projectIDPattern.MatchString(p.ID) {
		return fmt.Errorf("%w: project ID %q must match %s", ErrInvalid, p.ID, projectIDPattern)
	}
	return nil
}

type Dataset struct {
	ProjectID                    string
	ID                           string
	FriendlyName                 string
	Description                  string
	Location                     string
	Labels                       map[string]string
	DefaultTableExpirationMs     *int64
	DefaultPartitionExpirationMs *int64
	CreatedAt                    time.Time
	UpdatedAt                    time.Time
	// Hidden marks emulator-owned anonymous result datasets. BigQuery hides
	// anonymous datasets from datasets.list unless all=true.
	// https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored
	Hidden bool
}

func (d Dataset) Validate() error {
	if !projectIDPattern.MatchString(d.ProjectID) {
		return fmt.Errorf("%w: invalid project ID %q", ErrInvalid, d.ProjectID)
	}
	if len(d.ID) > 1024 || !resourceIDPattern.MatchString(d.ID) {
		return fmt.Errorf("%w: invalid dataset ID %q", ErrInvalid, d.ID)
	}
	if d.DefaultTableExpirationMs != nil && *d.DefaultTableExpirationMs < 0 {
		return fmt.Errorf("%w: default table expiration must be non-negative", ErrInvalid)
	}
	if d.DefaultPartitionExpirationMs != nil && *d.DefaultPartitionExpirationMs < 0 {
		return fmt.Errorf("%w: default partition expiration must be non-negative", ErrInvalid)
	}
	return nil
}

type Field struct {
	Name        string
	Type        string
	Mode        string
	Description string
	// Precision and Scale retain parameter presence from the BigQuery schema.
	// Storage adapters resolve omitted parameters without mutating this model.
	Precision *int64
	Scale     *int64
	Fields    []Field
}

func (f Field) Validate() error {
	if len(f.Name) > 1024 || !resourceIDPattern.MatchString(f.Name) {
		return fmt.Errorf("%w: invalid field name %q", ErrInvalid, f.Name)
	}
	t := strings.ToUpper(f.Type)
	switch t {
	case "GEOGRAPHY":
		return fmt.Errorf("%w: capability=%s type GEOGRAPHY has no local semantic implementation", ErrUnsupported, GapGeographyUnsupportedV1)
	case "BOOL", "BOOLEAN", "INT64", "INTEGER", "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "STRING", "BYTES", "DATE", "DATETIME", "TIME", "TIMESTAMP", "JSON", "RECORD", "STRUCT":
	default:
		return fmt.Errorf("%w: unsupported field type %q", ErrInvalid, f.Type)
	}
	if t == "NUMERIC" || t == "BIGNUMERIC" {
		if _, err := f.EffectiveDecimalParameters(); err != nil {
			return err
		}
	} else if f.Precision != nil || f.Scale != nil {
		return fmt.Errorf("%w: precision and scale are valid only for NUMERIC or BIGNUMERIC field %q", ErrInvalid, f.Name)
	}
	mode := strings.ToUpper(f.Mode)
	if mode != "" && mode != "NULLABLE" && mode != "REQUIRED" && mode != "REPEATED" {
		return fmt.Errorf("%w: unsupported field mode %q", ErrInvalid, f.Mode)
	}
	if (t == "RECORD" || t == "STRUCT") && len(f.Fields) == 0 {
		return fmt.Errorf("%w: %s field %q requires nested fields", ErrInvalid, t, f.Name)
	}
	if t != "RECORD" && t != "STRUCT" && len(f.Fields) != 0 {
		return fmt.Errorf("%w: scalar field %q of type %s must not define nested fields", ErrInvalid, f.Name, t)
	}
	for _, nested := range f.Fields {
		if err := nested.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type TimePartitioning struct {
	Type         string
	Field        string
	ExpirationMs int64
}

type Range struct {
	Start    int64
	End      int64
	Interval int64
}

type RangePartitioning struct {
	Field string
	Range Range
}

type Table struct {
	ProjectID         string
	DatasetID         string
	ID                string
	FriendlyName      string
	Description       string
	Labels            map[string]string
	Type              string
	Schema            []Field
	Location          string
	ExpirationTime    *time.Time
	TimePartitioning  *TimePartitioning
	RangePartitioning *RangePartitioning
	ClusteringFields  []string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (t Table) Validate() error {
	if !projectIDPattern.MatchString(t.ProjectID) || len(t.DatasetID) > 1024 || len(t.ID) > 1024 || !resourceIDPattern.MatchString(t.DatasetID) || !resourceIDPattern.MatchString(t.ID) {
		return fmt.Errorf("%w: invalid table reference %s:%s.%s", ErrInvalid, t.ProjectID, t.DatasetID, t.ID)
	}
	if len(t.Schema) == 0 {
		return fmt.Errorf("%w: a table schema requires at least one field", ErrInvalid)
	}
	if err := validateFieldList(t.Schema, nil); err != nil {
		return err
	}
	return nil
}

func validateFieldList(fields []Field, parent []string) error {
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if err := field.Validate(); err != nil {
			return err
		}
		key := strings.ToLower(field.Name)
		if _, ok := seen[key]; ok {
			path := strings.Join(append(parent, field.Name), ".")
			return fmt.Errorf("%w: duplicate field %q", ErrInvalid, path)
		}
		seen[key] = struct{}{}
		if err := validateFieldList(field.Fields, append(parent, field.Name)); err != nil {
			return err
		}
	}
	return nil
}
