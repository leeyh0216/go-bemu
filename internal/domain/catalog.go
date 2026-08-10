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
	// RoundingMode retains the REST enum exactly. The empty value means the
	// client omitted roundingMode; ROUNDING_MODE_UNSPECIFIED remains a distinct
	// explicit value even though both use BigQuery's half-away default.
	RoundingMode RoundingMode
	Fields       []Field
}

func (f Field) Validate() error {
	if len(f.Name) > 1024 || !resourceIDPattern.MatchString(f.Name) {
		return fmt.Errorf("%w: invalid field name %q", ErrInvalid, f.Name)
	}
	if IsPartitionPseudoColumn(f.Name) {
		return fmt.Errorf("%w: field name %q is reserved for ingestion-time partitioning", ErrInvalid, f.Name)
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
		if _, err := f.EffectiveRoundingMode(); err != nil {
			return err
		}
	} else if f.Precision != nil || f.Scale != nil || f.RoundingMode != "" {
		return fmt.Errorf("%w: precision, scale, and rounding mode are valid only for NUMERIC or BIGNUMERIC field %q", ErrInvalid, f.Name)
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

const (
	PartitionTimePseudoColumn = "_PARTITIONTIME"
	PartitionDatePseudoColumn = "_PARTITIONDATE"
)

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
	// PrimaryKey holds the ordered, unenforced BigQuery table primary-key
	// columns. It is persisted metadata; enforcement belongs to callers that
	// opt into a feature such as Storage Write API CDC.
	PrimaryKey []string
	CreatedAt  time.Time
	UpdatedAt  time.Time
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
	if err := validateTablePartitioning(t); err != nil {
		return err
	}
	if err := validateTableClustering(t); err != nil {
		return err
	}
	if err := validateTablePrimaryKey(t); err != nil {
		return err
	}
	return nil
}

func validateTablePrimaryKey(table Table) error {
	if table.PrimaryKey == nil {
		return nil
	}
	if len(table.PrimaryKey) == 0 {
		return fmt.Errorf("%w: primary key must contain at least one column", ErrInvalid)
	}
	fields := make(map[string]Field, len(table.Schema))
	for _, field := range table.Schema {
		fields[strings.ToLower(field.Name)] = field
	}
	seen := make(map[string]struct{}, len(table.PrimaryKey))
	for _, name := range table.PrimaryKey {
		canonical := strings.ToLower(strings.TrimSpace(name))
		field, ok := fields[canonical]
		if !ok {
			return fmt.Errorf("%w: primary key column %q does not exist in the top-level schema", ErrInvalid, name)
		}
		if _, duplicate := seen[canonical]; duplicate {
			return fmt.Errorf("%w: primary key column %q is duplicated", ErrInvalid, name)
		}
		if strings.EqualFold(field.Mode, "REPEATED") {
			return fmt.Errorf("%w: primary key column %q cannot be repeated", ErrInvalid, name)
		}
		seen[canonical] = struct{}{}
	}
	return nil
}

func IsPartitionPseudoColumn(name string) bool {
	return strings.EqualFold(name, PartitionTimePseudoColumn) || strings.EqualFold(name, PartitionDatePseudoColumn)
}

func (t Table) IngestionTimePartitioning() (string, bool) {
	if t.TimePartitioning == nil || strings.TrimSpace(t.TimePartitioning.Field) != "" {
		return "", false
	}
	typ := strings.ToUpper(strings.TrimSpace(t.TimePartitioning.Type))
	switch typ {
	case "DAY", "HOUR", "MONTH", "YEAR":
		return typ, true
	default:
		return "", false
	}
}

func validateTablePartitioning(table Table) error {
	if table.TimePartitioning != nil && table.RangePartitioning != nil {
		return fmt.Errorf("%w: time and range partitioning are mutually exclusive", ErrInvalid)
	}
	if partitioning := table.TimePartitioning; partitioning != nil {
		typ := strings.ToUpper(strings.TrimSpace(partitioning.Type))
		switch typ {
		case "DAY", "HOUR", "MONTH", "YEAR":
		default:
			return fmt.Errorf("%w: invalid time partitioning type %q", ErrInvalid, partitioning.Type)
		}
		if partitioning.ExpirationMs < 0 {
			return fmt.Errorf("%w: time partition expiration must be non-negative", ErrInvalid)
		}
		if strings.TrimSpace(partitioning.Field) != "" {
			field, found := topLevelField(table.Schema, partitioning.Field)
			if !found {
				return fmt.Errorf("%w: time partition field %q does not exist", ErrInvalid, partitioning.Field)
			}
			switch strings.ToUpper(field.Type) {
			case "DATE":
				if typ == "HOUR" {
					return fmt.Errorf("%w: DATE partition field %q does not support HOUR partitioning", ErrInvalid, partitioning.Field)
				}
			case "DATETIME", "TIMESTAMP":
			default:
				return fmt.Errorf("%w: time partition field %q has type %q", ErrInvalid, partitioning.Field, field.Type)
			}
		}
	}
	if partitioning := table.RangePartitioning; partitioning != nil {
		field, found := topLevelField(table.Schema, partitioning.Field)
		if !found || !strings.EqualFold(field.Type, "INT64") && !strings.EqualFold(field.Type, "INTEGER") {
			return fmt.Errorf("%w: range partition field %q must be INT64", ErrInvalid, partitioning.Field)
		}
		if partitioning.Range.End <= partitioning.Range.Start || partitioning.Range.Interval <= 0 {
			return fmt.Errorf("%w: range partitioning requires end > start and interval > 0", ErrInvalid)
		}
	}
	return nil
}

func validateTableClustering(table Table) error {
	if table.ClusteringFields == nil {
		return nil
	}
	if len(table.ClusteringFields) == 0 || len(table.ClusteringFields) > 4 {
		return fmt.Errorf("%w: clustering requires between one and four fields", ErrInvalid)
	}
	seen := make(map[string]struct{}, len(table.ClusteringFields))
	for _, name := range table.ClusteringFields {
		field, found := topLevelField(table.Schema, name)
		if !found {
			return fmt.Errorf("%w: clustering field %q does not exist at the top level", ErrInvalid, name)
		}
		key := strings.ToLower(field.Name)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: duplicate clustering field %q", ErrInvalid, name)
		}
		seen[key] = struct{}{}
		if strings.EqualFold(field.Mode, "REPEATED") {
			return fmt.Errorf("%w: clustering field %q must not be repeated", ErrInvalid, name)
		}
		switch strings.ToUpper(field.Type) {
		case "BIGNUMERIC", "BOOL", "BOOLEAN", "DATE", "DATETIME", "INT64", "INTEGER", "NUMERIC", "STRING", "TIMESTAMP":
		default:
			return fmt.Errorf("%w: clustering field %q has unsupported type %q", ErrInvalid, name, field.Type)
		}
	}
	return nil
}

func topLevelField(fields []Field, name string) (Field, bool) {
	for _, field := range fields {
		if strings.EqualFold(field.Name, name) {
			return field, true
		}
	}
	return Field{}, false
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
