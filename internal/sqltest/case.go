package sqltest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

const (
	SchemaVersion  = 1
	maxFixtureSize = 1 << 20
)

type RowOrder string

const (
	RowOrderNone      RowOrder = "none"
	RowOrderOrdered   RowOrder = "ordered"
	RowOrderUnordered RowOrder = "unordered"
)

type ExpectedKind string

const (
	ExpectedRows     ExpectedKind = "rows"
	ExpectedAffected ExpectedKind = "affected"
	ExpectedError    ExpectedKind = "error"
)

type ErrorPhase string

const (
	ErrorPhaseAnalyze ErrorPhase = "analyze"
	ErrorPhaseExecute ErrorPhase = "execute"
)

type Field struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	Mode         string  `json:"mode"`
	Precision    *int64  `json:"precision,omitempty"`
	Scale        *int64  `json:"scale,omitempty"`
	RoundingMode *string `json:"roundingMode,omitempty"`
	Fields       []Field `json:"fields,omitempty"`
}

type Table struct {
	TableID           string             `json:"tableId"`
	Schema            []Field            `json:"schema"`
	TimePartitioning  *TimePartitioning  `json:"timePartitioning,omitempty"`
	RangePartitioning *RangePartitioning `json:"rangePartitioning,omitempty"`
	Rows              [][]any            `json:"rows"`
}

type TimePartitioning struct {
	Type         string `json:"type"`
	Field        string `json:"field"`
	ExpirationMs int64  `json:"expirationMs,omitempty"`
}

type IntegerRange struct {
	Start    int64 `json:"start"`
	End      int64 `json:"end"`
	Interval int64 `json:"interval"`
}

type RangePartitioning struct {
	Field string       `json:"field"`
	Range IntegerRange `json:"range"`
}

type Dataset struct {
	DatasetID string  `json:"datasetId"`
	Location  string  `json:"location"`
	Tables    []Table `json:"tables"`
}

type Project struct {
	ProjectID string    `json:"projectId"`
	Datasets  []Dataset `json:"datasets"`
}

type FixtureDataset struct {
	Projects []Project `json:"projects"`
}

type ExpectedFailure struct {
	Phase ErrorPhase `json:"phase"`
	Code  string     `json:"code"`
}

type ExpectedTable struct {
	ProjectID string   `json:"projectId"`
	DatasetID string   `json:"datasetId"`
	TableID   string   `json:"tableId"`
	Exists    bool     `json:"exists"`
	RowOrder  RowOrder `json:"rowOrder"`
	Schema    []Field  `json:"schema,omitempty"`
	Rows      [][]any  `json:"rows,omitempty"`
}

type Expected struct {
	Kind         ExpectedKind     `json:"kind"`
	Schema       []Field          `json:"schema,omitempty"`
	Rows         [][]any          `json:"rows,omitempty"`
	AffectedRows *int64           `json:"affectedRows,omitempty"`
	Error        *ExpectedFailure `json:"error,omitempty"`
	Tables       []ExpectedTable  `json:"tables,omitempty"`
}

type Case struct {
	ID             string
	DefaultProject string
	DefaultDataset string
	RowOrder       RowOrder
	Dataset        FixtureDataset
	SQL            string
	Expected       Expected
}

type descriptor struct {
	SchemaVersion  int      `json:"schemaVersion"`
	CaseID         string   `json:"caseId"`
	DefaultProject string   `json:"defaultProject"`
	DefaultDataset string   `json:"defaultDataset"`
	RowOrder       RowOrder `json:"rowOrder"`
}

var (
	caseIDPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)
	shortTargetToken = regexp.MustCompile(`(?i)\b` + "b" + `q\b`)
)

func Load(root string) ([]Case, error) {
	return LoadFS(os.DirFS("."), filepath.ToSlash(root))
}

func LoadFS(fsys fs.FS, root string) ([]Case, error) {
	entries, err := fs.ReadDir(fsys, root)
	if err != nil {
		return nil, fmt.Errorf("read SQL case root %q: %w", root, err)
	}
	cases := make([]Case, 0, len(entries))
	seen := make(map[string]string, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			return nil, fmt.Errorf("SQL case root contains unexpected file %q", entry.Name())
		}
		casePath := path.Join(root, entry.Name())
		loaded, err := loadCase(fsys, casePath)
		if err != nil {
			return nil, fmt.Errorf("load SQL case directory %q: %w", entry.Name(), err)
		}
		if previous, exists := seen[loaded.ID]; exists {
			return nil, fmt.Errorf("duplicate SQL case ID %q in %q and %q", loaded.ID, previous, entry.Name())
		}
		seen[loaded.ID] = entry.Name()
		cases = append(cases, loaded)
	}
	slices.SortFunc(cases, func(left, right Case) int { return strings.Compare(left.ID, right.ID) })
	if len(cases) == 0 {
		return nil, errors.New("SQL case root contains no cases")
	}
	return cases, nil
}

func loadCase(fsys fs.FS, root string) (Case, error) {
	const (
		descriptorName = "case.json"
		datasetName    = "dataset.json"
		queryName      = "query.sql"
		expectedName   = "expected.json"
	)
	required := []string{descriptorName, datasetName, expectedName, queryName}
	entries, err := fs.ReadDir(fsys, root)
	if err != nil {
		return Case{}, err
	}
	observed := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !slices.Contains(required, entry.Name()) {
			return Case{}, fmt.Errorf("unexpected case entry %q", entry.Name())
		}
		observed[entry.Name()] = struct{}{}
	}
	for _, name := range required {
		if _, ok := observed[name]; !ok {
			return Case{}, fmt.Errorf("required case file %q is missing", name)
		}
	}

	var metadata descriptor
	if err := decodeStrictJSON(fsys, path.Join(root, descriptorName), &metadata); err != nil {
		return Case{}, err
	}
	var dataset FixtureDataset
	if err := decodeStrictJSON(fsys, path.Join(root, datasetName), &dataset); err != nil {
		return Case{}, err
	}
	var expected Expected
	if err := decodeStrictJSON(fsys, path.Join(root, expectedName), &expected); err != nil {
		return Case{}, err
	}
	queryBytes, err := readBounded(fsys, path.Join(root, queryName))
	if err != nil {
		return Case{}, err
	}
	query := strings.TrimSpace(string(queryBytes))
	loaded := Case{
		ID: metadata.CaseID, DefaultProject: metadata.DefaultProject,
		DefaultDataset: metadata.DefaultDataset, RowOrder: metadata.RowOrder,
		Dataset: dataset, SQL: query, Expected: expected,
	}
	if err := validateCase(metadata, loaded); err != nil {
		return Case{}, err
	}
	return loaded, nil
}

func decodeStrictJSON(fsys fs.FS, name string, destination any) error {
	contents, err := readBounded(fsys, name)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("decode %s: trailing JSON value", name)
		}
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}

func readBounded(fsys fs.FS, name string) ([]byte, error) {
	file, err := fsys.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maxFixtureSize+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	if len(contents) > maxFixtureSize {
		return nil, fmt.Errorf("read %s: fixture exceeds %d bytes", name, maxFixtureSize)
	}
	return contents, nil
}

func validateCase(metadata descriptor, test Case) error {
	if metadata.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schemaVersion = %d, want %d", metadata.SchemaVersion, SchemaVersion)
	}
	if !caseIDPattern.MatchString(test.ID) {
		return fmt.Errorf("caseId %q must match %s", test.ID, caseIDPattern)
	}
	if test.DefaultProject == "" || test.DefaultDataset == "" {
		return errors.New("defaultProject and defaultDataset are required")
	}
	if test.SQL == "" {
		return errors.New("query.sql is empty")
	}
	if err := validatePortableGoogleSQL(test.SQL); err != nil {
		return err
	}
	if err := validateDataset(test.Dataset); err != nil {
		return err
	}
	if !containsDataset(test.Dataset, test.DefaultProject, test.DefaultDataset) {
		return fmt.Errorf("default dataset %q.%q is not present in dataset.json", test.DefaultProject, test.DefaultDataset)
	}
	return validateExpected(test.RowOrder, test.Expected)
}

func containsDataset(dataset FixtureDataset, projectID, datasetID string) bool {
	for _, project := range dataset.Projects {
		if project.ProjectID != projectID {
			continue
		}
		for _, candidate := range project.Datasets {
			if candidate.DatasetID == datasetID {
				return true
			}
		}
	}
	return false
}

func validateDataset(dataset FixtureDataset) error {
	projects := make(map[string]struct{}, len(dataset.Projects))
	for _, project := range dataset.Projects {
		if project.ProjectID == "" {
			return errors.New("dataset projectId is required")
		}
		if _, exists := projects[project.ProjectID]; exists {
			return fmt.Errorf("duplicate dataset projectId %q", project.ProjectID)
		}
		projects[project.ProjectID] = struct{}{}
		datasets := make(map[string]struct{}, len(project.Datasets))
		for _, dataset := range project.Datasets {
			if dataset.DatasetID == "" || dataset.Location == "" {
				return errors.New("datasetId and location are required")
			}
			if _, exists := datasets[dataset.DatasetID]; exists {
				return fmt.Errorf("duplicate datasetId %q", dataset.DatasetID)
			}
			datasets[dataset.DatasetID] = struct{}{}
			tables := make(map[string]struct{}, len(dataset.Tables))
			for _, table := range dataset.Tables {
				if table.TableID == "" {
					return errors.New("tableId is required")
				}
				if _, exists := tables[table.TableID]; exists {
					return fmt.Errorf("duplicate tableId %q", table.TableID)
				}
				tables[table.TableID] = struct{}{}
				if err := validateFields(table.Schema, "table "+table.TableID); err != nil {
					return err
				}
				if err := validateTablePartitioning(table); err != nil {
					return fmt.Errorf("table %q: %w", table.TableID, err)
				}
				for rowIndex, row := range table.Rows {
					if len(row) != len(table.Schema) {
						return fmt.Errorf("table %q row %d has %d values, want %d", table.TableID, rowIndex, len(row), len(table.Schema))
					}
				}
			}
		}
	}
	return nil
}

func validateTablePartitioning(table Table) error {
	if table.TimePartitioning != nil && table.RangePartitioning != nil {
		return errors.New("timePartitioning and rangePartitioning are mutually exclusive")
	}
	if partitioning := table.TimePartitioning; partitioning != nil {
		switch strings.ToUpper(partitioning.Type) {
		case "DAY", "HOUR", "MONTH", "YEAR":
		default:
			return fmt.Errorf("timePartitioning type %q is invalid", partitioning.Type)
		}
		if partitioning.Field != "" {
			field, found := fixtureTopLevelField(table.Schema, partitioning.Field)
			if !found {
				return fmt.Errorf("timePartitioning field %q does not exist", partitioning.Field)
			}
			switch canonicalType(field.Type) {
			case "DATE", "DATETIME", "TIMESTAMP":
			default:
				return fmt.Errorf("timePartitioning field %q has type %q", partitioning.Field, field.Type)
			}
		}
		if partitioning.ExpirationMs < 0 {
			return errors.New("timePartitioning expirationMs must be non-negative")
		}
	}
	if partitioning := table.RangePartitioning; partitioning != nil {
		field, found := fixtureTopLevelField(table.Schema, partitioning.Field)
		if !found {
			return fmt.Errorf("rangePartitioning field %q does not exist", partitioning.Field)
		}
		if canonicalType(field.Type) != "INT64" {
			return fmt.Errorf("rangePartitioning field %q has type %q", partitioning.Field, field.Type)
		}
		if partitioning.Range.End <= partitioning.Range.Start || partitioning.Range.Interval <= 0 {
			return errors.New("rangePartitioning requires end > start and interval > 0")
		}
	}
	return nil
}

func fixtureTopLevelField(fields []Field, name string) (Field, bool) {
	if name == "" {
		return Field{}, false
	}
	for _, field := range fields {
		if strings.EqualFold(field.Name, name) {
			return field, true
		}
	}
	return Field{}, false
}

func validateFields(fields []Field, owner string) error {
	if len(fields) == 0 {
		return fmt.Errorf("%s schema is empty", owner)
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		name := strings.ToLower(field.Name)
		if name == "" || field.Type == "" {
			return fmt.Errorf("%s contains a field without name or type", owner)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("%s contains duplicate field %q", owner, field.Name)
		}
		seen[name] = struct{}{}
		if field.Mode != "NULLABLE" && field.Mode != "REQUIRED" && field.Mode != "REPEATED" {
			return fmt.Errorf("%s field %q has invalid mode %q", owner, field.Name, field.Mode)
		}
		if len(field.Fields) > 0 {
			if !strings.EqualFold(field.Type, "RECORD") && !strings.EqualFold(field.Type, "STRUCT") {
				return fmt.Errorf("%s field %q has nested fields with type %q", owner, field.Name, field.Type)
			}
			if err := validateFields(field.Fields, owner+"."+field.Name); err != nil {
				return err
			}
		}
		canonical := fixtureFieldsToDomain([]Field{field})[0]
		if err := canonical.Validate(); err != nil {
			return fmt.Errorf("%s field %q: %w", owner, field.Name, err)
		}
	}
	return nil
}

func validateExpected(order RowOrder, expected Expected) error {
	switch expected.Kind {
	case ExpectedRows:
		if order != RowOrderOrdered && order != RowOrderUnordered {
			return errors.New("row result requires explicit ordered or unordered rowOrder")
		}
		if expected.AffectedRows != nil || expected.Error != nil {
			return errors.New("row result cannot define affectedRows or error")
		}
		if err := validateFields(expected.Schema, "expected"); err != nil {
			return err
		}
		for index, row := range expected.Rows {
			if len(row) != len(expected.Schema) {
				return fmt.Errorf("expected row %d has %d values, want %d", index, len(row), len(expected.Schema))
			}
		}
	case ExpectedAffected:
		if order != RowOrderNone {
			return errors.New("affected result requires rowOrder none")
		}
		if expected.AffectedRows == nil || *expected.AffectedRows < 0 || len(expected.Schema) != 0 || len(expected.Rows) != 0 || expected.Error != nil {
			return errors.New("affected result requires only a non-negative affectedRows")
		}
	case ExpectedError:
		if order != RowOrderNone {
			return errors.New("error result requires rowOrder none")
		}
		if expected.Error == nil || expected.Error.Code == "" || expected.AffectedRows != nil || len(expected.Schema) != 0 || len(expected.Rows) != 0 {
			return errors.New("error result requires only phase and code")
		}
		if expected.Error.Phase != ErrorPhaseAnalyze && expected.Error.Phase != ErrorPhaseExecute {
			return fmt.Errorf("error phase %q is invalid", expected.Error.Phase)
		}
	default:
		return fmt.Errorf("expected kind %q is invalid", expected.Kind)
	}
	return validateExpectedTables(expected.Tables)
}

func validateExpectedTables(tables []ExpectedTable) error {
	seen := make(map[string]struct{}, len(tables))
	for index, table := range tables {
		if table.ProjectID == "" || table.DatasetID == "" || table.TableID == "" {
			return fmt.Errorf("expected table %d requires projectId, datasetId, and tableId", index)
		}
		key := tableOutcomeKey(table.ProjectID, table.DatasetID, table.TableID)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate expected table %q", key)
		}
		seen[key] = struct{}{}
		if !table.Exists {
			if table.RowOrder != RowOrderNone || len(table.Schema) != 0 || len(table.Rows) != 0 {
				return fmt.Errorf("absent expected table %q cannot define rowOrder, schema, or rows", key)
			}
			continue
		}
		if table.RowOrder != RowOrderOrdered && table.RowOrder != RowOrderUnordered {
			return fmt.Errorf("existing expected table %q requires explicit ordered or unordered rowOrder", key)
		}
		if err := validateFields(table.Schema, "expected table "+key); err != nil {
			return err
		}
		for rowIndex, row := range table.Rows {
			if len(row) != len(table.Schema) {
				return fmt.Errorf("expected table %q row %d has %d values, want %d", key, rowIndex, len(row), len(table.Schema))
			}
		}
	}
	return nil
}

func validatePortableGoogleSQL(query string) error {
	lower := strings.ToLower(query)
	for _, forbidden := range []string{
		"sp" + "ark", "py" + "sp" + "ark", "fl" + "ink", "google-cloud-" + "bigquery",
		"0." + "44.2", "2." + "1.31", "3." + "5.8",
		"timesta" + "mptz", "read_" + "parquet(", "pragma ", "struct_" + "pack(",
	} {
		if strings.Contains(lower, forbidden) {
			return fmt.Errorf("query.sql contains forbidden target or engine syntax %q", forbidden)
		}
	}
	if shortTargetToken.MatchString(query) {
		return errors.New("query.sql contains forbidden target token")
	}
	if strings.Contains(query, "::") {
		return errors.New("query.sql contains engine-specific cast syntax")
	}
	return nil
}
