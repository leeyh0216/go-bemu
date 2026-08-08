package sqltest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

func TestGoogleSQLRegressionCases(t *testing.T) {
	cases, err := Load("testdata/cases")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range cases {
		t.Run(test.ID, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			outcome, err := runRegressionCase(ctx, t, test)
			if err != nil {
				t.Fatalf("case %s setup: %v", test.ID, err)
			}
			if err := Compare(test, outcome); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func runRegressionCase(ctx context.Context, t *testing.T, test Case) (Outcome, error) {
	t.Helper()
	warehouse, err := duckdb.New("")
	if err != nil {
		return Outcome{}, err
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := regressionClock{instant: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(
		memory.NewCatalogRepository(), warehouse, clock,
		application.WithDDLStorage(warehouse), application.WithTableDataReader(warehouse),
	)
	if err := createFixtureCatalog(ctx, catalog, test.Dataset); err != nil {
		return Outcome{}, err
	}
	gateway, err := googlesqladapter.NewGateway(catalog)
	if err != nil {
		return Outcome{}, err
	}
	trackedGateway := &regressionGateway{inner: gateway}
	trackedExecutor := &regressionExecutor{inner: warehouse}
	queries, err := application.NewQueryService(
		memory.NewJobRepository(), clock, &regressionIDs{},
		application.WithGoogleSQLGateway(trackedGateway),
		application.WithStatementExecutor(trackedExecutor),
		application.WithStatementMaterializer(warehouse),
		application.WithQueryDestinationCatalog(catalog),
		application.WithQueryDDLExecutor(catalog),
	)
	if err != nil {
		return Outcome{}, err
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = queries.Close(closeCtx)
	})
	if err := seedFixtureRows(ctx, queries, test.Dataset); err != nil {
		return Outcome{}, err
	}

	trackedGateway.lastError = nil
	trackedExecutor.lastError = nil
	job, runErr := queries.RunSync(ctx, application.QueryInput{
		ProjectID: test.DefaultProject, DefaultProjectID: test.DefaultProject,
		DefaultDataset: test.DefaultDataset, Location: fixtureLocation(test), SQL: test.SQL,
	})
	var outcome Outcome
	if runErr != nil {
		outcome = failureOutcome(trackedGateway.lastError, trackedExecutor.lastError, runErr)
	} else if job.Error != nil {
		jobErr := errors.New(job.Error.Message)
		outcome = failureOutcome(trackedGateway.lastError, trackedExecutor.lastError, jobErr)
	} else {
		outcome = Outcome{Result: job.Result}
	}
	tables, err := observeExpectedTables(ctx, catalog, test.Expected.Tables)
	if err != nil {
		return Outcome{}, err
	}
	outcome.Tables = tables
	return outcome, nil
}

func observeExpectedTables(
	ctx context.Context,
	catalog *application.CatalogService,
	expected []ExpectedTable,
) (map[string]TableOutcome, error) {
	observed := make(map[string]TableOutcome, len(expected))
	for _, assertion := range expected {
		key := tableOutcomeKey(assertion.ProjectID, assertion.DatasetID, assertion.TableID)
		table, err := catalog.GetTable(ctx, assertion.ProjectID, assertion.DatasetID, assertion.TableID)
		if errors.Is(err, domain.ErrNotFound) {
			observed[key] = TableOutcome{}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("observe table %s metadata: %w", key, err)
		}
		page, err := catalog.ListTableData(
			ctx, assertion.ProjectID, assertion.DatasetID, assertion.TableID, 0,
			ports.TableDataMaxResults{Value: 10_000, Present: true},
		)
		if err != nil {
			return nil, fmt.Errorf("observe table %s rows: %w", key, err)
		}
		if int64(len(page.Rows)) != page.TotalRows {
			return nil, fmt.Errorf("observe table %s returned %d of %d rows", key, len(page.Rows), page.TotalRows)
		}
		observed[key] = TableOutcome{Exists: true, Schema: table.Schema, Rows: page.Rows}
	}
	return observed, nil
}

func createFixtureCatalog(ctx context.Context, catalog *application.CatalogService, fixture FixtureDataset) error {
	for _, project := range fixture.Projects {
		if _, err := catalog.CreateProject(ctx, domain.Project{ID: project.ProjectID}); err != nil {
			return fmt.Errorf("create project %s: %w", project.ProjectID, err)
		}
		for _, dataset := range project.Datasets {
			if _, err := catalog.CreateDataset(ctx, domain.Dataset{
				ProjectID: project.ProjectID, ID: dataset.DatasetID, Location: dataset.Location,
			}); err != nil {
				return fmt.Errorf("create dataset %s.%s: %w", project.ProjectID, dataset.DatasetID, err)
			}
			for _, table := range dataset.Tables {
				resource := domain.Table{
					ProjectID: project.ProjectID, DatasetID: dataset.DatasetID, ID: table.TableID,
					Type: "TABLE", Location: dataset.Location, Schema: fixtureFieldsToDomain(table.Schema),
				}
				if table.TimePartitioning != nil {
					resource.TimePartitioning = &domain.TimePartitioning{
						Type: table.TimePartitioning.Type, Field: table.TimePartitioning.Field,
						ExpirationMs: table.TimePartitioning.ExpirationMs,
					}
				}
				if table.RangePartitioning != nil {
					resource.RangePartitioning = &domain.RangePartitioning{
						Field: table.RangePartitioning.Field,
						Range: domain.Range{
							Start: table.RangePartitioning.Range.Start, End: table.RangePartitioning.Range.End,
							Interval: table.RangePartitioning.Range.Interval,
						},
					}
				}
				if _, err := catalog.CreateTable(ctx, resource); err != nil {
					return fmt.Errorf("create table %s.%s.%s: %w", project.ProjectID, dataset.DatasetID, table.TableID, err)
				}
			}
		}
	}
	return nil
}

func seedFixtureRows(ctx context.Context, queries *application.QueryService, fixture FixtureDataset) error {
	for _, project := range fixture.Projects {
		for _, dataset := range project.Datasets {
			for _, table := range dataset.Tables {
				if len(table.Rows) == 0 {
					continue
				}
				statement, err := fixtureInsertSQL(project.ProjectID, dataset.DatasetID, table)
				if err != nil {
					return err
				}
				job, err := queries.RunSync(ctx, application.QueryInput{
					ProjectID: project.ProjectID, DefaultProjectID: project.ProjectID,
					DefaultDataset: dataset.DatasetID, Location: dataset.Location, SQL: statement,
				})
				if err != nil {
					return fmt.Errorf("seed table %s: %w", table.TableID, err)
				}
				if job.Error != nil {
					return fmt.Errorf("seed table %s: %s: %s", table.TableID, job.Error.Reason, job.Error.Message)
				}
			}
		}
	}
	return nil
}

func fixtureInsertSQL(projectID, datasetID string, table Table) (string, error) {
	columns := make([]string, len(table.Schema))
	for index, field := range table.Schema {
		columns[index] = quoteGoogleSQLIdentifier(field.Name)
	}
	rows := make([]string, len(table.Rows))
	for rowIndex, row := range table.Rows {
		values := make([]string, len(row))
		for columnIndex, value := range row {
			literal, err := fixtureLiteral(table.Schema[columnIndex], value)
			if err != nil {
				return "", fmt.Errorf("table %s row %d field %s: %w", table.TableID, rowIndex, table.Schema[columnIndex].Name, err)
			}
			values[columnIndex] = literal
		}
		rows[rowIndex] = "(" + strings.Join(values, ", ") + ")"
	}
	reference := quoteGoogleSQLIdentifier(projectID + "." + datasetID + "." + table.TableID)
	return "INSERT INTO " + reference + " (" + strings.Join(columns, ", ") + ") VALUES " + strings.Join(rows, ", "), nil
}

func fixtureLiteral(field Field, value any) (string, error) {
	if value == nil {
		return "CAST(NULL AS " + fixtureTypeSQL(field) + ")", nil
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		items, err := sequenceValues(value)
		if err != nil {
			return "", err
		}
		element := field
		element.Mode = "REQUIRED"
		literals := make([]string, len(items))
		for index, item := range items {
			literals[index], err = fixtureLiteral(element, item)
			if err != nil {
				return "", err
			}
		}
		return "ARRAY<" + fixtureTypeSQL(element) + ">[" + strings.Join(literals, ", ") + "]", nil
	}
	switch canonicalType(field.Type) {
	case "BOOL":
		value, ok := value.(bool)
		if !ok {
			return "", fmt.Errorf("value has type %T, want BOOL", value)
		}
		return strings.ToUpper(strconv.FormatBool(value)), nil
	case "INT64", "FLOAT64":
		number, ok := value.(json.Number)
		if !ok {
			return "", fmt.Errorf("value has type %T, want JSON number", value)
		}
		return number.String(), nil
	case "NUMERIC", "BIGNUMERIC":
		text, err := scalarText(value)
		if err != nil {
			return "", err
		}
		return canonicalType(field.Type) + " " + quoteGoogleSQLString(text), nil
	case "STRING":
		text, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("value has type %T, want STRING", value)
		}
		return quoteGoogleSQLString(text), nil
	case "BYTES":
		text, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("value has type %T, want base64 BYTES", value)
		}
		decoded, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return "", err
		}
		return "CAST(" + quoteGoogleSQLString(string(decoded)) + " AS BYTES)", nil
	case "DATE", "DATETIME", "TIME", "TIMESTAMP":
		text, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("value has type %T, want temporal string", value)
		}
		return canonicalType(field.Type) + " " + quoteGoogleSQLString(text), nil
	case "JSON":
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return "CAST(" + quoteGoogleSQLString(string(encoded)) + " AS JSON)", nil
	case "RECORD":
		object, ok := value.(map[string]any)
		if !ok {
			return "", fmt.Errorf("value has type %T, want object", value)
		}
		children := make([]string, len(field.Fields))
		for index, child := range field.Fields {
			childValue, exists := object[child.Name]
			if !exists {
				return "", fmt.Errorf("object is missing field %q", child.Name)
			}
			literal, err := fixtureLiteral(child, childValue)
			if err != nil {
				return "", err
			}
			children[index] = literal + " AS " + quoteGoogleSQLIdentifier(child.Name)
		}
		return "STRUCT(" + strings.Join(children, ", ") + ")", nil
	default:
		return "", fmt.Errorf("unsupported fixture type %q", field.Type)
	}
}

func fixtureTypeSQL(field Field) string {
	if canonicalType(field.Type) != "RECORD" {
		return canonicalType(field.Type)
	}
	children := make([]string, len(field.Fields))
	for index, child := range field.Fields {
		children[index] = quoteGoogleSQLIdentifier(child.Name) + " " + fixtureTypeSQL(child)
	}
	return "STRUCT<" + strings.Join(children, ", ") + ">"
}

func quoteGoogleSQLIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "\\`") + "`"
}

func quoteGoogleSQLString(value string) string {
	value = strings.ReplaceAll(value, "'", "''")
	return "'" + value + "'"
}

func fixtureLocation(test Case) string {
	for _, project := range test.Dataset.Projects {
		if project.ProjectID != test.DefaultProject {
			continue
		}
		for _, dataset := range project.Datasets {
			if dataset.DatasetID == test.DefaultDataset {
				return dataset.Location
			}
		}
	}
	return "US"
}

func failureOutcome(analysisErr, executionErr, fallback error) Outcome {
	phase := ErrorPhaseExecute
	cause := executionErr
	if analysisErr != nil {
		phase = ErrorPhaseAnalyze
		cause = analysisErr
	} else if cause == nil {
		cause = fallback
	}
	return Outcome{Failure: &Failure{Phase: phase, Code: stableErrorCode(cause)}}
}

var stableCodePattern = regexp.MustCompile(`(?:^|[ ;:])code=([A-Za-z0-9._-]+)`)

func stableErrorCode(err error) string {
	if err == nil {
		return "unknown"
	}
	if match := stableCodePattern.FindStringSubmatch(err.Error()); len(match) == 2 {
		return match[1]
	}
	for _, candidate := range []struct {
		target error
		code   string
	}{
		{domain.ErrInvalidQuery, "domain.invalid-query"},
		{domain.ErrInvalid, "domain.invalid"},
		{domain.ErrUnsupported, "domain.unsupported"},
		{domain.ErrNotFound, "domain.not-found"},
		{domain.ErrConflict, "domain.conflict"},
		{domain.ErrPrecondition, "domain.precondition"},
		{domain.ErrBackend, "domain.backend"},
	} {
		if errors.Is(err, candidate.target) {
			return candidate.code
		}
	}
	return "unknown"
}

type regressionClock struct{ instant time.Time }

func (clock regressionClock) Now() time.Time { return clock.instant }

type regressionIDs struct{ next int }

func (ids *regressionIDs) NewID() string {
	ids.next++
	return fmt.Sprintf("sql-regression-%d", ids.next)
}

type regressionGateway struct {
	inner     ports.GoogleSQLGateway
	lastError error
}

func (gateway *regressionGateway) Analyze(ctx context.Context, request ports.QueryRequest) (semantic.Statement, error) {
	statement, err := gateway.inner.Analyze(ctx, request)
	gateway.lastError = err
	return statement, err
}

type regressionExecutor struct {
	inner     ports.StatementExecutor
	lastError error
}

func (executor *regressionExecutor) ExecuteStatement(ctx context.Context, statement semantic.Statement) (domain.QueryResult, error) {
	result, err := executor.inner.ExecuteStatement(ctx, statement)
	executor.lastError = err
	return result, err
}
