package googlesql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"

	gsql "github.com/goccy/go-googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

const (
	CapabilityResolvedStatementV1 = "query.googlesql.resolved-statement-v1"
	ErrorTableNotFoundV1          = "query.googlesql.table-not-found-v1"
	ErrorColumnNotFoundV1         = "query.googlesql.column-not-found-v1"
	ErrorTypeNotFoundV1           = "query.googlesql.type-not-found-v1"
	ErrorAnalysisInvalidV1        = "query.googlesql.analysis-invalid-v1"
)

// Gateway resolves every supported statement class through the same pinned
// GoogleSQL parser and analyzer entrypoint. It rebuilds a per-request catalog
// snapshot so analysis observes one owned copy of canonical BQEMU metadata.
type Gateway struct {
	catalog ports.GoogleSQLCatalogReader
}

var _ ports.GoogleSQLGateway = (*Gateway)(nil)

type statementAnalysisError struct {
	kind  queryast.StatementKind
	cause error
}

var _ ports.GoogleSQLAnalysisError = (*statementAnalysisError)(nil)

func (err *statementAnalysisError) Error() string                         { return err.cause.Error() }
func (err *statementAnalysisError) Unwrap() error                         { return err.cause }
func (err *statementAnalysisError) StatementKind() queryast.StatementKind { return err.kind }

func NewGateway(catalog ports.GoogleSQLCatalogReader) (*Gateway, error) {
	if catalog == nil || isNilCatalogReader(catalog) {
		return nil, fmt.Errorf("%w: canonical catalog is required by the GoogleSQL analyzer", domain.ErrPrecondition)
	}
	if err := initialize(); err != nil {
		return nil, err
	}
	return &Gateway{catalog: catalog}, nil
}

func isNilCatalogReader(reader ports.GoogleSQLCatalogReader) bool {
	value := reflect.ValueOf(reader)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Analyze returns only an owned semantic projection of the parsed and
// resolved tree. Analyzer diagnostics retain their original cause.
func (gateway *Gateway) Analyze(ctx context.Context, request ports.QueryRequest) (semantic.Statement, error) {
	if err := ctx.Err(); err != nil {
		return semantic.Statement{}, err
	}
	document, err := parseExternal(request.SQL)
	if err != nil {
		return semantic.Statement{}, err
	}
	defer runtime.KeepAlive(document.owner)
	snapshot, err := buildCatalogSnapshot(ctx, gateway.catalog, request)
	if err != nil {
		return semantic.Statement{}, err
	}
	options, err := analyzerOptions(snapshot.language)
	if err != nil {
		return semantic.Statement{}, analyzerBoundaryFailure(err)
	}
	if len(document.statements) > 1 {
		return gateway.analyzeScript(ctx, request, document, snapshot, options)
	}
	return gateway.analyzeSingleStatement(ctx, request, document, snapshot, options)
}

func (gateway *Gateway) analyzeSingleStatement(
	ctx context.Context,
	request ports.QueryRequest,
	document parsedDocument,
	snapshot *catalogSnapshot,
	options *gsql.AnalyzerOptions,
) (semantic.Statement, error) {
	external := document.statements[0]
	kind, classified := externalStatementKind(external)
	statementFailure := func(err error) error {
		if err == nil || !classified {
			return err
		}
		return &statementAnalysisError{kind: kind, cause: err}
	}
	mapper := statementMapper{sourceDigest: document.source.Digest()}
	syntax, mappingErr := mapper.mapStatement(external)
	// CREATE VIEW is the sole statement whose target must be catalog-visible to
	// the official analyzer before it is resolved. For every other statement we
	// retain a mapper error until after analysis so analyzer diagnostics (for
	// example an unknown column type) keep their established public
	// classification.
	if create, isCreateView := syntax.(*queryast.CreateViewStatement); isCreateView {
		if mappingErr != nil {
			return semantic.Statement{}, statementFailure(mappingErr)
		}
		if err := registerCreateViewTarget(snapshot, request, create); err != nil {
			return semantic.Statement{}, statementFailure(err)
		}
	}
	if _, requiresCatalogBinding := external.(*gsql.ASTAlterTableStatement); requiresCatalogBinding {
		return projectCatalogStatement(request, syntax, snapshot)
	}
	output, err := gsql.AnalyzeStatementFromParserAST(
		external, options, request.SQL, snapshot.root, snapshot.typeFactory,
	)
	if err != nil {
		return semantic.Statement{}, statementFailure(fmt.Errorf("%w; input=%q", classifyAnalysisError(err), request.SQL))
	}
	if output == nil {
		return semantic.Statement{}, analyzerBoundaryFailure()
	}
	defer runtime.KeepAlive(output)
	resolved, err := output.ResolvedStatement()
	if err != nil || resolved == nil {
		return semantic.Statement{}, analyzerBoundaryFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return semantic.Statement{}, err
	}
	if mappingErr != nil {
		return semantic.Statement{}, statementFailure(mappingErr)
	}
	return projectResolvedStatement(ctx, request, syntax, snapshot, resolved)
}

func externalStatementKind(node gsql.ASTStatementNode) (queryast.StatementKind, bool) {
	switch node.(type) {
	case *gsql.ASTQueryStatement:
		return queryast.StatementSelect, true
	case *gsql.ASTInsertStatement:
		return queryast.StatementInsert, true
	case *gsql.ASTUpdateStatement:
		return queryast.StatementUpdate, true
	case *gsql.ASTDeleteStatement:
		return queryast.StatementDelete, true
	case *gsql.ASTMergeStatement:
		return queryast.StatementMerge, true
	case *gsql.ASTCreateTableStatement:
		return queryast.StatementCreateTable, true
	case *gsql.ASTDropStatement:
		return queryast.StatementDropTable, true
	case *gsql.ASTAlterTableStatement:
		return queryast.StatementAlterTable, true
	case *gsql.ASTTruncateStatement:
		return queryast.StatementTruncateTable, true
	case *gsql.ASTVariableDeclaration:
		return queryast.StatementDeclare, true
	case *gsql.ASTSingleAssignment:
		return queryast.StatementSet, true
	default:
		return "", false
	}
}

func projectCatalogStatement(
	request ports.QueryRequest,
	syntax queryast.Statement,
	snapshot *catalogSnapshot,
) (semantic.Statement, error) {
	projection := &resolvedProjection{snapshot: snapshot}
	bindings, err := projection.relationBindings(request, syntax)
	if err != nil {
		return semantic.Statement{}, err
	}
	return semantic.NewStatement(semantic.StatementDescriptor{
		Syntax: syntax, ResolvedKind: syntax.Kind(), RelationBindings: bindings,
		ExpressionsComplete: true,
	})
}

type catalogSnapshot struct {
	root        *gsql.SimpleCatalog
	typeFactory *gsql.TypeFactory
	language    *gsql.LanguageOptions
	tables      map[string]registeredTable
	datasets    map[string]*gsql.SimpleCatalog
}

type registeredTable struct {
	reference        domain.TableReference
	schema           []domain.Field
	timePartitioning *domain.TimePartitioning
	logical          []semantic.Type
}

func buildCatalogSnapshot(
	ctx context.Context,
	repository ports.GoogleSQLCatalogReader,
	request ports.QueryRequest,
) (*catalogSnapshot, error) {
	canonical, err := repository.GoogleSQLCatalogSnapshot(ctx)
	if err != nil {
		return nil, catalogReadFailure(err)
	}
	metadata := ownedCatalogMetadata(canonical)
	sort.Slice(metadata, func(i, j int) bool { return canonicalLess(metadata[i].Project.ID, metadata[j].Project.ID) })
	for projectIndex := range metadata {
		datasets := metadata[projectIndex].Datasets
		sort.Slice(datasets, func(i, j int) bool { return canonicalLess(datasets[i].Dataset.ID, datasets[j].Dataset.ID) })
		metadata[projectIndex].Datasets = datasets
		for datasetIndex := range datasets {
			tables := datasets[datasetIndex].Tables
			sort.Slice(tables, func(i, j int) bool { return canonicalLess(tables[i].ID, tables[j].ID) })
			metadata[projectIndex].Datasets[datasetIndex].Tables = tables
			views := datasets[datasetIndex].Views
			sort.Slice(views, func(i, j int) bool { return canonicalLess(views[i].ID, views[j].ID) })
			metadata[projectIndex].Datasets[datasetIndex].Views = views
		}
	}

	typeFactory, err := gsql.NewTypeFactory()
	if err != nil {
		return nil, analyzerBoundaryFailure()
	}
	language, err := analyzerLanguageOptions()
	if err != nil {
		return nil, analyzerBoundaryFailure()
	}
	root, err := gsql.NewSimpleCatalog("bqemu", typeFactory)
	if err != nil {
		return nil, analyzerBoundaryFailure()
	}
	if err := root.AddBuiltinFunctionsAndTypes(&gsql.BuiltinFunctionOptions{LanguageOptions: language}); err != nil {
		return nil, analyzerBoundaryFailure()
	}

	snapshot := &catalogSnapshot{
		root: root, typeFactory: typeFactory, language: language,
		tables: make(map[string]registeredTable), datasets: make(map[string]*gsql.SimpleCatalog),
	}
	defaultProject := request.DefaultProjectID
	if defaultProject == "" {
		defaultProject = request.ProjectID
	}
	var nextTableID int64 = 1
	for _, projectMetadata := range metadata {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := projectMetadata.Project.Validate(); err != nil {
			return nil, canonicalSchemaFailure(err)
		}
		projectCatalog, err := gsql.NewSimpleCatalog(projectMetadata.Project.ID, typeFactory)
		if err != nil {
			return nil, analyzerBoundaryFailure()
		}
		if err := root.AddCatalog2(projectMetadata.Project.ID, projectCatalog); err != nil {
			return nil, catalogShapeFailure()
		}
		for _, datasetMetadata := range projectMetadata.Datasets {
			if err := datasetMetadata.Dataset.Validate(); err != nil || datasetMetadata.Dataset.ProjectID != projectMetadata.Project.ID {
				return nil, canonicalSchemaFailure(domain.ErrInvalid)
			}
			datasetCatalog, err := gsql.NewSimpleCatalog(datasetMetadata.Dataset.ID, typeFactory)
			if err != nil {
				return nil, analyzerBoundaryFailure()
			}
			if err := projectCatalog.AddCatalog2(datasetMetadata.Dataset.ID, datasetCatalog); err != nil {
				return nil, catalogShapeFailure()
			}
			if projectMetadata.Project.ID == defaultProject {
				if err := root.AddCatalog2(datasetMetadata.Dataset.ID, datasetCatalog); err != nil {
					return nil, catalogShapeFailure()
				}
			}
			snapshot.datasets[catalogDatasetKey(projectMetadata.Project.ID, datasetMetadata.Dataset.ID)] = datasetCatalog
			for _, table := range datasetMetadata.Tables {
				if table.ProjectID != projectMetadata.Project.ID || table.DatasetID != datasetMetadata.Dataset.ID {
					return nil, canonicalSchemaFailure(domain.ErrInvalid)
				}
				registered, tableNode, err := registerTable(typeFactory, table, nextTableID)
				if err != nil {
					return nil, err
				}
				nextTableID++
				if err := datasetCatalog.AddTable2(table.ID, tableNode); err != nil {
					return nil, catalogShapeFailure()
				}
				// The official AST represents a fully quoted three-part path as one
				// identifier. Register the canonical dotted name as a root table
				// alias in addition to the nested catalogs.
				if err := root.AddTable2(tableFullName(registered.reference), tableNode); err != nil {
					return nil, catalogShapeFailure()
				}
				if projectMetadata.Project.ID == defaultProject {
					if err := root.AddTable2(datasetMetadata.Dataset.ID+"."+table.ID, tableNode); err != nil {
						return nil, catalogShapeFailure()
					}
				}
				if projectMetadata.Project.ID == defaultProject && datasetMetadata.Dataset.ID == request.DefaultDataset {
					if err := root.AddTable2(table.ID, tableNode); err != nil {
						return nil, catalogShapeFailure()
					}
				}
				snapshot.tables[tableKey(registered.reference)] = registered
			}
			for _, view := range datasetMetadata.Views {
				if view.ProjectID != projectMetadata.Project.ID || view.DatasetID != datasetMetadata.Dataset.ID {
					return nil, canonicalSchemaFailure(domain.ErrInvalid)
				}
				if err := view.Validate(); err != nil {
					return nil, canonicalSchemaFailure(err)
				}
				registered, tableNode, err := registerTable(typeFactory, domain.Table{
					ProjectID: view.ProjectID, DatasetID: view.DatasetID, ID: view.ID, Type: "TABLE", Schema: view.Schema,
				}, nextTableID)
				if err != nil {
					return nil, err
				}
				nextTableID++
				if _, exists := snapshot.tables[tableKey(registered.reference)]; exists {
					return nil, canonicalSchemaFailure(domain.ErrConflict)
				}
				if err := datasetCatalog.AddTable2(view.ID, tableNode); err != nil {
					return nil, catalogShapeFailure()
				}
				if err := root.AddTable2(tableFullName(registered.reference), tableNode); err != nil {
					return nil, catalogShapeFailure()
				}
				if projectMetadata.Project.ID == defaultProject {
					if err := root.AddTable2(datasetMetadata.Dataset.ID+"."+view.ID, tableNode); err != nil {
						return nil, catalogShapeFailure()
					}
				}
				if projectMetadata.Project.ID == defaultProject && datasetMetadata.Dataset.ID == request.DefaultDataset {
					if err := root.AddTable2(view.ID, tableNode); err != nil {
						return nil, catalogShapeFailure()
					}
				}
				snapshot.tables[tableKey(registered.reference)] = registered
			}
		}
	}
	return snapshot, nil
}

func registerCreateViewTarget(snapshot *catalogSnapshot, request ports.QueryRequest, statement *queryast.CreateViewStatement) error {
	if snapshot == nil || statement == nil || statement.Target() == nil {
		return analyzerBoundaryFailureAt("create-view-target")
	}
	reference, err := resolveAnalyzedPath(request, statement.Target().Path().Segments())
	if err != nil {
		return err
	}
	key := tableKey(reference)
	if _, exists := snapshot.tables[key]; exists {
		return nil
	}
	dataset := snapshot.datasets[catalogDatasetKey(reference.ProjectID, reference.DatasetID)]
	if dataset == nil {
		return fmt.Errorf("%w: %s", domain.ErrNotFound, tableFullName(reference))
	}
	node, err := gsql.NewSimpleTable(reference.TableID, int64(len(snapshot.tables)+1_000_000))
	if err != nil {
		return analyzerBoundaryFailure()
	}
	fullName := tableFullName(reference)
	if err := node.SetFullName(fullName); err != nil {
		return analyzerBoundaryFailure()
	}
	if err := dataset.AddTable2(reference.TableID, node); err != nil {
		return catalogShapeFailure()
	}
	if err := snapshot.root.AddTable2(fullName, node); err != nil {
		return catalogShapeFailure()
	}
	defaultProject := request.DefaultProjectID
	if defaultProject == "" {
		defaultProject = request.ProjectID
	}
	if reference.ProjectID == defaultProject {
		if err := snapshot.root.AddTable2(reference.DatasetID+"."+reference.TableID, node); err != nil {
			return catalogShapeFailure()
		}
	}
	if reference.ProjectID == defaultProject && reference.DatasetID == request.DefaultDataset {
		if err := snapshot.root.AddTable2(reference.TableID, node); err != nil {
			return catalogShapeFailure()
		}
	}
	snapshot.tables[key] = registeredTable{reference: reference}
	return nil
}

func ownedCatalogMetadata(snapshot ports.GoogleSQLCatalogSnapshot) []ports.GoogleSQLProjectSnapshot {
	projects := make([]ports.GoogleSQLProjectSnapshot, len(snapshot.Projects))
	for projectIndex, project := range snapshot.Projects {
		projects[projectIndex].Project = project.Project
		projects[projectIndex].Datasets = make([]ports.GoogleSQLDatasetSnapshot, len(project.Datasets))
		for datasetIndex, dataset := range project.Datasets {
			projects[projectIndex].Datasets[datasetIndex].Dataset = cloneDataset(dataset.Dataset)
			projects[projectIndex].Datasets[datasetIndex].Tables = make([]domain.Table, len(dataset.Tables))
			for tableIndex, table := range dataset.Tables {
				projects[projectIndex].Datasets[datasetIndex].Tables[tableIndex] = cloneTable(table)
			}
			projects[projectIndex].Datasets[datasetIndex].Views = make([]domain.View, len(dataset.Views))
			for viewIndex, view := range dataset.Views {
				projects[projectIndex].Datasets[datasetIndex].Views[viewIndex] = domain.CloneView(view)
			}
		}
	}
	return projects
}

func cloneDataset(dataset domain.Dataset) domain.Dataset {
	clone := dataset
	clone.Labels = cloneStringMap(dataset.Labels)
	clone.DefaultTableExpirationMs = domain.CloneOptionalInt64(dataset.DefaultTableExpirationMs)
	clone.DefaultPartitionExpirationMs = domain.CloneOptionalInt64(dataset.DefaultPartitionExpirationMs)
	return clone
}

func cloneTable(table domain.Table) domain.Table {
	clone := table
	clone.Labels = cloneStringMap(table.Labels)
	clone.Schema = domain.CloneFields(table.Schema)
	clone.ClusteringFields = append([]string(nil), table.ClusteringFields...)
	if table.ExpirationTime != nil {
		value := *table.ExpirationTime
		clone.ExpirationTime = &value
	}
	if table.TimePartitioning != nil {
		value := *table.TimePartitioning
		clone.TimePartitioning = &value
	}
	if table.RangePartitioning != nil {
		value := *table.RangePartitioning
		clone.RangePartitioning = &value
	}
	return clone
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func canonicalLess(left, right string) bool {
	leftFolded, rightFolded := strings.ToLower(left), strings.ToLower(right)
	if leftFolded == rightFolded {
		return left < right
	}
	return leftFolded < rightFolded
}

func registerTable(
	typeFactory *gsql.TypeFactory,
	table domain.Table,
	serializationID int64,
) (registeredTable, *gsql.SimpleTable, error) {
	reference := domain.TableReference{ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID}
	if err := table.Validate(); err != nil {
		return registeredTable{}, nil, fmt.Errorf("canonical table %s schema=%#v: %w", tableFullName(reference), table.Schema, canonicalSchemaFailure(err))
	}
	fullName := tableFullName(reference)
	tableNode, err := gsql.NewSimpleTable(table.ID, serializationID)
	if err != nil {
		return registeredTable{}, nil, analyzerBoundaryFailure()
	}
	if err := tableNode.SetFullName(fullName); err != nil {
		return registeredTable{}, nil, analyzerBoundaryFailure()
	}
	pseudoFields := ingestionTimePseudoFields(table)
	logical := make([]semantic.Type, 0, len(table.Schema)+len(pseudoFields))
	for _, field := range table.Schema {
		columnType, semanticType, err := catalogFieldType(typeFactory, field)
		if err != nil {
			return registeredTable{}, nil, err
		}
		column, err := gsql.NewSimpleColumn(fullName, field.Name, columnType, false, true)
		if err != nil {
			return registeredTable{}, nil, analyzerBoundaryFailure()
		}
		if err := tableNode.AddColumn2(column, false); err != nil {
			return registeredTable{}, nil, catalogShapeFailure()
		}
		logical = append(logical, semanticType)
	}
	for _, field := range pseudoFields {
		columnType, semanticType, err := catalogFieldType(typeFactory, field)
		if err != nil {
			return registeredTable{}, nil, err
		}
		writable := strings.EqualFold(field.Name, domain.PartitionTimePseudoColumn)
		column, err := gsql.NewSimpleColumn(fullName, field.Name, columnType, true, writable)
		if err != nil {
			return registeredTable{}, nil, analyzerBoundaryFailure()
		}
		if err := tableNode.AddColumn2(column, false); err != nil {
			return registeredTable{}, nil, catalogShapeFailure()
		}
		logical = append(logical, semanticType)
	}
	var timePartitioning *domain.TimePartitioning
	if table.TimePartitioning != nil {
		clone := *table.TimePartitioning
		timePartitioning = &clone
	}
	return registeredTable{
		reference: reference, schema: domain.CloneFields(table.Schema),
		timePartitioning: timePartitioning, logical: logical,
	}, tableNode, nil
}

func ingestionTimePseudoFields(table domain.Table) []domain.Field {
	typ, ok := table.IngestionTimePartitioning()
	if !ok {
		return nil
	}
	fields := []domain.Field{{Name: domain.PartitionTimePseudoColumn, Type: "TIMESTAMP"}}
	if typ == "DAY" {
		fields = append(fields, domain.Field{Name: domain.PartitionDatePseudoColumn, Type: "DATE"})
	}
	return fields
}

func catalogFieldType(typeFactory *gsql.TypeFactory, field domain.Field) (gsql.Googlesql_TypeNode, semantic.Type, error) {
	descriptor, err := semanticDescriptor(field)
	if err != nil {
		return nil, semantic.Type{}, err
	}
	logical, err := semantic.NewType(descriptor)
	if err != nil {
		return nil, semantic.Type{}, canonicalSchemaFailure(err)
	}
	typeNode, err := googleSQLType(typeFactory, descriptor)
	if err != nil {
		return nil, semantic.Type{}, err
	}
	return typeNode, logical, nil
}

func semanticDescriptor(field domain.Field) (semantic.TypeDescriptor, error) {
	var descriptor semantic.TypeDescriptor
	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		descriptor.Kind = semantic.TypeBool
	case "INT64", "INTEGER":
		descriptor.Kind = semantic.TypeInt64
	case "FLOAT64", "FLOAT":
		descriptor.Kind = semantic.TypeFloat64
	case "NUMERIC":
		descriptor.Kind = semantic.TypeNumeric
		descriptor.Precision = domain.CloneOptionalInt64(field.Precision)
		descriptor.Scale = domain.CloneOptionalInt64(field.Scale)
		descriptor.RoundingMode = field.RoundingMode
	case "BIGNUMERIC":
		descriptor.Kind = semantic.TypeBigNumeric
		descriptor.Precision = domain.CloneOptionalInt64(field.Precision)
		descriptor.Scale = domain.CloneOptionalInt64(field.Scale)
		descriptor.RoundingMode = field.RoundingMode
	case "STRING":
		descriptor.Kind = semantic.TypeString
	case "BYTES":
		descriptor.Kind = semantic.TypeBytes
	case "DATE":
		descriptor.Kind = semantic.TypeDate
	case "DATETIME":
		descriptor.Kind = semantic.TypeDatetime
	case "TIME":
		descriptor.Kind = semantic.TypeTime
	case "TIMESTAMP":
		descriptor.Kind = semantic.TypeTimestamp
	case "JSON":
		descriptor.Kind = semantic.TypeJSON
	case "STRUCT", "RECORD":
		descriptor.Kind = semantic.TypeStruct
		descriptor.Fields = make([]semantic.FieldDescriptor, 0, len(field.Fields))
		for _, nested := range field.Fields {
			nestedDescriptor, err := semanticDescriptor(nested)
			if err != nil {
				return semantic.TypeDescriptor{}, err
			}
			descriptor.Fields = append(descriptor.Fields, semantic.FieldDescriptor{Name: nested.Name, Type: nestedDescriptor})
		}
	case "GEOGRAPHY":
		return semantic.TypeDescriptor{}, fmt.Errorf(
			"%w: capability=%s canonical schema contains an unsupported logical type",
			domain.ErrUnsupported, domain.GapGeographyUnsupportedV1,
		)
	default:
		return semantic.TypeDescriptor{}, fmt.Errorf("%w: canonical schema contains an invalid logical type", domain.ErrInvalid)
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		element := descriptor
		descriptor = semantic.TypeDescriptor{Kind: semantic.TypeArray, Element: &element}
	}
	return descriptor, nil
}

func googleSQLType(typeFactory *gsql.TypeFactory, descriptor semantic.TypeDescriptor) (gsql.Googlesql_TypeNode, error) {
	var (
		typeNode gsql.Googlesql_TypeNode
		err      error
	)
	switch descriptor.Kind {
	case semantic.TypeBool:
		typeNode, err = typeFactory.GetBool()
	case semantic.TypeInt64:
		typeNode, err = typeFactory.GetInt64()
	case semantic.TypeFloat64:
		typeNode, err = typeFactory.GetDouble()
	case semantic.TypeNumeric:
		typeNode, err = typeFactory.GetNumeric()
	case semantic.TypeBigNumeric:
		typeNode, err = typeFactory.GetBignumeric()
	case semantic.TypeString:
		typeNode, err = typeFactory.GetString()
	case semantic.TypeBytes:
		typeNode, err = typeFactory.GetBytes()
	case semantic.TypeDate:
		typeNode, err = typeFactory.GetDate()
	case semantic.TypeDatetime:
		typeNode, err = typeFactory.GetDatetime()
	case semantic.TypeTime:
		typeNode, err = typeFactory.GetTime()
	case semantic.TypeTimestamp:
		typeNode, err = typeFactory.GetTimestamp()
	case semantic.TypeJSON:
		typeNode, err = typeFactory.GetJson()
	case semantic.TypeArray:
		if descriptor.Element == nil {
			return nil, catalogShapeFailure()
		}
		element, elementErr := googleSQLType(typeFactory, *descriptor.Element)
		if elementErr != nil {
			return nil, elementErr
		}
		typeNode, err = typeFactory.MakeArrayType2(element)
	case semantic.TypeStruct:
		fields := make([]*gsql.StructField, 0, len(descriptor.Fields))
		for _, field := range descriptor.Fields {
			fieldType, fieldErr := googleSQLType(typeFactory, field.Type)
			if fieldErr != nil {
				return nil, fieldErr
			}
			fields = append(fields, &gsql.StructField{Name: field.Name, Type_: fieldType})
		}
		typeNode, err = typeFactory.MakeStructType2(fields)
	default:
		return nil, fmt.Errorf("%w: capability=%s semantic type cannot be registered", domain.ErrUnsupported, CapabilityResolvedStatementV1)
	}
	if err != nil || typeNode == nil {
		return nil, analyzerBoundaryFailure()
	}
	return typeNode, nil
}

func analyzerLanguageOptions() (*gsql.LanguageOptions, error) {
	language, err := gsql.NewLanguageOptionsMaximumFeatures()
	if err != nil {
		return nil, err
	}
	if err := language.SetSupportsAllStatementKinds(); err != nil {
		return nil, err
	}
	if err := language.SetProductMode(gsql.ProductModeProductExternal); err != nil {
		return nil, err
	}
	return language, nil
}

func analyzerOptions(language *gsql.LanguageOptions) (*gsql.AnalyzerOptions, error) {
	options, err := gsql.NewAnalyzerOptions2()
	if err != nil {
		return nil, err
	}
	if err := options.SetLanguage(language); err != nil {
		return nil, err
	}
	if err := options.SetErrorMessageMode(gsql.ErrorMessageModeErrorMessageOneLine); err != nil {
		return nil, err
	}
	if err := options.SetErrorMessageStability(gsql.ErrorMessageStabilityProduction); err != nil {
		return nil, err
	}
	if err := options.SetPruneUnusedColumns(false); err != nil {
		return nil, err
	}
	if err := options.SetRecordParseLocations(true); err != nil {
		return nil, err
	}
	if err := options.SetParseLocationRecordType(gsql.ParseLocationRecordTypeParseLocationRecordFullNodeScope); err != nil {
		return nil, err
	}
	return options, nil
}

func tableFullName(reference domain.TableReference) string {
	return reference.ProjectID + "." + reference.DatasetID + "." + reference.TableID
}

func tableKey(reference domain.TableReference) string {
	return strings.ToLower(tableFullName(reference))
}

func catalogDatasetKey(projectID, datasetID string) string {
	return strings.ToLower(projectID + "\x00" + datasetID)
}

func catalogReadFailure(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: canonical catalog snapshot is unavailable: %v", domain.ErrBackend, err)
}

func canonicalSchemaFailure(err error) error {
	switch {
	case errors.Is(err, domain.ErrUnsupported):
		capability := domain.CapabilityEngineSchemaV1
		diagnostic := err.Error()
		switch {
		case strings.Contains(diagnostic, domain.CapabilityDecimalPrecision38V1):
			capability = domain.CapabilityDecimalPrecision38V1
		case strings.Contains(diagnostic, domain.GapGeographyUnsupportedV1):
			capability = domain.GapGeographyUnsupportedV1
		}
		return fmt.Errorf("%w: capability=%s canonical schema is outside the analyzer contract: %v", domain.ErrUnsupported, capability, err)
	case errors.Is(err, domain.ErrInvalid):
		return fmt.Errorf("%w: canonical schema is invalid: %v", domain.ErrInvalid, err)
	default:
		return analyzerBoundaryFailure(err)
	}
}

func catalogShapeFailure() error {
	return fmt.Errorf("%w: canonical catalog cannot be represented by the pinned analyzer", domain.ErrPrecondition)
}

func analyzerBoundaryFailure(causes ...error) error {
	if len(causes) > 0 && causes[0] != nil {
		return fmt.Errorf(
			"%w: capability=%s GoogleSQL analyzer boundary failed: %v",
			domain.ErrPrecondition, CapabilityResolvedStatementV1, causes[0],
		)
	}
	return fmt.Errorf("%w: capability=%s GoogleSQL analyzer boundary failed", domain.ErrPrecondition, CapabilityResolvedStatementV1)
}

func analyzerBoundaryFailureAt(stage string, causes ...error) error {
	if len(causes) > 0 && causes[0] != nil {
		return fmt.Errorf(
			"%w: capability=%s stage=%s GoogleSQL analyzer boundary failed: %v",
			domain.ErrPrecondition, CapabilityResolvedStatementV1, stage, causes[0],
		)
	}
	return fmt.Errorf(
		"%w: capability=%s stage=%s GoogleSQL analyzer boundary failed",
		domain.ErrPrecondition, CapabilityResolvedStatementV1, stage,
	)
}

func classifyAnalysisError(err error) error {
	diagnostic := strings.ToLower(err.Error())
	switch {
	case strings.Contains(diagnostic, "table not found"), strings.Contains(diagnostic, "not found: table"):
		return fmt.Errorf("%w: code=%s GoogleSQL table resolution failed: %v", domain.ErrNotFound, ErrorTableNotFoundV1, err)
	case strings.Contains(diagnostic, "unrecognized name"), strings.Contains(diagnostic, "name not found inside"):
		return fmt.Errorf("%w: code=%s GoogleSQL column resolution failed: %v", domain.ErrInvalidQuery, ErrorColumnNotFoundV1, err)
	case strings.Contains(diagnostic, "type not found"), strings.Contains(diagnostic, "unknown type"):
		return fmt.Errorf("%w: code=%s GoogleSQL type resolution failed: %v", domain.ErrInvalidQuery, ErrorTypeNotFoundV1, err)
	default:
		return fmt.Errorf("%w: code=%s GoogleSQL analysis rejected the statement: %v", domain.ErrInvalidQuery, ErrorAnalysisInvalidV1, err)
	}
}
