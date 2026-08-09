package googlesql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	gsql "github.com/goccy/go-googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const ddlCapability = "query.ddl.catalog-sync-v1"

var (
	initializeOnce sync.Once
	initializeErr  error
)

// Parser is safe for concurrent use. go-googlesql serializes access to its
// process-global transpiled module internally.
type Parser struct{}

var _ ports.DDLParser = (*Parser)(nil)

// NewParser initializes the pinned GoogleSQL module once per process.
func NewParser() (*Parser, error) {
	if err := initialize(); err != nil {
		return nil, err
	}
	return &Parser{}, nil
}

func initialize() error {
	initializeOnce.Do(func() {
		if err := gsql.Init(); err != nil {
			initializeErr = fmt.Errorf("%w: GoogleSQL parser initialization failed", domain.ErrPrecondition)
		}
	})
	return initializeErr
}

// ParseDDL parses exactly one statement and retains parser diagnostics.
func (*Parser) ParseDDL(ctx context.Context, request ports.QueryRequest) (domain.DDLCommand, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.DDLCommand{}, false, err
	}
	if err := initialize(); err != nil {
		return domain.DDLCommand{}, false, err
	}
	options, err := parserOptions()
	if err != nil {
		return domain.DDLCommand{}, false, parserFailure(err)
	}
	output, err := gsql.ParseStatement(request.SQL, options)
	if err != nil || output == nil {
		if isMultiStatementScript(request.SQL, options) {
			return domain.DDLCommand{}, false, fmt.Errorf(
				"%w: multi-statement queries are not implemented; capability=%s",
				domain.ErrUnsupported, domain.GapQueryScriptsUnsupportedV1,
			)
		}
		return domain.DDLCommand{}, false, invalidInput("invalid GoogleSQL statement syntax", request.SQL, err)
	}
	statement, err := output.Statement()
	if err != nil || statement == nil {
		return domain.DDLCommand{}, false, parserFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return domain.DDLCommand{}, false, err
	}

	switch node := statement.(type) {
	case *gsql.ASTCreateTableStatement:
		command, err := parseCreateTable(node, request)
		return command, true, err
	case *gsql.ASTDropStatement:
		command, err := parseDropTable(node, request)
		return command, true, err
	case *gsql.ASTAlterTableStatement:
		command, err := parseAlterTable(node, request)
		return command, true, err
	case *gsql.ASTTruncateStatement:
		command, err := parseTruncateTable(node, request)
		return command, true, err
	default:
		isDDL, inspectErr := statement.IsDdlStatement()
		if inspectErr != nil {
			return domain.DDLCommand{}, false, parserFailure()
		}
		if isDDL {
			return domain.DDLCommand{}, true, unsupported("DDL statement kind is not supported")
		}
		return domain.DDLCommand{}, false, nil
	}
}

func isMultiStatementScript(sql string, options *gsql.ParserOptions) bool {
	output, err := gsql.ParseScript(sql, options, nil)
	if err != nil || output == nil {
		return false
	}
	script, err := output.Script()
	if err != nil || script == nil {
		return false
	}
	statements, err := script.StatementListNode()
	if err != nil || statements == nil {
		return false
	}
	count, err := statements.NumChildren()
	return err == nil && count > 1
}

func parserOptions() (*gsql.ParserOptions, error) {
	language, err := gsql.NewLanguageOptionsMaximumFeatures()
	if err != nil {
		return nil, err
	}
	if err := language.SetSupportsAllStatementKinds(); err != nil {
		return nil, err
	}
	options, err := gsql.NewParserOptions()
	if err != nil {
		return nil, err
	}
	if err := options.SetLanguageOptions(language); err != nil {
		return nil, err
	}
	return options, nil
}

func parseCreateTable(node *gsql.ASTCreateTableStatement, request ports.QueryRequest) (domain.DDLCommand, error) {
	if enabled, err := node.IsIfNotExists(); err != nil {
		return domain.DDLCommand{}, parserFailure()
	} else if enabled {
		return domain.DDLCommand{}, unsupported("CREATE TABLE IF NOT EXISTS is not supported")
	}
	if enabled, err := node.IsOrReplace(); err != nil {
		return domain.DDLCommand{}, parserFailure()
	} else if enabled {
		return domain.DDLCommand{}, unsupported("CREATE OR REPLACE TABLE is not supported")
	}
	if unsupportedScope, err := hasUnsupportedCreateScope(node); err != nil {
		return domain.DDLCommand{}, err
	} else if unsupportedScope {
		return domain.DDLCommand{}, unsupported("temporary, public, and private tables are not supported")
	}

	name, err := node.Name()
	if err != nil || name == nil {
		return domain.DDLCommand{}, parserFailure()
	}
	reference, err := resolveTableReference(name, request)
	if err != nil {
		return domain.DDLCommand{}, err
	}
	if err := rejectCreateTableClauses(node); err != nil {
		return domain.DDLCommand{}, err
	}

	elements, err := node.TableElementList()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if elements == nil {
		return domain.DDLCommand{}, invalid("CREATE TABLE requires a column list")
	}
	hasConstraints, err := elements.HasConstraints()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if hasConstraints {
		return domain.DDLCommand{}, unsupported("table constraints are not supported")
	}
	children, err := astChildren(elements)
	if err != nil {
		return domain.DDLCommand{}, err
	}
	if len(children) == 0 {
		return domain.DDLCommand{}, invalid("CREATE TABLE requires at least one column")
	}

	fields := make([]domain.Field, 0, len(children))
	for _, child := range children {
		definition, ok := child.(*gsql.ASTColumnDefinition)
		if !ok {
			return domain.DDLCommand{}, unsupported("table constraints are not supported")
		}
		field, err := parseColumnDefinition(definition)
		if err != nil {
			return domain.DDLCommand{}, err
		}
		fields = append(fields, field)
	}
	table := domain.Table{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID,
		ID: reference.TableID, Type: "TABLE", Schema: fields,
	}
	if err := table.Validate(); err != nil {
		return domain.DDLCommand{}, normalizeDomainError(err)
	}
	return newCommand(domain.DDLCommandDescriptor{Kind: domain.DDLCreateTable, Table: reference, Schema: fields})
}

func hasUnsupportedCreateScope(node *gsql.ASTCreateTableStatement) (bool, error) {
	temp, err := node.IsTemp()
	if err != nil {
		return false, parserFailure()
	}
	public, err := node.IsPublic()
	if err != nil {
		return false, parserFailure()
	}
	private, err := node.IsPrivate()
	if err != nil {
		return false, parserFailure()
	}
	return temp || public || private, nil
}

func rejectCreateTableClauses(node *gsql.ASTCreateTableStatement) error {
	optional := []func() (any, error){
		func() (any, error) { return node.Collate() },
		func() (any, error) { return node.LikeTableName() },
		func() (any, error) { return node.OptionsList() },
		func() (any, error) { return node.WithConnectionClause() },
		func() (any, error) { return node.Query() },
		func() (any, error) { return node.PartitionBy() },
		func() (any, error) { return node.ClusterBy() },
		func() (any, error) { return node.CloneDataSource() },
		func() (any, error) { return node.CopyDataSource() },
		func() (any, error) { return node.SpannerOptions() },
		func() (any, error) { return node.Ttl() },
	}
	for _, get := range optional {
		value, err := get()
		if err != nil {
			return parserFailure()
		}
		if !isNil(value) {
			return unsupported("CREATE TABLE clauses beyond a column list are not supported")
		}
	}
	return nil
}

func parseDropTable(node *gsql.ASTDropStatement, request ports.QueryRequest) (domain.DDLCommand, error) {
	kind, err := node.SchemaObjectKind()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if kind != gsql.SchemaObjectKindKTable {
		return domain.DDLCommand{}, unsupported("only DROP TABLE is supported")
	}
	ifExists, err := node.IsIfExists()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if ifExists {
		return domain.DDLCommand{}, unsupported("DROP TABLE IF EXISTS is not supported")
	}
	mode, err := node.DropMode()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if mode != gsql.ASTDropStatementEnums_DropModeDropModeUnspecified {
		return domain.DDLCommand{}, unsupported("DROP TABLE modes are not supported")
	}
	name, err := node.Name()
	if err != nil || name == nil {
		return domain.DDLCommand{}, parserFailure()
	}
	reference, err := resolveTableReference(name, request)
	if err != nil {
		return domain.DDLCommand{}, err
	}
	return newCommand(domain.DDLCommandDescriptor{Kind: domain.DDLDropTable, Table: reference})
}

func parseTruncateTable(node *gsql.ASTTruncateStatement, request ports.QueryRequest) (domain.DDLCommand, error) {
	path, err := node.TargetPath()
	if err != nil || path == nil {
		return domain.DDLCommand{}, parserFailure()
	}
	where, err := node.Where()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if where != nil {
		return domain.DDLCommand{}, unsupported("TRUNCATE TABLE WHERE is not supported")
	}
	reference, err := resolveTableReference(path, request)
	if err != nil {
		return domain.DDLCommand{}, err
	}
	return newCommand(domain.DDLCommandDescriptor{Kind: domain.DDLTruncateTable, Table: reference})
}

func parseAlterTable(node *gsql.ASTAlterTableStatement, request ports.QueryRequest) (domain.DDLCommand, error) {
	ifExists, err := node.IsIfExists()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if ifExists {
		return domain.DDLCommand{}, unsupported("ALTER TABLE IF EXISTS is not supported")
	}
	path, err := node.Path()
	if err != nil || path == nil {
		return domain.DDLCommand{}, parserFailure()
	}
	reference, err := resolveTableReference(path, request)
	if err != nil {
		return domain.DDLCommand{}, err
	}
	actions, err := node.ActionList()
	if err != nil || actions == nil {
		return domain.DDLCommand{}, parserFailure()
	}
	children, err := astChildren(actions)
	if err != nil {
		return domain.DDLCommand{}, err
	}
	if len(children) != 1 {
		return domain.DDLCommand{}, unsupported("ALTER TABLE requires exactly one supported action")
	}

	switch action := children[0].(type) {
	case *gsql.ASTAddColumnAction:
		return parseAddColumn(action, reference)
	case *gsql.ASTDropColumnAction:
		return parseDropColumn(action, reference)
	case *gsql.ASTRenameColumnAction:
		return parseRenameColumn(action, reference)
	case *gsql.ASTAlterColumnTypeAction:
		return parseAlterColumnType(action, reference)
	default:
		return domain.DDLCommand{}, unsupported("ALTER TABLE action is not supported")
	}
}

func parseAddColumn(action *gsql.ASTAddColumnAction, reference domain.TableReference) (domain.DDLCommand, error) {
	ifNotExists, err := action.IsIfNotExists()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if ifNotExists {
		return domain.DDLCommand{}, unsupported("ADD COLUMN IF NOT EXISTS is not supported")
	}
	position, err := action.ColumnPosition()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	fill, err := action.FillExpression()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if position != nil || fill != nil {
		return domain.DDLCommand{}, unsupported("ADD COLUMN position and fill expressions are not supported")
	}
	definition, err := action.ColumnDefinition()
	if err != nil || definition == nil {
		return domain.DDLCommand{}, parserFailure()
	}
	field, err := parseColumnDefinition(definition)
	if err != nil {
		return domain.DDLCommand{}, err
	}
	return newCommand(domain.DDLCommandDescriptor{Kind: domain.DDLAddColumn, Table: reference, Field: field})
}

func parseDropColumn(action *gsql.ASTDropColumnAction, reference domain.TableReference) (domain.DDLCommand, error) {
	ifExists, err := action.IsIfExists()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if ifExists {
		return domain.DDLCommand{}, unsupported("DROP COLUMN IF EXISTS is not supported")
	}
	name, err := identifier(action.ColumnName())
	if err != nil {
		return domain.DDLCommand{}, err
	}
	return newCommand(domain.DDLCommandDescriptor{Kind: domain.DDLDropColumn, Table: reference, Name: name})
}

func parseRenameColumn(action *gsql.ASTRenameColumnAction, reference domain.TableReference) (domain.DDLCommand, error) {
	ifExists, err := action.IsIfExists()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if ifExists {
		return domain.DDLCommand{}, unsupported("RENAME COLUMN IF EXISTS is not supported")
	}
	name, err := identifier(action.ColumnName())
	if err != nil {
		return domain.DDLCommand{}, err
	}
	newName, err := identifier(action.NewColumnName())
	if err != nil {
		return domain.DDLCommand{}, err
	}
	return newCommand(domain.DDLCommandDescriptor{
		Kind: domain.DDLRenameColumn, Table: reference, Name: name, NewName: newName,
	})
}

func parseAlterColumnType(action *gsql.ASTAlterColumnTypeAction, reference domain.TableReference) (domain.DDLCommand, error) {
	ifExists, err := action.IsIfExists()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if ifExists {
		return domain.DDLCommand{}, unsupported("ALTER COLUMN IF EXISTS is not supported")
	}
	collate, err := action.Collate()
	if err != nil {
		return domain.DDLCommand{}, parserFailure()
	}
	if collate != nil {
		return domain.DDLCommand{}, unsupported("ALTER COLUMN collation is not supported")
	}
	name, err := identifier(action.ColumnName())
	if err != nil {
		return domain.DDLCommand{}, err
	}
	schema, err := action.Schema()
	if err != nil || schema == nil {
		return domain.DDLCommand{}, parserFailure()
	}
	field, err := parseColumnSchema(name, schema)
	if err != nil {
		return domain.DDLCommand{}, err
	}
	if field.Mode != "NULLABLE" {
		return domain.DDLCommand{}, unsupported("SET DATA TYPE cannot change column mode")
	}
	return newCommand(domain.DDLCommandDescriptor{
		Kind: domain.DDLAlterColumnType, Table: reference, Name: name, Field: field,
	})
}

func newCommand(descriptor domain.DDLCommandDescriptor) (domain.DDLCommand, error) {
	command, err := domain.NewDDLCommand(descriptor)
	if err != nil {
		return domain.DDLCommand{}, normalizeDomainError(err)
	}
	return command, nil
}

func parseColumnDefinition(definition *gsql.ASTColumnDefinition) (domain.Field, error) {
	name, err := identifier(definition.Name())
	if err != nil {
		return domain.Field{}, err
	}
	schema, err := definition.Schema()
	if err != nil || schema == nil {
		return domain.Field{}, parserFailure()
	}
	return parseColumnSchema(name, schema)
}

func parseColumnSchema(name string, schema gsql.ASTColumnSchemaNode) (domain.Field, error) {
	mode, err := columnModeAndAnnotations(schema)
	if err != nil {
		return domain.Field{}, err
	}

	var field domain.Field
	switch node := schema.(type) {
	case *gsql.ASTSimpleColumnSchema:
		field, err = parseSimpleColumn(name, node)
	case *gsql.ASTStructColumnSchema:
		field, err = parseStructColumn(name, node)
	case *gsql.ASTArrayColumnSchema:
		if mode != "NULLABLE" {
			return domain.Field{}, unsupported("array nullability annotations are not supported")
		}
		field, err = parseArrayColumn(name, node)
	default:
		return domain.Field{}, unsupported("column type is not supported")
	}
	if err != nil {
		return domain.Field{}, err
	}
	if field.Mode == "" {
		field.Mode = mode
	}
	if err := field.Validate(); err != nil {
		return domain.Field{}, normalizeDomainError(err)
	}
	return field, nil
}

func parseSimpleColumn(name string, schema *gsql.ASTSimpleColumnSchema) (domain.Field, error) {
	typeName, err := schema.TypeName()
	if err != nil || typeName == nil {
		return domain.Field{}, parserFailure()
	}
	parts, err := typeName.ToIdentifierVector()
	if err != nil {
		return domain.Field{}, parserFailure()
	}
	if len(parts) != 1 {
		return domain.Field{}, unsupported("named and qualified column types are not supported")
	}
	typ := canonicalType(parts[0])
	field := domain.Field{Name: name, Type: typ}
	parameters, err := schema.TypeParameters()
	if err != nil {
		return domain.Field{}, parserFailure()
	}
	values, err := integerTypeParameters(parameters)
	if err != nil {
		return domain.Field{}, err
	}
	if typ != "NUMERIC" && typ != "BIGNUMERIC" {
		if len(values) != 0 {
			return domain.Field{}, unsupported("type parameters are supported only for NUMERIC and BIGNUMERIC")
		}
		return field, nil
	}
	if len(values) > 2 {
		return domain.Field{}, invalid("decimal type accepts at most precision and scale")
	}
	if len(values) >= 1 {
		field.Precision = &values[0]
	}
	if len(values) == 2 {
		field.Scale = &values[1]
	}
	return field, nil
}

func parseStructColumn(name string, schema *gsql.ASTStructColumnSchema) (domain.Field, error) {
	parameters, err := schema.TypeParameters()
	if err != nil {
		return domain.Field{}, parserFailure()
	}
	if parameters != nil {
		return domain.Field{}, unsupported("STRUCT type parameters are not supported")
	}
	children, err := astChildren(schema)
	if err != nil {
		return domain.Field{}, err
	}
	nested := make([]domain.Field, 0, len(children))
	for _, child := range children {
		structField, ok := child.(*gsql.ASTStructColumnField)
		if _, attributeList := child.(*gsql.ASTColumnAttributeList); attributeList {
			continue
		}
		if !ok {
			return domain.Field{}, unsupported("STRUCT schema clauses are not supported")
		}
		fieldName, err := identifier(structField.Name())
		if err != nil {
			return domain.Field{}, invalid("STRUCT fields must be named")
		}
		fieldSchema, err := structField.Schema()
		if err != nil || fieldSchema == nil {
			return domain.Field{}, parserFailure()
		}
		field, err := parseColumnSchema(fieldName, fieldSchema)
		if err != nil {
			return domain.Field{}, err
		}
		nested = append(nested, field)
	}
	if len(nested) == 0 {
		return domain.Field{}, invalid("STRUCT requires at least one named field")
	}
	return domain.Field{Name: name, Type: "STRUCT", Fields: nested}, nil
}

func parseArrayColumn(name string, schema *gsql.ASTArrayColumnSchema) (domain.Field, error) {
	parameters, err := schema.TypeParameters()
	if err != nil {
		return domain.Field{}, parserFailure()
	}
	if parameters != nil {
		return domain.Field{}, unsupported("ARRAY type parameters are not supported")
	}
	element, err := schema.ElementSchema()
	if err != nil || element == nil {
		return domain.Field{}, parserFailure()
	}
	field, err := parseColumnSchema(name, element)
	if err != nil {
		return domain.Field{}, err
	}
	if field.Mode == "REPEATED" {
		return domain.Field{}, unsupported("nested ARRAY types are not supported")
	}
	if field.Mode == "REQUIRED" {
		return domain.Field{}, unsupported("array element nullability annotations are not supported")
	}
	field.Mode = "REPEATED"
	return field, nil
}

func columnModeAndAnnotations(schema gsql.ASTColumnSchemaNode) (string, error) {
	collate, err := schema.Collate()
	if err != nil {
		return "", parserFailure()
	}
	defaultExpression, err := schema.DefaultExpression()
	if err != nil {
		return "", parserFailure()
	}
	generated, err := schema.GeneratedColumnInfo()
	if err != nil {
		return "", parserFailure()
	}
	options, err := schema.OptionsList()
	if err != nil {
		return "", parserFailure()
	}
	if collate != nil || defaultExpression != nil || generated != nil || options != nil {
		return "", unsupported("column collation, defaults, generated values, and options are not supported")
	}

	mode := "NULLABLE"
	attributes, err := schema.Attributes()
	if err != nil {
		return "", parserFailure()
	}
	if attributes == nil {
		return mode, nil
	}
	children, err := astChildren(attributes)
	if err != nil {
		return "", err
	}
	for _, child := range children {
		if _, ok := child.(*gsql.ASTNotNullColumnAttribute); !ok {
			return "", unsupported("column constraints other than NOT NULL are not supported")
		}
		if mode == "REQUIRED" {
			return "", invalid("duplicate NOT NULL column attribute")
		}
		mode = "REQUIRED"
	}
	return mode, nil
}

func integerTypeParameters(parameters *gsql.ASTTypeParameterList) ([]int64, error) {
	if parameters == nil {
		return nil, nil
	}
	children, err := astChildren(parameters)
	if err != nil {
		return nil, err
	}
	values := make([]int64, 0, len(children))
	for _, child := range children {
		literal, ok := child.(*gsql.ASTIntLiteral)
		if !ok {
			return nil, invalid("type parameters must be integer literals")
		}
		image, err := literal.Image()
		if err != nil {
			return nil, parserFailure()
		}
		value, err := strconv.ParseInt(image, 10, 64)
		if err != nil {
			return nil, invalid("type parameter is outside the supported integer range")
		}
		values = append(values, value)
	}
	return values, nil
}

func resolveTableReference(path *gsql.ASTPathExpression, request ports.QueryRequest) (domain.TableReference, error) {
	parts, err := path.ToIdentifierVector()
	if err != nil {
		return domain.TableReference{}, parserFailure()
	}
	if len(parts) == 1 {
		quotedParts := strings.Split(parts[0], ".")
		if len(quotedParts) == 2 || len(quotedParts) == 3 {
			parts = quotedParts
		}
	}
	var reference domain.TableReference
	switch len(parts) {
	case 1:
		reference.ProjectID = request.DefaultProjectID
		if reference.ProjectID == "" {
			reference.ProjectID = request.ProjectID
		}
		reference.DatasetID = request.DefaultDataset
		reference.TableID = parts[0]
	case 2:
		reference.ProjectID = request.ProjectID
		reference.DatasetID = parts[0]
		reference.TableID = parts[1]
	case 3:
		reference.ProjectID = parts[0]
		reference.DatasetID = parts[1]
		reference.TableID = parts[2]
	default:
		return domain.TableReference{}, invalid("table reference must have one, two, or three parts")
	}
	if reference.ProjectID == "" || reference.DatasetID == "" || reference.TableID == "" {
		return domain.TableReference{}, invalid("table reference requires a project and dataset")
	}
	return reference, nil
}

func identifier(value *gsql.ASTIdentifier, err error) (string, error) {
	if err != nil || value == nil {
		return "", parserFailure()
	}
	name, err := value.GetAsString()
	if err != nil {
		return "", parserFailure()
	}
	if name == "" {
		return "", invalid("identifier must not be empty")
	}
	return name, nil
}

func astChildren(node gsql.ASTNode) ([]gsql.ASTNode, error) {
	count, err := node.NumChildren()
	if err != nil || count < 0 {
		return nil, parserFailure()
	}
	children := make([]gsql.ASTNode, 0, count)
	for index := int32(0); index < count; index++ {
		child, err := node.Child(index)
		if err != nil || child == nil {
			return nil, parserFailure()
		}
		children = append(children, child)
	}
	return children, nil
}

func canonicalType(value string) string {
	switch strings.ToUpper(value) {
	case "BOOL":
		return "BOOLEAN"
	case "INTEGER":
		return "INT64"
	case "FLOAT":
		return "FLOAT64"
	case "DECIMAL":
		return "NUMERIC"
	case "BIGDECIMAL":
		return "BIGNUMERIC"
	default:
		return strings.ToUpper(value)
	}
}

func normalizeDomainError(err error) error {
	switch {
	case errors.Is(err, domain.ErrUnsupported):
		return fmt.Errorf("%w: column definition is outside the emulator type contract; capability=%s", domain.ErrUnsupported, ddlCapability)
	case errors.Is(err, domain.ErrInvalid):
		return fmt.Errorf("%w: invalid table or column definition", domain.ErrInvalid)
	default:
		return parserFailure()
	}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	switch value := value.(type) {
	case *gsql.ASTCollate:
		return value == nil
	case *gsql.ASTPathExpression:
		return value == nil
	case *gsql.ASTOptionsList:
		return value == nil
	case *gsql.ASTWithConnectionClause:
		return value == nil
	case *gsql.ASTQuery:
		return value == nil
	case *gsql.ASTPartitionBy:
		return value == nil
	case *gsql.ASTClusterBy:
		return value == nil
	case *gsql.ASTCloneDataSource:
		return value == nil
	case *gsql.ASTCopyDataSource:
		return value == nil
	case *gsql.ASTSpannerTableOptions:
		return value == nil
	case *gsql.ASTTtlClause:
		return value == nil
	default:
		return false
	}
}

func parserFailure(causes ...error) error {
	if len(causes) > 0 && causes[0] != nil {
		return fmt.Errorf("%w: GoogleSQL parser could not inspect the syntax tree: %v", domain.ErrInvalid, causes[0])
	}
	return fmt.Errorf("%w: GoogleSQL parser could not inspect the syntax tree", domain.ErrInvalid)
}

func invalid(reason string, causes ...error) error {
	if len(causes) > 0 && causes[0] != nil {
		return fmt.Errorf("%w: %s: %v", domain.ErrInvalid, reason, causes[0])
	}
	return fmt.Errorf("%w: %s", domain.ErrInvalid, reason)
}

func invalidInput(reason, input string, cause error) error {
	if cause != nil {
		return fmt.Errorf("%w: %s; input=%q: %v", domain.ErrInvalid, reason, input, cause)
	}
	return fmt.Errorf("%w: %s; input=%q", domain.ErrInvalid, reason, input)
}

func unsupported(reason string) error {
	return fmt.Errorf("%w: %s; capability=%s", domain.ErrUnsupported, reason, ddlCapability)
}
