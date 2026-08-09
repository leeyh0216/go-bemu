package duckdb

// GoogleSQL lexical and DML provenance:
//   - https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
//   - https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
//   - https://duckdb.org/docs/stable/sql/statements/merge_into
//
// Query translation is isolated from physical catalog/schema administration so
// a future GoogleSQL parser or backend can replace it without changing metadata
// use cases.

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

var _ ports.QueryEngine = (*Warehouse)(nil)

func (w *Warehouse) Query(ctx context.Context, request ports.QueryRequest) (result domain.QueryResult, err error) {
	if operation, matched, operationErr := w.AnalyzeQueryOperation(ctx, request); matched {
		if operationErr != nil {
			return domain.QueryResult{}, operationErr
		}
		return domain.QueryResult{}, fmt.Errorf(
			"%w: connector semantic operation requires canonical catalog metadata; model_version=%s fix_hint=execute through QueryService",
			domain.ErrPrecondition, operation.ModelVersion,
		)
	}
	if err := validateSingleQueryStatement(request.SQL); err != nil {
		return domain.QueryResult{}, err
	}
	statement, adapterModel, err := translateSQLWithModel(request)
	if err != nil {
		return domain.QueryResult{}, err
	}
	statement, arguments, err := lowerQueryParameters(statement, request)
	if err != nil {
		return domain.QueryResult{}, err
	}
	queryAttrs := observability.PayloadAttrs("query", []byte(request.SQL))
	queryAttrs = append(queryAttrs,
		"project_id", request.ProjectID, "default_dataset", request.DefaultDataset,
		"statement_type", queryStatementType(request.SQL), "transaction_mode", "autocommit",
		"model_version", adapterModel,
	)
	started := observability.LogSideEffectStart(ctx, "duckdb", "query", queryAttrs...)
	defer func() {
		resultSummary := fmt.Sprintf("%v", result.Rows)
		columnsSummary := fmt.Sprintf("%v", result.Columns)
		observability.LogSideEffectEnd(ctx, "duckdb", "query", started, err,
			"project_id", request.ProjectID, "statement_type", queryStatementType(request.SQL),
			"row_count", len(result.Rows), "affected_rows", result.AffectedRows,
			"result_bytes", len(resultSummary), "result_digest", observability.Digest([]byte(resultSummary)),
			"schema_fingerprint", observability.Digest([]byte(columnsSummary)), "transaction_mode", "autocommit")
	}()
	if !returnsRows(statement) {
		execResult, err := w.db.ExecContext(ctx, statement, arguments...)
		if err != nil {
			return domain.QueryResult{}, fmt.Errorf("execute query: %w", err)
		}
		affected, _ := execResult.RowsAffected()
		return domain.QueryResult{AffectedRows: affected}, nil
	}
	rows, err := w.db.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return domain.QueryResult{}, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return domain.QueryResult{}, fmt.Errorf("read result schema: %w", err)
	}
	result = domain.QueryResult{Columns: make([]domain.Column, len(columnTypes))}
	for i, columnType := range columnTypes {
		result.Columns[i] = domain.Column{Name: columnType.Name(), Type: bigQueryType(columnType.DatabaseTypeName())}
	}
	for rows.Next() {
		values := make([]any, len(columnTypes))
		destinations := make([]any, len(columnTypes))
		for i := range values {
			destinations[i] = &values[i]
		}
		if err := rows.Scan(destinations...); err != nil {
			return domain.QueryResult{}, fmt.Errorf("scan result row: %w", err)
		}
		result.Rows = append(result.Rows, values)
	}
	if err := rows.Err(); err != nil {
		return domain.QueryResult{}, fmt.Errorf("read result rows: %w", err)
	}
	return result, nil
}

// lowerQueryParameters preserves literals/comments while replacing only real
// GoogleSQL parameter tokens with DB-API placeholders. Values are passed as
// driver arguments, never embedded into SQL text.
func lowerQueryParameters(statement string, request ports.QueryRequest) (string, []any, error) {
	if len(request.QueryParameters) == 0 {
		if request.ParameterMode != "" {
			return "", nil, fmt.Errorf("%w: parameterMode requires queryParameters", domain.ErrInvalid)
		}
		return statement, nil, nil
	}
	byName := map[string]domain.QueryParameter{}
	for _, p := range request.QueryParameters {
		byName[strings.ToLower(p.Name)] = p
	}
	usedNames := make(map[string]struct{}, len(byName))
	var out strings.Builder
	args := make([]any, 0, len(request.QueryParameters))
	positional := 0
	for i := 0; i < len(statement); {
		if statement[i] == '\'' || statement[i] == '"' || statement[i] == '`' {
			end, err := scanQuotedLiteral(statement, i, statement[i])
			if err != nil {
				return "", nil, err
			}
			out.WriteString(statement[i:end])
			i = end
			continue
		}
		if statement[i] == '-' && i+1 < len(statement) && statement[i+1] == '-' {
			end := scanLineComment(statement, i)
			out.WriteString(statement[i:end])
			i = end
			continue
		}
		if statement[i] == '#' {
			end := scanLineComment(statement, i)
			out.WriteString(statement[i:end])
			i = end
			continue
		}
		if statement[i] == '/' && i+1 < len(statement) && statement[i+1] == '*' {
			end, err := scanBlockComment(statement, i)
			if err != nil {
				return "", nil, err
			}
			out.WriteString(statement[i:end])
			i = end
			continue
		}
		if statement[i] == '@' && request.ParameterMode == domain.QueryParameterNamed && i+1 < len(statement) && isIdentifierStart(statement[i+1]) {
			end := i + 2
			for end < len(statement) && isIdentifierPart(statement[end]) {
				end++
			}
			name := strings.ToLower(statement[i+1 : end])
			parameter, ok := byName[name]
			if !ok {
				return "", nil, fmt.Errorf("%w: missing named query parameter", domain.ErrInvalid)
			}
			value, err := duckDBParameterValue(parameter)
			if err != nil {
				return "", nil, err
			}
			out.WriteByte('?')
			args = append(args, value)
			usedNames[name] = struct{}{}
			i = end
			continue
		}
		if statement[i] == '?' && request.ParameterMode == domain.QueryParameterPositional {
			if positional >= len(request.QueryParameters) {
				return "", nil, fmt.Errorf("%w: missing positional query parameter", domain.ErrInvalid)
			}
			value, err := duckDBParameterValue(request.QueryParameters[positional])
			if err != nil {
				return "", nil, err
			}
			positional++
			out.WriteByte('?')
			args = append(args, value)
			i++
			continue
		}
		out.WriteByte(statement[i])
		i++
	}
	if request.ParameterMode == domain.QueryParameterNamed {
		for _, p := range request.QueryParameters {
			if _, ok := usedNames[strings.ToLower(p.Name)]; !ok {
				return "", nil, fmt.Errorf("%w: unused named query parameter", domain.ErrInvalid)
			}
		}
	} else if positional != len(request.QueryParameters) {
		return "", nil, fmt.Errorf("%w: unused positional query parameter", domain.ErrInvalid)
	}
	return out.String(), args, nil
}

func duckDBParameterValue(parameter domain.QueryParameter) (driver.Value, error) {
	switch parameter.Type {
	case "BOOL":
		value, err := strconv.ParseBool(parameter.Value)
		return value, err
	case "INT64":
		value, err := strconv.ParseInt(parameter.Value, 10, 64)
		return value, err
	case "FLOAT64":
		value, err := strconv.ParseFloat(parameter.Value, 64)
		return value, err
	case "DATE":
		return time.Parse("2006-01-02", parameter.Value)
	case "DATETIME":
		return time.Parse("2006-01-02 15:04:05", parameter.Value)
	case "TIME":
		return time.Parse("15:04:05", parameter.Value)
	case "TIMESTAMP":
		return time.Parse(time.RFC3339Nano, parameter.Value)
	case "NUMERIC", "STRING", "JSON":
		return parameter.Value, nil
	default:
		return nil, fmt.Errorf("%w: unsupported query parameter type", domain.ErrInvalid)
	}
}

func queryStatementType(statement string) string {
	return leadingStatementKeyword(statement)
}

func translateSQL(request ports.QueryRequest) (string, error) {
	statement, _, err := translateSQLWithModel(request)
	return statement, err
}

func translateSQLWithModel(request ports.QueryRequest) (string, string, error) {
	if statement, matched, err := rewriteSparkStaticOverwrite(request); matched || err != nil {
		return statement, sparkStaticOverwriteModel, err
	}
	statement, err := rewriteGoogleSQLIdentifiers(request)
	return statement, "google-sql-identifiers-v1", err
}

// rewriteGoogleSQLIdentifiers is a lexical boundary, not a general GoogleSQL
// parser. It preserves quoted strings and comments byte-for-byte, converts
// backtick column/alias identifiers to DuckDB quotes, and maps only identifiers
// that occur where DML/DDL grammar expects a relation. Unsupported or malformed
// quoted input fails before reaching DuckDB.
func rewriteGoogleSQLIdentifiers(request ports.QueryRequest) (string, error) {
	var result strings.Builder
	result.Grow(len(request.SQL) + 32)
	expectRelation := false
	relationList := false
	relationDepth := 0
	depth := 0
	cteNames := make(map[string]struct{})
	expectCTEName := false

	for index := 0; index < len(request.SQL); {
		current := request.SQL[index]
		switch {
		case current == '\'' || current == '"':
			end, err := scanQuotedLiteral(request.SQL, index, current)
			if err != nil {
				return "", err
			}
			result.WriteString(request.SQL[index:end])
			index = end
		case current == '-' && index+1 < len(request.SQL) && request.SQL[index+1] == '-':
			end := scanLineComment(request.SQL, index)
			result.WriteString(request.SQL[index:end])
			index = end
		case current == '#':
			end := scanLineComment(request.SQL, index)
			result.WriteString(request.SQL[index:end])
			index = end
		case current == '/' && index+1 < len(request.SQL) && request.SQL[index+1] == '*':
			end, err := scanBlockComment(request.SQL, index)
			if err != nil {
				return "", err
			}
			result.WriteString(request.SQL[index:end])
			index = end
		case current == '`':
			identifier, end, err := scanBacktickIdentifier(request.SQL, index)
			if err != nil {
				return "", err
			}
			if expectCTEName {
				cteNames[strings.ToLower(identifier)] = struct{}{}
				result.WriteString(quoteIdentifier(identifier))
				expectCTEName = false
				expectRelation = false
			} else if expectRelation {
				translated, err := translateRelationIdentifier(request, identifier, cteNames)
				if err != nil {
					return "", err
				}
				result.WriteString(translated)
				expectRelation = false
			} else {
				result.WriteString(quoteIdentifier(identifier))
			}
			index = end
		case isIdentifierStart(current):
			end := index + 1
			for end < len(request.SQL) && isIdentifierPart(request.SQL[end]) {
				end++
			}
			word := request.SQL[index:end]
			upper := strings.ToUpper(word)
			if expectCTEName {
				cteNames[strings.ToLower(word)] = struct{}{}
				expectCTEName = false
			}
			if expectRelation && !isRelationModifier(upper) {
				expectRelation = false
			}
			switch upper {
			case "WITH":
				expectCTEName = true
			case "FROM":
				expectRelation, relationList, relationDepth = true, true, depth
			case "JOIN":
				expectRelation = true
			case "MERGE", "INTO", "UPDATE", "USING":
				expectRelation = true
			case "TABLE":
				expectRelation = true
			case "WHERE", "GROUP", "HAVING", "QUALIFY", "WINDOW", "ORDER", "LIMIT", "UNION", "EXCEPT", "INTERSECT", "RETURNING":
				if depth <= relationDepth {
					relationList = false
				}
			}
			result.WriteString(word)
			index = end
		case current == '(':
			if expectRelation {
				expectRelation = false
			}
			depth++
			result.WriteByte(current)
			index++
		case current == ')':
			if depth > 0 {
				depth--
			}
			result.WriteByte(current)
			index++
		case current == ',':
			if relationList && depth == relationDepth {
				expectRelation = true
			}
			result.WriteByte(current)
			index++
		default:
			result.WriteByte(current)
			index++
		}
	}
	return result.String(), nil
}

func translateRelationIdentifier(request ports.QueryRequest, reference string, cteNames map[string]struct{}) (string, error) {
	parts := strings.Split(reference, ".")
	switch len(parts) {
	case 3:
		return quoteIdentifier(physicalSchema(parts[0], parts[1])) + "." + quoteIdentifier(parts[2]), nil
	case 2:
		if request.ProjectID == "" {
			return "", fmt.Errorf("%w: project is required for table reference %q", domain.ErrInvalid, reference)
		}
		return quoteIdentifier(physicalSchema(request.ProjectID, parts[0])) + "." + quoteIdentifier(parts[1]), nil
	case 1:
		if _, isCTE := cteNames[strings.ToLower(reference)]; isCTE {
			return quoteIdentifier(reference), nil
		}
		if request.ProjectID == "" || request.DefaultDataset == "" {
			return "", fmt.Errorf("%w: default dataset is required for table reference %q", domain.ErrInvalid, reference)
		}
		defaultProjectID := request.DefaultProjectID
		if defaultProjectID == "" {
			defaultProjectID = request.ProjectID
		}
		return quoteIdentifier(physicalSchema(defaultProjectID, request.DefaultDataset)) + "." + quoteIdentifier(parts[0]), nil
	default:
		return "", fmt.Errorf("%w: malformed table reference %q", domain.ErrInvalid, reference)
	}
}

func scanQuotedLiteral(statement string, start int, quote byte) (int, error) {
	for index := start + 1; index < len(statement); index++ {
		if statement[index] == '\\' {
			index++
			continue
		}
		if statement[index] != quote {
			continue
		}
		if index+1 < len(statement) && statement[index+1] == quote {
			index++
			continue
		}
		return index + 1, nil
	}
	return 0, fmt.Errorf("%w: unterminated quoted SQL literal", domain.ErrInvalid)
}

func scanBacktickIdentifier(statement string, start int) (string, int, error) {
	var identifier strings.Builder
	for index := start + 1; index < len(statement); index++ {
		if statement[index] == '\\' && index+1 < len(statement) {
			index++
			identifier.WriteByte(statement[index])
			continue
		}
		if statement[index] == '`' {
			if identifier.Len() == 0 {
				return "", 0, fmt.Errorf("%w: quoted identifier cannot be empty", domain.ErrInvalid)
			}
			return identifier.String(), index + 1, nil
		}
		identifier.WriteByte(statement[index])
	}
	return "", 0, fmt.Errorf("%w: unterminated backtick identifier", domain.ErrInvalid)
}

func scanLineComment(statement string, start int) int {
	if newline := strings.IndexByte(statement[start:], '\n'); newline >= 0 {
		return start + newline + 1
	}
	return len(statement)
}

func scanBlockComment(statement string, start int) (int, error) {
	if end := strings.Index(statement[start+2:], "*/"); end >= 0 {
		return start + 2 + end + 2, nil
	}
	return 0, fmt.Errorf("%w: unterminated block comment", domain.ErrInvalid)
}

func isIdentifierStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func isIdentifierPart(value byte) bool {
	return isIdentifierStart(value) || value >= '0' && value <= '9'
}

func isRelationModifier(word string) bool {
	switch word {
	case "IF", "NOT", "EXISTS", "ONLY":
		return true
	default:
		return false
	}
}

func returnsRows(statement string) bool {
	switch leadingStatementKeyword(statement) {
	case "SELECT", "WITH", "VALUES", "SHOW", "DESCRIBE", "EXPLAIN", "PRAGMA":
		return true
	default:
		return false
	}
}

// leadingStatementKeyword skips GoogleSQL whitespace and comments before
// classifying whether a statement produces rows. Connector-generated queries
// can carry leading comments, and treating them as DML would discard the result
// and omit the anonymous destinationTable.
// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#comments
func leadingStatementKeyword(statement string) string {
	for index := 0; index < len(statement); {
		switch {
		case statement[index] == ' ' || statement[index] == '\t' || statement[index] == '\r' || statement[index] == '\n':
			index++
		case statement[index] == '#' || statement[index] == '-' && index+1 < len(statement) && statement[index+1] == '-':
			index = scanLineComment(statement, index)
		case statement[index] == '/' && index+1 < len(statement) && statement[index+1] == '*':
			end, err := scanBlockComment(statement, index)
			if err != nil {
				return "UNKNOWN"
			}
			index = end
		case isIdentifierStart(statement[index]):
			end := index + 1
			for end < len(statement) && isIdentifierPart(statement[end]) {
				end++
			}
			return strings.ToUpper(statement[index:end])
		default:
			return "UNKNOWN"
		}
	}
	return "UNKNOWN"
}

func bigQueryType(databaseType string) string {
	upper := strings.ToUpper(databaseType)
	switch {
	case strings.HasSuffix(upper, "[]"), strings.HasPrefix(upper, "LIST("):
		// ARRAY is an internal strict-gap marker. Publishing it as a scalar type
		// would make REST metadata disagree with the physical DuckDB list. The
		// recursive query-result schema mapper will replace this marker when the
		// nested/repeated type slice is implemented.
		return "ARRAY"
	case strings.Contains(upper, "BOOL"):
		return "BOOLEAN"
	case strings.Contains(upper, "INT"):
		return "INTEGER"
	case strings.Contains(upper, "DOUBLE"), strings.Contains(upper, "FLOAT"):
		return "FLOAT"
	case strings.Contains(upper, "DECIMAL"):
		return "NUMERIC"
	case strings.Contains(upper, "TIMESTAMP WITH TIME ZONE"), strings.Contains(upper, "TIMESTAMPTZ"):
		return "TIMESTAMP"
	case strings.Contains(upper, "TIMESTAMP"):
		return "DATETIME"
	case strings.Contains(upper, "DATE"):
		return "DATE"
	case strings.Contains(upper, "TIME"):
		return "TIME"
	case strings.Contains(upper, "BLOB"):
		return "BYTES"
	case strings.Contains(upper, "JSON"):
		return "JSON"
	case strings.Contains(upper, "STRUCT"):
		return "RECORD"
	default:
		return "STRING"
	}
}
