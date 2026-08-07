package duckdb

// Query analysis extracts only the structural facts required by the
// application boundary. SQL text remains inside this adapter and is never
// emitted in logs.
//
// Protocol/parser provenance:
//   - GoogleSQL quoted identifiers and table paths:
//     https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_identifiers
//   - BigQuery job location inference from referenced datasets:
//     https://cloud.google.com/bigquery/docs/locations#specify_locations
//   - spark-bigquery-connector 0.44.2 query/view materialization call path:
//     https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L457-L469

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

var _ ports.QueryAnalyzer = (*Warehouse)(nil)

func (w *Warehouse) AnalyzeQuery(ctx context.Context, request ports.QueryRequest) (ports.QueryAnalysis, error) {
	if err := validateSingleQueryStatement(request.SQL); err != nil {
		return ports.QueryAnalysis{}, err
	}
	statement := leadingStatementKeyword(request.SQL)
	switch statement {
	case "SELECT", "WITH", "VALUES", "INSERT", "UPDATE", "DELETE", "MERGE":
		// These are the declared query-engine statement classes. Catalog DDL
		// is classified below and rejected by the application boundary.
	case "CREATE", "ALTER", "DROP", "TRUNCATE":
	default:
		return ports.QueryAnalysis{}, fmt.Errorf("%w: query statement type is outside the declared engine subset; capability=%s",
			domain.ErrUnsupported, domain.GapQueryScriptsUnsupportedV1)
	}
	translated, model, err := translateSQLWithModel(request)
	if err != nil {
		return ports.QueryAnalysis{}, err
	}
	references, err := analyzeRelationReferences(request)
	if err != nil {
		return ports.QueryAnalysis{}, err
	}
	analysis := ports.QueryAnalysis{ReferencedTables: references, ProducesRows: returnsRows(translated)}
	switch statement {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		if len(references) > 0 {
			analysis.MutationTargets = append([]domain.TableReference(nil), references[0])
		}
	case "CREATE", "ALTER", "DROP", "TRUNCATE":
		// QueryService has no catalog-DDL reconciliation port. Mark the
		// statement so the application rejects it before a DuckDB side effect
		// can create metadata drift.
		analysis.RequiresCatalogMutation = true
	}
	attrs := []any{
		"event", "boundary.exit", "boundary", "duckdb.query_analysis",
		"model_version", model, "query_bytes", len(request.SQL),
		"query_digest", observability.Digest([]byte(request.SQL)),
		"statement_type", queryStatementType(request.SQL),
		"referenced_table_count", len(references), "mutation_target_count", len(analysis.MutationTargets),
		"produces_rows", analysis.ProducesRows, "requires_catalog_mutation", analysis.RequiresCatalogMutation,
	}
	slog.InfoContext(ctx, "query analysis", attrs...)
	return analysis, nil
}

// validateSingleQueryStatement permits one optional trailing semicolon but
// rejects scripts before translation or execution. Semicolons inside quoted
// literals, identifiers, and comments are data, not statement separators.
// Full BigQuery scripts require a parser and a multi-statement job transaction:
// https://cloud.google.com/bigquery/docs/multi-statement-queries
func validateSingleQueryStatement(sql string) error {
	for index := 0; index < len(sql); {
		switch {
		case sql[index] == '\'' || sql[index] == '"':
			end, err := scanQuotedLiteral(sql, index, sql[index])
			if err != nil {
				return err
			}
			index = end
		case sql[index] == '`':
			_, end, err := scanBacktickIdentifier(sql, index)
			if err != nil {
				return err
			}
			index = end
		case sql[index] == '-' && index+1 < len(sql) && sql[index+1] == '-':
			index = scanLineComment(sql, index)
		case sql[index] == '#':
			index = scanLineComment(sql, index)
		case sql[index] == '/' && index+1 < len(sql) && sql[index+1] == '*':
			end, err := scanBlockComment(sql, index)
			if err != nil {
				return err
			}
			index = end
		case sql[index] == ';':
			if queryScriptHasContent(sql, index+1) {
				return fmt.Errorf("%w: multi-statement queries are not implemented; capability=%s",
					domain.ErrUnsupported, domain.GapQueryScriptsUnsupportedV1)
			}
			return nil
		default:
			index++
		}
	}
	return nil
}

func queryScriptHasContent(sql string, start int) bool {
	for index := start; index < len(sql); {
		switch {
		case isSQLSpace(sql[index]):
			index++
		case sql[index] == '-' && index+1 < len(sql) && sql[index+1] == '-':
			index = scanLineComment(sql, index)
		case sql[index] == '#':
			index = scanLineComment(sql, index)
		case sql[index] == '/' && index+1 < len(sql) && sql[index+1] == '*':
			end, err := scanBlockComment(sql, index)
			if err != nil {
				return true
			}
			index = end
		default:
			return true
		}
	}
	return false
}

func isSQLSpace(value byte) bool {
	switch value {
	case ' ', '\t', '\r', '\n', '\f':
		return true
	default:
		return false
	}
}

func analyzeRelationReferences(request ports.QueryRequest) ([]domain.TableReference, error) {
	var references []domain.TableReference
	seen := make(map[string]struct{})
	cteNames := make(map[string]struct{})
	expectRelation := false
	relationList := false
	relationDepth := 0
	depth := 0
	expectCTEName := false

	appendReference := func(identifier string) error {
		reference, isCTE, err := queryTableReference(request, identifier, cteNames)
		if err != nil {
			return err
		}
		if isCTE {
			return nil
		}
		key := reference.ProjectID + "\x00" + reference.DatasetID + "\x00" + reference.TableID
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			references = append(references, reference)
		}
		return nil
	}

	for index := 0; index < len(request.SQL); {
		current := request.SQL[index]
		switch {
		case current == '\'' || current == '"':
			end, err := scanQuotedLiteral(request.SQL, index, current)
			if err != nil {
				return nil, err
			}
			index = end
		case current == '-' && index+1 < len(request.SQL) && request.SQL[index+1] == '-':
			index = scanLineComment(request.SQL, index)
		case current == '#':
			index = scanLineComment(request.SQL, index)
		case current == '/' && index+1 < len(request.SQL) && request.SQL[index+1] == '*':
			end, err := scanBlockComment(request.SQL, index)
			if err != nil {
				return nil, err
			}
			index = end
		case current == '`':
			identifier, end, err := scanBacktickIdentifier(request.SQL, index)
			if err != nil {
				return nil, err
			}
			if expectCTEName {
				cteNames[strings.ToLower(identifier)] = struct{}{}
				expectCTEName = false
				expectRelation = false
			} else if expectRelation {
				if err := appendReference(identifier); err != nil {
					return nil, err
				}
				expectRelation = false
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
			index = end
		case current == '(':
			if expectRelation {
				expectRelation = false
			}
			depth++
			index++
		case current == ')':
			if depth > 0 {
				depth--
			}
			index++
		case current == ',':
			if relationList && depth == relationDepth {
				expectRelation = true
			}
			index++
		default:
			index++
		}
	}
	return references, nil
}

func queryTableReference(request ports.QueryRequest, identifier string, cteNames map[string]struct{}) (domain.TableReference, bool, error) {
	parts := strings.Split(identifier, ".")
	for _, part := range parts {
		if part == "" {
			return domain.TableReference{}, false, fmt.Errorf("%w: malformed quoted table reference", domain.ErrInvalid)
		}
	}
	switch len(parts) {
	case 3:
		return domain.TableReference{ProjectID: parts[0], DatasetID: parts[1], TableID: parts[2]}, false, nil
	case 2:
		if request.ProjectID == "" {
			return domain.TableReference{}, false, fmt.Errorf("%w: project is required for two-part table reference", domain.ErrInvalid)
		}
		return domain.TableReference{ProjectID: request.ProjectID, DatasetID: parts[0], TableID: parts[1]}, false, nil
	case 1:
		if _, exists := cteNames[strings.ToLower(identifier)]; exists {
			return domain.TableReference{}, true, nil
		}
		if request.ProjectID == "" || request.DefaultDataset == "" {
			return domain.TableReference{}, false, fmt.Errorf("%w: default dataset is required for one-part table reference", domain.ErrInvalid)
		}
		defaultProjectID := request.DefaultProjectID
		if defaultProjectID == "" {
			defaultProjectID = request.ProjectID
		}
		return domain.TableReference{ProjectID: defaultProjectID, DatasetID: request.DefaultDataset, TableID: parts[0]}, false, nil
	default:
		return domain.TableReference{}, false, fmt.Errorf("%w: malformed quoted table reference", domain.ErrInvalid)
	}
}
