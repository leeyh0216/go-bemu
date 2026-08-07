package duckdb

// Spark connector dynamic time-partition overwrite parser.
//
// This parser recognizes one source-pinned token structure. It does not execute
// arbitrary BigQuery scripts and does not translate script text into DuckDB SQL.
// The accepted model is the output of getQueryForTimePartitionedTable and
// createOptimizedMergeQuery in spark-bigquery-connector 0.44.2.
//
// Sources:
//   - exact connector producer:
//     https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870
//   - BigQuery multi-statement transactions and script semantics:
//     https://cloud.google.com/bigquery/docs/multi-statement-queries
//   - BigQuery MERGE semantics:
//     https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const (
	sparkDynamicTimeOverwriteModel  = "spark-bigquery-connector-0.44.2/dynamic-time-partition-overwrite"
	sparkDynamicRangeOverwriteModel = "spark-bigquery-connector-0.44.2/dynamic-range-partition-overwrite"
)

var _ ports.QueryOperationAnalyzer = (*Warehouse)(nil)

// AnalyzeQueryOperation returns matched=true for connector-profile candidates,
// including drifted and range-partition variants. That distinction guarantees
// they fail with a stable model/gap diagnostic instead of reaching generic SQL.
func (w *Warehouse) AnalyzeQueryOperation(ctx context.Context, request ports.QueryRequest) (ports.QueryOperation, bool, error) {
	operation, matched, err := parseSparkDynamicTimeOverwrite(request)
	if matched && err != nil {
		attrs := []any{
			"event", "boundary.reject", "boundary", "duckdb.query_operation_analysis",
			"operation", "connector-dynamic-partition-overwrite", "model_version", operation.ModelVersion,
			"query_bytes", len(request.SQL), "query_digest", observability.Digest([]byte(request.SQL)),
			"fix_hint", "compare BigQueryUtil.java 0.44.2 token shape before updating the versioned model",
		}
		var drift *dynamicOverwriteShapeError
		if errors.As(err, &drift) {
			attrs = append(attrs, "capability", domain.CapabilitySparkDynamicTimePartitionOverwriteV1,
				"gap", domain.GapQueryScriptsUnsupportedV1, "token_index", drift.TokenIndex,
				"expected_shape", drift.ExpectedShape)
		} else if operation.ModelVersion == sparkDynamicRangeOverwriteModel {
			attrs = append(attrs, "gap", domain.GapSparkDynamicRangePartitionOverwriteV1,
				"token_index", -1, "expected_shape", "supported time-partition overwrite profile")
		} else {
			attrs = append(attrs, "capability", domain.CapabilitySparkDynamicTimePartitionOverwriteV1,
				"gap", domain.GapQueryScriptsUnsupportedV1, "token_index", -1,
				"expected_shape", "source-pinned connector script token profile")
		}
		attrs = append(attrs, observability.ErrorAttrs(err)...)
		slog.WarnContext(ctx, "connector query operation rejected", attrs...)
	}
	return operation, matched, err
}

func parseSparkDynamicTimeOverwrite(request ports.QueryRequest) (ports.QueryOperation, bool, error) {
	if leadingStatementKeyword(request.SQL) != "DECLARE" {
		return ports.QueryOperation{}, false, nil
	}
	words, err := scanDynamicOverwriteWords(request.SQL)
	if !hasDynamicOverwriteSignature(words) {
		return ports.QueryOperation{}, false, nil
	}
	if err != nil {
		return ports.QueryOperation{
				Kind: ports.QueryOperationSparkDynamicTimePartitionOverwrite, ModelVersion: sparkDynamicTimeOverwriteModel,
			}, true, dynamicOverwriteProfileError(&dynamicOverwriteShapeError{
				TokenIndex: len(words), ExpectedShape: "well-formed quoted literal, identifier, or comment", Cause: err,
			})
	}
	if containsDynamicWord(words, "RANGE_BUCKET") && containsDynamicWord(words, "GENERATE_ARRAY") {
		return ports.QueryOperation{ModelVersion: sparkDynamicRangeOverwriteModel}, true, fmt.Errorf(
			"%w: connector range-partition overwrite remains an explicit gap; capability=%s model_version=%s",
			domain.ErrUnsupported, domain.GapSparkDynamicRangePartitionOverwriteV1, sparkDynamicRangeOverwriteModel,
		)
	}

	tokens, err := lexDynamicTimeOverwrite(request.SQL)
	if err != nil {
		return ports.QueryOperation{ModelVersion: sparkDynamicTimeOverwriteModel}, true, dynamicOverwriteProfileError(err)
	}
	parser := dynamicOverwriteParser{tokens: tokens}
	operation, err := parser.parse(request)
	if err != nil {
		return ports.QueryOperation{ModelVersion: sparkDynamicTimeOverwriteModel}, true, dynamicOverwriteProfileError(err)
	}
	return operation, true, nil
}

func hasDynamicOverwriteSignature(words []string) bool {
	for _, required := range []string{
		"DECLARE", "DEFAULT", "ARRAY_AGG", "DISTINCT", "IGNORE", "NULLS", "MERGE",
	} {
		if !containsDynamicWord(words, required) {
			return false
		}
	}
	return true
}

func containsDynamicWord(words []string, expected string) bool {
	for _, word := range words {
		if word == expected {
			return true
		}
	}
	return false
}

// scanDynamicOverwriteWords is candidate classification only. The strict lexer
// and parser below still have to consume the complete input before it is trusted.
func scanDynamicOverwriteWords(statement string) ([]string, error) {
	words := make([]string, 0, 40)
	for index := 0; index < len(statement); {
		switch {
		case statement[index] == '\'' || statement[index] == '"':
			end, err := scanQuotedLiteral(statement, index, statement[index])
			if err != nil {
				return words, err
			}
			index = end
		case statement[index] == '`':
			_, end, err := scanBacktickIdentifier(statement, index)
			if err != nil {
				return words, err
			}
			index = end
		case statement[index] == '-' && index+1 < len(statement) && statement[index+1] == '-':
			index = scanLineComment(statement, index)
		case statement[index] == '#':
			index = scanLineComment(statement, index)
		case statement[index] == '/' && index+1 < len(statement) && statement[index+1] == '*':
			end, err := scanBlockComment(statement, index)
			if err != nil {
				return words, err
			}
			index = end
		case isIdentifierStart(statement[index]):
			end := index + 1
			for end < len(statement) && isIdentifierPart(statement[end]) {
				end++
			}
			words = append(words, strings.ToUpper(statement[index:end]))
			index = end
		default:
			index++
		}
	}
	return words, nil
}

func lexDynamicTimeOverwrite(statement string) ([]overwriteToken, error) {
	tokens := make([]overwriteToken, 0, 80)
	for index := 0; index < len(statement); {
		switch {
		case isSQLSpace(statement[index]):
			index++
		case statement[index] == '`':
			identifier, end, err := scanBacktickIdentifier(statement, index)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, overwriteToken{kind: overwriteQuotedIdentifier, value: identifier})
			index = end
		case isIdentifierStart(statement[index]):
			end := index + 1
			for end < len(statement) && isIdentifierPart(statement[end]) {
				end++
			}
			tokens = append(tokens, overwriteToken{kind: overwriteWord, value: strings.ToUpper(statement[index:end])})
			index = end
		case strings.ContainsRune("(),;.", rune(statement[index])):
			tokens = append(tokens, overwriteToken{kind: overwriteSymbol, value: statement[index : index+1]})
			index++
		default:
			return tokens, &dynamicOverwriteShapeError{
				TokenIndex: len(tokens), ExpectedShape: "connector keyword, backtick identifier, or punctuation",
				Cause: fmt.Errorf("unsupported token class at byte offset %d", index),
			}
		}
	}
	return tokens, nil
}

type dynamicOverwriteParser struct {
	tokens []overwriteToken
	index  int
}

func (p *dynamicOverwriteParser) parse(request ports.QueryRequest) (ports.QueryOperation, error) {
	if err := p.expectWord("DECLARE"); err != nil {
		return ports.QueryOperation{}, err
	}
	variable, err := p.expectWordValue()
	if err != nil || variable != "PARTITIONS_TO_DELETE" {
		return ports.QueryOperation{}, p.shapeError("partitions_to_delete variable")
	}
	for _, token := range []overwriteToken{
		{kind: overwriteWord, value: "DEFAULT"},
		{kind: overwriteSymbol, value: "("},
		{kind: overwriteWord, value: "SELECT"},
		{kind: overwriteWord, value: "ARRAY_AGG"},
		{kind: overwriteSymbol, value: "("},
		{kind: overwriteWord, value: "DISTINCT"},
		{kind: overwriteSymbol, value: "("},
	} {
		if err := p.expect(token); err != nil {
			return ports.QueryOperation{}, err
		}
	}
	sourceExpression, err := p.parsePartitionExpression("")
	if err != nil {
		return ports.QueryOperation{}, err
	}
	for _, token := range []overwriteToken{
		{kind: overwriteSymbol, value: ")"},
		{kind: overwriteWord, value: "IGNORE"},
		{kind: overwriteWord, value: "NULLS"},
		{kind: overwriteSymbol, value: ")"},
		{kind: overwriteWord, value: "FROM"},
	} {
		if err := p.expect(token); err != nil {
			return ports.QueryOperation{}, err
		}
	}
	sourceIdentifier, err := p.expectIdentifier()
	if err != nil {
		return ports.QueryOperation{}, err
	}
	for _, token := range []overwriteToken{
		{kind: overwriteSymbol, value: ")"},
		{kind: overwriteSymbol, value: ";"},
		{kind: overwriteWord, value: "MERGE"},
	} {
		if err := p.expect(token); err != nil {
			return ports.QueryOperation{}, err
		}
	}
	destinationIdentifier, err := p.expectIdentifier()
	if err != nil {
		return ports.QueryOperation{}, err
	}
	if err := p.expectWord("AS"); err != nil {
		return ports.QueryOperation{}, err
	}
	targetAlias, err := p.expectIdentifier()
	if err != nil || !isSparkConnectorAlias(targetAlias, "__target_") {
		return ports.QueryOperation{}, p.shapeError("connector target alias")
	}
	if err := p.expectWord("USING"); err != nil {
		return ports.QueryOperation{}, err
	}
	repeatedSourceIdentifier, err := p.expectIdentifier()
	if err != nil || repeatedSourceIdentifier != sourceIdentifier {
		return ports.QueryOperation{}, p.shapeError("identical source relation")
	}
	if err := p.expectWord("AS"); err != nil {
		return ports.QueryOperation{}, err
	}
	sourceAlias, err := p.expectIdentifier()
	if err != nil || !isSparkConnectorAlias(sourceAlias, "__source_") {
		return ports.QueryOperation{}, p.shapeError("connector source alias")
	}
	for _, word := range []string{"ON", "FALSE", "WHEN", "NOT", "MATCHED", "BY", "SOURCE", "AND"} {
		if err := p.expectWord(word); err != nil {
			return ports.QueryOperation{}, err
		}
	}
	for _, token := range []overwriteToken{
		{kind: overwriteSymbol, value: "("},
		{kind: overwriteWord, value: "TRUE"},
		{kind: overwriteSymbol, value: ")"},
		{kind: overwriteWord, value: "AND"},
	} {
		if err := p.expect(token); err != nil {
			return ports.QueryOperation{}, err
		}
	}
	targetExpression, err := p.parsePartitionExpression(targetAlias)
	if err != nil {
		return ports.QueryOperation{}, err
	}
	if targetExpression != sourceExpression {
		return ports.QueryOperation{}, p.shapeError("matching source and target partition expressions")
	}
	for _, token := range []overwriteToken{
		{kind: overwriteWord, value: "IN"},
		{kind: overwriteWord, value: "UNNEST"},
		{kind: overwriteSymbol, value: "("},
		{kind: overwriteWord, value: "PARTITIONS_TO_DELETE"},
		{kind: overwriteSymbol, value: ")"},
		{kind: overwriteWord, value: "THEN"},
		{kind: overwriteWord, value: "DELETE"},
		{kind: overwriteWord, value: "WHEN"},
		{kind: overwriteWord, value: "NOT"},
		{kind: overwriteWord, value: "MATCHED"},
		{kind: overwriteWord, value: "BY"},
		{kind: overwriteWord, value: "TARGET"},
		{kind: overwriteWord, value: "THEN"},
		{kind: overwriteWord, value: "INSERT"},
		{kind: overwriteSymbol, value: "("},
	} {
		if err := p.expect(token); err != nil {
			return ports.QueryOperation{}, err
		}
	}
	insertFields, err := p.parseIdentifierList()
	if err != nil {
		return ports.QueryOperation{}, err
	}
	if err := p.expectWord("VALUES"); err != nil {
		return ports.QueryOperation{}, err
	}
	if err := p.expect(overwriteToken{kind: overwriteSymbol, value: "("}); err != nil {
		return ports.QueryOperation{}, err
	}
	for index, field := range insertFields {
		if index > 0 {
			if err := p.expect(overwriteToken{kind: overwriteSymbol, value: ","}); err != nil {
				return ports.QueryOperation{}, err
			}
		}
		alias, err := p.expectIdentifier()
		if err != nil || alias != sourceAlias {
			return ports.QueryOperation{}, p.shapeError("source-qualified VALUES field")
		}
		if err := p.expect(overwriteToken{kind: overwriteSymbol, value: "."}); err != nil {
			return ports.QueryOperation{}, err
		}
		valueField, err := p.expectIdentifier()
		if err != nil || valueField != field {
			return ports.QueryOperation{}, p.shapeError("matching INSERT and VALUES fields")
		}
	}
	if err := p.expect(overwriteToken{kind: overwriteSymbol, value: ")"}); err != nil {
		return ports.QueryOperation{}, err
	}
	if p.index != len(p.tokens) {
		return ports.QueryOperation{}, p.shapeError("end of connector template")
	}

	destination, err := dynamicOverwriteTableReference(request, destinationIdentifier)
	if err != nil {
		return ports.QueryOperation{}, p.shapeError("two- or three-part destination table reference")
	}
	source, err := dynamicOverwriteTableReference(request, sourceIdentifier)
	if err != nil {
		return ports.QueryOperation{}, p.shapeError("two- or three-part source table reference")
	}
	if destination == source {
		return ports.QueryOperation{}, p.shapeError("distinct source and destination relations")
	}
	return ports.QueryOperation{
		Kind: ports.QueryOperationSparkDynamicTimePartitionOverwrite, ModelVersion: sparkDynamicTimeOverwriteModel,
		Destination: destination, Source: source,
		PartitionFunction: sourceExpression.function, PartitionField: sourceExpression.field,
		Granularity: sourceExpression.granularity, InsertFields: insertFields,
	}, nil
}

type dynamicPartitionExpression struct {
	function    string
	field       string
	granularity string
}

func (p *dynamicOverwriteParser) parsePartitionExpression(alias string) (dynamicPartitionExpression, error) {
	function, err := p.expectWordValue()
	if err != nil || function != "DATE_TRUNC" && function != "TIMESTAMP_TRUNC" {
		return dynamicPartitionExpression{}, p.shapeError("DATE_TRUNC or TIMESTAMP_TRUNC")
	}
	if err := p.expect(overwriteToken{kind: overwriteSymbol, value: "("}); err != nil {
		return dynamicPartitionExpression{}, err
	}
	if alias != "" {
		actualAlias, err := p.expectIdentifier()
		if err != nil || actualAlias != alias {
			return dynamicPartitionExpression{}, p.shapeError("target-qualified partition field")
		}
		if err := p.expect(overwriteToken{kind: overwriteSymbol, value: "."}); err != nil {
			return dynamicPartitionExpression{}, err
		}
	}
	field, err := p.expectIdentifier()
	if err != nil {
		return dynamicPartitionExpression{}, err
	}
	if err := p.expect(overwriteToken{kind: overwriteSymbol, value: ","}); err != nil {
		return dynamicPartitionExpression{}, err
	}
	granularity, err := p.expectWordValue()
	if err != nil || !validTimePartitionGranularity(granularity) {
		return dynamicPartitionExpression{}, p.shapeError("HOUR, DAY, MONTH, or YEAR granularity")
	}
	if err := p.expect(overwriteToken{kind: overwriteSymbol, value: ")"}); err != nil {
		return dynamicPartitionExpression{}, err
	}
	return dynamicPartitionExpression{function: function, field: field, granularity: granularity}, nil
}

func (p *dynamicOverwriteParser) parseIdentifierList() ([]string, error) {
	fields := make([]string, 0, 8)
	for {
		field, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
		if p.accept(overwriteToken{kind: overwriteSymbol, value: ","}) {
			continue
		}
		if err := p.expect(overwriteToken{kind: overwriteSymbol, value: ")"}); err != nil {
			return nil, err
		}
		return fields, nil
	}
}

func (p *dynamicOverwriteParser) expectWord(word string) error {
	return p.expect(overwriteToken{kind: overwriteWord, value: word})
}

func (p *dynamicOverwriteParser) expectWordValue() (string, error) {
	if p.index >= len(p.tokens) || p.tokens[p.index].kind != overwriteWord {
		return "", p.shapeError("word token")
	}
	value := p.tokens[p.index].value
	p.index++
	return value, nil
}

func (p *dynamicOverwriteParser) expectIdentifier() (string, error) {
	if p.index >= len(p.tokens) || p.tokens[p.index].kind != overwriteQuotedIdentifier {
		return "", p.shapeError("backtick identifier")
	}
	value := p.tokens[p.index].value
	p.index++
	return value, nil
}

func (p *dynamicOverwriteParser) expect(expected overwriteToken) error {
	if !p.accept(expected) {
		return p.shapeError("token " + expected.value)
	}
	return nil
}

func (p *dynamicOverwriteParser) accept(expected overwriteToken) bool {
	if p.index >= len(p.tokens) {
		return false
	}
	actual := p.tokens[p.index]
	if actual.kind != expected.kind || !strings.EqualFold(actual.value, expected.value) {
		return false
	}
	p.index++
	return true
}

type dynamicOverwriteShapeError struct {
	TokenIndex    int
	ExpectedShape string
	Cause         error
}

func (err *dynamicOverwriteShapeError) Error() string {
	if err.Cause != nil {
		return fmt.Sprintf("token_index=%d expected_shape=%s: %v", err.TokenIndex, err.ExpectedShape, err.Cause)
	}
	return fmt.Sprintf("token_index=%d expected_shape=%s", err.TokenIndex, err.ExpectedShape)
}

func (err *dynamicOverwriteShapeError) Unwrap() error {
	return err.Cause
}

func (p *dynamicOverwriteParser) shapeError(expected string) error {
	return &dynamicOverwriteShapeError{TokenIndex: p.index, ExpectedShape: expected}
}

func dynamicOverwriteTableReference(request ports.QueryRequest, identifier string) (domain.TableReference, error) {
	// BigQueryClient.fullTableName emits exactly dataset.table or
	// project.dataset.table; a one-part default-dataset relation is not produced
	// by the pinned connector path.
	// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L641-L647
	partCount := len(strings.Split(identifier, "."))
	if partCount != 2 && partCount != 3 {
		return domain.TableReference{}, fmt.Errorf("connector table reference must contain two or three parts")
	}
	reference, isCTE, err := queryTableReference(request, identifier, nil)
	if err != nil {
		return domain.TableReference{}, err
	}
	if isCTE {
		return domain.TableReference{}, fmt.Errorf("CTE is not valid in the connector overwrite model")
	}
	return reference, nil
}

func isSparkConnectorAlias(alias, prefix string) bool {
	if len(alias) != len(prefix)+32 || !strings.HasPrefix(alias, prefix) {
		return false
	}
	for _, value := range alias[len(prefix):] {
		if !(value >= '0' && value <= '9') && !(value >= 'a' && value <= 'f') {
			return false
		}
	}
	return true
}

func validTimePartitionGranularity(value string) bool {
	switch value {
	case "HOUR", "DAY", "MONTH", "YEAR":
		return true
	default:
		return false
	}
}

func dynamicOverwriteProfileError(cause error) error {
	return fmt.Errorf(
		"%w: connector query profile drift remains an unsupported script; capability=%s candidate_capability=%s model_version=%s fix_hint=compare BigQueryUtil.java 0.44.2 token shape: %w",
		domain.ErrUnsupported, domain.GapQueryScriptsUnsupportedV1,
		domain.CapabilitySparkDynamicTimePartitionOverwriteV1, sparkDynamicTimeOverwriteModel, cause,
	)
}
