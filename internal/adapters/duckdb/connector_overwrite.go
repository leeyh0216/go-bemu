package duckdb

// Spark connector static overwrite adapter.
//
// This is deliberately a versioned semantic adapter, not a general SQL
// rewrite. spark-bigquery-connector 0.44.2 emits one exact constant-false
// MERGE shape after direct writes to a temporary table. BigQuery defines that
// shape as an atomic replace: every source row is inserted and every target
// row absent from the source is deleted. DuckDB has the same atomic MERGE
// primitive but spells BigQuery's `INSERT ROW` as `INSERT BY NAME`.
//
// Sources:
//   - connector 0.44.2 producer: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java
//   - BigQuery constant-false MERGE: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
//   - DuckDB MERGE: https://duckdb.org/docs/current/sql/statements/merge_into

import (
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const sparkStaticOverwriteModel = "spark-bigquery-connector-0.44.2/static-overwrite"

type overwriteTokenKind uint8

const (
	overwriteWord overwriteTokenKind = iota + 1
	overwriteQuotedIdentifier
	overwriteSymbol
)

type overwriteToken struct {
	kind  overwriteTokenKind
	value string
}

// rewriteSparkStaticOverwrite returns matched=false for ordinary SQL. A MERGE
// containing BigQuery's INSERT ROW is treated as an adapter candidate; if its
// token shape differs from the pinned connector profile, it fails explicitly
// instead of falling through to a backend parser with ambiguous behavior.
func rewriteSparkStaticOverwrite(request ports.QueryRequest) (statement string, matched bool, err error) {
	if queryStatementType(request.SQL) != "MERGE" {
		return "", false, nil
	}
	candidate, err := containsUnquotedWordPair(request.SQL, "INSERT", "ROW")
	if err != nil || !candidate {
		return "", false, nil
	}
	matched = true
	tokens, err := lexStaticOverwrite(request.SQL)
	if err != nil {
		return "", true, staticOverwriteProfileError(err)
	}

	parser := overwriteParser{tokens: tokens}
	if err := parser.expectWord("MERGE"); err != nil {
		return "", true, parser.profileError(err)
	}
	destination, err := parser.expectIdentifier()
	if err != nil {
		return "", true, parser.profileError(err)
	}
	for _, expected := range []overwriteToken{
		{kind: overwriteWord, value: "USING"},
		{kind: overwriteSymbol, value: "("},
		{kind: overwriteWord, value: "SELECT"},
		{kind: overwriteSymbol, value: "*"},
		{kind: overwriteWord, value: "FROM"},
	} {
		if err := parser.expect(expected); err != nil {
			return "", true, parser.profileError(err)
		}
	}
	source, err := parser.expectIdentifier()
	if err != nil {
		return "", true, parser.profileError(err)
	}
	for _, wordOrSymbol := range []overwriteToken{
		{kind: overwriteSymbol, value: ")"},
		{kind: overwriteWord, value: "ON"},
		{kind: overwriteWord, value: "FALSE"},
		{kind: overwriteWord, value: "WHEN"},
		{kind: overwriteWord, value: "NOT"},
		{kind: overwriteWord, value: "MATCHED"},
		{kind: overwriteWord, value: "THEN"},
		{kind: overwriteWord, value: "INSERT"},
		{kind: overwriteWord, value: "ROW"},
		{kind: overwriteWord, value: "WHEN"},
		{kind: overwriteWord, value: "NOT"},
		{kind: overwriteWord, value: "MATCHED"},
		{kind: overwriteWord, value: "BY"},
		{kind: overwriteWord, value: "SOURCE"},
		{kind: overwriteWord, value: "THEN"},
		{kind: overwriteWord, value: "DELETE"},
	} {
		if err := parser.expect(wordOrSymbol); err != nil {
			return "", true, parser.profileError(err)
		}
	}
	if parser.more() {
		return "", true, parser.profileError(fmt.Errorf("unexpected trailing token %q", parser.peek().value))
	}

	destinationSQL, err := translateRelationIdentifier(request, destination, nil)
	if err != nil {
		return "", true, err
	}
	sourceSQL, err := translateRelationIdentifier(request, source, nil)
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf(
		"MERGE INTO %s\nUSING (SELECT * FROM %s) AS __bqemu_source_0442\nON FALSE\nWHEN NOT MATCHED THEN INSERT BY NAME\nWHEN NOT MATCHED BY SOURCE THEN DELETE",
		destinationSQL, sourceSQL,
	), true, nil
}

func lexStaticOverwrite(statement string) ([]overwriteToken, error) {
	tokens := make([]overwriteToken, 0, 24)
	for index := 0; index < len(statement); {
		switch {
		case statement[index] == ' ' || statement[index] == '\t' || statement[index] == '\r' || statement[index] == '\n':
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
		case statement[index] == '(' || statement[index] == ')' || statement[index] == '*':
			tokens = append(tokens, overwriteToken{kind: overwriteSymbol, value: statement[index : index+1]})
			index++
		default:
			return nil, fmt.Errorf("%w: unsupported token in %s", domain.ErrInvalid, sparkStaticOverwriteModel)
		}
	}
	return tokens, nil
}

func containsUnquotedWordPair(statement, first, second string) (bool, error) {
	previous := ""
	for index := 0; index < len(statement); {
		switch {
		case statement[index] == '\'' || statement[index] == '"':
			end, err := scanQuotedLiteral(statement, index, statement[index])
			if err != nil {
				return false, err
			}
			index, previous = end, ""
		case statement[index] == '`':
			_, end, err := scanBacktickIdentifier(statement, index)
			if err != nil {
				return false, err
			}
			index, previous = end, ""
		case statement[index] == '-' && index+1 < len(statement) && statement[index+1] == '-':
			index, previous = scanLineComment(statement, index), ""
		case statement[index] == '#':
			index, previous = scanLineComment(statement, index), ""
		case statement[index] == '/' && index+1 < len(statement) && statement[index+1] == '*':
			end, err := scanBlockComment(statement, index)
			if err != nil {
				return false, err
			}
			index, previous = end, ""
		case isIdentifierStart(statement[index]):
			end := index + 1
			for end < len(statement) && isIdentifierPart(statement[end]) {
				end++
			}
			word := strings.ToUpper(statement[index:end])
			if previous == strings.ToUpper(first) && word == strings.ToUpper(second) {
				return true, nil
			}
			previous, index = word, end
		default:
			index++
		}
	}
	return false, nil
}

func (t overwriteToken) isWord(word string) bool {
	return t.kind == overwriteWord && strings.EqualFold(t.value, word)
}

type overwriteParser struct {
	tokens []overwriteToken
	index  int
}

func (p *overwriteParser) expectWord(word string) error {
	return p.expect(overwriteToken{kind: overwriteWord, value: word})
}

func (p *overwriteParser) expectIdentifier() (string, error) {
	if !p.more() || p.peek().kind != overwriteQuotedIdentifier {
		return "", fmt.Errorf("expected a backtick-qualified table identifier")
	}
	token := p.peek()
	p.index++
	return token.value, nil
}

func (p *overwriteParser) expect(expected overwriteToken) error {
	if !p.more() {
		return fmt.Errorf("expected %q at end of statement", expected.value)
	}
	actual := p.peek()
	if actual.kind != expected.kind || !strings.EqualFold(actual.value, expected.value) {
		return fmt.Errorf("expected %q, got %q", expected.value, actual.value)
	}
	p.index++
	return nil
}

func (p *overwriteParser) more() bool { return p.index < len(p.tokens) }

func (p *overwriteParser) peek() overwriteToken { return p.tokens[p.index] }

func (p *overwriteParser) profileError(cause error) error {
	return staticOverwriteProfileError(cause)
}

func staticOverwriteProfileError(cause error) error {
	return fmt.Errorf("%w: SQL resembles %s but its token shape changed: %v", domain.ErrInvalid, sparkStaticOverwriteModel, cause)
}
