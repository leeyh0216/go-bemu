package duckdb

// Storage Read row_restriction accepts a GoogleSQL predicate, not a complete
// query. This deliberately small parser accepts the connector's common scalar
// pushdown shape and emits parameterized DuckDB SQL; unsupported grammar fails
// before any database side effect.
//
// Protocol sources:
//   - row_restriction: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession.tablereadoptions
//   - GoogleSQL lexical rules: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
)

type restrictionTokenKind uint8

const (
	restrictionEOF restrictionTokenKind = iota
	restrictionIdentifier
	restrictionString
	restrictionNumber
	restrictionOperator
	restrictionLeftParen
	restrictionRightParen
	restrictionDot
)

type restrictionToken struct {
	kind  restrictionTokenKind
	value string
}

type restrictionParser struct {
	tokens []restrictionToken
	index  int
	schema []catalogdomain.Field
	args   []any
}

func compileRowRestriction(input string, schema []catalogdomain.Field) (string, []any, error) {
	if strings.TrimSpace(input) == "" {
		return "", nil, nil
	}
	tokens, err := lexRowRestriction(input)
	if err != nil {
		return "", nil, err
	}
	parser := restrictionParser{tokens: tokens, schema: schema}
	expression, err := parser.parseOr()
	if err != nil {
		return "", nil, err
	}
	if parser.peek().kind != restrictionEOF {
		return "", nil, fmt.Errorf("unsupported row restriction token %q", parser.peek().value)
	}
	return expression, parser.args, nil
}

func lexRowRestriction(input string) ([]restrictionToken, error) {
	tokens := make([]restrictionToken, 0, 16)
	for index := 0; index < len(input); {
		current := input[index]
		switch {
		case unicode.IsSpace(rune(current)):
			index++
		case isIdentifierStart(current):
			end := index + 1
			for end < len(input) && isIdentifierPart(input[end]) {
				end++
			}
			tokens = append(tokens, restrictionToken{kind: restrictionIdentifier, value: input[index:end]})
			index = end
		case current == '`':
			identifier, end, err := scanBacktickIdentifier(input, index)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, restrictionToken{kind: restrictionIdentifier, value: identifier})
			index = end
		case current == '\'':
			value, end, err := scanRestrictionString(input, index)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, restrictionToken{kind: restrictionString, value: value})
			index = end
		case current >= '0' && current <= '9' || (current == '-' || current == '+') && index+1 < len(input) && input[index+1] >= '0' && input[index+1] <= '9':
			end := scanRestrictionNumber(input, index)
			tokens = append(tokens, restrictionToken{kind: restrictionNumber, value: input[index:end]})
			index = end
		case current == '(':
			tokens = append(tokens, restrictionToken{kind: restrictionLeftParen, value: "("})
			index++
		case current == ')':
			tokens = append(tokens, restrictionToken{kind: restrictionRightParen, value: ")"})
			index++
		case current == '.':
			tokens = append(tokens, restrictionToken{kind: restrictionDot, value: "."})
			index++
		case strings.ContainsRune("=<>!", rune(current)):
			end := index + 1
			if end < len(input) && (input[end] == '=' || current == '<' && input[end] == '>') {
				end++
			}
			operator := input[index:end]
			if operator == "!" {
				return nil, fmt.Errorf("unsupported row restriction operator %q", operator)
			}
			tokens = append(tokens, restrictionToken{kind: restrictionOperator, value: operator})
			index = end
		default:
			return nil, fmt.Errorf("unsupported row restriction character %q at byte %d", current, index)
		}
	}
	tokens = append(tokens, restrictionToken{kind: restrictionEOF})
	return tokens, nil
}

func scanRestrictionString(input string, start int) (string, int, error) {
	var value strings.Builder
	for index := start + 1; index < len(input); index++ {
		if input[index] != '\'' {
			if input[index] == '\\' {
				return "", 0, fmt.Errorf("backslash escapes are not supported in row restrictions")
			}
			value.WriteByte(input[index])
			continue
		}
		if index+1 < len(input) && input[index+1] == '\'' {
			value.WriteByte('\'')
			index++
			continue
		}
		return value.String(), index + 1, nil
	}
	return "", 0, fmt.Errorf("unterminated row restriction string")
}

func scanRestrictionNumber(input string, start int) int {
	index := start
	if input[index] == '-' || input[index] == '+' {
		index++
	}
	for index < len(input) && input[index] >= '0' && input[index] <= '9' {
		index++
	}
	if index < len(input) && input[index] == '.' {
		index++
		for index < len(input) && input[index] >= '0' && input[index] <= '9' {
			index++
		}
	}
	if index < len(input) && (input[index] == 'e' || input[index] == 'E') {
		index++
		if index < len(input) && (input[index] == '-' || input[index] == '+') {
			index++
		}
		for index < len(input) && input[index] >= '0' && input[index] <= '9' {
			index++
		}
	}
	return index
}

func (p *restrictionParser) parseOr() (string, error) {
	left, err := p.parseAnd()
	if err != nil {
		return "", err
	}
	for p.consumeKeyword("OR") {
		right, err := p.parseAnd()
		if err != nil {
			return "", err
		}
		left = "(" + left + " OR " + right + ")"
	}
	return left, nil
}

func (p *restrictionParser) parseAnd() (string, error) {
	left, err := p.parseUnary()
	if err != nil {
		return "", err
	}
	for p.consumeKeyword("AND") {
		right, err := p.parseUnary()
		if err != nil {
			return "", err
		}
		left = "(" + left + " AND " + right + ")"
	}
	return left, nil
}

func (p *restrictionParser) parseUnary() (string, error) {
	if p.consumeKeyword("NOT") {
		expression, err := p.parseUnary()
		if err != nil {
			return "", err
		}
		return "(NOT " + expression + ")", nil
	}
	if p.peek().kind == restrictionLeftParen {
		p.index++
		expression, err := p.parseOr()
		if err != nil {
			return "", err
		}
		if p.peek().kind != restrictionRightParen {
			return "", fmt.Errorf("row restriction is missing a closing parenthesis")
		}
		p.index++
		return "(" + expression + ")", nil
	}
	return p.parsePredicate()
}

func (p *restrictionParser) parsePredicate() (string, error) {
	path, field, err := p.parseFieldPath()
	if err != nil {
		return "", err
	}
	quoted := make([]string, len(path))
	for index, component := range path {
		quoted[index] = quoteIdentifier(component)
	}
	column := strings.Join(quoted, ".")
	if p.consumeKeyword("IS") {
		not := p.consumeKeyword("NOT")
		if !p.consumeKeyword("NULL") {
			return "", fmt.Errorf("row restriction IS supports only NULL")
		}
		if not {
			return column + " IS NOT NULL", nil
		}
		return column + " IS NULL", nil
	}
	operator := p.peek()
	if operator.kind != restrictionOperator {
		return "", fmt.Errorf("row restriction field %q requires a comparison or IS NULL", strings.Join(path, "."))
	}
	p.index++
	if operator.value == "==" {
		operator.value = "="
	}
	literal, err := p.parseLiteral(field)
	if err != nil {
		return "", err
	}
	p.args = append(p.args, literal)
	return column + " " + operator.value + " ?", nil
}

func (p *restrictionParser) parseFieldPath() ([]string, catalogdomain.Field, error) {
	first := p.peek()
	if first.kind != restrictionIdentifier || isRestrictionKeyword(first.value) {
		return nil, catalogdomain.Field{}, fmt.Errorf("row restriction requires a field identifier, got %q", first.value)
	}
	p.index++
	path := []string{first.value}
	for p.peek().kind == restrictionDot {
		p.index++
		next := p.peek()
		if next.kind != restrictionIdentifier || isRestrictionKeyword(next.value) {
			return nil, catalogdomain.Field{}, fmt.Errorf("invalid nested field path after %q", strings.Join(path, "."))
		}
		p.index++
		path = append(path, next.value)
	}
	field, found := findFieldPath(p.schema, path)
	if !found {
		return nil, catalogdomain.Field{}, fmt.Errorf("row restriction references unknown field %q", strings.Join(path, "."))
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		return nil, catalogdomain.Field{}, fmt.Errorf("row restriction on repeated field %q is not supported", strings.Join(path, "."))
	}
	return path, field, nil
}

func (p *restrictionParser) parseLiteral(field catalogdomain.Field) (any, error) {
	token := p.peek()
	p.index++
	switch token.kind {
	case restrictionString:
		return token.value, nil
	case restrictionNumber:
		upperType := strings.ToUpper(field.Type)
		if upperType == "INT64" || upperType == "INTEGER" {
			value, err := strconv.ParseInt(token.value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid INT64 literal %q", token.value)
			}
			return value, nil
		}
		value, err := strconv.ParseFloat(token.value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid numeric literal %q", token.value)
		}
		return value, nil
	case restrictionIdentifier:
		switch strings.ToUpper(token.value) {
		case "TRUE":
			return true, nil
		case "FALSE":
			return false, nil
		case "NULL":
			return nil, fmt.Errorf("use IS NULL instead of comparing with NULL")
		}
	}
	return nil, fmt.Errorf("unsupported row restriction literal %q", token.value)
}

func (p *restrictionParser) consumeKeyword(keyword string) bool {
	if p.peek().kind == restrictionIdentifier && strings.EqualFold(p.peek().value, keyword) {
		p.index++
		return true
	}
	return false
}

func (p *restrictionParser) peek() restrictionToken { return p.tokens[p.index] }

func isRestrictionKeyword(value string) bool {
	switch strings.ToUpper(value) {
	case "AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE":
		return true
	default:
		return false
	}
}

func findFieldPath(schema []catalogdomain.Field, path []string) (catalogdomain.Field, bool) {
	fields := schema
	for pathIndex, component := range path {
		found := false
		for _, field := range fields {
			if strings.EqualFold(field.Name, component) {
				if pathIndex == len(path)-1 {
					return field, true
				}
				fields = field.Fields
				found = true
				break
			}
		}
		if !found {
			return catalogdomain.Field{}, false
		}
	}
	return catalogdomain.Field{}, false
}
