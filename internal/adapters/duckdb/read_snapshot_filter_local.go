//go:build legacy_row_restriction_local

package duckdb

// This retired local parser is retained only for historical comparison. It is
// excluded from normal production and test builds: Storage Read restrictions
// enter through the official GoogleSQL AST adapter and its DuckDB visitor.

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

type localToken struct {
	text string
	kind localTokenKind
}
type localTokenKind uint8

const (
	localWord localTokenKind = iota
	localString
	localNumber
	localSymbol
	localEOF
)

type localNode interface{ localNode() }
type localPath []string

func (localPath) localNode() {}

type localLiteral struct {
	value    any
	temporal string
}

func (localLiteral) localNode() {}

type localUnary struct {
	op    string
	value localNode
}

func (localUnary) localNode() {}

type localLogical struct {
	op     string
	values []localNode
}

func (localLogical) localNode() {}

type localCompare struct {
	op          string
	left, right localNode
}

func (localCompare) localNode() {}

type localBetween struct {
	left, low, high localNode
	not             bool
}

func (localBetween) localNode() {}

type localIn struct {
	left   localNode
	values []localNode
	not    bool
}

func (localIn) localNode() {}

type localCall struct {
	name string
	args []localNode
}

func (localCall) localNode() {}

type localCast struct {
	value  localNode
	target string
}

func (localCast) localNode() {}

func compileLocalRowRestriction(input string, schema []catalogdomain.Field) (string, []any, readdomain.FilterShape, error) {
	if strings.TrimSpace(input) == "" {
		return "", nil, readdomain.FilterShape{}, nil
	}
	tokens, err := lexLocalRestriction(input)
	if err != nil {
		return "", nil, readdomain.FilterShape{}, err
	}
	p := localParser{tokens: tokens}
	node, err := p.parseOr()
	if err != nil {
		return "", nil, readdomain.FilterShape{}, err
	}
	if p.peek().kind != localEOF {
		return "", nil, readdomain.FilterShape{}, fmt.Errorf("invalid GoogleSQL row restriction syntax near %q", p.peek().text)
	}
	l := localLowerer{schema: schema}
	sql, err := l.lower(node, nil)
	if err != nil {
		return "", nil, readdomain.FilterShape{}, err
	}
	return sql, l.args, readdomain.FilterShape{PredicateCount: l.predicates, LogicalOperatorCount: l.logical}, nil
}

func lexLocalRestriction(s string) ([]localToken, error) {
	var out []localToken
	for i := 0; i < len(s); {
		if unicode.IsSpace(rune(s[i])) {
			i++
			continue
		}
		if s[i] == '\'' {
			start := i
			i++
			var b strings.Builder
			for {
				if i >= len(s) {
					return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax")
				}
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						b.WriteByte('\'')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(s[i])
				i++
			}
			_ = start
			out = append(out, localToken{b.String(), localString})
			continue
		}
		if s[i] == '`' {
			i++
			var b strings.Builder
			for i < len(s) && s[i] != '`' {
				b.WriteByte(s[i])
				i++
			}
			if i == len(s) {
				return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax")
			}
			i++
			if b.Len() == 0 {
				return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax")
			}
			out = append(out, localToken{b.String(), localWord})
			continue
		}
		if unicode.IsLetter(rune(s[i])) || s[i] == '_' {
			start := i
			i++
			for i < len(s) && (unicode.IsLetter(rune(s[i])) || unicode.IsDigit(rune(s[i])) || s[i] == '_') {
				i++
			}
			out = append(out, localToken{s[start:i], localWord})
			continue
		}
		if unicode.IsDigit(rune(s[i])) {
			start := i
			i++
			for i < len(s) && (unicode.IsDigit(rune(s[i])) || s[i] == '.' || s[i] == 'e' || s[i] == 'E' || s[i] == '+' || s[i] == '-') {
				if (s[i] == '+' || s[i] == '-') && s[i-1] != 'e' && s[i-1] != 'E' {
					break
				}
				i++
			}
			out = append(out, localToken{s[start:i], localNumber})
			continue
		}
		if i+1 < len(s) {
			two := s[i : i+2]
			if two == "<=" || two == ">=" || two == "!=" || two == "<>" {
				out = append(out, localToken{two, localSymbol})
				i += 2
				continue
			}
		}
		if strings.ContainsRune("()=<>.,+-", rune(s[i])) {
			out = append(out, localToken{s[i : i+1], localSymbol})
			i++
			continue
		}
		return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax")
	}
	return append(out, localToken{kind: localEOF}), nil
}

type localParser struct {
	tokens []localToken
	at     int
}

func (p *localParser) peek() localToken { return p.tokens[p.at] }
func (p *localParser) take() localToken {
	t := p.peek()
	if t.kind != localEOF {
		p.at++
	}
	return t
}
func (p *localParser) word(s string) bool {
	return p.peek().kind == localWord && strings.EqualFold(p.peek().text, s)
}
func (p *localParser) acceptWord(s string) bool {
	if p.word(s) {
		p.take()
		return true
	}
	return false
}
func (p *localParser) accept(s string) bool {
	if p.peek().text == s {
		p.take()
		return true
	}
	return false
}
func (p *localParser) require(s string) error {
	if !p.accept(s) {
		return fmt.Errorf("invalid GoogleSQL row restriction syntax: expected %q near %q", s, p.peek().text)
	}
	return nil
}

func (p *localParser) parseOr() (localNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	var values []localNode
	values = append(values, left)
	for p.acceptWord("OR") {
		n, e := p.parseAnd()
		if e != nil {
			return nil, e
		}
		values = append(values, n)
	}
	if len(values) == 1 {
		return left, nil
	}
	return localLogical{"OR", values}, nil
}
func (p *localParser) parseAnd() (localNode, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	var values []localNode
	values = append(values, left)
	for p.acceptWord("AND") {
		n, e := p.parseNot()
		if e != nil {
			return nil, e
		}
		values = append(values, n)
	}
	if len(values) == 1 {
		return left, nil
	}
	return localLogical{"AND", values}, nil
}
func (p *localParser) parseNot() (localNode, error) {
	if p.acceptWord("NOT") {
		n, e := p.parseNot()
		return localUnary{"NOT", n}, e
	}
	return p.parsePredicate()
}
func (p *localParser) parsePredicate() (localNode, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	// Parenthesized boolean expressions are already complete predicates. Only
	// continue into comparison parsing when the following token can start one.
	if p.peek().kind == localEOF || p.peek().text == ")" || p.word("AND") || p.word("OR") {
		return left, nil
	}
	if p.acceptWord("IS") {
		not := p.acceptWord("NOT")
		if !p.acceptWord("NULL") {
			return nil, fmt.Errorf("row restriction IS only supports NULL")
		}
		op := "IS"
		if not {
			op = "IS NOT"
		}
		return localCompare{op, left, localLiteral{value: nil}}, nil
	}
	not := p.acceptWord("NOT")
	if p.acceptWord("BETWEEN") {
		low, e := p.parsePrimary()
		if e != nil {
			return nil, e
		}
		if !p.acceptWord("AND") {
			return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax near %q", p.peek().text)
		}
		high, e := p.parsePrimary()
		return localBetween{left, low, high, not}, e
	}
	if p.acceptWord("IN") {
		if e := p.require("("); e != nil {
			return nil, e
		}
		var values []localNode
		for {
			n, e := p.parsePrimary()
			if e != nil {
				return nil, e
			}
			values = append(values, n)
			if !p.accept(",") {
				break
			}
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("row restriction IN requires at least one literal")
		}
		if e := p.require(")"); e != nil {
			return nil, e
		}
		return localIn{left, values, not}, nil
	}
	if not {
		return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax")
	}
	op := p.peek().text
	if op != "=" && op != "!=" && op != "<>" && op != "<" && op != "<=" && op != ">" && op != ">=" {
		return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax: comparison near %q", op)
	}
	p.take()
	right, e := p.parsePrimary()
	return localCompare{op, left, right}, e
}
func (p *localParser) parsePrimary() (localNode, error) {
	if p.accept("(") {
		n, e := p.parseOr()
		if e != nil {
			return nil, e
		}
		return n, p.require(")")
	}
	if p.accept("+") || p.accept("-") {
		op := p.tokens[p.at-1].text
		n, e := p.parsePrimary()
		return localUnary{op, n}, e
	}
	t := p.take()
	if t.kind == localString {
		return localLiteral{value: t.text}, nil
	}
	if t.kind == localNumber {
		n, e := strconv.ParseFloat(t.text, 64)
		if e != nil {
			return nil, fmt.Errorf("invalid numeric literal %q", t.text)
		}
		return localLiteral{value: n}, nil
	}
	if t.kind != localWord {
		return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax: primary near %q", t.text)
	}
	if strings.EqualFold(t.text, "TRUE") {
		return localLiteral{value: true}, nil
	}
	if strings.EqualFold(t.text, "FALSE") {
		return localLiteral{value: false}, nil
	}
	if strings.EqualFold(t.text, "NULL") {
		return localLiteral{value: nil}, nil
	}
	if (strings.EqualFold(t.text, "DATE") || strings.EqualFold(t.text, "TIMESTAMP")) && p.peek().kind == localString {
		value := p.take().text
		return localLiteral{value: value, temporal: strings.ToUpper(t.text)}, nil
	}
	if strings.EqualFold(t.text, "CAST") && p.accept("(") {
		value, e := p.parsePrimary()
		if e != nil {
			return nil, e
		}
		if !p.acceptWord("AS") {
			return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax")
		}
		target := p.take()
		if target.kind != localWord {
			return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax")
		}
		if e := p.require(")"); e != nil {
			return nil, e
		}
		return localCast{value, target.text}, nil
	}
	if p.accept("(") {
		var args []localNode
		if !p.accept(")") {
			for {
				n, e := p.parsePrimary()
				if e != nil {
					return nil, e
				}
				args = append(args, n)
				if p.accept(")") {
					break
				}
				if e := p.require(","); e != nil {
					return nil, e
				}
			}
		}
		return localCall{t.text, args}, nil
	}
	path := localPath{t.text}
	for p.accept(".") {
		next := p.take()
		if next.kind != localWord {
			return nil, fmt.Errorf("invalid GoogleSQL row restriction syntax")
		}
		path = append(path, next.text)
	}
	return path, nil
}

type localLowerer struct {
	schema              []catalogdomain.Field
	args                []any
	predicates, logical int
}

func (l *localLowerer) lower(n localNode, expected *catalogdomain.Field) (string, error) {
	switch v := n.(type) {
	case localPath:
		return l.path(v)
	case localLiteral:
		if v.value == nil {
			return "NULL", nil
		}
		value := v.value
		if n, ok := value.(float64); ok && expected != nil && (strings.EqualFold(expected.Type, "INT64") || strings.EqualFold(expected.Type, "INTEGER")) {
			i, e := strconv.ParseInt(strconv.FormatFloat(n, 'f', -1, 64), 10, 64)
			if e != nil {
				return "", fmt.Errorf("invalid INT64 literal")
			}
			value = i
		}
		l.args = append(l.args, value)
		if v.temporal == "DATE" {
			return "CAST(? AS DATE)", nil
		}
		if v.temporal == "TIMESTAMP" {
			return "CAST(? AS TIMESTAMPTZ)", nil
		}
		return "?", nil
	case localUnary:
		sql, e := l.lower(v.value, nil)
		if e != nil {
			return "", e
		}
		if v.op == "NOT" {
			l.logical++
			return "(NOT " + sql + ")", nil
		}
		return "(" + v.op + sql + ")", nil
	case localLogical:
		parts := make([]string, 0, len(v.values))
		for _, child := range v.values {
			s, e := l.lower(child, nil)
			if e != nil {
				return "", e
			}
			parts = append(parts, s)
		}
		l.logical += len(parts) - 1
		return "(" + strings.Join(parts, " "+v.op+" ") + ")", nil
	case localCompare:
		left, field, bound, e := l.comparisonLeft(v.left)
		if e != nil {
			return "", e
		}
		if v.op == "IS" || v.op == "IS NOT" {
			l.predicates++
			return left + " " + v.op + " NULL", nil
		}
		var want *catalogdomain.Field
		if bound {
			want = &field
		}
		right, e := l.lower(v.right, want)
		if e != nil {
			return "", e
		}
		l.predicates++
		return left + " " + v.op + " " + right, nil
	case localBetween:
		left, field, _, e := l.comparisonLeft(v.left)
		if e != nil {
			return "", e
		}
		low, e := l.lower(v.low, &field)
		if e != nil {
			return "", e
		}
		high, e := l.lower(v.high, &field)
		if e != nil {
			return "", e
		}
		l.predicates++
		not := ""
		if v.not {
			not = " NOT"
		}
		return left + not + " BETWEEN " + low + " AND " + high, nil
	case localIn:
		left, field, _, e := l.comparisonLeft(v.left)
		if e != nil {
			return "", e
		}
		parts := make([]string, 0, len(v.values))
		for _, item := range v.values {
			s, e := l.lower(item, &field)
			if e != nil {
				return "", e
			}
			parts = append(parts, s)
		}
		l.predicates++
		not := ""
		if v.not {
			not = " NOT"
		}
		return left + not + " IN (" + strings.Join(parts, ", ") + ")", nil
	case localCast:
		sql, e := l.lower(v.value, nil)
		if e != nil {
			return "", e
		}
		target, ok := duckDBCastTarget(v.target)
		if !ok {
			return "", fmt.Errorf("unsupported GoogleSQL CAST target %q", v.target)
		}
		return "CAST(" + sql + " AS " + target + ")", nil
	case localCall:
		return l.call(v)
	default:
		return "", fmt.Errorf("unsupported GoogleSQL row restriction expression")
	}
}
func (l *localLowerer) path(path localPath) (string, error) {
	field, ok := findFieldPath(l.schema, []string(path))
	if !ok {
		return "", fmt.Errorf("row restriction references unknown field %q", strings.Join(path, "."))
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		return "", fmt.Errorf("row restriction on repeated field %q is not supported", strings.Join(path, "."))
	}
	quoted := make([]string, len(path))
	for i, s := range path {
		quoted[i] = quoteIdentifier(s)
	}
	return strings.Join(quoted, "."), nil
}
func (l *localLowerer) comparisonLeft(n localNode) (string, catalogdomain.Field, bool, error) {
	if p, ok := n.(localPath); ok {
		field, found := findFieldPath(l.schema, []string(p))
		if !found {
			return "", catalogdomain.Field{}, false, fmt.Errorf("row restriction references unknown field %q", strings.Join(p, "."))
		}
		sql, e := l.path(p)
		return sql, field, true, e
	}
	sql, e := l.lower(n, nil)
	return sql, catalogdomain.Field{}, false, e
}
func (l *localLowerer) call(call localCall) (string, error) {
	name := strings.ToUpper(call.name)
	allowed := map[string]string{"LOWER": "LOWER", "UPPER": "UPPER", "STARTS_WITH": "starts_with", "ENDS_WITH": "ends_with", "LENGTH": "length", "DATE": "DATE", "TIMESTAMP": "CAST"}
	mapped, ok := allowed[name]
	if !ok {
		return "", fmt.Errorf("unsupported GoogleSQL row restriction function %q", call.name)
	}
	if len(call.args) != 1 && name != "STARTS_WITH" && name != "ENDS_WITH" {
		return "", fmt.Errorf("GoogleSQL function %q has invalid argument count", call.name)
	}
	if (name == "STARTS_WITH" || name == "ENDS_WITH") && len(call.args) != 2 {
		return "", fmt.Errorf("GoogleSQL function %q has invalid argument count", call.name)
	}
	parts := make([]string, 0, len(call.args))
	for _, arg := range call.args {
		s, e := l.lower(arg, nil)
		if e != nil {
			return "", e
		}
		parts = append(parts, s)
	}
	if name == "TIMESTAMP" {
		return "CAST(" + parts[0] + " AS TIMESTAMPTZ)", nil
	}
	return mapped + "(" + strings.Join(parts, ", ") + ")", nil
}
