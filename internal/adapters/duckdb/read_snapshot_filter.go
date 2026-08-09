package duckdb

// Storage Read row_restriction is recognized solely by the GoogleSQL parser.
// This adapter validates the resulting canonical AST against the table schema
// and lowers it to DuckDB with bound values; it never tokenizes client SQL.

import (
	"context"
	"fmt"
	"strings"

	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

func compileRowRestriction(input string, schema []catalogdomain.Field) (string, []any, error) {
	if strings.TrimSpace(input) == "" {
		return "", nil, nil
	}
	parser, err := googlesqladapter.NewParser()
	if err != nil {
		return "", nil, fmt.Errorf("initialize GoogleSQL row restriction parser: %w", err)
	}
	expression, err := parser.ParseExpression(context.Background(), input)
	if err != nil {
		return "", nil, err
	}
	lowerer := restrictionLowerer{schema: schema}
	if err := lowerer.validate(expression); err != nil {
		return "", nil, err
	}
	renderer := duckDBStatementRenderer{}
	sql, err := renderer.renderExpression(expression)
	if err != nil {
		return "", nil, fmt.Errorf("lower GoogleSQL row restriction: %w", err)
	}
	return sql, append([]any(nil), renderer.arguments...), nil
}

type restrictionLowerer struct {
	schema []catalogdomain.Field
}

func (lowerer restrictionLowerer) validate(expression queryast.Expression) error {
	if expression == nil {
		return fmt.Errorf("row restriction has an empty expression")
	}
	switch value := expression.(type) {
	case *queryast.IdentifierExpression:
		return lowerer.validatePath(value.Path())
	case *queryast.NullLiteral, *queryast.BooleanLiteral, *queryast.IntegerLiteral,
		*queryast.FloatLiteral, *queryast.DecimalLiteral, *queryast.StringLiteral,
		*queryast.TemporalLiteral:
		return nil
	case *queryast.ParenthesizedExpression:
		return lowerer.validate(value.Inner())
	case *queryast.UnaryExpression:
		operator := strings.ToUpper(strings.TrimSpace(string(value.Operator())))
		if operator != "NOT" && operator != "+" && operator != "-" {
			return fmt.Errorf("unsupported GoogleSQL row restriction unary operator %q", operator)
		}
		return lowerer.validate(value.Value())
	case *queryast.BinaryExpression:
		operator := strings.ToUpper(strings.TrimSpace(string(value.Operator())))
		switch operator {
		case "AND", "OR", "=", "!=", "<>", "<", "<=", ">", ">=", "IS", "IS NOT":
		default:
			return fmt.Errorf("unsupported GoogleSQL row restriction operator %q", operator)
		}
		if err := lowerer.validate(value.Left()); err != nil {
			return err
		}
		return lowerer.validate(value.Right())
	case *queryast.InExpression:
		if value.Subquery() != nil || value.Unnest() != nil || len(value.Options()) == 0 {
			return fmt.Errorf("row restriction IN supports only a non-empty literal list")
		}
		if err := lowerer.validate(value.Value()); err != nil {
			return err
		}
		for _, option := range value.Options() {
			if err := lowerer.validate(option); err != nil {
				return err
			}
		}
		return nil
	case *queryast.CastExpression:
		if value.Safe() {
			return fmt.Errorf("SAFE_CAST is not supported in row restrictions")
		}
		if !restrictionCastType(value.Type().Kind()) {
			return fmt.Errorf("unsupported GoogleSQL CAST target %q", value.Type().Kind())
		}
		return lowerer.validate(value.Value())
	case *queryast.FunctionCall:
		if value.Distinct() || value.NullHandling() != queryast.FunctionNullHandlingDefault {
			return fmt.Errorf("row restriction function modifiers are not supported")
		}
		name := value.Name().Segments()
		if len(name) != 1 || !restrictionFunction(name[0], len(value.Arguments())) {
			return fmt.Errorf("unsupported GoogleSQL row restriction function")
		}
		for _, argument := range value.Arguments() {
			if err := lowerer.validate(argument); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported GoogleSQL row restriction expression %T", expression)
	}
}

func (lowerer restrictionLowerer) validatePath(path queryast.IdentifierPath) error {
	segments := path.Segments()
	field, found := findFieldPath(lowerer.schema, segments)
	if !found {
		return fmt.Errorf("row restriction references unknown field %q", strings.Join(segments, "."))
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		return fmt.Errorf("row restriction on repeated field %q is not supported", strings.Join(segments, "."))
	}
	return nil
}

func restrictionCastType(kind queryast.TypeKind) bool {
	switch kind {
	case queryast.TypeString, queryast.TypeBool, queryast.TypeInt64, queryast.TypeFloat64,
		queryast.TypeDate, queryast.TypeDatetime, queryast.TypeTime, queryast.TypeTimestamp:
		return true
	default:
		return false
	}
}

func restrictionFunction(name string, arguments int) bool {
	switch strings.ToUpper(name) {
	case "LOWER":
		return arguments == 1
	case "STARTS_WITH":
		return arguments == 2
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
