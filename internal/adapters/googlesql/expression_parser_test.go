package googlesql_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

func TestExpressionParserMapsStorageReadPredicate(t *testing.T) {
	parser := newParser(t)
	expression, err := parser.ParseExpression(context.Background(), "`payload`.`score` >= 1 AND active = TRUE")
	if err != nil {
		t.Fatalf("ParseExpression() error = %v", err)
	}
	root, ok := expression.(*queryast.BinaryExpression)
	if !ok || root.Operator() != "AND" {
		t.Fatalf("expression = %#v", expression)
	}
	left, ok := root.Left().(*queryast.BinaryExpression)
	if !ok || left.Operator() != ">=" {
		t.Fatalf("left predicate = %#v", root.Left())
	}
	path := left.Left().(*queryast.IdentifierExpression).Path()
	if got := strings.Join(path.Segments(), "."); got != "payload.score" {
		t.Fatalf("path = %q", got)
	}
	if expression.NodeKey().Fingerprint() == "" {
		t.Fatal("expression omitted stable node identity")
	}

	expression, err = parser.ParseExpression(context.Background(), "BIGNUMERIC '001.2500'")
	if err != nil {
		t.Fatal(err)
	}
	decimal := expression.(*queryast.DecimalLiteral)
	if decimal.Type() != queryast.TypeBigNumeric || decimal.CanonicalValue() != "1.25" {
		t.Fatalf("decimal = (%q, %q)", decimal.Type(), decimal.CanonicalValue())
	}

	expression, err = parser.ParseExpression(context.Background(), "id BETWEEN 2 AND 4")
	if err != nil {
		t.Fatal(err)
	}
	between, ok := expression.(*queryast.BetweenExpression)
	if !ok || between.Not() || between.Value().Kind() != queryast.ExpressionIdentifier ||
		between.Low().Kind() != queryast.ExpressionInteger || between.High().Kind() != queryast.ExpressionInteger {
		t.Fatalf("BETWEEN expression = %#v", expression)
	}
}

func TestExpressionParserFailsClosedAndRedactsSubmittedPredicate(t *testing.T) {
	parser := newParser(t)
	tests := []struct {
		input string
		kind  error
	}{
		{input: "customer_secret ->> 'name'", kind: domain.ErrInvalid},
		{input: "customer_secret = 1; DELETE FROM dataset.table", kind: domain.ErrInvalid},
	}
	for _, tt := range tests {
		expression, err := parser.ParseExpression(context.Background(), tt.input)
		if !errors.Is(err, tt.kind) {
			t.Fatalf("ParseExpression(%q) = (%#v, %v), want %v", tt.input, expression, err, tt.kind)
		}
		if strings.Contains(err.Error(), "customer_secret") || strings.Contains(err.Error(), tt.input) {
			t.Fatalf("error leaked predicate: %v", err)
		}
	}
}
