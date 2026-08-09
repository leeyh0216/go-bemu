package googlesql

import (
	"context"
	"testing"

	gsql "github.com/goccy/go-googlesql"
)

// Storage Read predicates are parsed by wrapping them in a single SELECT.
// Keep this traversal pinned before replacing the DuckDB-local tokenizer with
// a typed GoogleSQL AST lowering visitor.
func TestStorageReadPredicateASTTraversal(t *testing.T) {
	parser, err := NewParser()
	if err != nil {
		t.Fatal(err)
	}
	expression, err := parser.ParseStorageReadPredicate(context.Background(), "active = TRUE")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := expression.(*gsql.ASTBinaryExpression); !ok {
		t.Fatalf("predicate type = %T, want *ASTBinaryExpression", expression)
	}
}
