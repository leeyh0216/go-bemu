package duckdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestRowRestrictionVisitorUsesCanonicalBindingsAndOnlyBoundLiterals(t *testing.T) {
	schema := []catalogdomain.Field{{
		Name: "Profile", Type: "RECORD", Fields: []catalogdomain.Field{{Name: "Rank", Type: "FLOAT64"}},
	}}
	expression := mustParseReadRestriction(t, "profile.rank >= 3.5")
	sql, args, err := compileRowRestriction(expression, schema)
	if err != nil {
		t.Fatal(err)
	}
	if sql != `"Profile"."Rank" >= ?` {
		t.Fatalf("canonical predicate SQL = %q", sql)
	}
	if len(args) != 1 || args[0] != 3.5 {
		t.Fatalf("predicate args = %#v", args)
	}
	if strings.Contains(sql, "3.5") || strings.Contains(sql, "profile") || strings.Contains(sql, "rank") {
		t.Fatalf("predicate SQL retained submitted token or literal: %s", sql)
	}
}

func TestRowRestrictionVisitorFailsClosedWithoutLeakingInput(t *testing.T) {
	schema := []catalogdomain.Field{
		{Name: "id", Type: "INT64"},
		{Name: "tags", Type: "STRING", Mode: "REPEATED"},
	}
	for _, input := range []string{
		"customer_secret = 'literal-secret'",
		"tags = 'literal-secret'",
		"LOWER(id) = 'literal-secret'",
		"id LIKE 'literal-secret'",
	} {
		expression := mustParseReadRestriction(t, input)
		sql, args, err := compileRowRestriction(expression, schema)
		if err == nil {
			t.Fatalf("unsupported predicate %q compiled as %s args=%#v", input, sql, args)
		}
		if sql != "" || args != nil {
			t.Fatalf("failed predicate returned a partial plan: sql=%q args=%#v", sql, args)
		}
		for _, secret := range []string{"customer_secret", "literal-secret", "LOWER", "LIKE", "tags"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("predicate error leaked %q: %v", secret, err)
			}
		}
		if !errors.Is(err, catalogdomain.ErrInvalid) && !errors.Is(err, catalogdomain.ErrUnsupported) {
			t.Fatalf("predicate error category = %v", err)
		}
	}
}

func TestUnsupportedRowRestrictionFailsBeforeSchemaResolution(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	resolver := &countingRowRestrictionResolver{}
	materializer, err := NewReadSnapshotMaterializer(
		warehouse, resolver, readSnapshotTestConfig(t.TempDir(), 1<<20),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = materializer.Materialize(ctx, readports.MaterializeRequest{
		Table: "projects/data-project/datasets/analytics/tables/events", Format: readdomain.FormatArrow,
		RowRestriction: mustParseReadRestriction(t, "LOWER(id) = 'literal-secret'"),
	})
	if readdomain.CodeOf(err) != readdomain.ErrorInvalidArgument {
		t.Fatalf("unsupported predicate code = %s: %v", readdomain.CodeOf(err), err)
	}
	if resolver.calls != 0 {
		t.Fatalf("schema resolver calls = %d, want 0", resolver.calls)
	}
}

type countingRowRestrictionResolver struct{ calls int }

func (resolver *countingRowRestrictionResolver) GetTable(context.Context, string, string, string) (catalogdomain.Table, error) {
	resolver.calls++
	return catalogdomain.Table{}, catalogdomain.ErrNotFound
}

func newReadRestrictionParser(t testing.TB) readports.RowRestrictionParser {
	t.Helper()
	parser, err := googlesqladapter.NewParser()
	if err != nil {
		t.Fatal(err)
	}
	return parser
}

func mustParseReadRestriction(t testing.TB, input string) queryast.Expression {
	t.Helper()
	expression, err := newReadRestrictionParser(t).ParseExpression(context.Background(), input)
	if err != nil {
		t.Fatalf("parse row restriction: %v", err)
	}
	return expression
}
