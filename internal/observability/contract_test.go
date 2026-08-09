package observability

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCoreIsProtocolNeutralAndEventLiteralsAreVocabulary(t *testing.T) {
	root := filepath.Join("..")
	allowed := map[string]bool{"boundary.enter": true, "boundary.exit": true, "boundary.reject": true, "side_effect.before": true, "side_effect.after": true, "side_effect.error": true, "domain.transition": true}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		if filepath.Clean(filepath.Dir(path)) == filepath.Join("..", "observability") {
			for _, spec := range file.Imports {
				got := strings.Trim(spec.Path.Value, "\"")
				if got == "net/http" || strings.HasPrefix(got, "google.golang.org/grpc") || strings.HasPrefix(got, "google.golang.org/protobuf") {
					t.Errorf("observability core imports transport dependency %q", got)
				}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			for i := 0; i+1 < len(call.Args); i++ {
				key, ok := call.Args[i].(*ast.BasicLit)
				if !ok || key.Kind != token.STRING || strings.Trim(key.Value, "\"") != "event" {
					continue
				}
				value, ok := call.Args[i+1].(*ast.BasicLit)
				if !ok || value.Kind != token.STRING {
					continue
				}
				if !allowed[strings.Trim(value.Value, "\"")] {
					t.Errorf("%s has non-vocabulary event %s", path, value.Value)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestEventCatalogDocumentsDescriptor(t *testing.T) {
	for _, catalog := range []string{filepath.Join("..", "..", "docs", "en", "observability-events.md"), filepath.Join("..", "..", "docs", "ko", "observability-events.md")} {
		contents, err := os.ReadFile(catalog)
		if err != nil {
			t.Fatal(err)
		}
		for _, kind := range []EventKind{BoundaryEnter, BoundaryExit, BoundaryReject, SideEffectBefore, SideEffectAfter, SideEffectError, DomainTransition} {
			if !strings.Contains(string(contents), "`"+string(kind)+"`") {
				t.Errorf("%s omits %s", catalog, kind)
			}
		}
	}
}
