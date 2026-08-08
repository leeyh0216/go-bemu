package contract

import (
	"bufio"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const contractTestImport = "github.com/leeyh0216/go-bemu/internal/contracttest"

var (
	pythonOperationMarker = regexp.MustCompile(`^\s*@pytest\.mark\.operation\(["']([^"']+)["']\)\s*$`)
	bqOperationMarker     = regexp.MustCompile(`^\s*@operation\(["']([^"']+)["']\)\s*$`)
)

func ValidateOperationAnnotations(repositoryRoot string, manifest OperationManifest) error {
	annotations, err := collectOperationAnnotations(repositoryRoot)
	if err != nil {
		return err
	}
	known := make(map[string]Operation, len(manifest.Operations))
	for _, operation := range manifest.Operations {
		known[operation.ID] = operation
	}
	for operationID, tests := range annotations {
		operation, exists := known[operationID]
		if !exists {
			return fmt.Errorf("operation annotation references unknown operation %q", operationID)
		}
		declared := stringSet(operation.Tests...)
		for testID := range tests {
			if !declared[testID] {
				return fmt.Errorf("operation %s annotation in %s is missing from manifest tests", operationID, testID)
			}
		}
	}
	for _, operation := range manifest.Operations {
		annotated := annotations[operation.ID]
		for _, testID := range operation.Tests {
			if !annotated[testID] {
				return fmt.Errorf("operation %s declares test %s without an operation annotation", operation.ID, testID)
			}
		}
		if err := validateVerificationTests(operation.Verification, operation.Tests); err != nil {
			return fmt.Errorf("operation %s: %w", operation.ID, err)
		}
	}
	return nil
}

type testVerificationLevel int

const (
	testLevelNone testVerificationLevel = iota
	testLevelUnit
	testLevelApplication
	testLevelTransport
	testLevelPublicProcess
)

func validateVerificationTests(verification OperationVerification, testIDs []string) error {
	required, known := map[OperationVerification]testVerificationLevel{
		VerificationNone:          testLevelNone,
		VerificationUnit:          testLevelUnit,
		VerificationApplication:   testLevelApplication,
		VerificationTransport:     testLevelTransport,
		VerificationPublicProcess: testLevelPublicProcess,
	}[verification]
	if !known {
		return fmt.Errorf("unknown verification %q", verification)
	}
	if required == testLevelNone {
		if len(testIDs) != 0 {
			return errors.New("none verification does not allow tests")
		}
		return nil
	}
	meetsPrimary := false
	for _, testID := range testIDs {
		level, err := verificationLevelForTest(testID)
		if err != nil {
			return err
		}
		if level >= required {
			meetsPrimary = true
		}
	}
	if !meetsPrimary {
		return fmt.Errorf("%s verification requires at least one test at that level", verification)
	}
	return nil
}

func verificationLevelForTest(testID string) (testVerificationLevel, error) {
	kind, remainder, _ := strings.Cut(testID, ":")
	switch kind {
	case "python", "spark", "bq":
		return testLevelPublicProcess, nil
	case "go":
		path, _, _ := strings.Cut(remainder, ":")
		switch {
		case strings.HasPrefix(path, "internal/transport/"), strings.HasPrefix(path, "internal/admin/"):
			return testLevelTransport, nil
		case strings.Contains(path, "/application"):
			return testLevelApplication, nil
		default:
			return testLevelUnit, nil
		}
	default:
		return testLevelNone, fmt.Errorf("unknown test kind in %s", testID)
	}
}

func collectOperationAnnotations(repositoryRoot string) (map[string]map[string]bool, error) {
	annotations := make(map[string]map[string]bool)
	err := filepath.WalkDir(repositoryRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != repositoryRoot && ignoredAnnotationDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		relative, err := filepath.Rel(repositoryRoot, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		switch {
		case strings.HasSuffix(path, "_test.go"):
			return collectGoAnnotations(path, relative, annotations)
		case strings.HasSuffix(path, ".py") && (strings.HasPrefix(relative, "tests/python/") || strings.HasPrefix(relative, "tests/spark/") || strings.HasPrefix(relative, "tests/bqcli/")):
			return collectPythonAnnotations(path, relative, annotations)
		default:
			return nil
		}
	})
	return annotations, err
}

func ignoredAnnotationDirectory(name string) bool {
	switch name {
	case ".git", ".artifacts", ".venv", "vendor", "node_modules", "__pycache__":
		return true
	default:
		return false
	}
}

func collectGoAnnotations(path, relative string, annotations map[string]map[string]bool) error {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		return fmt.Errorf("parse Go test annotations %s: %w", relative, err)
	}
	aliases := make(map[string]bool)
	for _, imported := range file.Imports {
		importPath, err := strconv.Unquote(imported.Path.Value)
		if err != nil || importPath != contractTestImport {
			continue
		}
		alias := "contracttest"
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		aliases[alias] = true
	}
	if len(aliases) == 0 {
		return nil
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv != nil || !strings.HasPrefix(function.Name.Name, "Test") || function.Body == nil {
			continue
		}
		testID := "go:" + strings.TrimSuffix(relative, "_test.go") + ":" + function.Name.Name
		var annotationErr error
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if annotationErr != nil {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Operation" || len(call.Args) != 2 {
				return true
			}
			identifier, ok := selector.X.(*ast.Ident)
			if !ok || !aliases[identifier.Name] {
				return true
			}
			literal, ok := call.Args[1].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				annotationErr = fmt.Errorf("%s %s: operation annotation must use a string literal", relative, function.Name.Name)
				return false
			}
			operationID, err := strconv.Unquote(literal.Value)
			if err != nil {
				annotationErr = fmt.Errorf("%s %s: decode operation annotation: %w", relative, function.Name.Name, err)
				return false
			}
			addOperationAnnotation(annotations, operationID, testID)
			return true
		})
		if annotationErr != nil {
			return annotationErr
		}
	}
	return nil
}

func collectPythonAnnotations(path, relative string, annotations map[string]map[string]bool) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	kind := "python"
	marker := pythonOperationMarker
	if strings.HasPrefix(relative, "tests/spark/") {
		kind = "spark"
	} else if strings.HasPrefix(relative, "tests/bqcli/") {
		kind = "bq"
		marker = bqOperationMarker
	}
	var pending []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if match := marker.FindStringSubmatch(line); match != nil {
			pending = append(pending, match[1])
			continue
		}
		trimmed := strings.TrimSpace(line)
		if (kind == "bq" && strings.HasPrefix(trimmed, "@operation(")) ||
			(kind != "bq" && strings.HasPrefix(trimmed, "@pytest.mark.operation(")) {
			return fmt.Errorf("%s: operation marker must contain only one literal operation ID", relative)
		}
		if strings.HasPrefix(trimmed, "@") || trimmed == "" {
			continue
		}
		if len(pending) == 0 {
			continue
		}
		if (kind != "bq" && !strings.HasPrefix(trimmed, "def test_")) ||
			(kind == "bq" && !strings.HasPrefix(trimmed, "def ")) {
			return fmt.Errorf("%s: operation marker must immediately decorate a test function", relative)
		}
		name := strings.TrimPrefix(trimmed, "def ")
		name, _, _ = strings.Cut(name, "(")
		testID := kind + ":" + relative + ":" + name
		for _, operationID := range pending {
			addOperationAnnotation(annotations, operationID, testID)
		}
		pending = nil
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(pending) != 0 {
		return fmt.Errorf("%s: operation marker has no following test function", relative)
	}
	return nil
}

func addOperationAnnotation(annotations map[string]map[string]bool, operationID, testID string) {
	if annotations[operationID] == nil {
		annotations[operationID] = make(map[string]bool)
	}
	annotations[operationID][testID] = true
}
