package integrationcontract

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	pytestOperationMarker    = regexp.MustCompile(`^\s*@pytest\.mark\.operation\(["']([^"']+)["']\)\s*$`)
	commandOperationMarker   = regexp.MustCompile(`^\s*@operation\(["']([^"']+)["']\)\s*$`)
	integrationTestIDPattern = regexp.MustCompile(`^(python|spark|bq):tests/integration/(python|spark|bqcli)/[^:]+:[A-Za-z_][A-Za-z0-9_]*$`)
)

func ValidateIntegrationOperationAnnotations(repositoryRoot string, manifest ConsumerManifest, operationIDs map[string]bool) error {
	annotations, err := collectIntegrationOperationAnnotations(repositoryRoot)
	if err != nil {
		return err
	}
	declared := make(map[string]map[string]bool)
	for _, scenario := range manifest.Scenarios {
		operations := sliceSet(scenario.OperationIDs)
		for _, testID := range scenario.TestEvidence {
			if !integrationTestIDPattern.MatchString(testID) {
				return fmt.Errorf("scenario %s has invalid integration test evidence %q", scenario.ID, testID)
			}
			if declared[testID] == nil {
				declared[testID] = make(map[string]bool)
			}
			for operationID := range operations {
				declared[testID][operationID] = true
			}
		}
	}
	for operationID, tests := range annotations {
		if !operationIDs[operationID] {
			return fmt.Errorf("integration operation annotation references unknown operation %q", operationID)
		}
		for testID := range tests {
			if !declared[testID][operationID] {
				return fmt.Errorf("integration operation %s annotation in %s is absent from scenario evidence", operationID, testID)
			}
		}
	}
	for testID, operations := range declared {
		matched := false
		for operationID := range operations {
			if annotations[operationID][testID] {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("integration test evidence %s has no matching operation annotation", testID)
		}
	}
	return nil
}

func collectIntegrationOperationAnnotations(repositoryRoot string) (map[string]map[string]bool, error) {
	annotations := make(map[string]map[string]bool)
	directories := []struct {
		path string
		kind string
	}{
		{path: "tests/integration/python", kind: "python"},
		{path: "tests/integration/spark", kind: "spark"},
		{path: "tests/integration/bqcli", kind: "bq"},
	}
	for _, directory := range directories {
		root := filepath.Join(repositoryRoot, filepath.FromSlash(directory.path))
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if path != root && (entry.Name() == "__pycache__" || strings.HasPrefix(entry.Name(), ".")) {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".py" {
				return nil
			}
			relative, err := filepath.Rel(repositoryRoot, path)
			if err != nil {
				return err
			}
			return collectIntegrationPythonAnnotations(path, filepath.ToSlash(relative), directory.kind, annotations)
		})
		if err != nil {
			return nil, err
		}
	}
	return annotations, nil
}

func collectIntegrationPythonAnnotations(path, relative, kind string, annotations map[string]map[string]bool) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	marker := pytestOperationMarker
	if kind == "bq" {
		marker = commandOperationMarker
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
			return fmt.Errorf("%s: operation marker must contain one literal operation ID", relative)
		}
		if strings.HasPrefix(trimmed, "@") || trimmed == "" {
			continue
		}
		if len(pending) == 0 {
			continue
		}
		if (kind == "bq" && !strings.HasPrefix(trimmed, "def ")) ||
			(kind != "bq" && !strings.HasPrefix(trimmed, "def test_")) {
			return fmt.Errorf("%s: operation marker must immediately decorate a test function", relative)
		}
		name := strings.TrimPrefix(trimmed, "def ")
		name, _, _ = strings.Cut(name, "(")
		testID := kind + ":" + relative + ":" + name
		for _, operationID := range pending {
			if annotations[operationID] == nil {
				annotations[operationID] = make(map[string]bool)
			}
			annotations[operationID][testID] = true
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
