// Package contract compiles literal operation annotations from integration
// sources. It is deliberately independent from runtime clients and matrices.
package contract

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var annotation = regexp.MustCompile(`^\s*#\s*bqemu:operation\s+([a-z0-9.-]+)\s+scenario=([a-z0-9.-]+)\s*$`)

type Operation struct {
	ID       string `json:"id"`
	Scenario string `json:"scenario"`
	Source   string `json:"source"`
}

type Exception struct {
	Scenario string `json:"scenario"`
	Reason   string `json:"reason"`
}

// Compile discovers only literal source annotations. Scenario order and
// cardinality remain explicit in callers because they cannot be inferred.
func Compile(root string, selectors map[string][]string) ([]Operation, error) {
	seen := map[string]Operation{}
	var operations []Operation
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".py") {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for line := 1; scanner.Scan(); line++ {
			match := annotation.FindStringSubmatch(scanner.Text())
			if match == nil {
				continue
			}
			op := Operation{ID: match[1], Scenario: match[2], Source: fmt.Sprintf("%s:%d", path, line)}
			if previous, exists := seen[op.ID]; exists {
				return fmt.Errorf("duplicate operation annotation %q at %s and %s", op.ID, previous.Source, op.Source)
			}
			seen[op.ID] = op
			operations = append(operations, op)
		}
		return scanner.Err()
	})
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for scenario, ids := range selectors {
		for _, id := range ids {
			op, ok := seen[id]
			if !ok {
				return nil, fmt.Errorf("selector %q references unknown operation %q", scenario, id)
			}
			if op.Scenario != scenario {
				return nil, fmt.Errorf("selector %q mismatches operation %q scenario %q", scenario, id, op.Scenario)
			}
			selected[id] = true
		}
	}
	for _, op := range operations {
		if !selected[op.ID] {
			return nil, fmt.Errorf("operation %q at %s is not selected by a scenario", op.ID, op.Source)
		}
	}
	sort.Slice(operations, func(i, j int) bool { return operations[i].ID < operations[j].ID })
	return operations, nil
}

func ValidateExceptions(exceptions []Exception) error {
	seen := map[string]bool{}
	for _, exception := range exceptions {
		if exception.Scenario == "" || strings.TrimSpace(exception.Reason) == "" {
			return fmt.Errorf("runner-only exception requires scenario and reason")
		}
		if seen[exception.Scenario] {
			return fmt.Errorf("duplicate runner-only exception %q", exception.Scenario)
		}
		seen[exception.Scenario] = true
	}
	return nil
}
