package integrationcontract

import (
	"fmt"
	"sort"
	"strings"
)

const (
	trafficSourceAnnotations    = "annotations"
	trafficSourceRunnerEvidence = "runner-evidence"
)

// DeriveIntegrationScenarioEvidence resolves authored selectors and literal
// AST annotations into the operationIds/testEvidence fields consumed by the
// normalized runtime manifest. Those fields are never authored in YAML.
func DeriveIntegrationScenarioEvidence(repositoryRoot string, manifest ConsumerManifest, operationIDs map[string]bool) (ConsumerManifest, error) {
	annotations, err := collectIntegrationAnnotations(repositoryRoot)
	if err != nil {
		return ConsumerManifest{}, err
	}
	for _, annotation := range annotations {
		if !validAnnotationFamily(annotation.Family) || annotation.Test == "" || len(annotation.OperationIDs) == 0 {
			return ConsumerManifest{}, fmt.Errorf("integration annotation is incomplete: %#v", annotation)
		}
		if annotation.Family != "bq" && annotation.Scenario != "" {
			return ConsumerManifest{}, fmt.Errorf("integration annotation %s cannot declare a scenario label", annotation.Test)
		}
		for _, operationID := range annotation.OperationIDs {
			if !operationIDs[operationID] {
				return ConsumerManifest{}, fmt.Errorf("integration operation annotation references unknown operation %q", operationID)
			}
		}
	}

	scenarios := make(map[string]*ConsumerScenario, len(manifest.Scenarios))
	for index := range manifest.Scenarios {
		scenario := &manifest.Scenarios[index]
		if scenario.ID == "" {
			return ConsumerManifest{}, fmt.Errorf("scenario ID must not be empty")
		}
		if scenarios[scenario.ID] != nil {
			return ConsumerManifest{}, fmt.Errorf("duplicate scenario ID %s", scenario.ID)
		}
		if len(scenario.Selectors) == 0 || firstDuplicate(scenario.Selectors) != "" {
			return ConsumerManifest{}, fmt.Errorf("scenario %s must define unique test selectors", scenario.ID)
		}
		scenario.OperationIDs = nil
		scenario.TestEvidence = nil
		switch scenario.TrafficSource.Kind {
		case trafficSourceAnnotations:
			if scenario.TrafficSource.Reason != "" || len(scenario.TrafficSource.OperationIDs) != 0 {
				return ConsumerManifest{}, fmt.Errorf("annotation scenario %s cannot declare a manual reason or operation list", scenario.ID)
			}
		case trafficSourceRunnerEvidence:
			if strings.TrimSpace(scenario.TrafficSource.Reason) == "" {
				return ConsumerManifest{}, fmt.Errorf("runner-evidence scenario %s must declare a reason", scenario.ID)
			}
			if len(scenario.TrafficSource.OperationIDs) != 0 {
				return ConsumerManifest{}, fmt.Errorf("runner-evidence scenario %s derives operations from operationExpectations", scenario.ID)
			}
			for _, selector := range scenario.Selectors {
				prefix, _, found := strings.Cut(selector, ":")
				if !found || prefix != "load" {
					return ConsumerManifest{}, fmt.Errorf("runner-evidence scenario %s is reserved for load selectors", scenario.ID)
				}
			}
			if len(scenario.OperationExpectations) == 0 {
				return ConsumerManifest{}, fmt.Errorf("runner-evidence scenario %s must declare operationExpectations", scenario.ID)
			}
			for _, expectation := range scenario.OperationExpectations {
				if expectation.OperationID == "" {
					return ConsumerManifest{}, fmt.Errorf("runner-evidence scenario %s has an empty expectation operation", scenario.ID)
				}
				scenario.OperationIDs = appendUnique(scenario.OperationIDs, expectation.OperationID)
			}
			sort.Strings(scenario.OperationIDs)
		default:
			return ConsumerManifest{}, fmt.Errorf("scenario %s has unknown trafficSource kind %q", scenario.ID, scenario.TrafficSource.Kind)
		}
		scenarios[scenario.ID] = scenario
	}

	derivedTests := make(map[string]map[string]bool, len(manifest.Scenarios))
	for _, annotation := range annotations {
		scenario, err := annotationScenario(annotation, scenarios)
		if err != nil {
			return ConsumerManifest{}, err
		}
		if scenario == nil {
			continue
		}
		if derivedTests[scenario.ID] == nil {
			derivedTests[scenario.ID] = make(map[string]bool)
		}
		derivedTests[scenario.ID][annotation.Test] = true
		for _, operationID := range annotation.OperationIDs {
			scenario.OperationIDs = appendUnique(scenario.OperationIDs, operationID)
		}
	}
	for index := range manifest.Scenarios {
		scenario := &manifest.Scenarios[index]
		if scenario.TrafficSource.Kind == trafficSourceRunnerEvidence {
			continue
		}
		tests := derivedTests[scenario.ID]
		if len(tests) == 0 || len(scenario.OperationIDs) == 0 {
			return ConsumerManifest{}, fmt.Errorf("annotation scenario %s has no matching test annotations", scenario.ID)
		}
		for testID := range tests {
			scenario.TestEvidence = append(scenario.TestEvidence, testID)
		}
		sort.Strings(scenario.OperationIDs)
		sort.Strings(scenario.TestEvidence)
	}
	return manifest, nil
}

// ValidateIntegrationOperationAnnotations remains a focused test hook for the
// source-to-scenario projection used by CompileConsumerManifest.
func ValidateIntegrationOperationAnnotations(repositoryRoot string, manifest ConsumerManifest, operationIDs map[string]bool) error {
	_, err := DeriveIntegrationScenarioEvidence(repositoryRoot, manifest, operationIDs)
	return err
}

func validAnnotationFamily(family string) bool {
	return family == "python" || family == "spark" || family == "bq"
}

func annotationScenario(annotation integrationAnnotation, scenarios map[string]*ConsumerScenario) (*ConsumerScenario, error) {
	if annotation.Scenario != "" {
		scenario := scenarios[annotation.Scenario]
		if scenario == nil {
			return nil, fmt.Errorf("integration annotation %s references unknown scenario %s", annotation.Test, annotation.Scenario)
		}
		if annotation.Family != "bq" || scenario.TrafficSource.Kind != trafficSourceAnnotations || !scenarioHasSelectorPrefix(*scenario, "bq") {
			return nil, fmt.Errorf("integration annotation %s has an incompatible scenario label %s", annotation.Test, annotation.Scenario)
		}
		return scenario, nil
	}
	var matched *ConsumerScenario
	for _, scenario := range scenarios {
		if scenario.TrafficSource.Kind != trafficSourceAnnotations || !annotationMatchesScenario(annotation, *scenario) {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("integration annotation %s is selected by both %s and %s", annotation.Test, matched.ID, scenario.ID)
		}
		matched = scenario
	}
	if matched == nil {
		return nil, fmt.Errorf("integration annotation %s is not selected by an annotation scenario", annotation.Test)
	}
	return matched, nil
}

func annotationMatchesScenario(annotation integrationAnnotation, scenario ConsumerScenario) bool {
	for _, selector := range scenario.Selectors {
		prefix, value, found := strings.Cut(selector, ":")
		if !found || prefix != annotationSelectorPrefix(annotation.Family) {
			continue
		}
		if annotation.Family == "bq" {
			if value == strings.TrimPrefix(annotation.Test, "bq:") {
				return true
			}
			continue
		}
		path, _, found := strings.Cut(strings.TrimPrefix(annotation.Test, annotation.Family+":"), ":")
		if found && value == path {
			return true
		}
	}
	return false
}

func annotationSelectorPrefix(family string) string {
	if family == "bq" {
		return "bq"
	}
	return "pytest"
}

func scenarioHasSelectorPrefix(scenario ConsumerScenario, prefix string) bool {
	for _, selector := range scenario.Selectors {
		selectorPrefix, _, found := strings.Cut(selector, ":")
		if found && selectorPrefix == prefix {
			return true
		}
	}
	return false
}
