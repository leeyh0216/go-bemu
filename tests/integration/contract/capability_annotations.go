package integrationcontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

const (
	capabilityIndexPath     = "tests/integration/contract/capabilities.normalized.json"
	capabilitySchemaVersion = "1"
)

var (
	contractCaseIDPattern = regexp.MustCompile(`^SBQ-[A-Z0-9]+(?:-[A-Z0-9]+)*-V[1-9][0-9]*$`)
)

// CapabilityCaseState is the tested status of one exact caller behavior. It is
// intentionally narrower than the older cross-product matrix: an entry exists
// only when a literal test annotation owns it.
type CapabilityCaseState string

const (
	CapabilityCaseVerified CapabilityCaseState = "verified"
	CapabilityCasePartial  CapabilityCaseState = "partial"
	CapabilityCaseGap      CapabilityCaseState = "gap"
)

// CapabilityCase is emitted from literal contract_case(...) annotations in
// integration tests. Cases are generated artifacts; the annotations are the
// only hand-maintained input.
type CapabilityCase struct {
	ID           string              `json:"id"`
	State        CapabilityCaseState `json:"state"`
	Category     string              `json:"category"`
	Summary      string              `json:"summary"`
	Profile      string              `json:"profile"`
	WireFlow     string              `json:"wireFlow,omitempty"`
	Tests        []string            `json:"tests"`
	OperationIDs []string            `json:"operationIds"`
	Issue        string              `json:"issue,omitempty"`
	Limitation   string              `json:"limitation,omitempty"`
	StrictXFail  bool                `json:"strictXfail,omitempty"`
}

type CapabilityAPICoverage struct {
	OperationID string   `json:"operationId"`
	CaseIDs     []string `json:"caseIds"`
	Tests       []string `json:"tests"`
}

type CapabilityIndex struct {
	SchemaVersion string                  `json:"schemaVersion"`
	Cases         []CapabilityCase        `json:"cases"`
	APICoverage   []CapabilityAPICoverage `json:"apiCoverage"`
}

type capabilityAnnotation struct {
	CapabilityCase
	Test string `json:"test"`
}

// integrationAnnotation is the one literal AST projection for Python, Spark,
// and bq integration sources. Capability claims are the Spark-only subset.
type integrationAnnotation struct {
	Family       string              `json:"family"`
	Test         string              `json:"test"`
	OperationIDs []string            `json:"operationIds"`
	Scenario     string              `json:"scenario"`
	CapabilityID string              `json:"capabilityId"`
	State        CapabilityCaseState `json:"state"`
	Category     string              `json:"category"`
	Summary      string              `json:"summary"`
	Profile      string              `json:"profile"`
	WireFlow     string              `json:"wireFlow"`
	Issue        string              `json:"issue"`
	Limitation   string              `json:"limitation"`
	StrictXFail  bool                `json:"strictXfail"`
}

// CompileCapabilityIndex derives the capability index and public operation
// coverage from test-local literal annotations. It never reads the retired
// matrix or committed runtime trace output.
func CompileCapabilityIndex(repositoryRoot string) (CapabilityIndex, error) {
	operationIDs, err := loadNormalizedOperationIDs(repositoryRoot)
	if err != nil {
		return CapabilityIndex{}, err
	}
	annotations, err := collectCapabilityAnnotations(repositoryRoot)
	if err != nil {
		return CapabilityIndex{}, err
	}
	return normalizeCapabilityAnnotations(annotations, operationIDs)
}

func collectCapabilityAnnotations(repositoryRoot string) ([]capabilityAnnotation, error) {
	annotations, err := collectIntegrationAnnotations(repositoryRoot)
	if err != nil {
		return nil, err
	}
	capabilities := make([]capabilityAnnotation, 0, len(annotations))
	for _, annotation := range annotations {
		if annotation.Family != "spark" || annotation.CapabilityID == "" {
			continue
		}
		capabilities = append(capabilities, capabilityAnnotation{
			CapabilityCase: CapabilityCase{
				ID:           annotation.CapabilityID,
				State:        annotation.State,
				Category:     annotation.Category,
				Summary:      annotation.Summary,
				Profile:      annotation.Profile,
				WireFlow:     annotation.WireFlow,
				OperationIDs: annotation.OperationIDs,
				Issue:        annotation.Issue,
				Limitation:   annotation.Limitation,
				StrictXFail:  annotation.StrictXFail,
			},
			Test: annotation.Test,
		})
	}
	return capabilities, nil
}

func collectIntegrationAnnotations(repositoryRoot string) ([]integrationAnnotation, error) {
	script := filepath.Join(repositoryRoot, "tests", "integration", "contract", "extract_integration_annotations.py")
	command := exec.Command("python3", script, "--root", repositoryRoot)
	command.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("extract capability annotations: %w: %s", err, strings.TrimSpace(string(output)))
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	var annotations []integrationAnnotation
	if err := decoder.Decode(&annotations); err != nil {
		return nil, fmt.Errorf("decode capability annotations: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode capability annotations: %w", err)
	}
	return annotations, nil
}

func normalizeCapabilityAnnotations(annotations []capabilityAnnotation, operationIDs map[string]bool) (CapabilityIndex, error) {
	byID := make(map[string]*CapabilityCase, len(annotations))
	for _, annotation := range annotations {
		if err := validateCapabilityAnnotation(annotation, operationIDs); err != nil {
			return CapabilityIndex{}, err
		}
		current := byID[annotation.ID]
		if current == nil {
			copy := annotation.CapabilityCase
			copy.Tests = []string{annotation.Test}
			copy.OperationIDs = append([]string(nil), annotation.OperationIDs...)
			byID[annotation.ID] = &copy
			continue
		}
		if !sameCapabilityClaim(*current, annotation.CapabilityCase) {
			return CapabilityIndex{}, fmt.Errorf("capability %s has conflicting annotations", annotation.ID)
		}
		current.Tests = appendUnique(current.Tests, annotation.Test)
		for _, operationID := range annotation.OperationIDs {
			current.OperationIDs = appendUnique(current.OperationIDs, operationID)
		}
	}
	cases := make([]CapabilityCase, 0, len(byID))
	for _, capability := range byID {
		sort.Strings(capability.Tests)
		sort.Strings(capability.OperationIDs)
		cases = append(cases, *capability)
	}
	sort.Slice(cases, func(i, j int) bool { return cases[i].ID < cases[j].ID })
	return CapabilityIndex{
		SchemaVersion: capabilitySchemaVersion,
		Cases:         cases,
		APICoverage:   buildCapabilityAPICoverage(cases),
	}, nil
}

func validateCapabilityAnnotation(annotation capabilityAnnotation, operationIDs map[string]bool) error {
	capability := annotation.CapabilityCase
	if !contractCaseIDPattern.MatchString(capability.ID) {
		return fmt.Errorf("capability has invalid ID %q", capability.ID)
	}
	if !validCapabilityCategory(capability.Category) {
		return fmt.Errorf("capability %s has unknown category %q", capability.ID, capability.Category)
	}
	profile, found := DefaultRegistry().Profile(capability.Profile)
	if !found {
		return fmt.Errorf("capability %s references unknown profile %q", capability.ID, capability.Profile)
	}
	if capability.Category == "bootstrap" {
		if capability.WireFlow != "" {
			return fmt.Errorf("bootstrap capability %s must not claim a wire_flow", capability.ID)
		}
	} else if capability.WireFlow == "" || len(profile.Flows[capability.WireFlow]) == 0 {
		return fmt.Errorf("capability %s references unknown profile wire_flow %q", capability.ID, capability.WireFlow)
	}
	if capability.Category != "bootstrap" && len(annotation.OperationIDs) == 0 {
		return fmt.Errorf("capability %s must declare at least one literal public operation in contract_case", capability.ID)
	}
	for _, operationID := range annotation.OperationIDs {
		if !operationIDs[operationID] {
			return fmt.Errorf("capability %s test %s references unknown operation %q", capability.ID, annotation.Test, operationID)
		}
	}
	switch capability.State {
	case CapabilityCaseVerified:
		if capability.Issue != "" || capability.Limitation != "" || capability.StrictXFail {
			return fmt.Errorf("verified capability %s cannot declare an issue, limitation, or strict xfail", capability.ID)
		}
	case CapabilityCasePartial:
		if !validCapabilityIssue(capability.Issue) || capability.Limitation == "" || capability.StrictXFail {
			return fmt.Errorf("partial capability %s must declare a GitHub issue and limitation without strict_xfail", capability.ID)
		}
	case CapabilityCaseGap:
		if !validCapabilityIssue(capability.Issue) || capability.Limitation == "" || !capability.StrictXFail {
			return fmt.Errorf("gap capability %s must declare strict_xfail=True, a GitHub issue, and a limitation", capability.ID)
		}
	default:
		return fmt.Errorf("capability %s has unknown state %q", capability.ID, capability.State)
	}
	return nil
}

func validCapabilityCategory(category string) bool {
	switch category {
	case "read", "write", "streaming", "bootstrap":
		return true
	default:
		return false
	}
}

func validCapabilityIssue(issue string) bool {
	if !strings.HasPrefix(issue, "https://github.com/leeyh0216/go-bemu/issues/") {
		return false
	}
	value := strings.TrimPrefix(issue, "https://github.com/leeyh0216/go-bemu/issues/")
	if value == "" {
		return false
	}
	for _, character := range value {
		if !unicode.IsDigit(character) {
			return false
		}
	}
	return true
}

func sameCapabilityClaim(left, right CapabilityCase) bool {
	return left.ID == right.ID && left.State == right.State && left.Category == right.Category &&
		left.Summary == right.Summary && left.Profile == right.Profile && left.WireFlow == right.WireFlow &&
		left.Issue == right.Issue && left.Limitation == right.Limitation && left.StrictXFail == right.StrictXFail
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func buildCapabilityAPICoverage(cases []CapabilityCase) []CapabilityAPICoverage {
	byOperation := make(map[string]*CapabilityAPICoverage)
	for _, capability := range cases {
		for _, operationID := range capability.OperationIDs {
			coverage := byOperation[operationID]
			if coverage == nil {
				coverage = &CapabilityAPICoverage{OperationID: operationID}
				byOperation[operationID] = coverage
			}
			coverage.CaseIDs = appendUnique(coverage.CaseIDs, capability.ID)
			for _, testID := range capability.Tests {
				coverage.Tests = appendUnique(coverage.Tests, testID)
			}
		}
	}
	coverage := make([]CapabilityAPICoverage, 0, len(byOperation))
	for _, value := range byOperation {
		sort.Strings(value.CaseIDs)
		sort.Strings(value.Tests)
		coverage = append(coverage, *value)
	}
	sort.Slice(coverage, func(i, j int) bool { return coverage[i].OperationID < coverage[j].OperationID })
	return coverage
}

func MarshalCapabilityIndex(index CapabilityIndex) ([]byte, error) {
	contents, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(contents, '\n'), nil
}

func DecodeCapabilityIndex(contents []byte) (CapabilityIndex, error) {
	if err := rejectDuplicateJSONKeys(contents); err != nil {
		return CapabilityIndex{}, fmt.Errorf("decode capability index: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var index CapabilityIndex
	if err := decoder.Decode(&index); err != nil {
		return CapabilityIndex{}, fmt.Errorf("decode capability index: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return CapabilityIndex{}, err
	}
	if index.SchemaVersion != capabilitySchemaVersion {
		return CapabilityIndex{}, fmt.Errorf("capability index schemaVersion = %q, want %s", index.SchemaVersion, capabilitySchemaVersion)
	}
	return index, nil
}
