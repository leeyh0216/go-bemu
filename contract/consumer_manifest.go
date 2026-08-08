package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	consumerManifestPath   = "contract/consumers.yaml"
	consumerCasesDirectory = "contract/cases"
	consumerNormalizedPath = "contract/consumers.normalized.json"
)

var (
	consumerCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	consumerDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type ConsumerManifest struct {
	SchemaVersion         string                 `yaml:"schemaVersion" json:"schemaVersion"`
	RuntimeProfiles       []RuntimeProfile       `yaml:"runtimeProfiles" json:"runtimeProfiles"`
	RunnerAdapters        []RunnerAdapter        `yaml:"runnerAdapters" json:"runnerAdapters"`
	CompatibilityProfiles []CompatibilityProfile `yaml:"compatibilityProfiles" json:"compatibilityProfiles"`
	Scenarios             []ConsumerScenario     `yaml:"scenarios" json:"scenarios"`
	ScenarioSets          []ScenarioSet          `yaml:"scenarioSets" json:"scenarioSets"`
}

type RuntimeProfile struct {
	ID       string            `yaml:"id" json:"id"`
	Family   string            `yaml:"family" json:"family"`
	Kind     string            `yaml:"kind" json:"kind"`
	Versions map[string]string `yaml:"-" json:"versions"`
}

type RunnerAdapter struct {
	ID                     string            `yaml:"id" json:"id"`
	Family                 string            `yaml:"family" json:"family"`
	RuntimeKind            string            `yaml:"runtimeKind" json:"runtimeKind"`
	SelectorPrefix         string            `yaml:"selectorPrefix" json:"selectorPrefix"`
	RequiredVersions       []string          `yaml:"requiredVersions" json:"requiredVersions"`
	RequiredArtifactUsages []string          `yaml:"requiredArtifactUsages" json:"requiredArtifactUsages"`
	Bootstrap              map[string]string `yaml:"bootstrap" json:"bootstrap"`
	SetupOperationIDs      []string          `yaml:"setupOperationIds" json:"setupOperationIds"`
}

type CompatibilityProfile struct {
	ID               string                     `yaml:"id" json:"id"`
	ScenarioIDs      []string                   `yaml:"scenarioIds" json:"scenarioIds"`
	SourceProvenance []ConsumerSourceProvenance `yaml:"sourceProvenance" json:"sourceProvenance"`
}

type ConsumerSourceProvenance struct {
	Name    string `yaml:"name" json:"name"`
	Version string `yaml:"version" json:"version"`
	URI     string `yaml:"uri" json:"uri"`
}

type ConsumerScenario struct {
	ID                    string                         `yaml:"id" json:"id"`
	OperationIDs          []string                       `yaml:"operationIds" json:"operationIds"`
	Selectors             []string                       `yaml:"selectors" json:"selectors"`
	OperationExpectations []ConsumerOperationExpectation `yaml:"operationExpectations" json:"operationExpectations"`
}

type ConsumerOperationExpectation struct {
	OperationID string   `yaml:"operationId" json:"operationId"`
	Min         int      `yaml:"min" json:"min"`
	Max         int      `yaml:"max" json:"max"`
	After       []string `yaml:"after" json:"after"`
}

type ScenarioSet struct {
	ID          string   `yaml:"id" json:"id"`
	ScenarioIDs []string `yaml:"scenarioIds" json:"scenarioIds"`
}

type ConsumerArtifact struct {
	ID     string `yaml:"id" json:"id"`
	Role   string `yaml:"role" json:"role"`
	Usage  string `yaml:"usage" json:"usage"`
	URI    string `yaml:"uri" json:"uri"`
	SHA256 string `yaml:"sha256" json:"sha256"`
}

type ConsumerCase struct {
	SchemaVersion          string             `yaml:"schemaVersion" json:"schemaVersion"`
	ID                     string             `yaml:"id" json:"id"`
	DisplayName            string             `yaml:"displayName" json:"displayName"`
	Family                 string             `yaml:"family" json:"family"`
	Lane                   string             `yaml:"lane" json:"lane"`
	RuntimeProfileID       string             `yaml:"runtimeProfile" json:"runtimeProfileId"`
	RunnerAdapterID        string             `yaml:"runnerAdapter" json:"runnerAdapterId"`
	CompatibilityProfileID string             `yaml:"compatibilityProfile" json:"compatibilityProfileId"`
	ScenarioSetID          string             `yaml:"scenarioSet" json:"scenarioSetId"`
	Versions               map[string]string  `yaml:"versions" json:"versions"`
	Artifacts              []ConsumerArtifact `yaml:"artifacts" json:"artifacts"`
}

type NormalizedConsumerManifest struct {
	SchemaVersion string                 `json:"schemaVersion"`
	Cases         []ExpandedConsumerCase `json:"cases"`
}

type ExpandedConsumerCase struct {
	ID                   string               `json:"id"`
	DisplayName          string               `json:"displayName"`
	Family               string               `json:"family"`
	Lane                 string               `json:"lane"`
	RuntimeProfile       RuntimeProfile       `json:"runtimeProfile"`
	RunnerAdapter        RunnerAdapter        `json:"runnerAdapter"`
	CompatibilityProfile CompatibilityProfile `json:"compatibilityProfile"`
	ScenarioSet          ExpandedScenarioSet  `json:"scenarioSet"`
	Artifacts            []ConsumerArtifact   `json:"artifacts"`
}

type ExpandedScenarioSet struct {
	ID        string             `json:"id"`
	Scenarios []ConsumerScenario `json:"scenarios"`
}

func DecodeConsumerManifest(contents []byte) (ConsumerManifest, error) {
	var manifest ConsumerManifest
	if err := decodeStrictYAML(contents, &manifest); err != nil {
		return ConsumerManifest{}, fmt.Errorf("decode consumer manifest: %w", err)
	}
	return manifest, nil
}

func DecodeConsumerCase(contents []byte) (ConsumerCase, error) {
	var consumerCase ConsumerCase
	if err := decodeStrictYAML(contents, &consumerCase); err != nil {
		return ConsumerCase{}, fmt.Errorf("decode consumer case: %w", err)
	}
	return consumerCase, nil
}

func decodeStrictYAML(contents []byte, destination any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple YAML documents are not allowed")
		}
		return err
	}
	return nil
}

func CompileConsumerManifest(repositoryRoot string) (NormalizedConsumerManifest, error) {
	operationIDs, err := loadNormalizedOperationIDs(repositoryRoot)
	if err != nil {
		return NormalizedConsumerManifest{}, err
	}
	return compileConsumerManifest(repositoryRoot, operationIDs)
}

func compileConsumerManifest(repositoryRoot string, operationIDs map[string]bool) (NormalizedConsumerManifest, error) {
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, consumerManifestPath))
	if err != nil {
		return NormalizedConsumerManifest{}, fmt.Errorf("read %s: %w", consumerManifestPath, err)
	}
	manifest, err := DecodeConsumerManifest(contents)
	if err != nil {
		return NormalizedConsumerManifest{}, err
	}
	cases, err := loadConsumerCases(repositoryRoot)
	if err != nil {
		return NormalizedConsumerManifest{}, err
	}
	return NormalizeConsumerManifest(manifest, cases, operationIDs)
}

func loadConsumerCases(repositoryRoot string) ([]ConsumerCase, error) {
	paths, err := filepath.Glob(filepath.Join(repositoryRoot, consumerCasesDirectory, "*.yaml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, errors.New("consumer manifest has no case YAML files")
	}
	cases := make([]ConsumerCase, 0, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		consumerCase, err := DecodeConsumerCase(contents)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
		}
		cases = append(cases, consumerCase)
	}
	return cases, nil
}

func loadNormalizedOperationIDs(repositoryRoot string) (map[string]bool, error) {
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, "contract/operations.normalized.json"))
	if err != nil {
		return nil, err
	}
	manifest, err := decodeNormalizedOperationManifest(contents)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(manifest.Operations))
	for _, operation := range manifest.Operations {
		ids[operation.ID] = true
	}
	return ids, nil
}

func NormalizeConsumerManifest(manifest ConsumerManifest, cases []ConsumerCase, operationIDs map[string]bool) (NormalizedConsumerManifest, error) {
	if manifest.SchemaVersion != "1" {
		return NormalizedConsumerManifest{}, fmt.Errorf("consumer manifest schemaVersion = %q, want 1", manifest.SchemaVersion)
	}
	for index := range manifest.RunnerAdapters {
		adapter := &manifest.RunnerAdapters[index]
		if adapter.Bootstrap == nil {
			adapter.Bootstrap = map[string]string{}
		}
		adapter.SetupOperationIDs = append([]string{}, adapter.SetupOperationIDs...)
	}
	for index := range manifest.Scenarios {
		scenario := &manifest.Scenarios[index]
		overrides := make(map[string]ConsumerOperationExpectation, len(scenario.OperationExpectations))
		for _, expectation := range scenario.OperationExpectations {
			if _, duplicate := overrides[expectation.OperationID]; duplicate {
				return NormalizedConsumerManifest{}, fmt.Errorf("scenario %s duplicates operation expectation %s", scenario.ID, expectation.OperationID)
			}
			overrides[expectation.OperationID] = expectation
		}
		expandedExpectations := make([]ConsumerOperationExpectation, 0, len(scenario.OperationIDs))
		for _, operationID := range scenario.OperationIDs {
			expectation, overridden := overrides[operationID]
			if !overridden {
				expectation = ConsumerOperationExpectation{OperationID: operationID, Min: 1}
			} else {
				delete(overrides, operationID)
			}
			expectation.After = append([]string{}, expectation.After...)
			expandedExpectations = append(expandedExpectations, expectation)
		}
		if len(overrides) != 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("scenario %s has an expectation for an undeclared operation", scenario.ID)
		}
		scenario.OperationExpectations = expandedExpectations
	}
	runtimes, err := indexByID("runtime profile", manifest.RuntimeProfiles, func(value RuntimeProfile) string { return value.ID })
	if err != nil {
		return NormalizedConsumerManifest{}, err
	}
	adapters, err := indexByID("runner adapter", manifest.RunnerAdapters, func(value RunnerAdapter) string { return value.ID })
	if err != nil {
		return NormalizedConsumerManifest{}, err
	}
	profiles, err := indexByID("compatibility profile", manifest.CompatibilityProfiles, func(value CompatibilityProfile) string { return value.ID })
	if err != nil {
		return NormalizedConsumerManifest{}, err
	}
	scenarios, err := indexByID("scenario", manifest.Scenarios, func(value ConsumerScenario) string { return value.ID })
	if err != nil {
		return NormalizedConsumerManifest{}, err
	}
	sets, err := indexByID("scenario set", manifest.ScenarioSets, func(value ScenarioSet) string { return value.ID })
	if err != nil {
		return NormalizedConsumerManifest{}, err
	}
	for _, runtime := range manifest.RuntimeProfiles {
		if runtime.Family == "" || runtime.Kind == "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("runtime profile %s must define family and kind", runtime.ID)
		}
	}
	for _, adapter := range manifest.RunnerAdapters {
		if adapter.Family == "" || adapter.RuntimeKind == "" || adapter.SelectorPrefix == "" || len(adapter.RequiredVersions) == 0 || len(adapter.RequiredArtifactUsages) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("runner adapter %s is incomplete", adapter.ID)
		}
		if duplicate := firstDuplicate(adapter.RequiredVersions); duplicate != "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("runner adapter %s duplicates required version %s", adapter.ID, duplicate)
		}
		if duplicate := firstDuplicate(adapter.RequiredArtifactUsages); duplicate != "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("runner adapter %s duplicates required artifact usage %s", adapter.ID, duplicate)
		}
		for _, usage := range adapter.RequiredArtifactUsages {
			if !validConsumerArtifactUsage(usage) {
				return NormalizedConsumerManifest{}, fmt.Errorf("runner adapter %s has unknown artifact usage %s", adapter.ID, usage)
			}
		}
		for tool, version := range adapter.Bootstrap {
			if tool == "" || version == "" {
				return NormalizedConsumerManifest{}, fmt.Errorf("runner adapter %s has an empty bootstrap tool or version", adapter.ID)
			}
		}
		if len(adapter.SetupOperationIDs) != 0 {
			if err := uniqueReferences("setup operation", adapter.ID, adapter.SetupOperationIDs, operationIDs); err != nil {
				return NormalizedConsumerManifest{}, err
			}
		}
	}
	for _, scenario := range manifest.Scenarios {
		if len(scenario.OperationIDs) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("scenario %s has no operations", scenario.ID)
		}
		if err := uniqueReferences("operation", scenario.ID, scenario.OperationIDs, operationIDs); err != nil {
			return NormalizedConsumerManifest{}, err
		}
		if len(scenario.Selectors) == 0 || firstDuplicate(scenario.Selectors) != "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("scenario %s must define unique test selectors", scenario.ID)
		}
		declaredOperations := sliceSet(scenario.OperationIDs)
		for _, expectation := range scenario.OperationExpectations {
			if expectation.Min < 0 || expectation.Max < 0 || (expectation.Max != 0 && expectation.Max < expectation.Min) {
				return NormalizedConsumerManifest{}, fmt.Errorf("scenario %s operation %s has invalid cardinality", scenario.ID, expectation.OperationID)
			}
			if len(expectation.After) != 0 {
				if err := uniqueReferences("ordering dependency", expectation.OperationID, expectation.After, declaredOperations); err != nil {
					return NormalizedConsumerManifest{}, err
				}
				if sliceSet(expectation.After)[expectation.OperationID] {
					return NormalizedConsumerManifest{}, fmt.Errorf("scenario %s operation %s cannot depend on itself", scenario.ID, expectation.OperationID)
				}
			}
		}
		if err := validateConsumerOrdering(scenario); err != nil {
			return NormalizedConsumerManifest{}, err
		}
	}
	for _, set := range manifest.ScenarioSets {
		if err := uniqueReferences("scenario", set.ID, set.ScenarioIDs, keySet(scenarios)); err != nil {
			return NormalizedConsumerManifest{}, err
		}
		owners := make(map[string]string)
		for _, scenarioID := range set.ScenarioIDs {
			for _, operationID := range scenarios[scenarioID].OperationIDs {
				if owner := owners[operationID]; owner != "" {
					return NormalizedConsumerManifest{}, fmt.Errorf("scenario set %s assigns operation %s to both %s and %s", set.ID, operationID, owner, scenarioID)
				}
				owners[operationID] = scenarioID
			}
		}
	}
	for _, profile := range manifest.CompatibilityProfiles {
		if err := uniqueReferences("scenario", profile.ID, profile.ScenarioIDs, keySet(scenarios)); err != nil {
			return NormalizedConsumerManifest{}, err
		}
		for _, provenance := range profile.SourceProvenance {
			if provenance.Name == "" || provenance.Version == "" || provenance.URI == "" {
				return NormalizedConsumerManifest{}, fmt.Errorf("compatibility profile %s has incomplete source provenance", profile.ID)
			}
			if err := validateSourceProvenance(provenance); err != nil {
				return NormalizedConsumerManifest{}, fmt.Errorf("compatibility profile %s: %w", profile.ID, err)
			}
		}
	}

	caseIDs := make(map[string]bool, len(cases))
	expanded := make([]ExpandedConsumerCase, 0, len(cases))
	for _, consumerCase := range cases {
		if consumerCase.SchemaVersion != "1" {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s schemaVersion = %q, want 1", consumerCase.ID, consumerCase.SchemaVersion)
		}
		if consumerCase.ID == "" || caseIDs[consumerCase.ID] {
			return NormalizedConsumerManifest{}, fmt.Errorf("duplicate or empty consumer case ID %q", consumerCase.ID)
		}
		caseIDs[consumerCase.ID] = true
		if consumerCase.DisplayName == "" || consumerCase.Family == "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s must define displayName and family", consumerCase.ID)
		}
		if consumerCase.Lane != "required" && consumerCase.Lane != "preview" && consumerCase.Lane != "nightly" {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s has unknown lane %q", consumerCase.ID, consumerCase.Lane)
		}
		runtime, ok := runtimes[consumerCase.RuntimeProfileID]
		if !ok {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s references unknown runtime profile %s", consumerCase.ID, consumerCase.RuntimeProfileID)
		}
		adapter, ok := adapters[consumerCase.RunnerAdapterID]
		if !ok {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s references unknown runner adapter %s", consumerCase.ID, consumerCase.RunnerAdapterID)
		}
		profile, ok := profiles[consumerCase.CompatibilityProfileID]
		if !ok {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s references unknown compatibility profile %s", consumerCase.ID, consumerCase.CompatibilityProfileID)
		}
		set, ok := sets[consumerCase.ScenarioSetID]
		if !ok {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s references unknown scenario set %s", consumerCase.ID, consumerCase.ScenarioSetID)
		}
		if runtime.Family != consumerCase.Family || adapter.Family != consumerCase.Family || adapter.RuntimeKind != runtime.Kind {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s runtime/adapter family or kind mismatch", consumerCase.ID)
		}
		if len(consumerCase.Versions) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s has no runtime versions", consumerCase.ID)
		}
		for _, required := range adapter.RequiredVersions {
			if consumerCase.Versions[required] == "" {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s is missing runtime version %s required by adapter %s", consumerCase.ID, required, adapter.ID)
			}
		}
		for key, version := range consumerCase.Versions {
			if key == "" || version == "" {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s has an empty runtime version", consumerCase.ID)
			}
		}
		allowed := sliceSet(profile.ScenarioIDs)
		setupOperations := sliceSet(adapter.SetupOperationIDs)
		for _, scenarioID := range set.ScenarioIDs {
			if !allowed[scenarioID] {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s scenario %s is outside compatibility profile %s", consumerCase.ID, scenarioID, profile.ID)
			}
			for _, selector := range scenarios[scenarioID].Selectors {
				prefix, value, found := strings.Cut(selector, ":")
				if !found || prefix != adapter.SelectorPrefix || value == "" {
					return NormalizedConsumerManifest{}, fmt.Errorf("case %s scenario %s selector %q is incompatible with adapter %s", consumerCase.ID, scenarioID, selector, adapter.ID)
				}
			}
			for _, operationID := range scenarios[scenarioID].OperationIDs {
				if setupOperations[operationID] {
					return NormalizedConsumerManifest{}, fmt.Errorf("case %s operation %s is both setup and scenario traffic", consumerCase.ID, operationID)
				}
			}
		}
		if len(consumerCase.Artifacts) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s has no immutable artifacts", consumerCase.ID)
		}
		artifactIDs := make([]string, 0, len(consumerCase.Artifacts))
		artifactUsageCounts := make(map[string]int, len(consumerCase.Artifacts))
		allowedArtifactUsages := sliceSet(adapter.RequiredArtifactUsages)
		for _, artifact := range consumerCase.Artifacts {
			artifactIDs = append(artifactIDs, artifact.ID)
			artifactUsageCounts[artifact.Usage]++
			if artifact.ID == "" || (artifact.Role != "execution" && artifact.Role != "tool-provenance") || !validConsumerArtifactUsage(artifact.Usage) || artifact.URI == "" || !consumerDigestPattern.MatchString(artifact.SHA256) {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s artifact %s must define an immutable URI and lowercase SHA-256", consumerCase.ID, artifact.ID)
			}
			if !allowedArtifactUsages[artifact.Usage] {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s artifact %s usage %s is not accepted by adapter %s", consumerCase.ID, artifact.ID, artifact.Usage, adapter.ID)
			}
			if (artifact.Usage == "cloud-sdk-image") != (artifact.Role == "tool-provenance") {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s artifact %s role %s is incompatible with usage %s", consumerCase.ID, artifact.ID, artifact.Role, artifact.Usage)
			}
			if strings.HasPrefix(artifact.URI, "oci://") && !strings.Contains(artifact.URI, "@sha256:"+artifact.SHA256) {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s OCI artifact %s URI digest does not match its SHA-256", consumerCase.ID, artifact.ID)
			}
		}
		if duplicate := firstDuplicate(artifactIDs); duplicate != "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s duplicates artifact %s", consumerCase.ID, duplicate)
		}
		for _, usage := range adapter.RequiredArtifactUsages {
			if artifactUsageCounts[usage] != 1 {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s must define exactly one %s artifact for adapter %s", consumerCase.ID, usage, adapter.ID)
			}
		}
		runtime.Versions = cloneStringMap(consumerCase.Versions)
		caseScenarios := make([]ConsumerScenario, 0, len(set.ScenarioIDs))
		for _, id := range set.ScenarioIDs {
			caseScenarios = append(caseScenarios, scenarios[id])
		}
		expanded = append(expanded, ExpandedConsumerCase{ID: consumerCase.ID, DisplayName: consumerCase.DisplayName, Family: consumerCase.Family, Lane: consumerCase.Lane, RuntimeProfile: runtime, RunnerAdapter: adapter, CompatibilityProfile: profile, ScenarioSet: ExpandedScenarioSet{ID: set.ID, Scenarios: caseScenarios}, Artifacts: append([]ConsumerArtifact(nil), consumerCase.Artifacts...)})
	}
	sort.Slice(expanded, func(i, j int) bool { return expanded[i].ID < expanded[j].ID })
	return NormalizedConsumerManifest{SchemaVersion: "1", Cases: expanded}, nil
}

func indexByID[T any](kind string, values []T, id func(T) string) (map[string]T, error) {
	result := make(map[string]T, len(values))
	for _, value := range values {
		key := id(value)
		if key == "" {
			return nil, fmt.Errorf("%s ID must not be empty", kind)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate %s ID %s", kind, key)
		}
		result[key] = value
	}
	return result, nil
}

func uniqueReferences(kind, owner string, values []string, known map[string]bool) error {
	if len(values) == 0 {
		return fmt.Errorf("%s %s has no %ss", kind, owner, kind)
	}
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if seen[value] {
			return fmt.Errorf("%s %s duplicates %s %s", kind, owner, kind, value)
		}
		seen[value] = true
		if !known[value] {
			return fmt.Errorf("%s %s references unknown %s %s", kind, owner, kind, value)
		}
	}
	return nil
}

func validateConsumerOrdering(scenario ConsumerScenario) error {
	dependencies := make(map[string][]string, len(scenario.OperationExpectations))
	for _, expectation := range scenario.OperationExpectations {
		dependencies[expectation.OperationID] = expectation.After
	}
	states := make(map[string]uint8, len(dependencies))
	var visit func(string) error
	visit = func(operationID string) error {
		switch states[operationID] {
		case 1:
			return fmt.Errorf("scenario %s has an ordering dependency cycle at %s", scenario.ID, operationID)
		case 2:
			return nil
		}
		states[operationID] = 1
		for _, dependency := range dependencies[operationID] {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		states[operationID] = 2
		return nil
	}
	for _, operationID := range scenario.OperationIDs {
		if err := visit(operationID); err != nil {
			return err
		}
	}
	return nil
}

func validConsumerArtifactUsage(usage string) bool {
	switch usage {
	case "python-wheel", "cloud-sdk-image", "spark-connector-dsv1-jar", "spark-connector-dsv2-jar", "spark-runtime":
		return true
	default:
		return false
	}
}

func keySet[T any](values map[string]T) map[string]bool {
	result := make(map[string]bool, len(values))
	for key := range values {
		result[key] = true
	}
	return result
}
func sliceSet(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

func cloneStringMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func validateSourceProvenance(provenance ConsumerSourceProvenance) error {
	parsed, err := url.Parse(provenance.URI)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("source provenance %s must use an absolute HTTPS URI", provenance.Name)
	}
	if parsed.Host != "github.com" {
		if parsed.Fragment == "" {
			return fmt.Errorf("source provenance %s must select an immutable release anchor", provenance.Name)
		}
		return nil
	}
	segments := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(segments) < 4 || (segments[2] != "tree" && segments[2] != "blob") {
		return fmt.Errorf("GitHub source provenance %s must select an exact tree or blob ref", provenance.Name)
	}
	ref := segments[3]
	switch strings.ToLower(ref) {
	case "main", "master", "head", "develop", "latest":
		return fmt.Errorf("GitHub source provenance %s uses mutable ref %s", provenance.Name, ref)
	}
	if consumerCommitPattern.MatchString(ref) {
		return nil
	}
	if !strings.Contains(ref, provenance.Version) {
		return fmt.Errorf("GitHub source provenance %s ref %s does not identify version %s", provenance.Name, ref, provenance.Version)
	}
	return nil
}
func firstDuplicate(values []string) string {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if seen[value] {
			return value
		}
		seen[value] = true
	}
	return ""
}

func MarshalNormalizedConsumerManifest(manifest NormalizedConsumerManifest) ([]byte, error) {
	contents, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(contents, '\n'), nil
}

func DecodeNormalizedConsumerManifest(contents []byte) (NormalizedConsumerManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var manifest NormalizedConsumerManifest
	if err := decoder.Decode(&manifest); err != nil {
		return NormalizedConsumerManifest{}, fmt.Errorf("decode normalized consumer manifest: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return NormalizedConsumerManifest{}, err
	}
	if manifest.SchemaVersion != "1" {
		return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer schemaVersion = %q, want 1", manifest.SchemaVersion)
	}
	return manifest, nil
}

func ConsumerMatrix(repositoryRoot, family, lanes string) ([]byte, error) {
	payload, _, err := ConsumerMatrixWithCount(repositoryRoot, family, lanes)
	return payload, err
}

func ConsumerMatrixWithCount(repositoryRoot, family, lanes string) ([]byte, int, error) {
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, consumerNormalizedPath))
	if err != nil {
		return nil, 0, err
	}
	manifest, err := DecodeNormalizedConsumerManifest(contents)
	if err != nil {
		return nil, 0, err
	}
	laneFilter := make(map[string]bool)
	if lanes != "" {
		for _, lane := range strings.Split(lanes, ",") {
			if lane != "required" && lane != "preview" && lane != "nightly" {
				return nil, 0, fmt.Errorf("unknown consumer lane %q", lane)
			}
			if laneFilter[lane] {
				return nil, 0, fmt.Errorf("duplicate consumer lane %q", lane)
			}
			laneFilter[lane] = true
		}
	}
	rows := make([]ExpandedConsumerCase, 0)
	for _, consumerCase := range manifest.Cases {
		if (family == "" || consumerCase.Family == family) && (len(laneFilter) == 0 || laneFilter[consumerCase.Lane]) {
			rows = append(rows, consumerCase)
		}
	}
	payload, err := json.Marshal(struct {
		Include []ExpandedConsumerCase `json:"include"`
	}{Include: rows})
	if err != nil {
		return nil, 0, err
	}
	return payload, len(rows), nil
}

func FileSHA256(path string) (string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(contents)
	return hex.EncodeToString(digest[:]), nil
}

func renderConsumerCompatibility(manifest NormalizedConsumerManifest, language string) []byte {
	var output strings.Builder
	if language == "ko" {
		output.WriteString("<!-- doc-id: consumer-compatibility -->\n<!-- lang: ko -->\n\n[English](../en/consumer-compatibility.md) | [한국어](consumer-compatibility.md)\n\n# 소비자 호환성\n\n<!-- section: generated-cases -->\n이 문서는 `contract/consumers.normalized.json`에서 생성됩니다. 공개 동작은 [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest)를 기준으로 검증합니다. 각 소비자의 정확한 버전과 변경되지 않는 출처는 아래 표에 표시합니다. `execution` 산출물은 해시를 확인한 뒤 실행에 사용합니다. `tool-provenance` 산출물은 별도로 설치하고 버전을 확인한 도구의 릴리스 출처만 나타냅니다.\n\n| 사례 | 실행 계열 | 상태 | 런타임 | 시나리오 | 출처 | 산출물 역할/용도 |\n|---|---|---|---|---|---|---|\n")
	} else {
		output.WriteString("<!-- doc-id: consumer-compatibility -->\n<!-- lang: en -->\n\n[English](consumer-compatibility.md) | [한국어](../ko/consumer-compatibility.md)\n\n# Consumer Compatibility\n\n<!-- section: generated-cases -->\nThis page is generated from `contract/consumers.normalized.json`. Public behavior is verified against the [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest). The table records every consumer's exact version and immutable source. An `execution` artifact is digest-verified and used by the runner. A `tool-provenance` artifact records only the release provenance of a separately installed, version-verified tool.\n\n| Case | Family | Lane | Runtime | Scenarios | Sources | Artifact role/usage |\n|---|---|---|---|---|---|---|\n")
	}
	for _, consumerCase := range manifest.Cases {
		versionKeys := make([]string, 0, len(consumerCase.RuntimeProfile.Versions))
		for key := range consumerCase.RuntimeProfile.Versions {
			versionKeys = append(versionKeys, key)
		}
		sort.Strings(versionKeys)
		versions := make([]string, 0, len(versionKeys))
		for _, key := range versionKeys {
			versions = append(versions, key+" "+consumerCase.RuntimeProfile.Versions[key])
		}
		scenarios := make([]string, 0, len(consumerCase.ScenarioSet.Scenarios))
		for _, scenario := range consumerCase.ScenarioSet.Scenarios {
			scenarios = append(scenarios, "`"+scenario.ID+"`")
		}
		artifacts := make([]string, 0, len(consumerCase.Artifacts))
		for _, artifact := range consumerCase.Artifacts {
			artifacts = append(artifacts, "`"+artifact.ID+"` (`"+artifact.Role+"` / `"+artifact.Usage+"`)")
		}
		sources := make([]string, 0, len(consumerCase.CompatibilityProfile.SourceProvenance))
		for _, source := range consumerCase.CompatibilityProfile.SourceProvenance {
			sources = append(sources, fmt.Sprintf("[%s %s](%s)", source.Name, source.Version, source.URI))
		}
		fmt.Fprintf(&output, "| `%s` | %s | %s | %s | %s | %s | %s |\n", consumerCase.ID, consumerCase.Family, consumerCase.Lane, strings.Join(versions, ", "), strings.Join(scenarios, "<br>"), strings.Join(sources, "<br>"), strings.Join(artifacts, "<br>"))
	}
	return []byte(output.String())
}
