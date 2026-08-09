package integrationcontract

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

	productcontract "github.com/leeyh0216/go-bemu/contract"
	"gopkg.in/yaml.v3"
)

const (
	consumerManifestPath          = "tests/integration/contract/consumers.yaml"
	consumerCasesDirectory        = "tests/integration/contract/cases"
	consumerNormalizedPath        = "tests/integration/contract/consumers.normalized.json"
	consumerManifestSchemaVersion = "3"
	consumerSchemaVersion         = "2"
)

var (
	consumerCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	consumerDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	consumerCaseIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	integrationTestIDPattern = regexp.MustCompile(`^(python|spark|bq):tests/integration/(python|spark|bqcli)/[^:]+:[A-Za-z_][A-Za-z0-9_]*$`)
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
	ID          string   `yaml:"id" json:"id"`
	ScenarioIDs []string `yaml:"scenarioIds" json:"scenarioIds"`
}

type ConsumerSourceProvenance struct {
	Name    string `yaml:"name" json:"name"`
	Version string `yaml:"version" json:"version"`
	URI     string `yaml:"uri" json:"uri"`
}

type ConsumerSourceReference struct {
	Name       string `yaml:"name" json:"name"`
	VersionKey string `yaml:"versionKey" json:"versionKey"`
	Version    string `yaml:"version" json:"version"`
	URI        string `yaml:"uri" json:"uri"`
}

type ConsumerScenario struct {
	ID                    string                         `yaml:"id" json:"id"`
	TrafficSource         ScenarioTrafficSource          `yaml:"trafficSource" json:"trafficSource"`
	OperationIDs          []string                       `yaml:"-" json:"operationIds"`
	Selectors             []string                       `yaml:"selectors" json:"selectors"`
	TestEvidence          []string                       `yaml:"-" json:"testEvidence"`
	OperationExpectations []ConsumerOperationExpectation `yaml:"operationExpectations" json:"operationExpectations"`
}

// ScenarioTrafficSource is the reviewed origin of scenario operation IDs.
// Annotation scenarios derive their operation IDs and evidence from source;
// runner-evidence is reserved for load runners that do not select test code.
type ScenarioTrafficSource struct {
	Kind         string   `yaml:"kind" json:"kind"`
	Reason       string   `yaml:"reason" json:"reason"`
	OperationIDs []string `yaml:"operationIds,omitempty" json:"-"`
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
	SchemaVersion    string                    `yaml:"schemaVersion" json:"schemaVersion"`
	ID               string                    `yaml:"id" json:"id"`
	DisplayName      string                    `yaml:"displayName" json:"displayName"`
	Family           string                    `yaml:"family" json:"family"`
	Lane             string                    `yaml:"lane" json:"lane"`
	RuntimeProfileID string                    `yaml:"runtimeProfile" json:"runtimeProfileId"`
	Versions         map[string]string         `yaml:"versions" json:"versions"`
	Artifacts        []ConsumerArtifact        `yaml:"artifacts" json:"artifacts"`
	SourceProvenance []ConsumerSourceReference `yaml:"sourceProvenance" json:"sourceProvenance"`
	Executions       []ConsumerExecution       `yaml:"executions" json:"executions"`
}

type ConsumerExecution struct {
	ID                     string `yaml:"id" json:"id"`
	RunnerAdapterID        string `yaml:"runnerAdapter" json:"runnerAdapterId"`
	CompatibilityProfileID string `yaml:"compatibilityProfile" json:"compatibilityProfileId"`
	ScenarioSetID          string `yaml:"scenarioSet" json:"scenarioSetId"`
}

type NormalizedConsumerManifest struct {
	SchemaVersion string                 `json:"schemaVersion"`
	Cases         []ExpandedConsumerCase `json:"cases"`
}

type ExpandedConsumerCase struct {
	ID               string                      `json:"id"`
	DisplayName      string                      `json:"displayName"`
	Family           string                      `json:"family"`
	Lane             string                      `json:"lane"`
	RuntimeProfile   RuntimeProfile              `json:"runtimeProfile"`
	Artifacts        []ConsumerArtifact          `json:"artifacts"`
	SourceProvenance []ConsumerSourceProvenance  `json:"sourceProvenance"`
	Executions       []ExpandedConsumerExecution `json:"executions"`
}

type ExpandedConsumerExecution struct {
	ID                   string               `json:"id"`
	RunnerAdapter        RunnerAdapter        `json:"runnerAdapter"`
	CompatibilityProfile CompatibilityProfile `json:"compatibilityProfile"`
	ScenarioSet          ExpandedScenarioSet  `json:"scenarioSet"`
}

type ConsumerMatrixRow struct {
	ID                   string                     `json:"id"`
	DisplayName          string                     `json:"displayName"`
	Family               string                     `json:"family"`
	Lane                 string                     `json:"lane"`
	RuntimeProfile       RuntimeProfile             `json:"runtimeProfile"`
	Artifacts            []ConsumerArtifact         `json:"artifacts"`
	SourceProvenance     []ConsumerSourceProvenance `json:"sourceProvenance"`
	ExecutionID          string                     `json:"executionId"`
	RunnerAdapter        RunnerAdapter              `json:"runnerAdapter"`
	CompatibilityProfile CompatibilityProfile       `json:"compatibilityProfile"`
	ScenarioSet          ExpandedScenarioSet        `json:"scenarioSet"`
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
	manifest, err = DeriveIntegrationScenarioEvidence(repositoryRoot, manifest, operationIDs)
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
	manifest, err := productcontract.DecodeNormalizedOperationManifest(contents)
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
	if manifest.SchemaVersion != consumerManifestSchemaVersion {
		return NormalizedConsumerManifest{}, fmt.Errorf("consumer manifest schemaVersion = %q, want %s", manifest.SchemaVersion, consumerManifestSchemaVersion)
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
		if duplicate := firstDuplicate(scenario.TestEvidence); duplicate != "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("scenario %s duplicates test evidence %s", scenario.ID, duplicate)
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
	}

	caseIDs := make(map[string]bool, len(cases))
	expanded := make([]ExpandedConsumerCase, 0, len(cases))
	for _, consumerCase := range cases {
		if consumerCase.SchemaVersion != consumerSchemaVersion {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s schemaVersion = %q, want %s", consumerCase.ID, consumerCase.SchemaVersion, consumerSchemaVersion)
		}
		if !consumerCaseIDPattern.MatchString(consumerCase.ID) {
			return NormalizedConsumerManifest{}, fmt.Errorf("invalid consumer case ID %q", consumerCase.ID)
		}
		if caseIDs[consumerCase.ID] {
			return NormalizedConsumerManifest{}, fmt.Errorf("duplicate consumer case ID %q", consumerCase.ID)
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
		if runtime.Family != consumerCase.Family {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s runtime family mismatch", consumerCase.ID)
		}
		if len(consumerCase.Versions) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s has no runtime versions", consumerCase.ID)
		}
		for key, version := range consumerCase.Versions {
			if key == "" || version == "" {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s has an empty runtime version", consumerCase.ID)
			}
		}
		if len(consumerCase.Executions) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s has no executions", consumerCase.ID)
		}
		executionIDs := make(map[string]bool, len(consumerCase.Executions))
		requiredVersions := make(map[string]bool)
		requiredArtifactUsages := make(map[string]bool)
		expandedExecutions := make([]ExpandedConsumerExecution, 0, len(consumerCase.Executions))
		for _, execution := range consumerCase.Executions {
			if !consumerCaseIDPattern.MatchString(execution.ID) {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s has invalid execution ID %q", consumerCase.ID, execution.ID)
			}
			if executionIDs[execution.ID] {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s duplicates execution ID %q", consumerCase.ID, execution.ID)
			}
			executionIDs[execution.ID] = true
			adapter, ok := adapters[execution.RunnerAdapterID]
			if !ok {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s execution %s references unknown runner adapter %s", consumerCase.ID, execution.ID, execution.RunnerAdapterID)
			}
			profile, ok := profiles[execution.CompatibilityProfileID]
			if !ok {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s execution %s references unknown compatibility profile %s", consumerCase.ID, execution.ID, execution.CompatibilityProfileID)
			}
			set, ok := sets[execution.ScenarioSetID]
			if !ok {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s execution %s references unknown scenario set %s", consumerCase.ID, execution.ID, execution.ScenarioSetID)
			}
			if adapter.Family != consumerCase.Family || adapter.RuntimeKind != runtime.Kind {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s execution %s runtime/adapter family or kind mismatch", consumerCase.ID, execution.ID)
			}
			for _, required := range adapter.RequiredVersions {
				requiredVersions[required] = true
				if consumerCase.Versions[required] == "" {
					return NormalizedConsumerManifest{}, fmt.Errorf("case %s is missing runtime version %s required by execution %s", consumerCase.ID, required, execution.ID)
				}
			}
			for _, usage := range adapter.RequiredArtifactUsages {
				requiredArtifactUsages[usage] = true
			}
			allowed := sliceSet(profile.ScenarioIDs)
			setupOperations := sliceSet(adapter.SetupOperationIDs)
			caseScenarios := make([]ConsumerScenario, 0, len(set.ScenarioIDs))
			for _, scenarioID := range set.ScenarioIDs {
				if !allowed[scenarioID] {
					return NormalizedConsumerManifest{}, fmt.Errorf("case %s execution %s scenario %s is outside compatibility profile %s", consumerCase.ID, execution.ID, scenarioID, profile.ID)
				}
				scenario := scenarios[scenarioID]
				for _, selector := range scenario.Selectors {
					prefix, value, found := strings.Cut(selector, ":")
					if !found || prefix != adapter.SelectorPrefix || value == "" {
						return NormalizedConsumerManifest{}, fmt.Errorf("case %s execution %s scenario %s selector %q is incompatible with adapter %s", consumerCase.ID, execution.ID, scenarioID, selector, adapter.ID)
					}
				}
				for _, operationID := range scenario.OperationIDs {
					if setupOperations[operationID] {
						return NormalizedConsumerManifest{}, fmt.Errorf("case %s execution %s operation %s is both setup and scenario traffic", consumerCase.ID, execution.ID, operationID)
					}
				}
				caseScenarios = append(caseScenarios, scenario)
			}
			expandedExecutions = append(expandedExecutions, ExpandedConsumerExecution{
				ID: execution.ID, RunnerAdapter: adapter, CompatibilityProfile: profile,
				ScenarioSet: ExpandedScenarioSet{ID: set.ID, Scenarios: caseScenarios},
			})
		}
		if len(consumerCase.Versions) != len(requiredVersions) {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s defines runtime versions outside its execution contracts", consumerCase.ID)
		}
		for version := range consumerCase.Versions {
			if !requiredVersions[version] {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s defines runtime version %s outside its execution contracts", consumerCase.ID, version)
			}
		}
		if consumerCase.Family == "spark" && !strings.HasPrefix(consumerCase.Versions["scala"], consumerCase.Versions["scalaBinary"]+".") {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s Scala runtime does not match its binary version", consumerCase.ID)
		}
		if len(consumerCase.SourceProvenance) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s has no source provenance", consumerCase.ID)
		}
		sourceNames := make(map[string]bool, len(consumerCase.SourceProvenance))
		expandedSources := make([]ConsumerSourceProvenance, 0, len(consumerCase.SourceProvenance))
		for _, source := range consumerCase.SourceProvenance {
			if source.Name == "" || source.URI == "" || sourceNames[source.Name] || (source.VersionKey == "") == (source.Version == "") {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s has incomplete or duplicate source provenance %q", consumerCase.ID, source.Name)
			}
			sourceNames[source.Name] = true
			version := source.Version
			if source.VersionKey != "" {
				version = consumerCase.Versions[source.VersionKey]
				if version == "" {
					return NormalizedConsumerManifest{}, fmt.Errorf("case %s source provenance %s references unknown version key %s", consumerCase.ID, source.Name, source.VersionKey)
				}
			}
			provenance := ConsumerSourceProvenance{Name: source.Name, Version: version, URI: source.URI}
			if err := validateSourceProvenance(provenance); err != nil {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s source provenance %s: %w", consumerCase.ID, source.Name, err)
			}
			expandedSources = append(expandedSources, provenance)
		}
		if len(consumerCase.Artifacts) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s has no immutable artifacts", consumerCase.ID)
		}
		artifactIDs := make([]string, 0, len(consumerCase.Artifacts))
		artifactUsageCounts := make(map[string]int, len(consumerCase.Artifacts))
		for _, artifact := range consumerCase.Artifacts {
			artifactIDs = append(artifactIDs, artifact.ID)
			artifactUsageCounts[artifact.Usage]++
			if artifact.ID == "" || (artifact.Role != "execution" && artifact.Role != "tool-provenance") || !validConsumerArtifactUsage(artifact.Usage) || artifact.URI == "" || !consumerDigestPattern.MatchString(artifact.SHA256) {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s artifact %s must define an immutable URI and lowercase SHA-256", consumerCase.ID, artifact.ID)
			}
			if !requiredArtifactUsages[artifact.Usage] {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s artifact %s usage %s is not accepted by any execution", consumerCase.ID, artifact.ID, artifact.Usage)
			}
			if (artifact.Usage == "cloud-sdk-release-provenance") != (artifact.Role == "tool-provenance") {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s artifact %s role %s is incompatible with usage %s", consumerCase.ID, artifact.ID, artifact.Role, artifact.Usage)
			}
			if err := validateConsumerArtifactURI(artifact); err != nil {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s artifact %s: %w", consumerCase.ID, artifact.ID, err)
			}
		}
		if duplicate := firstDuplicate(artifactIDs); duplicate != "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("case %s duplicates artifact %s", consumerCase.ID, duplicate)
		}
		for usage := range requiredArtifactUsages {
			if artifactUsageCounts[usage] != 1 {
				return NormalizedConsumerManifest{}, fmt.Errorf("case %s must define exactly one %s artifact for its executions", consumerCase.ID, usage)
			}
		}
		runtime.Versions = cloneStringMap(consumerCase.Versions)
		sort.Slice(expandedExecutions, func(i, j int) bool { return expandedExecutions[i].ID < expandedExecutions[j].ID })
		expanded = append(expanded, ExpandedConsumerCase{
			ID: consumerCase.ID, DisplayName: consumerCase.DisplayName, Family: consumerCase.Family,
			Lane: consumerCase.Lane, RuntimeProfile: runtime,
			Artifacts:        append([]ConsumerArtifact(nil), consumerCase.Artifacts...),
			SourceProvenance: expandedSources, Executions: expandedExecutions,
		})
	}
	sort.Slice(expanded, func(i, j int) bool { return expanded[i].ID < expanded[j].ID })
	return NormalizedConsumerManifest{SchemaVersion: consumerSchemaVersion, Cases: expanded}, nil
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
	case "python-wheel", "cloud-sdk-release-provenance", "spark-connector-dsv1-jar", "spark-connector-dsv2-jar", "spark-python-bridge", "spark-runtime", "hadoop-gcs-connector-jar":
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
		if !strings.Contains(compactVersionToken(parsed.Fragment), compactVersionToken(provenance.Version)) {
			return fmt.Errorf("source provenance %s release anchor does not identify version %s", provenance.Name, provenance.Version)
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

func compactVersionToken(value string) string {
	var result strings.Builder
	for _, character := range strings.ToLower(value) {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') {
			result.WriteRune(character)
		}
	}
	return result.String()
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
	if err := rejectDuplicateJSONKeys(contents); err != nil {
		return NormalizedConsumerManifest{}, fmt.Errorf("decode normalized consumer manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var manifest NormalizedConsumerManifest
	if err := decoder.Decode(&manifest); err != nil {
		return NormalizedConsumerManifest{}, fmt.Errorf("decode normalized consumer manifest: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return NormalizedConsumerManifest{}, err
	}
	if manifest.SchemaVersion != consumerSchemaVersion {
		return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer schemaVersion = %q, want %s", manifest.SchemaVersion, consumerSchemaVersion)
	}
	seenCaseIDs := make(map[string]bool, len(manifest.Cases))
	for _, consumerCase := range manifest.Cases {
		if !consumerCaseIDPattern.MatchString(consumerCase.ID) {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case has unsafe ID %q", consumerCase.ID)
		}
		if seenCaseIDs[consumerCase.ID] {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer manifest duplicates case ID %q", consumerCase.ID)
		}
		seenCaseIDs[consumerCase.ID] = true
		if consumerCase.Lane != "required" && consumerCase.Lane != "preview" && consumerCase.Lane != "nightly" {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has unknown lane %q", consumerCase.ID, consumerCase.Lane)
		}
		if consumerCase.DisplayName == "" || consumerCase.Family == "" || consumerCase.RuntimeProfile.ID == "" || consumerCase.RuntimeProfile.Family != consumerCase.Family || consumerCase.RuntimeProfile.Kind == "" {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has an invalid runtime profile", consumerCase.ID)
		}
		if len(consumerCase.Executions) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has no executions", consumerCase.ID)
		}
		if len(consumerCase.SourceProvenance) == 0 {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has no source provenance", consumerCase.ID)
		}
		sourceNames := make(map[string]bool, len(consumerCase.SourceProvenance))
		for _, provenance := range consumerCase.SourceProvenance {
			if provenance.Name == "" || provenance.Version == "" || provenance.URI == "" || sourceNames[provenance.Name] {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has invalid source provenance", consumerCase.ID)
			}
			sourceNames[provenance.Name] = true
			if err := validateSourceProvenance(provenance); err != nil {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has invalid source provenance: %w", consumerCase.ID, err)
			}
		}
		artifactIDs := make(map[string]bool, len(consumerCase.Artifacts))
		usageCounts := make(map[string]int, len(consumerCase.Artifacts))
		for _, artifact := range consumerCase.Artifacts {
			if artifact.ID == "" || artifactIDs[artifact.ID] || !validConsumerArtifactUsage(artifact.Usage) || !consumerDigestPattern.MatchString(artifact.SHA256) || (artifact.Role != "execution" && artifact.Role != "tool-provenance") {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has an invalid artifact", consumerCase.ID)
			}
			if (artifact.Usage == "cloud-sdk-release-provenance") != (artifact.Role == "tool-provenance") {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has an artifact role/usage mismatch", consumerCase.ID)
			}
			if err := validateConsumerArtifactURI(artifact); err != nil {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has an invalid artifact URI: %w", consumerCase.ID, err)
			}
			artifactIDs[artifact.ID] = true
			usageCounts[artifact.Usage]++
		}
		executionIDs := make(map[string]bool, len(consumerCase.Executions))
		requiredVersions := make(map[string]bool)
		requiredArtifactUsages := make(map[string]bool)
		for _, execution := range consumerCase.Executions {
			if !consumerCaseIDPattern.MatchString(execution.ID) || executionIDs[execution.ID] {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has an invalid or duplicate execution ID %q", consumerCase.ID, execution.ID)
			}
			executionIDs[execution.ID] = true
			if !validNormalizedRunnerAdapterID(execution.RunnerAdapter.ID) {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s has unknown runner adapter %q", consumerCase.ID, execution.ID, execution.RunnerAdapter.ID)
			}
			expectedAdapter, _ := normalizedRunnerAdapterContract(execution.RunnerAdapter.ID)
			if execution.RunnerAdapter.Family != consumerCase.Family || execution.RunnerAdapter.RuntimeKind != consumerCase.RuntimeProfile.Kind {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s has a runtime/adapter mismatch", consumerCase.ID, execution.ID)
			}
			if execution.RunnerAdapter.Family != expectedAdapter.Family ||
				execution.RunnerAdapter.RuntimeKind != expectedAdapter.RuntimeKind ||
				execution.RunnerAdapter.SelectorPrefix != expectedAdapter.SelectorPrefix ||
				!equalStringSlices(execution.RunnerAdapter.RequiredVersions, expectedAdapter.RequiredVersions) ||
				!equalStringSlices(execution.RunnerAdapter.RequiredArtifactUsages, expectedAdapter.RequiredArtifactUsages) ||
				!equalStringMaps(execution.RunnerAdapter.Bootstrap, expectedAdapter.Bootstrap) ||
				!equalStringSlices(execution.RunnerAdapter.SetupOperationIDs, expectedAdapter.SetupOperationIDs) {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s has runner adapter contract drift", consumerCase.ID, execution.ID)
			}
			if execution.CompatibilityProfile.ID == "" || execution.ScenarioSet.ID == "" || len(execution.ScenarioSet.Scenarios) == 0 {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s has an incomplete compatibility contract", consumerCase.ID, execution.ID)
			}
			for _, requiredVersion := range execution.RunnerAdapter.RequiredVersions {
				requiredVersions[requiredVersion] = true
				if consumerCase.RuntimeProfile.Versions[requiredVersion] == "" {
					return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s is missing runtime version %s", consumerCase.ID, execution.ID, requiredVersion)
				}
			}
			for _, requiredUsage := range execution.RunnerAdapter.RequiredArtifactUsages {
				requiredArtifactUsages[requiredUsage] = true
			}
			for _, scenario := range execution.ScenarioSet.Scenarios {
				if scenario.ID == "" || len(scenario.OperationIDs) == 0 || len(scenario.Selectors) == 0 {
					return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s has an invalid scenario", consumerCase.ID, execution.ID)
				}
				if duplicate := firstDuplicate(scenario.TestEvidence); duplicate != "" {
					return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s duplicates test evidence", consumerCase.ID, execution.ID)
				}
				for _, testID := range scenario.TestEvidence {
					if !integrationTestIDPattern.MatchString(testID) {
						return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s has invalid test evidence", consumerCase.ID, execution.ID)
					}
				}
				for _, selector := range scenario.Selectors {
					prefix, value, found := strings.Cut(selector, ":")
					if !found || prefix != execution.RunnerAdapter.SelectorPrefix || value == "" {
						return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s execution %s has an invalid scenario selector", consumerCase.ID, execution.ID)
					}
				}
			}
		}
		if len(consumerCase.RuntimeProfile.Versions) != len(requiredVersions) {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has runtime version contract drift", consumerCase.ID)
		}
		for version, value := range consumerCase.RuntimeProfile.Versions {
			if value == "" || !requiredVersions[version] {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has runtime version contract drift", consumerCase.ID)
			}
		}
		if consumerCase.Family == "spark" && !strings.HasPrefix(consumerCase.RuntimeProfile.Versions["scala"], consumerCase.RuntimeProfile.Versions["scalaBinary"]+".") {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has Scala binary version drift", consumerCase.ID)
		}
		for requiredUsage := range requiredArtifactUsages {
			if !validConsumerArtifactUsage(requiredUsage) || usageCounts[requiredUsage] != 1 {
				return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has invalid artifact cardinality for usage %s", consumerCase.ID, requiredUsage)
			}
		}
		if len(usageCounts) != len(requiredArtifactUsages) {
			return NormalizedConsumerManifest{}, fmt.Errorf("normalized consumer case %s has an artifact outside its execution contracts", consumerCase.ID)
		}
	}
	return manifest, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("decode normalized consumer manifest: multiple JSON documents are not allowed")
		}
		return fmt.Errorf("decode normalized consumer manifest: %w", err)
	}
	return nil
}

func rejectDuplicateJSONKeys(contents []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	var visit func() error
	visit = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, structured := token.(json.Delim)
		if !structured {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key is not a string")
				}
				if seen[key] {
					return fmt.Errorf("duplicate JSON key %q", key)
				}
				seen[key] = true
				if err := visit(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := visit(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
	}
	return visit()
}

func validNormalizedRunnerAdapterID(id string) bool {
	_, ok := normalizedRunnerAdapterContract(id)
	return ok
}

func validateConsumerArtifactURI(artifact ConsumerArtifact) error {
	parsed, err := url.Parse(artifact.URI)
	if err != nil || parsed.Host == "" {
		return errors.New("artifact URI must be absolute")
	}
	if artifact.Role == "execution" {
		if parsed.Scheme != "https" {
			return errors.New("execution artifact URI must use HTTPS")
		}
		return nil
	}
	if parsed.Scheme != "oci" || !strings.HasSuffix(artifact.URI, "@sha256:"+artifact.SHA256) {
		return errors.New("provenance artifact must use a digest-pinned OCI URI")
	}
	return nil
}

func normalizedRunnerAdapterContract(id string) (RunnerAdapter, bool) {
	contracts := map[string]RunnerAdapter{
		"python-pytest-v1": {
			ID: "python-pytest-v1", Family: "python", RuntimeKind: "python-pytest", SelectorPrefix: "pytest",
			RequiredVersions: []string{"python", "client"}, RequiredArtifactUsages: []string{"python-wheel"}, Bootstrap: map[string]string{},
			SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create", "bqemu.projects.delete"},
		},
		"bq-cli-v1": {
			ID: "bq-cli-v1", Family: "bq", RuntimeKind: "bq-cli", SelectorPrefix: "bq",
			RequiredVersions: []string{"cloudSdk", "bq"}, RequiredArtifactUsages: []string{"cloud-sdk-release-provenance"}, Bootstrap: map[string]string{},
			SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create"},
		},
		"spark-pyspark-pytest-v1": {
			ID: "spark-pyspark-pytest-v1", Family: "spark", RuntimeKind: "spark-pyspark", SelectorPrefix: "pytest",
			RequiredVersions:       []string{"spark", "connector", "scala", "scalaBinary", "java", "python"},
			RequiredArtifactUsages: []string{"spark-connector-dsv1-jar", "spark-connector-dsv2-jar", "spark-python-bridge", "spark-runtime"},
			Bootstrap:              map[string]string{}, SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create", "bigquery.datasets.insert"},
		},
		"spark-scala-shell-v1": {
			ID: "spark-scala-shell-v1", Family: "spark", RuntimeKind: "spark-scala", SelectorPrefix: "pytest",
			RequiredVersions:       []string{"spark", "connector", "scala", "scalaBinary", "java", "python"},
			RequiredArtifactUsages: []string{"spark-connector-dsv1-jar", "spark-python-bridge", "spark-runtime"},
			Bootstrap:              map[string]string{}, SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create", "bigquery.datasets.insert"},
		},
		"python-indirect-load-v1": {
			ID: "python-indirect-load-v1", Family: "python", RuntimeKind: "python-pytest", SelectorPrefix: "load",
			RequiredVersions: []string{"python", "client"}, RequiredArtifactUsages: []string{"python-wheel"}, Bootstrap: map[string]string{},
			SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create"},
		},
		"bq-indirect-load-v1": {
			ID: "bq-indirect-load-v1", Family: "bq", RuntimeKind: "bq-cli", SelectorPrefix: "load",
			RequiredVersions: []string{"cloudSdk", "bq"}, RequiredArtifactUsages: []string{"cloud-sdk-release-provenance"}, Bootstrap: map[string]string{},
			SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create", "bqemu.discovery.get"},
		},
		"spark-pyspark-indirect-load-v1": {
			ID: "spark-pyspark-indirect-load-v1", Family: "spark", RuntimeKind: "spark-pyspark", SelectorPrefix: "load",
			RequiredVersions:       []string{"spark", "connector", "scala", "scalaBinary", "java", "python", "hadoopGcsConnector"},
			RequiredArtifactUsages: []string{"spark-connector-dsv1-jar", "spark-python-bridge", "spark-runtime", "hadoop-gcs-connector-jar"},
			Bootstrap:              map[string]string{}, SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create"},
		},
		"spark-scala-indirect-load-v1": {
			ID: "spark-scala-indirect-load-v1", Family: "spark", RuntimeKind: "spark-scala", SelectorPrefix: "load",
			RequiredVersions:       []string{"spark", "connector", "scala", "scalaBinary", "java", "python", "hadoopGcsConnector"},
			RequiredArtifactUsages: []string{"spark-connector-dsv1-jar", "spark-python-bridge", "spark-runtime", "hadoop-gcs-connector-jar"},
			Bootstrap:              map[string]string{}, SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create"},
		},
	}
	adapter, ok := contracts[id]
	return adapter, ok
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalStringMaps(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func stringInSlice(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func ConsumerMatrix(repositoryRoot, family, lanes string, executionFilters ...string) ([]byte, error) {
	payload, _, err := ConsumerMatrixWithCount(repositoryRoot, family, lanes, executionFilters...)
	return payload, err
}

func ConsumerMatrixWithCount(repositoryRoot, family, lanes string, executionFilters ...string) ([]byte, int, error) {
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
	executionFilter := make(map[string]bool)
	if len(executionFilters) > 1 {
		return nil, 0, errors.New("consumer execution filter may be specified at most once")
	}
	if len(executionFilters) == 1 && executionFilters[0] != "" {
		for _, executionID := range strings.Split(executionFilters[0], ",") {
			if !consumerCaseIDPattern.MatchString(executionID) {
				return nil, 0, fmt.Errorf("invalid consumer execution ID %q", executionID)
			}
			if executionFilter[executionID] {
				return nil, 0, fmt.Errorf("duplicate consumer execution ID %q", executionID)
			}
			executionFilter[executionID] = true
		}
	}
	rows := make([]ConsumerMatrixRow, 0)
	for _, consumerCase := range manifest.Cases {
		if (family != "" && consumerCase.Family != family) || (len(laneFilter) != 0 && !laneFilter[consumerCase.Lane]) {
			continue
		}
		for _, execution := range consumerCase.Executions {
			if len(executionFilter) != 0 && !executionFilter[execution.ID] {
				continue
			}
			rows = append(rows, ConsumerMatrixRow{
				ID: consumerCase.ID, DisplayName: consumerCase.DisplayName, Family: consumerCase.Family,
				Lane: consumerCase.Lane, RuntimeProfile: consumerCase.RuntimeProfile,
				Artifacts: consumerCase.Artifacts, SourceProvenance: consumerCase.SourceProvenance, ExecutionID: execution.ID,
				RunnerAdapter: execution.RunnerAdapter, CompatibilityProfile: execution.CompatibilityProfile,
				ScenarioSet: execution.ScenarioSet,
			})
		}
	}
	payload, err := json.Marshal(struct {
		Include []ConsumerMatrixRow `json:"include"`
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
		output.WriteString("<!-- doc-id: consumer-compatibility -->\n<!-- lang: ko -->\n\n[English](../en/consumer-compatibility.md) | [한국어](consumer-compatibility.md)\n\n# 소비자 호환성\n\n<!-- section: generated-cases -->\n이 문서는 `tests/integration/contract/consumers.normalized.json`에서 생성됩니다. 공개 동작은 [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest)를 기준으로 검증합니다. 각 소비자의 정확한 버전과 변경되지 않는 출처는 아래 표에 표시합니다. `execution` 산출물은 해시를 확인한 뒤 실행에 사용합니다. `tool-provenance` 산출물은 별도로 설치하고 버전을 확인한 도구의 릴리스 출처만 나타냅니다.\n\n| 사례 | 실행 | 실행 계열 | 상태 | 런타임 | 시나리오 | 출처 | 산출물 역할/용도 |\n|---|---|---|---|---|---|---|---|\n")
	} else {
		output.WriteString("<!-- doc-id: consumer-compatibility -->\n<!-- lang: en -->\n\n[English](consumer-compatibility.md) | [한국어](../ko/consumer-compatibility.md)\n\n# Consumer Compatibility\n\n<!-- section: generated-cases -->\nThis page is generated from `tests/integration/contract/consumers.normalized.json`. Public behavior is verified against the [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest). The table records every consumer's exact version and immutable source. An `execution` artifact is digest-verified and used by the runner. A `tool-provenance` artifact records only the release provenance of a separately installed, version-verified tool.\n\n| Case | Execution | Family | Lane | Runtime | Scenarios | Sources | Artifact role/usage |\n|---|---|---|---|---|---|---|---|\n")
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
		artifacts := make([]string, 0, len(consumerCase.Artifacts))
		for _, artifact := range consumerCase.Artifacts {
			artifacts = append(artifacts, "`"+artifact.ID+"` (`"+artifact.Role+"` / `"+artifact.Usage+"`)")
		}
		sources := make([]string, 0, len(consumerCase.SourceProvenance))
		for _, source := range consumerCase.SourceProvenance {
			sources = append(sources, fmt.Sprintf("[%s %s](%s)", source.Name, source.Version, source.URI))
		}
		for _, execution := range consumerCase.Executions {
			scenarios := make([]string, 0, len(execution.ScenarioSet.Scenarios))
			for _, scenario := range execution.ScenarioSet.Scenarios {
				scenarios = append(scenarios, "`"+scenario.ID+"`")
			}
			fmt.Fprintf(&output, "| `%s` | `%s` | %s | %s | %s | %s | %s | %s |\n", consumerCase.ID, execution.ID, consumerCase.Family, consumerCase.Lane, strings.Join(versions, ", "), strings.Join(scenarios, "<br>"), strings.Join(sources, "<br>"), strings.Join(artifacts, "<br>"))
		}
	}
	return []byte(output.String())
}
