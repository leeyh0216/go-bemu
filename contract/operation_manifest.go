package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type OperationProtocol string

const (
	ProtocolREST OperationProtocol = "rest"
	ProtocolGRPC OperationProtocol = "grpc"
)

type OperationSupport string

const (
	SupportImplemented OperationSupport = "implemented"
	SupportPartial     OperationSupport = "partial"
	SupportRegistered  OperationSupport = "registered"
	SupportUnsupported OperationSupport = "unsupported"
)

type OperationVerification string

const (
	VerificationPublicProcess OperationVerification = "public-process"
	VerificationTransport     OperationVerification = "transport"
	VerificationApplication   OperationVerification = "application"
	VerificationUnit          OperationVerification = "unit"
	VerificationNone          OperationVerification = "none"
)

type LocalizedText struct {
	EN string `json:"en" yaml:"en"`
	KO string `json:"ko" yaml:"ko"`
}

type LimitationPolicy string

const (
	LimitationNone     LimitationPolicy = "none"
	LimitationByDesign LimitationPolicy = "by-design"
	LimitationTracked  LimitationPolicy = "tracked"
	LimitationMixed    LimitationPolicy = "mixed"
)

type ByDesignScope string

const (
	ByDesignGoogleControlPlane ByDesignScope = "google-control-plane"
	ByDesignGoogleIAM          ByDesignScope = "google-iam"
)

type OperationLimitations struct {
	Policy   LimitationPolicy `json:"policy" yaml:"policy"`
	ByDesign []ByDesignScope  `json:"byDesign" yaml:"byDesign"`
	EN       string           `json:"en" yaml:"en"`
	KO       string           `json:"ko" yaml:"ko"`
}

type ConditionEffect string

const (
	ConditionInputBranch         ConditionEffect = "input-branch"
	ConditionOperationExposure   ConditionEffect = "operation-exposure"
	ConditionServiceAvailability ConditionEffect = "service-availability"
)

type OperationCondition struct {
	Setting      string                `json:"setting" yaml:"setting"`
	Equals       bool                  `json:"equals" yaml:"equals"`
	Effect       ConditionEffect       `json:"effect" yaml:"effect"`
	Verification OperationVerification `json:"verification" yaml:"verification"`
	Tests        []string              `json:"tests" yaml:"tests"`
	EN           string                `json:"en" yaml:"en"`
	KO           string                `json:"ko" yaml:"ko"`
}

type OperationSource struct {
	ID  string `json:"id" yaml:"id"`
	URL string `json:"url" yaml:"url"`
}

type RESTOperation struct {
	Method    string `json:"method" yaml:"method"`
	Path      string `json:"path" yaml:"path"`
	Discovery bool   `json:"discovery" yaml:"discovery"`
}

type GRPCOperation struct {
	Service string `json:"service" yaml:"service"`
	Method  string `json:"method" yaml:"method"`
}

type Operation struct {
	ID             string                `json:"id" yaml:"id"`
	Protocol       OperationProtocol     `json:"protocol" yaml:"protocol"`
	Component      string                `json:"component" yaml:"component"`
	Summary        LocalizedText         `json:"summary" yaml:"summary"`
	Support        OperationSupport      `json:"support" yaml:"support"`
	Verification   OperationVerification `json:"verification" yaml:"verification"`
	REST           *RESTOperation        `json:"rest,omitempty" yaml:"rest,omitempty"`
	GRPC           *GRPCOperation        `json:"grpc,omitempty" yaml:"grpc,omitempty"`
	SupportedInput LocalizedText         `json:"supportedInput" yaml:"supportedInput"`
	Conditions     []OperationCondition  `json:"conditions" yaml:"conditions"`
	Limitations    OperationLimitations  `json:"limitations" yaml:"limitations"`
	Issues         []string              `json:"issues" yaml:"issues"`
	Sources        []string              `json:"sources" yaml:"sources"`
	Tests          []string              `json:"tests" yaml:"tests"`
}

type OperationManifest struct {
	SchemaVersion int               `json:"schemaVersion" yaml:"schemaVersion"`
	Sources       []OperationSource `json:"sources" yaml:"sources"`
	Operations    []Operation       `json:"operations" yaml:"operations"`
}

var (
	operationIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[._-][A-Za-z0-9]+)+$`)
	componentPattern   = regexp.MustCompile(`^(public-(?:core|query|tabledata|console)|admin|storage-read|storage-write|grpc-health|grpc-reflection)$`)
	issuePattern       = regexp.MustCompile(`^#[1-9][0-9]*$`)
	testIDPattern      = regexp.MustCompile(`^(?:go|python|spark|bq):[^:]+:[A-Za-z_][A-Za-z0-9_]*$`)
	grpcNamePattern    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.]*$`)
)

func stringSet(values ...string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

func DecodeOperationManifest(contents []byte) (OperationManifest, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	var manifest OperationManifest
	if err := decoder.Decode(&manifest); err != nil {
		return OperationManifest{}, fmt.Errorf("decode operation manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return OperationManifest{}, errors.New("decode operation manifest: multiple YAML documents are not allowed")
		}
		return OperationManifest{}, fmt.Errorf("decode operation manifest: %w", err)
	}
	if err := ValidateOperationManifest(manifest); err != nil {
		return OperationManifest{}, err
	}
	return normalizeOperationManifest(manifest), nil
}

func ValidateOperationManifest(manifest OperationManifest) error {
	if manifest.SchemaVersion != 1 {
		return fmt.Errorf("operation manifest: unsupported schemaVersion %d", manifest.SchemaVersion)
	}
	if len(manifest.Sources) == 0 {
		return errors.New("operation manifest: sources must not be empty")
	}
	knownSources := make(map[string]struct{}, len(manifest.Sources))
	for index, source := range manifest.Sources {
		location := fmt.Sprintf("operation manifest: sources[%d]", index)
		if strings.TrimSpace(source.ID) == "" || strings.TrimSpace(source.URL) == "" {
			return fmt.Errorf("%s: id and url are required", location)
		}
		if _, exists := knownSources[source.ID]; exists {
			return fmt.Errorf("%s: duplicate source id %q", location, source.ID)
		}
		knownSources[source.ID] = struct{}{}
		if !strings.HasPrefix(source.URL, "https://") {
			return fmt.Errorf("%s: source URL must use https", location)
		}
	}
	if len(manifest.Operations) == 0 {
		return errors.New("operation manifest: operations must not be empty")
	}
	knownIDs := make(map[string]struct{}, len(manifest.Operations))
	knownREST := make(map[string]string)
	knownGRPC := make(map[string]string)
	for index, operation := range manifest.Operations {
		location := fmt.Sprintf("operation manifest: operations[%d]", index)
		if !operationIDPattern.MatchString(operation.ID) {
			return fmt.Errorf("%s: invalid operation id %q", location, operation.ID)
		}
		if _, exists := knownIDs[operation.ID]; exists {
			return fmt.Errorf("%s: duplicate operation id %q", location, operation.ID)
		}
		knownIDs[operation.ID] = struct{}{}
		if !componentPattern.MatchString(operation.Component) {
			return fmt.Errorf("%s %s: unknown component %q", location, operation.ID, operation.Component)
		}
		if err := validateLocalizedText(operation.Summary); err != nil {
			return fmt.Errorf("%s %s summary: %w", location, operation.ID, err)
		}
		if err := validateLocalizedText(operation.SupportedInput); err != nil {
			return fmt.Errorf("%s %s supportedInput: %w", location, operation.ID, err)
		}
		if err := validateOperationConditions(operation); err != nil {
			return fmt.Errorf("%s %s conditions: %w", location, operation.ID, err)
		}
		if err := validateOperationLimitations(operation); err != nil {
			return fmt.Errorf("%s %s limitations: %w", location, operation.ID, err)
		}
		if err := validateOperationClassification(operation); err != nil {
			return fmt.Errorf("%s %s: %w", location, operation.ID, err)
		}
		switch operation.Protocol {
		case ProtocolREST:
			if operation.REST == nil || operation.GRPC != nil {
				return fmt.Errorf("%s %s: REST operation requires only rest shape", location, operation.ID)
			}
			if operation.REST.Method == "" || operation.REST.Method != strings.ToUpper(operation.REST.Method) {
				return fmt.Errorf("%s %s: REST method must be uppercase", location, operation.ID)
			}
			if _, ok := allowedHTTPMethods[operation.REST.Method]; !ok {
				return fmt.Errorf("%s %s: unsupported REST method %q", location, operation.ID, operation.REST.Method)
			}
			if !strings.HasPrefix(operation.REST.Path, "/") || strings.Contains(operation.REST.Path, " ") {
				return fmt.Errorf("%s %s: invalid REST path %q", location, operation.ID, operation.REST.Path)
			}
			key := restListener(operation.Component) + " " + operation.REST.Method + " " + operation.REST.Path
			if existing := knownREST[key]; existing != "" {
				return fmt.Errorf("%s %s: REST shape duplicates %s", location, operation.ID, existing)
			}
			knownREST[key] = operation.ID
		case ProtocolGRPC:
			if operation.GRPC == nil || operation.REST != nil {
				return fmt.Errorf("%s %s: gRPC operation requires only grpc shape", location, operation.ID)
			}
			if !grpcNamePattern.MatchString(operation.GRPC.Service) || !grpcNamePattern.MatchString(operation.GRPC.Method) || strings.Contains(operation.GRPC.Method, ".") {
				return fmt.Errorf("%s %s: invalid gRPC service or method", location, operation.ID)
			}
			key := operation.GRPC.Service + "/" + operation.GRPC.Method
			if existing := knownGRPC[key]; existing != "" {
				return fmt.Errorf("%s %s: gRPC shape duplicates %s", location, operation.ID, existing)
			}
			knownGRPC[key] = operation.ID
		default:
			return fmt.Errorf("%s %s: unknown protocol %q", location, operation.ID, operation.Protocol)
		}
		for _, source := range operation.Sources {
			if _, exists := knownSources[source]; !exists {
				return fmt.Errorf("%s %s: unknown source %q", location, operation.ID, source)
			}
		}
		if len(operation.Sources) == 0 {
			return fmt.Errorf("%s %s: at least one official source is required", location, operation.ID)
		}
		if err := validateUniqueReferences(operation.Issues, issuePattern, "issue"); err != nil {
			return fmt.Errorf("%s %s: %w", location, operation.ID, err)
		}
		if err := validateUniqueReferences(operation.Tests, testIDPattern, "test"); err != nil {
			return fmt.Errorf("%s %s: %w", location, operation.ID, err)
		}
	}
	return nil
}

func restListener(component string) string {
	if component == "admin" {
		return "admin"
	}
	return "public"
}

var allowedHTTPMethods = map[string]struct{}{
	http.MethodDelete: {}, http.MethodGet: {}, http.MethodPatch: {}, http.MethodPost: {}, http.MethodPut: {},
}

func validateOperationClassification(operation Operation) error {
	switch operation.Support {
	case SupportImplemented, SupportPartial:
		if operation.Verification == VerificationNone || len(operation.Tests) == 0 {
			return errors.New("implemented and partial operations require verification and tests")
		}
	case SupportRegistered:
		if operation.Verification != VerificationTransport || len(operation.Tests) == 0 {
			return errors.New("registered operations require transport verification and tests")
		}
	case SupportUnsupported:
		if operation.Verification != VerificationNone || len(operation.Tests) != 0 {
			return errors.New("unsupported operations require verification=none and no tests")
		}
	default:
		return fmt.Errorf("unknown support %q", operation.Support)
	}
	switch operation.Verification {
	case VerificationPublicProcess, VerificationTransport, VerificationApplication, VerificationUnit, VerificationNone:
	default:
		return fmt.Errorf("unknown verification %q", operation.Verification)
	}
	return nil
}

func validateOperationConditions(operation Operation) error {
	conditions := operation.Conditions
	allowed := map[string]ConditionEffect{
		"admin.enabled":         ConditionOperationExposure,
		"load.enabled":          ConditionInputBranch,
		"storage.read.enabled":  ConditionServiceAvailability,
		"storage.write.enabled": ConditionServiceAvailability,
		"ui.enabled":            ConditionOperationExposure,
	}
	seen := make(map[string]bool, len(conditions))
	operationTests := stringSet(operation.Tests...)
	for _, condition := range conditions {
		if !condition.Equals {
			return fmt.Errorf("%s must currently require equals=true", condition.Setting)
		}
		expected, ok := allowed[condition.Setting]
		if !ok || condition.Effect != expected {
			return fmt.Errorf("unknown setting/effect pair %q/%q", condition.Setting, condition.Effect)
		}
		if strings.TrimSpace(condition.EN) == "" || strings.TrimSpace(condition.KO) == "" {
			return fmt.Errorf("%s requires both en and ko", condition.Setting)
		}
		if len(condition.Tests) == 0 {
			return fmt.Errorf("%s requires verification tests", condition.Setting)
		}
		if err := validateUniqueReferences(condition.Tests, testIDPattern, "condition test"); err != nil {
			return fmt.Errorf("%s: %w", condition.Setting, err)
		}
		for _, testID := range condition.Tests {
			if !operationTests[testID] {
				return fmt.Errorf("%s test %s is absent from operation tests", condition.Setting, testID)
			}
		}
		if err := validateVerificationTests(condition.Verification, condition.Tests); err != nil {
			return fmt.Errorf("%s: %w", condition.Setting, err)
		}
		key := condition.Setting + "/" + string(condition.Effect)
		if seen[key] {
			return fmt.Errorf("duplicate condition %q", key)
		}
		seen[key] = true
	}
	return nil
}

func validateOperationLimitations(operation Operation) error {
	limitations := operation.Limitations
	if strings.TrimSpace(limitations.EN) == "" || strings.TrimSpace(limitations.KO) == "" {
		return errors.New("both en and ko are required")
	}
	seen := make(map[ByDesignScope]bool, len(limitations.ByDesign))
	for _, scope := range limitations.ByDesign {
		switch scope {
		case ByDesignGoogleControlPlane, ByDesignGoogleIAM:
		default:
			return fmt.Errorf("unknown by-design scope %q", scope)
		}
		if seen[scope] {
			return fmt.Errorf("duplicate by-design scope %q", scope)
		}
		seen[scope] = true
	}
	switch limitations.Policy {
	case LimitationNone:
		if len(limitations.ByDesign) != 0 || len(operation.Issues) != 0 {
			return errors.New("none requires no by-design scopes or issues")
		}
	case LimitationByDesign:
		if len(limitations.ByDesign) == 0 || len(operation.Issues) != 0 {
			return errors.New("by-design requires approved scopes and no issues")
		}
	case LimitationTracked:
		if len(limitations.ByDesign) != 0 || len(operation.Issues) == 0 {
			return errors.New("tracked requires issues and no by-design scopes")
		}
	case LimitationMixed:
		if len(limitations.ByDesign) == 0 || len(operation.Issues) == 0 {
			return errors.New("mixed requires approved scopes and issues")
		}
	default:
		return fmt.Errorf("unknown policy %q", limitations.Policy)
	}
	if operation.Support != SupportImplemented && limitations.Policy == LimitationNone {
		return fmt.Errorf("%s support requires a tracked or by-design limitation", operation.Support)
	}
	return nil
}

func validateLocalizedText(value LocalizedText) error {
	if strings.TrimSpace(value.EN) == "" || strings.TrimSpace(value.KO) == "" {
		return errors.New("both en and ko are required")
	}
	return nil
}

func validateUniqueReferences(values []string, pattern *regexp.Regexp, kind string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !pattern.MatchString(value) {
			return fmt.Errorf("invalid %s reference %q", kind, value)
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("duplicate %s reference %q", kind, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func normalizeOperationManifest(manifest OperationManifest) OperationManifest {
	result := manifest
	result.Sources = append([]OperationSource(nil), manifest.Sources...)
	sort.Slice(result.Sources, func(i, j int) bool { return result.Sources[i].ID < result.Sources[j].ID })
	result.Operations = append([]Operation(nil), manifest.Operations...)
	for index := range result.Operations {
		result.Operations[index].Conditions = append([]OperationCondition(nil), result.Operations[index].Conditions...)
		for conditionIndex := range result.Operations[index].Conditions {
			result.Operations[index].Conditions[conditionIndex].Tests = sortedStrings(result.Operations[index].Conditions[conditionIndex].Tests)
		}
		sort.Slice(result.Operations[index].Conditions, func(i, j int) bool {
			left, right := result.Operations[index].Conditions[i], result.Operations[index].Conditions[j]
			if left.Setting == right.Setting {
				return left.Effect < right.Effect
			}
			return left.Setting < right.Setting
		})
		if result.Operations[index].Conditions == nil {
			result.Operations[index].Conditions = []OperationCondition{}
		}
		result.Operations[index].Limitations.ByDesign = append([]ByDesignScope(nil), result.Operations[index].Limitations.ByDesign...)
		sort.Slice(result.Operations[index].Limitations.ByDesign, func(i, j int) bool {
			return result.Operations[index].Limitations.ByDesign[i] < result.Operations[index].Limitations.ByDesign[j]
		})
		if result.Operations[index].Limitations.ByDesign == nil {
			result.Operations[index].Limitations.ByDesign = []ByDesignScope{}
		}
		result.Operations[index].Issues = sortedStrings(result.Operations[index].Issues)
		result.Operations[index].Sources = sortedStrings(result.Operations[index].Sources)
		result.Operations[index].Tests = sortedStrings(result.Operations[index].Tests)
	}
	sort.Slice(result.Operations, func(i, j int) bool { return result.Operations[i].ID < result.Operations[j].ID })
	return result
}

func sortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	if result == nil {
		return []string{}
	}
	return result
}

func MarshalNormalizedOperationManifest(manifest OperationManifest) ([]byte, error) {
	normalized := normalizeOperationManifest(manifest)
	contents, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(contents, '\n'), nil
}

func cloneOperation(operation Operation) Operation {
	result := operation
	result.Conditions = append([]OperationCondition(nil), operation.Conditions...)
	for index := range result.Conditions {
		result.Conditions[index].Tests = append([]string(nil), operation.Conditions[index].Tests...)
	}
	result.Limitations.ByDesign = append([]ByDesignScope(nil), operation.Limitations.ByDesign...)
	result.Issues = append([]string(nil), operation.Issues...)
	result.Sources = append([]string(nil), operation.Sources...)
	result.Tests = append([]string(nil), operation.Tests...)
	if operation.REST != nil {
		value := *operation.REST
		result.REST = &value
	}
	if operation.GRPC != nil {
		value := *operation.GRPC
		result.GRPC = &value
	}
	return result
}
