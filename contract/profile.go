package contract

// Official sources:
//   - https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2
//   - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2

import (
	"crypto/sha256"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"sync"
)

type CapabilityState string

const (
	CapabilityVerified   CapabilityState = "verified"
	CapabilityPartial    CapabilityState = "partial"
	CapabilityRegistered CapabilityState = "registered"
	CapabilityPlanned    CapabilityState = "planned"
)

type CallSpec struct {
	Stage          string   `json:"stage"`
	Protocol       string   `json:"protocol"`
	Method         string   `json:"method"`
	Target         string   `json:"target"`
	RequiredFields []string `json:"requiredFields,omitempty"`
}

// ConsumerSpec binds a protocol profile to exact client or connector releases.
// Exact matching is intentional: a new release must provide or explicitly adopt
// a reviewed profile instead of silently inheriting an adjacent wire contract.
type ConsumerSpec struct {
	Kind     string   `json:"kind"`
	Name     string   `json:"name"`
	Versions []string `json:"versions"`
}

type Profile struct {
	SchemaVersion     string                     `json:"schemaVersion"`
	ID                string                     `json:"id"`
	ConnectorVersions []string                   `json:"connectorVersions,omitempty"` // Legacy profile compatibility.
	Consumers         []ConsumerSpec             `json:"consumers,omitempty"`
	Description       string                     `json:"description"`
	Sources           []string                   `json:"sources"`
	Capabilities      map[string]CapabilityState `json:"capabilities"`
	Flows             map[string][]CallSpec      `json:"flows"`
}

type WireEvent struct {
	Stage    string         `json:"stage"`
	Protocol string         `json:"protocol"`
	Method   string         `json:"method"`
	Target   string         `json:"target"`
	Fields   map[string]any `json:"fields,omitempty"`
}

type Trace struct {
	ProfileID        string      `json:"profileId"`
	ConnectorVersion string      `json:"connectorVersion,omitempty"`
	ConsumerKind     string      `json:"consumerKind,omitempty"`
	ConsumerName     string      `json:"consumerName,omitempty"`
	ConsumerVersion  string      `json:"consumerVersion,omitempty"`
	FixtureKind      string      `json:"fixtureKind"`
	SourceRefs       []string    `json:"sourceRefs"`
	Flow             string      `json:"flow"`
	Events           []WireEvent `json:"events"`
}

//go:embed profiles/*.json golden/*.json
var assets embed.FS

type Registry struct {
	profiles  map[string]Profile
	versions  map[string]string
	consumers map[string]string
}

// DriftError is safe to surface in CI and protocol logs. Shape describes only
// method/path/field names, while Fingerprint is a digest of the normalized
// fixture; neither contains authorization headers or full row payloads.
type DriftError struct {
	Version     string
	Operation   string
	Stage       string
	Shape       string
	Fingerprint string
	FixHint     string
	Diff        string
}

func (e *DriftError) Error() string {
	return fmt.Sprintf(
		"contract drift: version=%s operation=%s stage=%s shape=%s fingerprint=%s fix_hint=%s:\n%s",
		e.Version, e.Operation, e.Stage, e.Shape, e.Fingerprint, e.FixHint, e.Diff,
	)
}

var (
	defaultRegistry *Registry
	defaultOnce     sync.Once
)

func DefaultRegistry() *Registry {
	defaultOnce.Do(func() {
		registry, err := loadRegistry()
		if err != nil {
			panic(err)
		}
		defaultRegistry = registry
	})
	return defaultRegistry
}

func loadRegistry() (*Registry, error) {
	paths, err := fs.Glob(assets, "profiles/*.json")
	if err != nil {
		return nil, err
	}
	registry := &Registry{
		profiles:  make(map[string]Profile),
		versions:  make(map[string]string),
		consumers: make(map[string]string),
	}
	for _, path := range paths {
		contents, err := assets.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var profile Profile
		if err := json.Unmarshal(contents, &profile); err != nil {
			return nil, fmt.Errorf("decode contract profile %s: %w", path, err)
		}
		if err := validateProfile(path, profile); err != nil {
			return nil, err
		}
		if _, exists := registry.profiles[profile.ID]; exists {
			return nil, fmt.Errorf("duplicate contract profile %s", profile.ID)
		}
		registry.profiles[profile.ID] = profile
		for _, version := range profile.ConnectorVersions {
			if existing := registry.versions[version]; existing != "" {
				return nil, fmt.Errorf("connector version %s appears in profiles %s and %s", version, existing, profile.ID)
			}
			registry.versions[version] = profile.ID
		}
		for _, consumer := range profile.Consumers {
			if err := registry.registerConsumer(profile.ID, consumer); err != nil {
				return nil, err
			}
		}
	}
	return registry, nil
}

func (r *Registry) registerConsumer(profileID string, consumer ConsumerSpec) error {
	for _, version := range consumer.Versions {
		key := consumerKey(consumer.Kind, consumer.Name, version)
		if existing := r.consumers[key]; existing != "" {
			return fmt.Errorf("consumer %s appears in profiles %s and %s", key, existing, profileID)
		}
		r.consumers[key] = profileID
		if consumer.Kind == "connector" {
			if existing := r.versions[version]; existing != "" {
				return fmt.Errorf("connector version %s appears in profiles %s and %s", version, existing, profileID)
			}
			r.versions[version] = profileID
		}
	}
	return nil
}

func consumerKey(kind, name, version string) string {
	return kind + ":" + name + "@" + version
}

func (r *Registry) Profile(id string) (Profile, bool) {
	profile, ok := r.profiles[id]
	if !ok {
		return Profile{}, false
	}
	return cloneProfile(profile), true
}

func (r *Registry) ForConnectorVersion(version string) (Profile, bool) {
	id, ok := r.versions[version]
	if !ok {
		return Profile{}, false
	}
	return r.Profile(id)
}

func (r *Registry) ForConsumerVersion(kind, name, version string) (Profile, bool) {
	id, ok := r.consumers[consumerKey(kind, name, version)]
	if !ok {
		return Profile{}, false
	}
	return r.Profile(id)
}

func (r *Registry) ForClientVersion(name, version string) (Profile, bool) {
	return r.ForConsumerVersion("client", name, version)
}

func (r *Registry) Profiles() []Profile {
	profiles := make([]Profile, 0, len(r.profiles))
	for _, profile := range r.profiles {
		profiles = append(profiles, cloneProfile(profile))
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].ID < profiles[j].ID })
	return profiles
}

func GoldenTraces() ([]Trace, error) {
	paths, err := fs.Glob(assets, "golden/*.json")
	if err != nil {
		return nil, err
	}
	traces := make([]Trace, 0, len(paths))
	for _, path := range paths {
		contents, err := assets.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var trace Trace
		if err := json.Unmarshal(contents, &trace); err != nil {
			return nil, fmt.Errorf("decode golden trace %s: %w", path, err)
		}
		traces = append(traces, trace)
	}
	sort.Slice(traces, func(i, j int) bool {
		if traces[i].ProfileID == traces[j].ProfileID {
			return traces[i].Flow < traces[j].Flow
		}
		return traces[i].ProfileID < traces[j].ProfileID
	})
	return traces, nil
}

func (r *Registry) Validate(trace Trace) error {
	profile, ok := r.Profile(trace.ProfileID)
	if !ok {
		return fmt.Errorf("contract profile %q is not registered", trace.ProfileID)
	}
	versionProfile, version, ok := r.profileForTrace(trace)
	if !ok {
		return newDriftError(trace, "profile_selection", fmt.Sprintf("unsupported consumer version %q", version))
	}
	if versionProfile.ID != profile.ID {
		return newDriftError(trace, "profile_selection", fmt.Sprintf(
			"consumer version %s resolves to profile %s, not %s", version, versionProfile.ID, profile.ID,
		))
	}
	if trace.FixtureKind != "source-derived" {
		return fmt.Errorf("contract flow %s has unsupported fixture kind %q", trace.Flow, trace.FixtureKind)
	}
	if len(trace.SourceRefs) == 0 {
		return fmt.Errorf("contract flow %s has no provenance source", trace.Flow)
	}
	for _, source := range trace.SourceRefs {
		if !strings.HasPrefix(source, "https://") {
			return fmt.Errorf("contract flow %s has non-HTTPS provenance source %q", trace.Flow, source)
		}
	}
	expected, ok := profile.Flows[trace.Flow]
	if !ok {
		return fmt.Errorf("contract profile %s has no flow %q", profile.ID, trace.Flow)
	}
	if len(trace.Events) != len(expected) {
		return newDriftError(trace, "call_count", fmt.Sprintf("expected=%d actual=%d", len(expected), len(trace.Events)))
	}
	for index, call := range expected {
		event := trace.Events[index]
		stage := call.Stage
		if event.Stage != call.Stage {
			return newDriftError(trace, call.Stage, fmt.Sprintf("expected stage %q, actual %q", call.Stage, event.Stage))
		}
		if event.Protocol != call.Protocol || event.Method != call.Method || event.Target != call.Target {
			return newDriftError(trace, stage, wireDiff(call, event))
		}
		for _, field := range call.RequiredFields {
			if _, exists := event.Fields[field]; !exists {
				return newDriftError(trace, stage, fmt.Sprintf("missing required wire field %q", field))
			}
		}
	}
	return nil
}

func (r *Registry) profileForTrace(trace Trace) (Profile, string, bool) {
	if trace.ConsumerKind != "" || trace.ConsumerName != "" || trace.ConsumerVersion != "" {
		identity := consumerKey(trace.ConsumerKind, trace.ConsumerName, trace.ConsumerVersion)
		profile, ok := r.ForConsumerVersion(trace.ConsumerKind, trace.ConsumerName, trace.ConsumerVersion)
		return profile, identity, ok
	}
	profile, ok := r.ForConnectorVersion(trace.ConnectorVersion)
	return profile, consumerKey("connector", "legacy", trace.ConnectorVersion), ok
}

func validateProfile(path string, profile Profile) error {
	if profile.SchemaVersion != "1" || profile.ID == "" || profile.Description == "" ||
		(len(profile.ConnectorVersions) == 0 && len(profile.Consumers) == 0) || len(profile.Capabilities) == 0 ||
		len(profile.Flows) == 0 || len(profile.Sources) == 0 {
		return fmt.Errorf("contract profile %s is incomplete", path)
	}
	validStates := map[CapabilityState]bool{
		CapabilityVerified: true, CapabilityPartial: true, CapabilityRegistered: true, CapabilityPlanned: true,
	}
	for capability, state := range profile.Capabilities {
		if capability == "" || !validStates[state] {
			return fmt.Errorf("contract profile %s has invalid capability %q state %q", path, capability, state)
		}
	}
	for _, consumer := range profile.Consumers {
		if consumer.Kind == "" || consumer.Name == "" || len(consumer.Versions) == 0 {
			return fmt.Errorf("contract profile %s has an incomplete consumer", path)
		}
		for _, version := range consumer.Versions {
			if version == "" {
				return fmt.Errorf("contract profile %s has an empty consumer version", path)
			}
		}
	}
	for flow, calls := range profile.Flows {
		if flow == "" || len(calls) == 0 {
			return fmt.Errorf("contract profile %s has empty flow %q", path, flow)
		}
		stages := make(map[string]struct{}, len(calls))
		for _, call := range calls {
			if call.Stage == "" || call.Protocol == "" || call.Method == "" || call.Target == "" {
				return fmt.Errorf("contract profile %s flow %s has an incomplete call", path, flow)
			}
			if _, exists := stages[call.Stage]; exists {
				return fmt.Errorf("contract profile %s flow %s repeats stage %q", path, flow, call.Stage)
			}
			stages[call.Stage] = struct{}{}
		}
	}
	for _, source := range profile.Sources {
		if !strings.HasPrefix(source, "https://") {
			return fmt.Errorf("contract profile %s has non-HTTPS source %q", path, source)
		}
	}
	return nil
}

func cloneProfile(profile Profile) Profile {
	clone := profile
	clone.ConnectorVersions = append([]string(nil), profile.ConnectorVersions...)
	clone.Consumers = make([]ConsumerSpec, len(profile.Consumers))
	for index, consumer := range profile.Consumers {
		clone.Consumers[index] = consumer
		clone.Consumers[index].Versions = append([]string(nil), consumer.Versions...)
	}
	clone.Sources = append([]string(nil), profile.Sources...)
	clone.Capabilities = make(map[string]CapabilityState, len(profile.Capabilities))
	for capability, state := range profile.Capabilities {
		clone.Capabilities[capability] = state
	}
	clone.Flows = make(map[string][]CallSpec, len(profile.Flows))
	for flow, calls := range profile.Flows {
		clonedCalls := make([]CallSpec, len(calls))
		for index, call := range calls {
			clonedCalls[index] = call
			clonedCalls[index].RequiredFields = append([]string(nil), call.RequiredFields...)
		}
		clone.Flows[flow] = clonedCalls
	}
	return clone
}

func CompareGolden(expected, actual Trace) error {
	if reflect.DeepEqual(expected, actual) {
		return nil
	}
	stage := "trace"
	limit := len(expected.Events)
	if len(actual.Events) < limit {
		limit = len(actual.Events)
	}
	for i := 0; i < limit; i++ {
		if !reflect.DeepEqual(expected.Events[i], actual.Events[i]) {
			stage = expected.Events[i].Stage
			break
		}
	}
	expectedJSON, _ := json.MarshalIndent(expected, "", "  ")
	actualJSON, _ := json.MarshalIndent(actual, "", "  ")
	return newDriftError(actual, stage, lineDiff(string(expectedJSON), string(actualJSON)))
}

func wireDiff(expected CallSpec, actual WireEvent) string {
	expectedWire := map[string]any{"stage": expected.Stage, "protocol": expected.Protocol, "method": expected.Method, "target": expected.Target}
	actualWire := map[string]any{"stage": actual.Stage, "protocol": actual.Protocol, "method": actual.Method, "target": actual.Target}
	expectedJSON, _ := json.MarshalIndent(expectedWire, "", "  ")
	actualJSON, _ := json.MarshalIndent(actualWire, "", "  ")
	return lineDiff(string(expectedJSON), string(actualJSON))
}

func newDriftError(trace Trace, stage, diff string) error {
	encoded, _ := json.Marshal(trace)
	version := trace.ConnectorVersion
	if trace.ConsumerVersion != "" {
		version = consumerKey(trace.ConsumerKind, trace.ConsumerName, trace.ConsumerVersion)
	} else {
		version = consumerKey("connector", "legacy", trace.ConnectorVersion)
	}
	shape := "none"
	for _, event := range trace.Events {
		if event.Stage == stage {
			keys := make([]string, 0, len(event.Fields))
			for key := range event.Fields {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			shape = fmt.Sprintf("%s:%s:%s fields=%s", event.Protocol, event.Method, event.Target, strings.Join(keys, ","))
			break
		}
	}
	return &DriftError{
		Version:     version,
		Operation:   trace.Flow,
		Stage:       stage,
		Shape:       shape,
		Fingerprint: fmt.Sprintf("sha256:%x", sha256.Sum256(encoded)),
		FixHint:     "select the exact consumer profile, inspect the wire diff, then add a source-derived golden fixture",
		Diff:        diff,
	}
}

func lineDiff(expected, actual string) string {
	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")
	var result strings.Builder
	result.WriteString("--- expected\n+++ actual\n")
	maximum := len(expectedLines)
	if len(actualLines) > maximum {
		maximum = len(actualLines)
	}
	for i := 0; i < maximum; i++ {
		var expectedLine, actualLine string
		if i < len(expectedLines) {
			expectedLine = expectedLines[i]
		}
		if i < len(actualLines) {
			actualLine = actualLines[i]
		}
		if expectedLine == actualLine {
			continue
		}
		if expectedLine != "" {
			result.WriteString("- " + expectedLine + "\n")
		}
		if actualLine != "" {
			result.WriteString("+ " + actualLine + "\n")
		}
	}
	return result.String()
}
