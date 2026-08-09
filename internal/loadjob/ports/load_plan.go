package ports

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

type ResolvedObject struct {
	Fingerprint string
	Size        int64
}

type LoadPlanRequest struct {
	Destination       domain.Table
	CreateDestination bool
	UpdateDestination bool
	SchemaPlan        engine.SchemaPlan
	SourceFormat      domain.SourceFormat
	WriteDisposition  domain.WriteDisposition
	Objects           []ResolvedObject
}

// LoadAdapterPlanner performs a pure adapter-specific check and returns only a
// digest of its internal representation. SQL, physical type names, paths, and
// source URIs must never be returned.
type LoadAdapterPlanner interface {
	ValidateLoadRequest(context.Context, LoadPlanRequest) (string, error)
}

type loadPlanIssuer struct{ marker byte }

type Planner struct {
	capabilities engine.Capabilities
	adapter      LoadAdapterPlanner
	issuer       *loadPlanIssuer
}

type LoadPlan struct {
	engineIdentity        engine.Identity
	capabilityFingerprint string
	request               LoadPlanRequest
	requestFingerprint    string
	adapterProof          string
	fingerprint           string
	issuer                *loadPlanIssuer
}

func NewPlanner(capabilities engine.Capabilities, adapter LoadAdapterPlanner) (*Planner, error) {
	detached, err := engine.NewCapabilities(capabilities.Descriptor())
	if err != nil || capabilities.Fingerprint() == "" || detached.Fingerprint() != capabilities.Fingerprint() {
		return nil, fmt.Errorf("%w: load planner capabilities are invalid", domain.ErrInvalid)
	}
	if interfaceIsNil(adapter) {
		return nil, fmt.Errorf("%w: load adapter planner is required", domain.ErrInvalid)
	}
	return &Planner{capabilities: detached, adapter: adapter, issuer: &loadPlanIssuer{marker: 1}}, nil
}

func (planner *Planner) Plan(ctx context.Context, request LoadPlanRequest) (LoadPlan, error) {
	if err := planner.validateRuntime(); err != nil {
		return LoadPlan{}, err
	}
	request = cloneLoadPlanRequest(request)
	if err := validateLoadPlanRequest(planner.capabilities, request); err != nil {
		return LoadPlan{}, err
	}
	proof, err := planner.adapter.ValidateLoadRequest(ctx, cloneLoadPlanRequest(request))
	if err != nil {
		return LoadPlan{}, classifyAdapterPlanningError(err)
	}
	if !fingerprintValid(proof) {
		return LoadPlan{}, fmt.Errorf("%w: load adapter proof is invalid", domain.ErrInvalid)
	}
	plan := LoadPlan{
		engineIdentity:        planner.capabilities.Identity(),
		capabilityFingerprint: planner.capabilities.Fingerprint(),
		request:               request, requestFingerprint: loadPlanRequestFingerprint(request),
		adapterProof: proof, issuer: planner.issuer,
	}
	plan.fingerprint = loadPlanFingerprint(plan)
	return plan, nil
}

// ValidateExecution must run before the adapter starts a transaction or opens
// any local artifact. It revalidates adapter representation and exact artifact
// identity/size binding.
func (planner *Planner) ValidateExecution(
	ctx context.Context,
	plan LoadPlan,
	objects []LocalObject,
) (LoadPlanRequest, error) {
	if err := planner.validateRuntime(); err != nil {
		return LoadPlanRequest{}, err
	}
	if plan.fingerprint == "" || plan.fingerprint != loadPlanFingerprint(plan) ||
		plan.requestFingerprint == "" || plan.requestFingerprint != loadPlanRequestFingerprint(plan.request) {
		return LoadPlanRequest{}, fmt.Errorf("%w: load plan fingerprint is invalid", domain.ErrInvalid)
	}
	if plan.engineIdentity != planner.capabilities.Identity() ||
		plan.capabilityFingerprint != planner.capabilities.Fingerprint() || plan.issuer != planner.issuer {
		return LoadPlanRequest{}, fmt.Errorf("%w: load plan does not belong to this engine runtime", domain.ErrPrecondition)
	}
	if len(objects) != len(plan.request.Objects) {
		return LoadPlanRequest{}, fmt.Errorf("%w: local load object count differs from the plan", domain.ErrPrecondition)
	}
	for index, object := range objects {
		planned := plan.request.Objects[index]
		if strings.TrimSpace(object.Path) == "" || object.Fingerprint != planned.Fingerprint || object.Size != planned.Size {
			return LoadPlanRequest{}, fmt.Errorf("%w: local load object differs from the plan", domain.ErrPrecondition)
		}
	}
	proof, err := planner.adapter.ValidateLoadRequest(ctx, cloneLoadPlanRequest(plan.request))
	if err != nil {
		return LoadPlanRequest{}, classifyAdapterPlanningError(err)
	}
	if proof != plan.adapterProof {
		return LoadPlanRequest{}, fmt.Errorf("%w: load adapter representation changed after planning", domain.ErrPrecondition)
	}
	return cloneLoadPlanRequest(plan.request), nil
}

func (planner *Planner) validateRuntime() error {
	if planner == nil || planner.issuer == nil || interfaceIsNil(planner.adapter) {
		return fmt.Errorf("%w: load planner is invalid", domain.ErrPrecondition)
	}
	return nil
}

func (plan LoadPlan) EngineIdentity() engine.Identity { return plan.engineIdentity }
func (plan LoadPlan) SchemaPlanFingerprint() string   { return plan.request.SchemaPlan.Fingerprint() }
func (plan LoadPlan) RequestFingerprint() string      { return plan.requestFingerprint }
func (plan LoadPlan) AdapterProofFingerprint() string { return plan.adapterProof }
func (plan LoadPlan) Fingerprint() string             { return plan.fingerprint }
func (plan LoadPlan) Request() LoadPlanRequest        { return cloneLoadPlanRequest(plan.request) }

type LoadPlanningError struct {
	code       LoadPlanningErrorCode
	capability string
	cause      error
}

type LoadPlanningErrorCode string

const (
	LoadPlanningInvalid      LoadPlanningErrorCode = "LOAD_PLAN_INVALID"
	LoadPlanningUnsupported  LoadPlanningErrorCode = "LOAD_PLAN_UNSUPPORTED"
	LoadPlanningPrecondition LoadPlanningErrorCode = "LOAD_PLAN_PRECONDITION"
)

var capabilityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func UnsupportedLoadPlan(capability string) error {
	if !capabilityPattern.MatchString(capability) {
		capability = "LOAD_ADAPTER_CAPABILITY"
	}
	return &LoadPlanningError{code: LoadPlanningUnsupported, capability: capability}
}

func InvalidLoadPlan() error {
	return &LoadPlanningError{code: LoadPlanningInvalid, capability: "LOAD_INPUT"}
}

func StaleLoadPlan() error {
	return &LoadPlanningError{code: LoadPlanningPrecondition, capability: "LOAD_PLAN_BINDING"}
}

func (err *LoadPlanningError) Error() string {
	if err == nil {
		return "<nil>"
	}
	message := "load planning failed: code=" + string(err.code) + " capability=" + err.capability
	if err.cause != nil {
		message += ": " + err.cause.Error()
	}
	return message
}

func (err *LoadPlanningError) Unwrap() []error {
	if err == nil {
		return nil
	}
	causes := make([]error, 0, 2)
	switch err.code {
	case LoadPlanningUnsupported:
		causes = append(causes, domain.ErrUnsupported)
	case LoadPlanningPrecondition:
		causes = append(causes, domain.ErrPrecondition)
	default:
		causes = append(causes, domain.ErrInvalid)
	}
	if err.cause != nil {
		causes = append(causes, err.cause)
	}
	return causes
}

func validateLoadPlanRequest(capabilities engine.Capabilities, request LoadPlanRequest) error {
	if request.SchemaPlan.Fingerprint() == "" || request.SchemaPlan.EngineIdentity() != capabilities.Identity() ||
		request.SchemaPlan.CapabilityFingerprint() != capabilities.Fingerprint() {
		return fmt.Errorf("%w: schema plan does not belong to the load engine", domain.ErrPrecondition)
	}
	schemaIntent := request.SchemaPlan.Intent()
	expectedTarget := catalogdomain.TableReference{
		ProjectID: request.Destination.Reference.ProjectID,
		DatasetID: request.Destination.Reference.DatasetID,
		TableID:   request.Destination.Reference.TableID,
	}
	if request.CreateDestination && request.UpdateDestination {
		return fmt.Errorf("%w: load destination cannot be created and updated together", domain.ErrInvalid)
	}
	expectedOperation := engine.SchemaOperationValidate
	if request.CreateDestination {
		expectedOperation = engine.SchemaOperationCreate
	} else if request.UpdateDestination {
		expectedOperation = engine.SchemaOperationUpdate
	}
	if schemaIntent.Operation() != expectedOperation || schemaIntent.Target() != expectedTarget ||
		!reflect.DeepEqual(schemaIntent.AfterSchema(), request.Destination.Schema) {
		return fmt.Errorf("%w: schema plan does not match the load destination", domain.ErrPrecondition)
	}
	table := catalogdomain.Table{
		ProjectID:         request.Destination.Reference.ProjectID,
		DatasetID:         request.Destination.Reference.DatasetID,
		ID:                request.Destination.Reference.TableID,
		Schema:            request.Destination.Schema,
		TimePartitioning:  request.Destination.TimePartitioning,
		RangePartitioning: request.Destination.RangePartitioning,
		ClusteringFields:  request.Destination.ClusteringFields,
	}
	if err := table.Validate(); err != nil {
		if errors.Is(err, catalogdomain.ErrUnsupported) {
			return fmt.Errorf("%w: destination schema is unsupported", domain.ErrUnsupported)
		}
		return fmt.Errorf("%w: destination table is invalid", domain.ErrInvalid)
	}
	switch request.SourceFormat {
	case domain.FormatCSV, domain.FormatNewlineDelimitedJSON, domain.FormatAvro,
		domain.FormatParquet, domain.FormatORC:
	default:
		return fmt.Errorf("%w: source format is invalid", domain.ErrInvalid)
	}
	switch request.WriteDisposition {
	case domain.WriteAppend, domain.WriteEmpty, domain.WriteTruncate:
	default:
		return fmt.Errorf("%w: write disposition is invalid", domain.ErrInvalid)
	}
	if !capabilities.SupportsTransaction(engine.TransactionScopeSingleTable) {
		return UnsupportedLoadPlan("LOAD_TRANSACTION_SINGLE_TABLE")
	}
	if request.WriteDisposition == domain.WriteTruncate &&
		!capabilities.SupportsAtomicReplacement(engine.AtomicReplacementTable) {
		return UnsupportedLoadPlan("LOAD_ATOMIC_REPLACEMENT_TABLE")
	}
	if len(request.Objects) == 0 {
		return fmt.Errorf("%w: resolved load objects are required", domain.ErrInvalid)
	}
	seen := make(map[string]struct{}, len(request.Objects))
	for _, object := range request.Objects {
		if !fingerprintValid(object.Fingerprint) || object.Size < 0 {
			return fmt.Errorf("%w: resolved load object descriptor is invalid", domain.ErrInvalid)
		}
		if _, duplicate := seen[object.Fingerprint]; duplicate {
			return fmt.Errorf("%w: resolved load object fingerprint is duplicated", domain.ErrInvalid)
		}
		seen[object.Fingerprint] = struct{}{}
	}
	return nil
}

func classifyAdapterPlanningError(err error) error {
	if err == context.Canceled || err == context.DeadlineExceeded {
		return err
	}
	var planningErr *LoadPlanningError
	if errors.As(err, &planningErr) {
		return planningErr
	}
	return &LoadPlanningError{code: LoadPlanningUnsupported, capability: "LOAD_ADAPTER_CAPABILITY", cause: err}
}

func cloneLoadPlanRequest(input LoadPlanRequest) LoadPlanRequest {
	result := input
	result.Destination = domain.CloneTable(input.Destination)
	result.Objects = append([]ResolvedObject(nil), input.Objects...)
	return result
}

func loadPlanRequestFingerprint(request LoadPlanRequest) string {
	return fingerprint(struct {
		Destination       domain.TableReference            `json:"destination"`
		Location          string                           `json:"location"`
		Schema            []domain.Field                   `json:"schema"`
		TimePartitioning  *catalogdomain.TimePartitioning  `json:"timePartitioning,omitempty"`
		RangePartitioning *catalogdomain.RangePartitioning `json:"rangePartitioning,omitempty"`
		ClusteringFields  []string                         `json:"clusteringFields,omitempty"`
		CreateDestination bool                             `json:"createDestination"`
		UpdateDestination bool                             `json:"updateDestination"`
		SchemaPlan        string                           `json:"schemaPlan"`
		SourceFormat      domain.SourceFormat              `json:"sourceFormat"`
		WriteDisposition  domain.WriteDisposition          `json:"writeDisposition"`
		Objects           []ResolvedObject                 `json:"objects"`
	}{
		Destination: request.Destination.Reference, Location: request.Destination.Location,
		Schema:            request.Destination.Schema,
		TimePartitioning:  request.Destination.TimePartitioning,
		RangePartitioning: request.Destination.RangePartitioning,
		ClusteringFields:  request.Destination.ClusteringFields,
		CreateDestination: request.CreateDestination,
		UpdateDestination: request.UpdateDestination,
		SchemaPlan:        request.SchemaPlan.Fingerprint(),
		SourceFormat:      request.SourceFormat, WriteDisposition: request.WriteDisposition,
		Objects: request.Objects,
	})
}

func loadPlanFingerprint(plan LoadPlan) string {
	return fingerprint(struct {
		EngineIdentity        string `json:"engineIdentity"`
		CapabilityFingerprint string `json:"capabilityFingerprint"`
		RequestFingerprint    string `json:"requestFingerprint"`
		AdapterProof          string `json:"adapterProof"`
	}{
		EngineIdentity:        plan.engineIdentity.ID() + "@" + plan.engineIdentity.Version(),
		CapabilityFingerprint: plan.capabilityFingerprint,
		RequestFingerprint:    plan.requestFingerprint, AdapterProof: plan.adapterProof,
	})
}

func fingerprint(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func fingerprintValid(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func interfaceIsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
