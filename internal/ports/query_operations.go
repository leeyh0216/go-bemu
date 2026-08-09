package ports

// Connector-owned query operations stay separate from the generic QueryEngine
// contract. A versioned parser produces this logical value; an engine executes
// it only after the application binds canonical catalog metadata.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

type QueryOperationKind string

const (
	QueryOperationSparkStaticOverwrite      QueryOperationKind = "spark-static-overwrite"
	QueryOperationSparkDynamicTimeOverwrite QueryOperationKind = "spark-dynamic-time-partition-overwrite"
)

type QueryOperationDescriptor struct {
	Kind              QueryOperationKind
	ProfileID         string
	Destination       domain.TableReference
	Source            domain.TableReference
	PartitionFunction string
	PartitionField    string
	Granularity       string
	InsertFields      []string
	Request           QueryRequest
}

// QueryOperation is an immutable semantic command. requestFingerprint binds
// every request field that affected parsing; bindingFingerprint combines the
// request, source profile, and complete semantic payload. No fingerprint
// contains SQL text.
type QueryOperation struct {
	kind                QueryOperationKind
	profileID           string
	destination         domain.TableReference
	source              domain.TableReference
	partitionFunction   string
	partitionField      string
	granularity         string
	insertFields        []string
	semanticFingerprint string
	sqlFingerprint      string
	requestFingerprint  string
	bindingFingerprint  string
}

var queryOperationProfilePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$`)

func NewQueryOperation(descriptor QueryOperationDescriptor) (QueryOperation, error) {
	if descriptor.Kind != QueryOperationSparkStaticOverwrite &&
		descriptor.Kind != QueryOperationSparkDynamicTimeOverwrite {
		return QueryOperation{}, fmt.Errorf("%w: semantic query operation kind is invalid", domain.ErrInvalid)
	}
	if !queryOperationProfilePattern.MatchString(descriptor.ProfileID) {
		return QueryOperation{}, fmt.Errorf("%w: semantic query operation profile is invalid", domain.ErrInvalid)
	}
	if err := validateOperationReference(descriptor.Destination); err != nil {
		return QueryOperation{}, fmt.Errorf("%w: semantic destination reference is invalid", domain.ErrInvalid)
	}
	if err := validateOperationReference(descriptor.Source); err != nil {
		return QueryOperation{}, fmt.Errorf("%w: semantic source reference is invalid", domain.ErrInvalid)
	}
	if descriptor.Destination == descriptor.Source {
		return QueryOperation{}, fmt.Errorf("%w: semantic source and destination must differ", domain.ErrInvalid)
	}
	if strings.TrimSpace(descriptor.Request.SQL) == "" {
		return QueryOperation{}, fmt.Errorf("%w: semantic query operation SQL is empty", domain.ErrInvalid)
	}
	if descriptor.Kind == QueryOperationSparkStaticOverwrite {
		if descriptor.PartitionFunction != "" || descriptor.PartitionField != "" || descriptor.Granularity != "" ||
			len(descriptor.InsertFields) != 0 {
			return QueryOperation{}, fmt.Errorf("%w: static overwrite cannot carry dynamic partition fields", domain.ErrInvalid)
		}
	} else {
		if descriptor.PartitionFunction == "" || descriptor.PartitionField == "" || descriptor.Granularity == "" ||
			len(descriptor.InsertFields) == 0 {
			return QueryOperation{}, fmt.Errorf("%w: dynamic overwrite requires partition and insert fields", domain.ErrInvalid)
		}
		if descriptor.PartitionFunction != "DATE_TRUNC" && descriptor.PartitionFunction != "TIMESTAMP_TRUNC" {
			return QueryOperation{}, fmt.Errorf("%w: dynamic overwrite partition function is invalid", domain.ErrInvalid)
		}
		switch descriptor.Granularity {
		case "HOUR", "DAY", "MONTH", "YEAR":
		default:
			return QueryOperation{}, fmt.Errorf("%w: dynamic overwrite granularity is invalid", domain.ErrInvalid)
		}
		if err := validateOperationFieldName(descriptor.PartitionField); err != nil {
			return QueryOperation{}, fmt.Errorf("%w: dynamic overwrite partition field is invalid", domain.ErrInvalid)
		}
		seen := make(map[string]struct{}, len(descriptor.InsertFields))
		for _, field := range descriptor.InsertFields {
			if err := validateOperationFieldName(field); err != nil {
				return QueryOperation{}, fmt.Errorf("%w: dynamic overwrite insert field is invalid", domain.ErrInvalid)
			}
			key := strings.ToLower(field)
			if _, exists := seen[key]; exists {
				return QueryOperation{}, fmt.Errorf("%w: dynamic overwrite insert field is duplicated", domain.ErrInvalid)
			}
			seen[key] = struct{}{}
		}
	}
	insertFields := append([]string(nil), descriptor.InsertFields...)
	semanticFingerprint := queryOperationSemanticFingerprint(
		descriptor.Kind, descriptor.Destination, descriptor.Source,
		descriptor.PartitionFunction, descriptor.PartitionField, descriptor.Granularity, insertFields,
	)
	requestFingerprint := queryOperationRequestFingerprint(descriptor.Request)
	operation := QueryOperation{
		kind: descriptor.Kind, profileID: descriptor.ProfileID,
		destination: descriptor.Destination, source: descriptor.Source,
		partitionFunction: descriptor.PartitionFunction, partitionField: descriptor.PartitionField,
		granularity: descriptor.Granularity, insertFields: insertFields,
		semanticFingerprint: semanticFingerprint,
		sqlFingerprint:      digestOperationBytes([]byte(descriptor.Request.SQL)), requestFingerprint: requestFingerprint,
	}
	operation.bindingFingerprint = queryOperationBindingFingerprint(
		operation.profileID, requestFingerprint, semanticFingerprint,
	)
	return operation, nil
}

func (operation QueryOperation) Kind() QueryOperationKind           { return operation.kind }
func (operation QueryOperation) ProfileID() string                  { return operation.profileID }
func (operation QueryOperation) Destination() domain.TableReference { return operation.destination }
func (operation QueryOperation) Source() domain.TableReference      { return operation.source }
func (operation QueryOperation) PartitionFunction() string          { return operation.partitionFunction }
func (operation QueryOperation) PartitionField() string             { return operation.partitionField }
func (operation QueryOperation) Granularity() string                { return operation.granularity }
func (operation QueryOperation) InsertFields() []string {
	return append([]string(nil), operation.insertFields...)
}
func (operation QueryOperation) SemanticFingerprint() string { return operation.semanticFingerprint }
func (operation QueryOperation) SQLFingerprint() string      { return operation.sqlFingerprint }
func (operation QueryOperation) RequestFingerprint() string  { return operation.requestFingerprint }
func (operation QueryOperation) BindingFingerprint() string  { return operation.bindingFingerprint }

// ValidateBinding proves that neither the raw request nor the source-pinned
// parser profile changed between analysis and execution.
func (operation QueryOperation) ValidateBinding(request QueryRequest, expectedProfileID string) error {
	if operation.profileID == "" || operation.profileID != expectedProfileID {
		return fmt.Errorf("%w: semantic operation profile binding changed", domain.ErrPrecondition)
	}
	requestFingerprint := queryOperationRequestFingerprint(request)
	if operation.sqlFingerprint != digestOperationBytes([]byte(request.SQL)) ||
		operation.requestFingerprint != requestFingerprint ||
		operation.semanticFingerprint == "" ||
		operation.bindingFingerprint != queryOperationBindingFingerprint(
			expectedProfileID, requestFingerprint, operation.semanticFingerprint,
		) {
		return fmt.Errorf("%w: semantic operation request binding changed", domain.ErrPrecondition)
	}
	return nil
}

// ValidateRequest is the engine-side replay/TOCTOU check. It verifies the raw
// request against the immutable profile-bound fingerprint without requiring an
// engine adapter to know which connector version produced the operation.
func (operation QueryOperation) ValidateRequest(request QueryRequest) error {
	return operation.ValidateBinding(request, operation.profileID)
}

// QueryOperationAnalyzer recognizes connector-specific semantic operations and
// owns verification of its versioned profile proof. Implementations that admit
// public SQL must rebuild the proof through the configured GoogleSQL gateway,
// never through a connector-local parser. matched=false delegates to the
// generic query path.
type QueryOperationAnalyzer interface {
	AnalyzeQueryOperation(context.Context, QueryRequest) (operation QueryOperation, matched bool, err error)
	VerifyQueryOperation(QueryRequest, QueryOperation) error
}

// AnalyzedQueryOperationAnalyzer recognizes a connector-owned operation from
// the immutable GoogleSQL statement produced by the single gateway. It is the
// production path: implementations must inspect the owned AST and semantic
// bindings, never tokenize or parse request.SQL.
type AnalyzedQueryOperationAnalyzer interface {
	AnalyzeStatementOperation(context.Context, semantic.Statement, QueryRequest) (operation QueryOperation, matched bool, err error)
}

// QueryOperationEngine receives a verified semantic operation and canonical
// metadata. Request is supplied only for digest verification; implementations
// must not parse or execute its SQL.
type QueryOperationEngine interface {
	ExecuteQueryOperation(context.Context, QueryRequest, QueryOperation, domain.Table, domain.Table) (domain.QueryResult, error)
}

// QueryOperationCatalog keeps canonical metadata stable across the complete
// backend transaction.
type QueryOperationCatalog interface {
	WithCanonicalTables(
		context.Context,
		domain.TableReference,
		domain.TableReference,
		func(destination domain.Table, source domain.Table) (domain.QueryResult, error),
	) (domain.QueryResult, error)
}

func validateOperationReference(reference domain.TableReference) error {
	return (domain.Table{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID, ID: reference.TableID,
		Schema: []domain.Field{{Name: "placeholder", Type: "STRING"}},
	}).Validate()
}

func validateOperationFieldName(name string) error {
	return (domain.Field{Name: name, Type: "STRING"}).Validate()
}

func queryOperationRequestFingerprint(request QueryRequest) string {
	document := struct {
		ProjectID        string `json:"projectId"`
		DefaultProjectID string `json:"defaultProjectId"`
		DefaultDataset   string `json:"defaultDataset"`
		SQL              string `json:"sql"`
	}{
		ProjectID: request.ProjectID, DefaultProjectID: request.DefaultProjectID,
		DefaultDataset: request.DefaultDataset, SQL: request.SQL,
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(fmt.Sprintf("marshal query operation request fingerprint: %v", err))
	}
	return digestOperationBytes(encoded)
}

func queryOperationSemanticFingerprint(
	kind QueryOperationKind,
	destination domain.TableReference,
	source domain.TableReference,
	partitionFunction string,
	partitionField string,
	granularity string,
	insertFields []string,
) string {
	document := struct {
		Kind              QueryOperationKind    `json:"kind"`
		Destination       domain.TableReference `json:"destination"`
		Source            domain.TableReference `json:"source"`
		PartitionFunction string                `json:"partitionFunction"`
		PartitionField    string                `json:"partitionField"`
		Granularity       string                `json:"granularity"`
		InsertFields      []string              `json:"insertFields"`
	}{
		Kind: kind, Destination: destination, Source: source,
		PartitionFunction: partitionFunction, PartitionField: partitionField,
		Granularity: granularity, InsertFields: insertFields,
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(fmt.Sprintf("marshal query operation semantic fingerprint: %v", err))
	}
	return digestOperationBytes(encoded)
}

func queryOperationBindingFingerprint(profileID, requestFingerprint, semanticFingerprint string) string {
	return digestOperationBytes([]byte(profileID + "\x00" + requestFingerprint + "\x00" + semanticFingerprint))
}

func digestOperationBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}
