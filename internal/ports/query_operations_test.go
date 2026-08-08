package ports

import (
	"errors"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestQueryOperationOwnsSemanticFieldsAndBindsCompleteRequest(t *testing.T) {
	request := QueryRequest{
		ProjectID: "test-project", DefaultProjectID: "data-project", DefaultDataset: "analytics",
		SQL: "DECLARE connector_script DEFAULT 1",
	}
	fields := []string{"id", "event_time"}
	operation, err := NewQueryOperation(QueryOperationDescriptor{
		Kind:              QueryOperationSparkDynamicTimeOverwrite,
		ProfileID:         "spark-bigquery-connector-0.44.2/dynamic-time-partition-overwrite",
		Destination:       domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "destination"},
		Source:            domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "temporary"},
		PartitionFunction: "DATE_TRUNC", PartitionField: "event_time", Granularity: "DAY",
		InsertFields: fields, Request: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	fields[0] = "changed"
	detached := operation.InsertFields()
	detached[0] = "changed-again"
	if got := operation.InsertFields(); len(got) != 2 || got[0] != "id" {
		t.Fatalf("immutable insert fields = %#v", got)
	}
	if err := operation.ValidateBinding(request, operation.ProfileID()); err != nil {
		t.Fatalf("valid binding rejected: %v", err)
	}
	if operation.SQLFingerprint() == request.SQL || operation.RequestFingerprint() == request.SQL ||
		operation.SemanticFingerprint() == request.SQL || operation.BindingFingerprint() == request.SQL ||
		!strings.HasPrefix(operation.SQLFingerprint(), "sha256:") {
		t.Fatal("semantic operation exposed raw SQL instead of digests")
	}
	forgedDescriptor := QueryOperationDescriptor{
		Kind: operation.Kind(), ProfileID: operation.ProfileID(),
		Destination:       operation.Destination(),
		Source:            domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "other_source"},
		PartitionFunction: operation.PartitionFunction(), PartitionField: operation.PartitionField(),
		Granularity: operation.Granularity(), InsertFields: operation.InsertFields(), Request: request,
	}
	forged, err := NewQueryOperation(forgedDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	if forged.RequestFingerprint() != operation.RequestFingerprint() ||
		forged.SemanticFingerprint() == operation.SemanticFingerprint() ||
		forged.BindingFingerprint() == operation.BindingFingerprint() {
		t.Fatal("semantic payload was not included in the operation binding")
	}

	for name, mutate := range map[string]func(*QueryRequest){
		"sql":             func(value *QueryRequest) { value.SQL += " changed" },
		"project":         func(value *QueryRequest) { value.ProjectID = "other-project" },
		"default project": func(value *QueryRequest) { value.DefaultProjectID = "other-project" },
		"default dataset": func(value *QueryRequest) { value.DefaultDataset = "other_dataset" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if err := operation.ValidateRequest(changed); !errors.Is(err, domain.ErrPrecondition) {
				t.Fatalf("changed request binding error = %v", err)
			}
		})
	}
	if err := operation.ValidateBinding(request, "spark-bigquery-connector-0.44.3/profile"); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("changed profile binding error = %v", err)
	}
}

func TestQueryOperationDescriptorsFailClosed(t *testing.T) {
	valid := QueryOperationDescriptor{
		Kind: QueryOperationSparkStaticOverwrite, ProfileID: "spark-bigquery-connector-0.44.2/static-overwrite",
		Destination: domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "destination"},
		Source:      domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "temporary"},
		Request:     QueryRequest{ProjectID: "test-project", SQL: "MERGE connector template"},
	}
	for name, mutate := range map[string]func(*QueryOperationDescriptor){
		"zero": func(value *QueryOperationDescriptor) { *value = QueryOperationDescriptor{} },
		"profile whitespace": func(value *QueryOperationDescriptor) {
			value.ProfileID = " spark-bigquery-connector-0.44.2/static-overwrite"
		},
		"invalid destination": func(value *QueryOperationDescriptor) { value.Destination.TableID = "bad table" },
		"same source":         func(value *QueryOperationDescriptor) { value.Source = value.Destination },
		"empty SQL":           func(value *QueryOperationDescriptor) { value.Request.SQL = " " },
		"static dynamic fields": func(value *QueryOperationDescriptor) {
			value.PartitionFunction = "DATE_TRUNC"
		},
	} {
		t.Run(name, func(t *testing.T) {
			descriptor := valid
			mutate(&descriptor)
			if _, err := NewQueryOperation(descriptor); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("descriptor error = %v, want invalid", err)
			}
		})
	}

	dynamic := valid
	dynamic.Kind = QueryOperationSparkDynamicTimeOverwrite
	dynamic.PartitionFunction = "DATE_TRUNC"
	dynamic.PartitionField = "event_time"
	dynamic.Granularity = "DAY"
	dynamic.InsertFields = []string{"id", "ID"}
	if _, err := NewQueryOperation(dynamic); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("duplicate dynamic fields error = %v", err)
	}
	dynamic.InsertFields = []string{"id", "event_time"}
	dynamic.Granularity = "WEEK"
	if _, err := NewQueryOperation(dynamic); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("unsupported granularity error = %v", err)
	}
	if err := (QueryOperation{}).ValidateRequest(QueryRequest{}); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("zero operation request validation = %v", err)
	}
}
