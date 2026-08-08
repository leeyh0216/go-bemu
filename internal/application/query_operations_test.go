package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const testQueryOperationProfile = "test-query-operation-v1"

type recordingOperationAnalyzer struct {
	operation   ports.QueryOperation
	matched     bool
	analyzeErr  error
	verifyErr   error
	analyzeCall int
	verifyCall  int
}

func (analyzer *recordingOperationAnalyzer) AnalyzeQueryOperation(
	context.Context,
	ports.QueryRequest,
) (ports.QueryOperation, bool, error) {
	analyzer.analyzeCall++
	return analyzer.operation, analyzer.matched, analyzer.analyzeErr
}

func (analyzer *recordingOperationAnalyzer) VerifyQueryOperation(
	request ports.QueryRequest,
	operation ports.QueryOperation,
) error {
	analyzer.verifyCall++
	if analyzer.verifyErr != nil {
		return analyzer.verifyErr
	}
	return operation.ValidateBinding(request, testQueryOperationProfile)
}

type recordingOperationExecutor struct {
	calls       int
	request     ports.QueryRequest
	operation   ports.QueryOperation
	destination domain.Table
	source      domain.Table
}

func (executor *recordingOperationExecutor) ExecuteQueryOperation(
	_ context.Context,
	request ports.QueryRequest,
	operation ports.QueryOperation,
	destination domain.Table,
	source domain.Table,
) (domain.QueryResult, error) {
	executor.calls++
	executor.request = request
	executor.operation = operation
	executor.destination = destination
	executor.source = source
	return domain.QueryResult{AffectedRows: 2}, nil
}

type recordingOperationCatalog struct {
	calls       int
	destination domain.Table
	source      domain.Table
}

func (catalog *recordingOperationCatalog) WithCanonicalTables(
	ctx context.Context,
	destination domain.TableReference,
	source domain.TableReference,
	execute func(domain.Table, domain.Table) (domain.QueryResult, error),
) (domain.QueryResult, error) {
	catalog.calls++
	if destination != operationTableReference(catalog.destination) || source != operationTableReference(catalog.source) {
		return domain.QueryResult{}, errors.New("unexpected canonical table references")
	}
	return execute(catalog.destination, catalog.source)
}

func TestQuerySemanticOperationUsesSeparatelyInjectedPorts(t *testing.T) {
	request, operation, destination, source := queryOperationFixture(t)
	analyzer := &recordingOperationAnalyzer{operation: operation, matched: true}
	executor := &recordingOperationExecutor{}
	catalog := &recordingOperationCatalog{destination: destination, source: source}
	generic := &countingQueryEngine{}
	service, err := NewQueryService(
		memory.NewJobRepository(), generic, analyzer, executor, catalog,
		fixedClock{now: time.Unix(1, 0)}, fixedQueryID("semantic"),
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := service.executeQueryWithoutDestination(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", result.AffectedRows)
	}
	if analyzer.analyzeCall != 1 || analyzer.verifyCall != 1 || catalog.calls != 1 || executor.calls != 1 {
		t.Fatalf("semantic port calls = analyze:%d verify:%d catalog:%d execute:%d, want one each",
			analyzer.analyzeCall, analyzer.verifyCall, catalog.calls, executor.calls)
	}
	if generic.calls.Load() != 0 {
		t.Fatalf("semantic operation reached generic query engine %d times", generic.calls.Load())
	}
	if executor.request != request || executor.operation.BindingFingerprint() != operation.BindingFingerprint() ||
		operationTableReference(executor.destination) != operationTableReference(destination) ||
		operationTableReference(executor.source) != operationTableReference(source) {
		t.Fatal("semantic executor did not receive the verified operation and canonical tables")
	}
}

func TestQuerySemanticOperationVerificationFailurePreventsCatalogAndExecution(t *testing.T) {
	request, operation, destination, source := queryOperationFixture(t)
	verificationErr := errors.New("profile proof rejected")
	analyzer := &recordingOperationAnalyzer{operation: operation, matched: true, verifyErr: verificationErr}
	executor := &recordingOperationExecutor{}
	catalog := &recordingOperationCatalog{destination: destination, source: source}
	service, err := NewQueryService(
		memory.NewJobRepository(), &countingQueryEngine{}, analyzer, executor, catalog,
		fixedClock{now: time.Unix(1, 0)}, fixedQueryID("verification"),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.executeQueryWithoutDestination(t.Context(), request)
	if !errors.Is(err, verificationErr) {
		t.Fatalf("verification error = %v, want %v", err, verificationErr)
	}
	if analyzer.verifyCall != 1 || catalog.calls != 0 || executor.calls != 0 {
		t.Fatalf("calls after verification failure = verify:%d catalog:%d execute:%d, want 1/0/0",
			analyzer.verifyCall, catalog.calls, executor.calls)
	}
}

func TestNewQueryServiceRejectsMissingSemanticPorts(t *testing.T) {
	validAnalyzer := &recordingOperationAnalyzer{}
	validExecutor := &recordingOperationExecutor{}
	validCatalog := &recordingOperationCatalog{}
	var typedNilAnalyzer *recordingOperationAnalyzer
	var typedNilExecutor *recordingOperationExecutor
	var typedNilCatalog *recordingOperationCatalog

	for _, test := range []struct {
		name     string
		analyzer ports.QueryOperationAnalyzer
		executor ports.QueryOperationEngine
		catalog  ports.QueryOperationCatalog
	}{
		{name: "nil analyzer", executor: validExecutor, catalog: validCatalog},
		{name: "typed nil analyzer", analyzer: typedNilAnalyzer, executor: validExecutor, catalog: validCatalog},
		{name: "nil executor", analyzer: validAnalyzer, catalog: validCatalog},
		{name: "typed nil executor", analyzer: validAnalyzer, executor: typedNilExecutor, catalog: validCatalog},
		{name: "nil catalog", analyzer: validAnalyzer, executor: validExecutor},
		{name: "typed nil catalog", analyzer: validAnalyzer, executor: validExecutor, catalog: typedNilCatalog},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, err := NewQueryService(
				memory.NewJobRepository(), &countingQueryEngine{}, test.analyzer, test.executor, test.catalog,
				fixedClock{now: time.Unix(1, 0)}, fixedQueryID("missing"),
			)
			if service != nil || !errors.Is(err, domain.ErrPrecondition) {
				t.Fatalf("service=%v error=%v, want nil precondition", service, err)
			}
		})
	}
}

func queryOperationFixture(t *testing.T) (ports.QueryRequest, ports.QueryOperation, domain.Table, domain.Table) {
	t.Helper()
	request := ports.QueryRequest{
		ProjectID: "test-project", DefaultProjectID: "test-project", DefaultDataset: "dataset",
		SQL: "MERGE connector-owned semantic command",
	}
	destination := domain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "destination",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}
	source := domain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "source",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}
	operation, err := ports.NewQueryOperation(ports.QueryOperationDescriptor{
		Kind: ports.QueryOperationSparkStaticOverwrite, ProfileID: testQueryOperationProfile,
		Destination: operationTableReference(destination), Source: operationTableReference(source), Request: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	return request, operation, destination, source
}

func operationTableReference(table domain.Table) domain.TableReference {
	return domain.TableReference{ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID}
}
