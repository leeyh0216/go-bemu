package ports

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

type fakeLoadAdapterPlanner struct {
	proof string
	err   error
	calls int
}

func (adapter *fakeLoadAdapterPlanner) ValidateLoadRequest(context.Context, LoadPlanRequest) (string, error) {
	adapter.calls++
	return adapter.proof, adapter.err
}

type fakeSchemaPlanner struct{}

func (fakeSchemaPlanner) ValidateSchemaIntent(context.Context, engine.SchemaIntent) error { return nil }

func TestLoadPlanIsImmutableAndContainsNoArtifactLocation(t *testing.T) {
	precision, scale := int64(20), int64(2)
	schema := []domain.Field{{Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale}}
	capabilities, schemaPlan := testLoadSchemaPlan(t, schema)
	adapter := &fakeLoadAdapterPlanner{proof: strings.Repeat("a", 64)}
	planner, err := NewPlanner(capabilities, adapter)
	if err != nil {
		t.Fatal(err)
	}
	request := testLoadPlanRequest(schemaPlan, schema)
	request.Destination.TimePartitioning = &catalogdomain.TimePartitioning{Type: "DAY", ExpirationMs: 86_400_000}
	request.Destination.ClusteringFields = []string{"amount"}
	plan, err := planner.Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	precision = 1
	request.Destination.Schema[0].Name = "changed"
	request.Destination.TimePartitioning.ExpirationMs = 1
	request.Destination.ClusteringFields[0] = "changed"
	request.Objects[0].Fingerprint = strings.Repeat("c", 64)
	detached := plan.Request()
	detached.Destination.Schema[0].Name = "also_changed"
	detached.Destination.TimePartitioning.Type = "HOUR"
	detached.Destination.ClusteringFields[0] = "also_changed"
	detached.Objects[0].Size = 999

	planned := plan.Request()
	if planned.Destination.Schema[0].Name != "amount" || *planned.Destination.Schema[0].Precision != 20 ||
		planned.Destination.TimePartitioning.ExpirationMs != 86_400_000 || planned.Destination.ClusteringFields[0] != "amount" ||
		planned.Objects[0].Fingerprint != strings.Repeat("b", 64) || planned.Objects[0].Size != 7 {
		t.Fatalf("load plan retained mutable input: %#v", planned)
	}
	if plan.EngineIdentity() != capabilities.Identity() || plan.Fingerprint() == "" ||
		plan.SchemaPlanFingerprint() != schemaPlan.Fingerprint() {
		t.Fatalf("load plan binding = %#v", plan)
	}
	rendered := fmt.Sprintf("%#v", plan)
	if strings.Contains(rendered, "file://") || strings.Contains(rendered, "/tmp/") || strings.Contains(rendered, "SELECT ") {
		t.Fatalf("load plan contains a source location or SQL: %s", rendered)
	}
}

func TestLoadPlanRejectsForeignPlannerAndArtifactDrift(t *testing.T) {
	schema := []domain.Field{{Name: "id", Type: "INT64"}}
	capabilities, schemaPlan := testLoadSchemaPlan(t, schema)
	adapter := &fakeLoadAdapterPlanner{proof: strings.Repeat("a", 64)}
	planner, err := NewPlanner(capabilities, adapter)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planner.Plan(context.Background(), testLoadPlanRequest(schemaPlan, schema))
	if err != nil {
		t.Fatal(err)
	}
	object := LocalObject{Path: "/tmp/source.parquet", Fingerprint: strings.Repeat("b", 64), Size: 7}
	if _, err := planner.ValidateExecution(context.Background(), plan, []LocalObject{object}); err != nil {
		t.Fatal(err)
	}

	foreign, err := NewPlanner(capabilities, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.ValidateExecution(context.Background(), plan, []LocalObject{object}); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("foreign planner error = %v", err)
	}
	changedFingerprint := object
	changedFingerprint.Fingerprint = strings.Repeat("c", 64)
	if _, err := planner.ValidateExecution(context.Background(), plan, []LocalObject{changedFingerprint}); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("artifact fingerprint drift error = %v", err)
	}
	changedSize := object
	changedSize.Size++
	if _, err := planner.ValidateExecution(context.Background(), plan, []LocalObject{changedSize}); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("artifact size drift error = %v", err)
	}

	adapter.proof = strings.Repeat("d", 64)
	if _, err := planner.ValidateExecution(context.Background(), plan, []LocalObject{object}); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("adapter proof drift error = %v", err)
	}
}

func TestLoadPlannerRejectsInvalidInputBeforeAdapterAndRetainsRawCause(t *testing.T) {
	schema := []domain.Field{{Name: "id", Type: "INT64"}}
	capabilities, schemaPlan := testLoadSchemaPlan(t, schema)
	const secret = "duckdb_sql_secret_marker"
	adapter := &fakeLoadAdapterPlanner{proof: strings.Repeat("a", 64)}
	planner, err := NewPlanner(capabilities, adapter)
	if err != nil {
		t.Fatal(err)
	}
	invalid := testLoadPlanRequest(schemaPlan, schema)
	invalid.Objects[0].Fingerprint = "invalid"
	if _, err := planner.Plan(context.Background(), invalid); !errors.Is(err, domain.ErrInvalid) || adapter.calls != 0 {
		t.Fatalf("invalid request error=%v adapter_calls=%d", err, adapter.calls)
	}

	adapter.err = errors.New(secret)
	_, err = planner.Plan(context.Background(), testLoadPlanRequest(schemaPlan, schema))
	if !errors.Is(err, domain.ErrUnsupported) || !strings.Contains(fmt.Sprint(err), secret) {
		t.Fatalf("raw adapter error was omitted: %v", err)
	}
	unsafe := UnsupportedLoadPlan(secret + " with spaces")
	if strings.Contains(unsafe.Error(), secret) {
		t.Fatalf("unsafe capability marker leaked: %v", unsafe)
	}
}

func testLoadSchemaPlan(t *testing.T, schema []domain.Field) (engine.Capabilities, engine.SchemaPlan) {
	t.Helper()
	identity, err := engine.NewIdentity("load-test", "1")
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := engine.NewCapabilities(engine.CapabilitiesDescriptor{
		Identity:  identity,
		Decimal:   engine.DecimalCapabilities{Supported: true, MaxPrecision: 38, MaxScale: 38},
		Composite: engine.CompositeCapabilities{MaxStructDepth: 15, MaxListDepth: 15},
		Transactions: map[engine.TransactionScope]bool{
			engine.TransactionScopeSingleTable: true,
		},
		AtomicReplacements: map[engine.AtomicReplacementScope]bool{
			engine.AtomicReplacementTable: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	schemaPlanner, err := engine.NewSchemaPlanner(capabilities, fakeSchemaPlanner{})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation:   engine.SchemaOperationValidate,
		Target:      catalogdomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		AfterSchema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := schemaPlanner.Plan(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	return capabilities, plan
}

func testLoadPlanRequest(schemaPlan engine.SchemaPlan, schema []domain.Field) LoadPlanRequest {
	return LoadPlanRequest{
		Destination: domain.Table{
			Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
			Location:  "US", Schema: schema,
		},
		SchemaPlan: schemaPlan, SourceFormat: domain.FormatParquet, WriteDisposition: domain.WriteAppend,
		Objects: []ResolvedObject{{Fingerprint: strings.Repeat("b", 64), Size: 7}},
	}
}
