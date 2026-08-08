package enginetest

import (
	"context"
	"errors"
	"strings"
	"testing"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type PlanningAdapter interface {
	Capabilities() engine.Capabilities
	PlanSchema(context.Context, engine.SchemaIntent) (engine.SchemaPlan, error)
	PlanLoad(context.Context, loadports.LoadPlanRequest) (loadports.LoadPlan, error)
}

type Factory func(testing.TB) PlanningAdapter

// RunPlanningConformance fixes the engine-neutral planning behavior every
// storage adapter must satisfy. Adapter-specific physical execution remains in
// each adapter package.
func RunPlanningConformance(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("immutable capabilities and nested schema bounds", func(t *testing.T) {
		adapter := factory(t)
		capabilities := adapter.Capabilities()
		descriptor := capabilities.Descriptor()
		descriptor.Decimal.MaxPrecision = 1
		if adapter.Capabilities().Decimal().MaxPrecision != catalogdomain.SparkDecimalMaxPrecision {
			t.Fatal("adapter exposed mutable capabilities")
		}
		intent := mustSchemaIntent(t, nestedSchema(capabilities.Composite().MaxStructDepth+1))
		if _, err := adapter.PlanSchema(context.Background(), intent); !errors.Is(err, catalogdomain.ErrUnsupported) {
			t.Fatalf("nested schema bound error = %v", err)
		}
	})

	t.Run("foreign schema plan and schema drift", func(t *testing.T) {
		adapter := factory(t)
		foreign := factory(t)
		schema := []catalogdomain.Field{{Name: "id", Type: "INT64"}}
		intent := mustSchemaIntent(t, schema)
		foreignPlan, err := foreign.PlanSchema(context.Background(), intent)
		if err != nil {
			t.Fatal(err)
		}
		request := loadRequest(foreignPlan, schema)
		if _, err := adapter.PlanLoad(context.Background(), request); !errors.Is(err, loadDomain.ErrPrecondition) {
			t.Fatalf("foreign schema plan error = %v", err)
		}

		localPlan, err := adapter.PlanSchema(context.Background(), intent)
		if err != nil {
			t.Fatal(err)
		}
		request = loadRequest(localPlan, []catalogdomain.Field{{Name: "id", Type: "STRING"}})
		if _, err := adapter.PlanLoad(context.Background(), request); !errors.Is(err, loadDomain.ErrPrecondition) {
			t.Fatalf("schema drift error = %v", err)
		}
	})

	t.Run("flat parquet load plan", func(t *testing.T) {
		adapter := factory(t)
		schema := []catalogdomain.Field{{Name: "id", Type: "INT64"}}
		intent := mustSchemaIntent(t, schema)
		schemaPlan, err := adapter.PlanSchema(context.Background(), intent)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := adapter.PlanLoad(context.Background(), loadRequest(schemaPlan, schema))
		if err != nil {
			t.Fatal(err)
		}
		if plan.EngineIdentity() != adapter.Capabilities().Identity() || plan.Fingerprint() == "" ||
			plan.SchemaPlanFingerprint() != schemaPlan.Fingerprint() {
			t.Fatalf("load plan binding = %#v", plan)
		}
		rendered := strings.ToLower(strings.ReplaceAll(plan.RequestFingerprint(), "-", ""))
		if strings.Contains(rendered, "file") || strings.Contains(rendered, "select") {
			t.Fatalf("load request fingerprint exposed a source location or SQL: %s", rendered)
		}
	})
}

func mustSchemaIntent(t testing.TB, schema []catalogdomain.Field) engine.SchemaIntent {
	t.Helper()
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation:   engine.SchemaOperationValidate,
		Target:      catalogdomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		AfterSchema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func nestedSchema(depth int) []catalogdomain.Field {
	field := catalogdomain.Field{Name: "value", Type: "STRING"}
	for index := depth; index > 0; index-- {
		field = catalogdomain.Field{Name: "level_" + strings.Repeat("x", index), Type: "STRUCT", Fields: []catalogdomain.Field{field}}
	}
	return []catalogdomain.Field{field}
}

func loadRequest(schemaPlan engine.SchemaPlan, schema []catalogdomain.Field) loadports.LoadPlanRequest {
	return loadports.LoadPlanRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
			Location:  "US", Schema: schema,
		},
		SchemaPlan: schemaPlan, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteAppend,
		Objects:          []loadports.ResolvedObject{{Fingerprint: strings.Repeat("a", 64), Size: 7}},
	}
}
