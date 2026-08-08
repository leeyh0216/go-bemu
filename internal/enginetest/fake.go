package enginetest

import (
	"context"
	"errors"
	"strings"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type FakeEngine struct {
	capabilities  engine.Capabilities
	schemaPlanner *engine.SchemaPlanner
	loadPlanner   *loadports.Planner
}

type fakeSchemaAdapter struct{}

func (fakeSchemaAdapter) ValidateSchemaIntent(context.Context, engine.SchemaIntent) error { return nil }

type fakeLoadAdapter struct{ schemaPlanner *engine.SchemaPlanner }

func (adapter fakeLoadAdapter) ValidateLoadRequest(
	_ context.Context,
	request loadports.LoadPlanRequest,
) (string, error) {
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation: engine.SchemaOperationValidate,
		Target: catalogdomain.TableReference{
			ProjectID: request.Destination.Reference.ProjectID,
			DatasetID: request.Destination.Reference.DatasetID,
			TableID:   request.Destination.Reference.TableID,
		},
		AfterSchema: request.Destination.Schema,
	})
	if err != nil {
		return "", classifyFakeSchemaError(err)
	}
	if err := adapter.schemaPlanner.ValidateBinding(request.SchemaPlan, intent); err != nil {
		return "", classifyFakeSchemaError(err)
	}
	if request.SourceFormat != loadDomain.FormatParquet {
		return "", loadports.UnsupportedLoadPlan("LOAD_SOURCE_PARQUET")
	}
	for _, field := range request.Destination.Schema {
		if len(field.Fields) != 0 || strings.EqualFold(field.Mode, "REPEATED") {
			return "", loadports.UnsupportedLoadPlan(loadDomain.CapabilityParquetNestedRepeatedV1)
		}
	}
	return strings.Repeat("f", 64), nil
}

func NewFakeEngine() *FakeEngine {
	identity, err := engine.NewIdentity("fake", "1")
	if err != nil {
		panic(err)
	}
	capabilities, err := engine.NewCapabilities(engine.CapabilitiesDescriptor{
		Identity: identity,
		Decimal: engine.DecimalCapabilities{
			Supported: true, MaxPrecision: catalogdomain.SparkDecimalMaxPrecision, MaxScale: catalogdomain.SparkDecimalMaxScale,
		},
		Composite:          engine.CompositeCapabilities{MaxStructDepth: 15, MaxListDepth: 15},
		Transactions:       map[engine.TransactionScope]bool{engine.TransactionScopeSingleTable: true},
		AtomicReplacements: map[engine.AtomicReplacementScope]bool{engine.AtomicReplacementTable: true},
	})
	if err != nil {
		panic(err)
	}
	schemaPlanner, err := engine.NewSchemaPlanner(capabilities, fakeSchemaAdapter{})
	if err != nil {
		panic(err)
	}
	loadPlanner, err := loadports.NewPlanner(capabilities, fakeLoadAdapter{schemaPlanner: schemaPlanner})
	if err != nil {
		panic(err)
	}
	return &FakeEngine{capabilities: capabilities, schemaPlanner: schemaPlanner, loadPlanner: loadPlanner}
}

func (adapter *FakeEngine) Capabilities() engine.Capabilities { return adapter.capabilities }

func (adapter *FakeEngine) PlanSchema(ctx context.Context, intent engine.SchemaIntent) (engine.SchemaPlan, error) {
	return adapter.schemaPlanner.Plan(ctx, intent)
}

func (adapter *FakeEngine) PlanLoad(ctx context.Context, request loadports.LoadPlanRequest) (loadports.LoadPlan, error) {
	return adapter.loadPlanner.Plan(ctx, request)
}

func classifyFakeSchemaError(err error) error {
	switch {
	case errors.Is(err, catalogdomain.ErrUnsupported):
		return loadports.UnsupportedLoadPlan(catalogdomain.CapabilityEngineSchemaV1)
	case errors.Is(err, catalogdomain.ErrInvalid):
		return loadports.InvalidLoadPlan()
	default:
		return loadports.StaleLoadPlan()
	}
}
