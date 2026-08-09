package duckdb

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

type duckDBLoadAdapterPlanner struct {
	schemaPlanner *engine.SchemaPlanner
}

func (planner duckDBLoadAdapterPlanner) ValidateLoadRequest(
	ctx context.Context,
	request loadports.LoadPlanRequest,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if planner.schemaPlanner == nil {
		return "", loadports.StaleLoadPlan()
	}
	operation := engine.SchemaOperationValidate
	if request.CreateDestination {
		operation = engine.SchemaOperationCreate
	} else if request.UpdateDestination {
		operation = engine.SchemaOperationUpdate
	}
	plannedIntent := request.SchemaPlan.Intent()
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation: operation,
		Target: catalogdomain.TableReference{
			ProjectID: request.Destination.Reference.ProjectID,
			DatasetID: request.Destination.Reference.DatasetID,
			TableID:   request.Destination.Reference.TableID,
		},
		BeforeSchema: plannedIntent.BeforeSchema(), AfterSchema: request.Destination.Schema,
		Additions: plannedIntent.Additions(), Relaxations: plannedIntent.Relaxations(),
	})
	if err != nil {
		return "", classifyLoadSchemaPlanningError(err)
	}
	if err := planner.schemaPlanner.ValidateBinding(request.SchemaPlan, intent); err != nil {
		return "", classifyLoadSchemaPlanningError(err)
	}
	if request.SourceFormat != loadDomain.FormatParquet {
		return "", loadports.UnsupportedLoadPlan("LOAD_SOURCE_PARQUET")
	}
	physicalTypes := make([]string, 0, len(request.Destination.Schema))
	for _, field := range request.Destination.Schema {
		physicalType, err := duckDBType(field)
		if err != nil {
			return "", classifyLoadSchemaPlanningError(err)
		}
		physicalTypes = append(physicalTypes, physicalType)
	}
	document, err := json.Marshal(struct {
		ModelVersion      string                         `json:"modelVersion"`
		CreateDestination bool                           `json:"createDestination"`
		UpdateDestination bool                           `json:"updateDestination"`
		PhysicalTypes     []string                       `json:"physicalTypes"`
		SourceFormat      loadDomain.SourceFormat        `json:"sourceFormat"`
		WriteDisposition  loadDomain.WriteDisposition    `json:"writeDisposition"`
		Partition         *loadDomain.PartitionDecorator `json:"partition,omitempty"`
	}{
		ModelVersion: "duckdb-parquet-load-plan-v4", PhysicalTypes: physicalTypes,
		CreateDestination: request.CreateDestination,
		UpdateDestination: request.UpdateDestination,
		SourceFormat:      request.SourceFormat, WriteDisposition: request.WriteDisposition,
		Partition: request.Partition,
	})
	if err != nil {
		return "", loadports.InvalidLoadPlan()
	}
	return strings.TrimPrefix(observability.Digest(document), "sha256:"), nil
}

func classifyLoadSchemaPlanningError(err error) error {
	switch {
	case errors.Is(err, catalogdomain.ErrUnsupported):
		return loadports.UnsupportedLoadPlan(catalogdomain.CapabilityEngineSchemaV1)
	case errors.Is(err, catalogdomain.ErrInvalid):
		return loadports.InvalidLoadPlan()
	default:
		return loadports.StaleLoadPlan()
	}
}
