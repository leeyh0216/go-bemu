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
		if len(field.Fields) != 0 || strings.EqualFold(field.Mode, "REPEATED") {
			return "", loadports.UnsupportedLoadPlan(loadDomain.CapabilityParquetNestedRepeatedV1)
		}
		physicalType, err := duckDBType(field)
		if err != nil {
			return "", classifyLoadSchemaPlanningError(err)
		}
		physicalTypes = append(physicalTypes, physicalType)
	}
	document, err := json.Marshal(struct {
		ModelVersion     string                      `json:"modelVersion"`
		PhysicalTypes    []string                    `json:"physicalTypes"`
		SourceFormat     loadDomain.SourceFormat     `json:"sourceFormat"`
		WriteDisposition loadDomain.WriteDisposition `json:"writeDisposition"`
	}{
		ModelVersion: "duckdb-parquet-load-plan-v1", PhysicalTypes: physicalTypes,
		SourceFormat: request.SourceFormat, WriteDisposition: request.WriteDisposition,
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
