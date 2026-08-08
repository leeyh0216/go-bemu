package duckdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

// executeStaticOverwrite applies the typed constant-false MERGE operation. The
// request SQL is used only for diagnostics after ExecuteQueryOperation has
// verified its digest; this adapter never reparses or executes the request text.
func (w *Warehouse) executeStaticOverwrite(
	ctx context.Context,
	request ports.QueryRequest,
	operation ports.QueryOperation,
	destination domain.Table,
	source domain.Table,
) (result domain.QueryResult, err error) {
	if err := validateStaticOverwriteTables(operation, destination, source); err != nil {
		return result, err
	}
	destinationReference := operation.Destination()
	sourceReference := operation.Source()
	statement := "MERGE INTO " + dynamicOverwriteRelation(destinationReference) +
		" USING (SELECT * FROM " + dynamicOverwriteRelation(sourceReference) + ") AS " +
		quoteIdentifier("__bqemu_static_source") +
		" ON FALSE WHEN NOT MATCHED THEN INSERT BY NAME WHEN NOT MATCHED BY SOURCE THEN DELETE"
	attrs := []any{
		"semantic_binding_fingerprint", operation.BindingFingerprint(), "query_bytes", len(request.SQL),
		"query_digest", operation.SQLFingerprint(), "transaction_mode", "atomic_statement",
		"destination_reference_fingerprint", observability.Digest([]byte(
			destinationReference.ProjectID + "\x00" + destinationReference.DatasetID + "\x00" + destinationReference.TableID,
		)),
		"source_reference_fingerprint", observability.Digest([]byte(
			sourceReference.ProjectID + "\x00" + sourceReference.DatasetID + "\x00" + sourceReference.TableID,
		)),
		"destination_schema_fingerprint", queryDestinationSchemaDigest(destination.Schema),
		"source_schema_fingerprint", queryDestinationSchemaDigest(source.Schema),
	}
	started := observability.LogSideEffectStart(ctx, "duckdb", "static_overwrite", attrs...)
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "static_overwrite", started, err,
			"affected_rows", result.AffectedRows, "transaction_mode", "atomic_statement")
	}()
	execution, err := w.db.ExecContext(ctx, statement)
	if err != nil {
		return result, classifyDynamicOverwriteQueryError("execute static overwrite", err)
	}
	result.AffectedRows, err = execution.RowsAffected()
	if err != nil {
		return domain.QueryResult{}, classifyDynamicOverwriteBackendError("read static overwrite row count", err)
	}
	return result, nil
}

func validateStaticOverwriteTables(operation ports.QueryOperation, destination, source domain.Table) error {
	if operation.Kind() != ports.QueryOperationSparkStaticOverwrite {
		return fmt.Errorf("%w: unknown static overwrite semantic operation", domain.ErrPrecondition)
	}
	if err := destination.Validate(); err != nil {
		return fmt.Errorf("%w: canonical destination metadata is invalid: %v", domain.ErrPrecondition, err)
	}
	if err := source.Validate(); err != nil {
		return fmt.Errorf("%w: canonical source metadata is invalid: %v", domain.ErrPrecondition, err)
	}
	destinationReference := operation.Destination()
	if destination.ProjectID != destinationReference.ProjectID || destination.DatasetID != destinationReference.DatasetID ||
		destination.ID != destinationReference.TableID {
		return fmt.Errorf("%w: canonical destination does not match semantic overwrite target", domain.ErrPrecondition)
	}
	sourceReference := operation.Source()
	if source.ProjectID != sourceReference.ProjectID || source.DatasetID != sourceReference.DatasetID ||
		source.ID != sourceReference.TableID {
		return fmt.Errorf("%w: canonical source does not match semantic overwrite source", domain.ErrPrecondition)
	}
	if len(destination.Schema) != len(source.Schema) {
		return fmt.Errorf("%w: static overwrite source and destination field counts differ", domain.ErrInvalidQuery)
	}
	for index := range destination.Schema {
		if !sameConnectorOverwriteFieldShape(destination.Schema[index], source.Schema[index]) {
			return fmt.Errorf("%w: static overwrite field shapes differ; field_index=%d", domain.ErrInvalidQuery, index)
		}
	}
	return nil
}

func sameConnectorOverwriteFieldShape(destination, source domain.Field) bool {
	if !strings.EqualFold(destination.Name, source.Name) ||
		canonicalDynamicOverwriteType(destination.Type) != canonicalDynamicOverwriteType(source.Type) ||
		canonicalDynamicOverwriteMode(destination.Mode) != canonicalDynamicOverwriteMode(source.Mode) ||
		len(destination.Fields) != len(source.Fields) {
		return false
	}
	fieldType := canonicalDynamicOverwriteType(destination.Type)
	if fieldType == "NUMERIC" || fieldType == "BIGNUMERIC" {
		destinationDecimal, destinationErr := destination.EffectiveDecimalParameters()
		sourceDecimal, sourceErr := source.EffectiveDecimalParameters()
		destinationRounding, destinationRoundingErr := destination.EffectiveRoundingMode()
		sourceRounding, sourceRoundingErr := source.EffectiveRoundingMode()
		if destinationErr != nil || sourceErr != nil || destinationRoundingErr != nil || sourceRoundingErr != nil ||
			destinationDecimal != sourceDecimal || destinationRounding != sourceRounding {
			return false
		}
	}
	for index := range destination.Fields {
		if !sameConnectorOverwriteFieldShape(destination.Fields[index], source.Fields[index]) {
			return false
		}
	}
	return true
}
