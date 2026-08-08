package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

type StatementDestinationDescriptor struct {
	Reference         domain.TableReference
	Exists            bool
	Schema            []domain.Field
	WriteDisposition  domain.WriteDisposition
	CreateDisposition domain.CreateDisposition
}

// StatementDestination owns canonical catalog metadata for query-result
// materialization. It never contains unresolved GoogleSQL relation syntax.
type StatementDestination struct {
	reference         domain.TableReference
	exists            bool
	schema            []domain.Field
	writeDisposition  domain.WriteDisposition
	createDisposition domain.CreateDisposition
}

func NewStatementDestination(descriptor StatementDestinationDescriptor) (StatementDestination, error) {
	if _, err := renderPhysicalTable(descriptor.Reference); err != nil {
		return StatementDestination{}, fmt.Errorf("%w: statement destination reference is invalid", domain.ErrInvalid)
	}
	switch descriptor.WriteDisposition {
	case domain.WriteAppend, domain.WriteEmpty, domain.WriteTruncate:
	default:
		return StatementDestination{}, fmt.Errorf("%w: statement destination write disposition is invalid", domain.ErrInvalid)
	}
	switch descriptor.CreateDisposition {
	case domain.CreateIfNeeded, domain.CreateNever:
	default:
		return StatementDestination{}, fmt.Errorf("%w: statement destination create disposition is invalid", domain.ErrInvalid)
	}
	if descriptor.Exists && len(descriptor.Schema) == 0 {
		return StatementDestination{}, fmt.Errorf("%w: existing statement destination schema is missing", domain.ErrPrecondition)
	}
	if !descriptor.Exists && len(descriptor.Schema) != 0 {
		return StatementDestination{}, fmt.Errorf("%w: missing statement destination cannot have a canonical schema", domain.ErrPrecondition)
	}
	for _, field := range descriptor.Schema {
		if err := field.Validate(); err != nil {
			return StatementDestination{}, fmt.Errorf("%w: statement destination schema is invalid", domain.ErrInvalid)
		}
	}
	return StatementDestination{
		reference:         descriptor.Reference,
		exists:            descriptor.Exists,
		schema:            domain.CloneFields(descriptor.Schema),
		writeDisposition:  descriptor.WriteDisposition,
		createDisposition: descriptor.CreateDisposition,
	}, nil
}

func (destination StatementDestination) Reference() domain.TableReference {
	return destination.reference
}

func (destination StatementDestination) Exists() bool { return destination.exists }

func (destination StatementDestination) Schema() []domain.Field {
	return domain.CloneFields(destination.schema)
}

func (destination StatementDestination) WriteDisposition() domain.WriteDisposition {
	return destination.writeDisposition
}

func (destination StatementDestination) CreateDisposition() domain.CreateDisposition {
	return destination.createDisposition
}

type StatementMaterializationResult struct {
	QueryResult        domain.QueryResult
	DestinationCreated bool
}

var _ ports.StatementMaterializer = (*Warehouse)(nil)

// MaterializeAnalyzedStatement adapts the engine-neutral application request
// to the DuckDB-private destination value before any physical side effect.
func (w *Warehouse) MaterializeAnalyzedStatement(
	ctx context.Context,
	statement semantic.Statement,
	request ports.StatementMaterializationRequest,
) (ports.StatementMaterializationResult, error) {
	destination, err := NewStatementDestination(StatementDestinationDescriptor{
		Reference: request.Destination, Exists: request.DestinationExists,
		Schema: request.DestinationSchema, WriteDisposition: request.WriteDisposition,
		CreateDisposition: request.CreateDisposition,
	})
	if err != nil {
		return ports.StatementMaterializationResult{}, err
	}
	result, err := w.MaterializeStatement(ctx, statement, destination)
	if err != nil {
		return ports.StatementMaterializationResult{}, err
	}
	return ports.StatementMaterializationResult{
		QueryResult: result.QueryResult, DestinationCreated: result.DestinationCreated,
	}, nil
}

// MaterializeStatement evaluates the already analyzed statement once into a
// transaction-local table, then applies the destination disposition in that
// same transaction.
func (w *Warehouse) MaterializeStatement(
	ctx context.Context,
	statement semantic.Statement,
	destination StatementDestination,
) (result StatementMaterializationResult, err error) {
	plan, err := lowerDuckDBStatement(statement)
	if err != nil {
		return result, err
	}
	if !plan.returnsRows() {
		return result, fmt.Errorf("%w: destination requires a row-producing statement", domain.ErrInvalidQuery)
	}
	output, err := newDuckDBStatementOutput(statement, true)
	if err != nil {
		return result, err
	}
	if !destination.exists && destination.createDisposition == domain.CreateNever {
		return result, fmt.Errorf("%w: statement destination does not exist", domain.ErrNotFound)
	}
	destinationName, err := renderPhysicalTable(destination.reference)
	if err != nil {
		return result, fmt.Errorf("%w: statement destination is invalid", domain.ErrInvalid)
	}

	started := observability.LogSideEffectStart(ctx, "duckdb", "materialize_statement",
		"statement_kind", string(statement.Kind()),
		"analysis_fingerprint", plan.semanticAnalysisFingerprint(),
		"destination_fingerprint", statementDestinationFingerprint(destination.reference),
		"write_disposition", destination.writeDisposition,
		"create_disposition", destination.createDisposition,
		"destination_exists", destination.exists,
		"destination_schema_fingerprint", queryDestinationSchemaDigest(destination.schema),
		"bind_count", len(plan.bindArguments()),
		"transaction_mode", "explicit",
	)
	defer func() {
		err = sanitizeDuckDBStatementError(err)
		observability.LogSideEffectEnd(ctx, "duckdb", "materialize_statement", started, err,
			"statement_kind", string(statement.Kind()),
			"analysis_fingerprint", plan.semanticAnalysisFingerprint(),
			"destination_fingerprint", statementDestinationFingerprint(destination.reference),
			"write_disposition", destination.writeDisposition,
			"destination_created", result.DestinationCreated,
			"row_count", len(result.QueryResult.Rows),
			"schema_fingerprint", queryMaterializationSchemaDigest(result.QueryResult.Columns),
			"transaction_mode", "explicit",
		)
	}()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	staging := fmt.Sprintf("__bqemu_statement_result_%d", queryMaterializationSequence.Add(1))
	if err := stageDuckDBStatementPlan(ctx, tx, staging, plan); err != nil {
		return result, err
	}
	queryResult, err := readMaterializedQueryResult(ctx, tx, staging, output.schemaHints())
	if err != nil {
		return result, err
	}
	canonicalOutput, err := canonicalizeDuckDBStatementOutput(queryResult.Columns, output)
	if err != nil {
		return result, err
	}
	if err := alignMaterializedStatementColumnNames(ctx, tx, staging, queryResult.Columns, canonicalOutput); err != nil {
		return result, err
	}
	queryResult.Columns = canonicalOutput
	if destination.exists {
		if err := validateExistingQueryDestinationShape(queryResult.Columns, destination.schema, destination.writeDisposition); err != nil {
			return result, err
		}
		queryResult.Columns = queryResultSchemaFromDestination(destination.schema)
	}
	result.QueryResult = queryResult

	if destination.exists {
		if err := applyExistingQueryDestination(ctx, tx, destinationName, staging, destination.writeDisposition); err != nil {
			return result, err
		}
	} else {
		create := "CREATE TABLE " + destinationName + " AS SELECT * FROM " + quoteIdentifier(staging)
		if _, err := tx.ExecContext(ctx, create); err != nil {
			return result, err
		}
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE "+quoteIdentifier(staging)); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}
	committed = true
	result.DestinationCreated = !destination.exists
	return result, nil
}

func stageDuckDBStatementPlan(
	ctx context.Context,
	tx *sql.Tx,
	staging string,
	plan duckDBStatementPlan,
) error {
	if tx == nil || staging == "" || !plan.returnsRows() {
		return fmt.Errorf("%w: materialization plan is invalid", domain.ErrPrecondition)
	}
	statement := "CREATE TEMP TABLE " + quoteIdentifier(staging) + " AS " + plan.statementSQL()
	_, err := tx.ExecContext(ctx, statement, plan.bindArguments()...)
	return err
}

func alignMaterializedStatementColumnNames(
	ctx context.Context,
	tx *sql.Tx,
	staging string,
	observed, expected []domain.Field,
) error {
	if tx == nil || staging == "" || len(observed) != len(expected) {
		return invalidDuckDBStatementOutput()
	}
	targets := make(map[string]struct{}, len(expected))
	occupied := make(map[string]struct{}, len(observed)+len(expected))
	for _, field := range observed {
		occupied[strings.ToLower(field.Name)] = struct{}{}
	}
	for _, field := range expected {
		key := strings.ToLower(field.Name)
		if key == "" {
			return invalidDuckDBStatementOutput()
		}
		if _, duplicate := targets[key]; duplicate {
			return invalidDuckDBStatementOutput()
		}
		targets[key] = struct{}{}
		occupied[key] = struct{}{}
	}

	type columnRename struct {
		temporary string
		target    string
	}
	sequence := queryMaterializationSequence.Add(1)
	renamed := make([]columnRename, 0, len(expected))
	for index := range expected {
		if observed[index].Name == expected[index].Name {
			continue
		}
		temporary := fmt.Sprintf("__bqemu_output_column_%d_%d", sequence, index)
		for suffix := 1; ; suffix++ {
			if _, collision := occupied[strings.ToLower(temporary)]; !collision {
				break
			}
			temporary = fmt.Sprintf("__bqemu_output_column_%d_%d_%d", sequence, index, suffix)
		}
		occupied[strings.ToLower(temporary)] = struct{}{}
		if _, err := tx.ExecContext(ctx,
			"ALTER TABLE "+quoteIdentifier(staging)+" RENAME COLUMN "+
				quoteIdentifier(observed[index].Name)+" TO "+quoteIdentifier(temporary),
		); err != nil {
			return err
		}
		renamed = append(renamed, columnRename{temporary: temporary, target: expected[index].Name})
	}
	for _, rename := range renamed {
		if _, err := tx.ExecContext(ctx,
			"ALTER TABLE "+quoteIdentifier(staging)+" RENAME COLUMN "+
				quoteIdentifier(rename.temporary)+" TO "+quoteIdentifier(rename.target),
		); err != nil {
			return err
		}
	}
	return nil
}

func statementDestinationFingerprint(reference domain.TableReference) string {
	return observability.Digest([]byte(reference.ProjectID + "\x00" + reference.DatasetID + "\x00" + reference.TableID))
}
