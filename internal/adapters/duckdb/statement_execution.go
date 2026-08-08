package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

const (
	duckDBStatementBackendFailureV1      = "query.googlesql.duckdb-execution.backend-failure-v1"
	duckDBStatementInvalidFailureV1      = "query.googlesql.duckdb-execution.invalid-v1"
	duckDBStatementUnsupportedFailureV1  = "query.googlesql.duckdb-execution.unsupported-v1"
	duckDBStatementPreconditionFailureV1 = "query.googlesql.duckdb-execution.precondition-v1"
	duckDBStatementConflictFailureV1     = "query.googlesql.duckdb-execution.conflict-v1"
	duckDBStatementNotFoundFailureV1     = "query.googlesql.duckdb-execution.not-found-v1"
	duckDBMergeSourceCardinalityV1       = "query.googlesql.merge-source-cardinality-v1"
)

type duckDBStatementRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type duckDBStatementContractError struct {
	kind   error
	code   string
	detail string
}

func (failure *duckDBStatementContractError) Error() string {
	return fmt.Sprintf("%v: code=%s: %s", failure.kind, failure.code, failure.detail)
}

func (failure *duckDBStatementContractError) Unwrap() error { return failure.kind }

// ExecuteStatement lowers an analyzed GoogleSQL statement exactly once and
// sends only the adapter-private DuckDB plan to the backend.
func (w *Warehouse) ExecuteStatement(
	ctx context.Context,
	statement semantic.Statement,
) (result domain.QueryResult, err error) {
	if statement.Kind() == queryast.StatementScript {
		result, err := w.executeGoogleSQLScript(ctx, statement)
		return result, classifyDuckDBStatementError(err)
	}
	plan, err := lowerDuckDBStatement(statement)
	if err != nil {
		return domain.QueryResult{}, err
	}
	output, err := newDuckDBStatementOutput(statement, plan.returnsRows())
	if err != nil {
		return domain.QueryResult{}, err
	}

	transactionMode := "autocommit"
	if plan.requiresTransaction() {
		transactionMode = "explicit"
	}
	started := observability.LogSideEffectStart(ctx, "duckdb", "execute_statement",
		"statement_kind", string(statement.Kind()),
		"analysis_fingerprint", plan.semanticAnalysisFingerprint(),
		"bind_count", len(plan.bindArguments()),
		"transaction_mode", transactionMode,
	)
	defer func() {
		err = classifyDuckDBStatementError(err)
		observability.LogSideEffectEnd(ctx, "duckdb", "execute_statement", started, err,
			"statement_kind", string(statement.Kind()),
			"analysis_fingerprint", plan.semanticAnalysisFingerprint(),
			"row_count", len(result.Rows),
			"affected_rows", result.AffectedRows,
			"schema_fingerprint", queryMaterializationSchemaDigest(result.Columns),
			"transaction_mode", transactionMode,
		)
	}()

	if !plan.requiresTransaction() {
		return executeDuckDBStatementPlan(ctx, w.db, plan, output)
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.QueryResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	result, err = executeDuckDBStatementPlan(ctx, tx, plan, output)
	if err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}
	committed = true
	return result, nil
}

func executeDuckDBStatementPlan(
	ctx context.Context,
	runner duckDBStatementRunner,
	plan duckDBStatementPlan,
	output duckDBStatementOutput,
) (domain.QueryResult, error) {
	if runner == nil {
		return domain.QueryResult{}, fmt.Errorf("%w: DuckDB statement runner is missing", domain.ErrPrecondition)
	}
	if err := validateDuckDBStatementPreconditions(ctx, runner, plan); err != nil {
		return domain.QueryResult{}, err
	}
	arguments := plan.bindArguments()
	if !plan.returnsRows() {
		execution, err := runner.ExecContext(ctx, plan.statementSQL(), arguments...)
		if err != nil {
			return domain.QueryResult{}, err
		}
		affectedRows, err := execution.RowsAffected()
		if err != nil {
			return domain.QueryResult{}, err
		}
		return domain.QueryResult{AffectedRows: affectedRows}, nil
	}

	rows, err := runner.QueryContext(ctx, plan.statementSQL(), arguments...)
	if err != nil {
		return domain.QueryResult{}, err
	}
	defer rows.Close()
	return readDuckDBStatementRows(rows, output)
}

func validateDuckDBStatementPreconditions(
	ctx context.Context,
	runner duckDBStatementRunner,
	plan duckDBStatementPlan,
) error {
	for _, precondition := range plan.statementPreconditions() {
		if precondition.statement == "" || precondition.errorCode == "" {
			return fmt.Errorf("%w: DuckDB statement precondition is invalid", domain.ErrPrecondition)
		}
		rows, err := runner.QueryContext(ctx, precondition.statement, precondition.arguments...)
		if err != nil {
			return err
		}
		violated := rows.Next()
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			return rowsErr
		}
		if closeErr := rows.Close(); closeErr != nil {
			return closeErr
		}
		if violated {
			return &duckDBStatementContractError{
				kind: domain.ErrInvalidQuery, code: precondition.errorCode,
				detail: "multiple source rows match one MERGE target row",
			}
		}
	}
	return nil
}

func readDuckDBStatementRows(rows *sql.Rows, output duckDBStatementOutput) (domain.QueryResult, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return domain.QueryResult{}, err
	}
	observed, err := queryResultSchema(columnTypes, output.schemaHints())
	if err != nil {
		return domain.QueryResult{}, err
	}
	columns, err := canonicalizeDuckDBStatementOutput(observed, output)
	if err != nil {
		return domain.QueryResult{}, err
	}
	result := domain.QueryResult{Columns: columns}
	for rows.Next() {
		values := make([]any, len(columnTypes))
		destinations := make([]any, len(columnTypes))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return domain.QueryResult{}, err
		}
		normalized, err := normalizeSnapshotRow(result.Columns, values)
		if err != nil {
			return domain.QueryResult{}, err
		}
		result.Rows = append(result.Rows, tableDataCanonicalRow(result.Columns, normalized))
	}
	if err := rows.Err(); err != nil {
		return domain.QueryResult{}, err
	}
	return result, nil
}

func classifyDuckDBStatementError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var contractError *duckDBStatementContractError
	if errors.As(err, &contractError) {
		return err
	}
	switch {
	case errors.Is(err, domain.ErrUnsupported):
		return fmt.Errorf("%w: code=%s: %v", domain.ErrUnsupported, duckDBStatementUnsupportedFailureV1, err)
	case errors.Is(err, domain.ErrPrecondition):
		return fmt.Errorf("%w: code=%s: %v", domain.ErrPrecondition, duckDBStatementPreconditionFailureV1, err)
	case errors.Is(err, domain.ErrConflict):
		return fmt.Errorf("%w: code=%s: %v", domain.ErrConflict, duckDBStatementConflictFailureV1, err)
	case errors.Is(err, domain.ErrNotFound):
		return fmt.Errorf("%w: code=%s: %v", domain.ErrNotFound, duckDBStatementNotFoundFailureV1, err)
	case errors.Is(err, domain.ErrInvalid), errors.Is(err, domain.ErrInvalidQuery):
		return fmt.Errorf("%w: code=%s: %v", domain.ErrInvalidQuery, duckDBStatementInvalidFailureV1, err)
	default:
		return fmt.Errorf("%w: code=%s: %v", domain.ErrBackend, duckDBStatementBackendFailureV1, err)
	}
}
