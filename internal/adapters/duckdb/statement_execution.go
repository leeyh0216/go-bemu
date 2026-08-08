package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

const (
	duckDBStatementBackendFailureV1      = "query.googlesql.duckdb-execution.backend-failure-v1"
	duckDBStatementInvalidFailureV1      = "query.googlesql.duckdb-execution.invalid-v1"
	duckDBStatementUnsupportedFailureV1  = "query.googlesql.duckdb-execution.unsupported-v1"
	duckDBStatementPreconditionFailureV1 = "query.googlesql.duckdb-execution.precondition-v1"
	duckDBStatementConflictFailureV1     = "query.googlesql.duckdb-execution.conflict-v1"
	duckDBStatementNotFoundFailureV1     = "query.googlesql.duckdb-execution.not-found-v1"
)

type duckDBStatementRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// ExecuteStatement lowers an analyzed GoogleSQL statement exactly once and
// sends only the adapter-private DuckDB plan to the backend.
func (w *Warehouse) ExecuteStatement(
	ctx context.Context,
	statement semantic.Statement,
) (result domain.QueryResult, err error) {
	plan, err := lowerDuckDBStatement(statement)
	if err != nil {
		return domain.QueryResult{}, err
	}

	started := observability.LogSideEffectStart(ctx, "duckdb", "execute_statement",
		"statement_kind", string(statement.Kind()),
		"analysis_fingerprint", plan.semanticAnalysisFingerprint(),
		"bind_count", len(plan.bindArguments()),
		"transaction_mode", "autocommit",
	)
	defer func() {
		err = sanitizeDuckDBStatementError(err)
		observability.LogSideEffectEnd(ctx, "duckdb", "execute_statement", started, err,
			"statement_kind", string(statement.Kind()),
			"analysis_fingerprint", plan.semanticAnalysisFingerprint(),
			"row_count", len(result.Rows),
			"affected_rows", result.AffectedRows,
			"schema_fingerprint", queryMaterializationSchemaDigest(result.Columns),
			"transaction_mode", "autocommit",
		)
	}()

	return executeDuckDBStatementPlan(ctx, w.db, plan, nil)
}

func executeDuckDBStatementPlan(
	ctx context.Context,
	runner duckDBStatementRunner,
	plan duckDBStatementPlan,
	schemaHints []domain.Field,
) (domain.QueryResult, error) {
	if runner == nil {
		return domain.QueryResult{}, fmt.Errorf("%w: DuckDB statement runner is missing", domain.ErrPrecondition)
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
	return readDuckDBStatementRows(rows, schemaHints)
}

func readDuckDBStatementRows(rows *sql.Rows, schemaHints []domain.Field) (domain.QueryResult, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return domain.QueryResult{}, err
	}
	columns, err := queryResultSchema(columnTypes, schemaHints)
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

// sanitizeDuckDBStatementError deliberately discards backend and schema error
// text. DuckDB can include generated SQL, identifiers, and bound values in its
// diagnostics; those details must not cross the adapter boundary.
func sanitizeDuckDBStatementError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, domain.ErrUnsupported):
		return fmt.Errorf("%w: code=%s", domain.ErrUnsupported, duckDBStatementUnsupportedFailureV1)
	case errors.Is(err, domain.ErrPrecondition):
		return fmt.Errorf("%w: code=%s", domain.ErrPrecondition, duckDBStatementPreconditionFailureV1)
	case errors.Is(err, domain.ErrConflict):
		return fmt.Errorf("%w: code=%s", domain.ErrConflict, duckDBStatementConflictFailureV1)
	case errors.Is(err, domain.ErrNotFound):
		return fmt.Errorf("%w: code=%s", domain.ErrNotFound, duckDBStatementNotFoundFailureV1)
	case errors.Is(err, domain.ErrInvalid), errors.Is(err, domain.ErrInvalidQuery):
		return fmt.Errorf("%w: code=%s", domain.ErrInvalidQuery, duckDBStatementInvalidFailureV1)
	default:
		return fmt.Errorf("%w: code=%s", domain.ErrBackend, duckDBStatementBackendFailureV1)
	}
}
