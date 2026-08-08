package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

var scriptVariableSequence atomic.Uint64

func (w *Warehouse) executeGoogleSQLScript(
	ctx context.Context,
	statement semantic.Statement,
) (result domain.QueryResult, err error) {
	children, err := validatedGoogleSQLScriptStatements(statement)
	if err != nil {
		return result, err
	}

	started := observability.LogSideEffectStart(ctx, "duckdb", "execute_script",
		"statement_kind", string(statement.Kind()),
		"analysis_fingerprint", statement.AnalysisFingerprint(),
		"statement_count", len(children),
		"transaction_mode", "explicit",
	)
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "execute_script", started, err,
			"statement_kind", string(statement.Kind()),
			"analysis_fingerprint", statement.AnalysisFingerprint(),
			"statement_count", len(children),
			"row_count", len(result.Rows),
			"affected_rows", result.AffectedRows,
			"transaction_mode", "explicit",
		)
	}()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin GoogleSQL script transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				err = errors.Join(err, fmt.Errorf("rollback GoogleSQL script transaction: %w", rollbackErr))
			}
		}
	}()

	result, err = executeGoogleSQLScriptTransaction(ctx, tx, statement, children,
		func(ctx context.Context, tx *sql.Tx, plan duckDBStatementPlan) (domain.QueryResult, error) {
			output, outputErr := newDuckDBStatementOutput(statement, plan.returnsRows())
			if outputErr != nil {
				return domain.QueryResult{}, outputErr
			}
			return executeDuckDBStatementPlan(ctx, tx, plan, output)
		})
	if err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit GoogleSQL script transaction: %w", err)
	}
	committed = true
	return result, nil
}

type duckDBScriptFinalStatement func(
	context.Context,
	*sql.Tx,
	duckDBStatementPlan,
) (domain.QueryResult, error)

func validatedGoogleSQLScriptStatements(statement semantic.Statement) ([]queryast.Statement, error) {
	script, ok := statement.Syntax().(*queryast.ScriptStatement)
	if !ok || !statement.RelationsComplete() {
		return nil, fmt.Errorf("%w: analyzed script is invalid", domain.ErrPrecondition)
	}
	children := script.Statements()
	if len(children) < 2 {
		return nil, fmt.Errorf("%w: analyzed script has too few statements", domain.ErrPrecondition)
	}
	return children, nil
}

// executeGoogleSQLScriptTransaction evaluates declarations, assignments, and
// intermediate statements before delegating the final statement. The caller
// owns the transaction, so execution and result publication can commit as one
// unit.
func executeGoogleSQLScriptTransaction(
	ctx context.Context,
	tx *sql.Tx,
	statement semantic.Statement,
	children []queryast.Statement,
	final duckDBScriptFinalStatement,
) (result domain.QueryResult, err error) {
	if tx == nil || final == nil || len(children) < 2 {
		return result, fmt.Errorf("%w: GoogleSQL script transaction is invalid", domain.ErrPrecondition)
	}

	variables := make(map[string]string)
	createdTables := make([]string, 0)
	sequence := scriptVariableSequence.Add(1)
	for index, child := range children {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		last := index == len(children)-1
		switch value := child.(type) {
		case *queryast.DeclareStatement:
			tables, declareErr := declareDuckDBScriptVariables(ctx, tx, statement, value, variables, sequence, index)
			if declareErr != nil {
				return result, declareErr
			}
			for name, table := range tables {
				variables[name] = table
				createdTables = append(createdTables, table)
			}
		case *queryast.SetStatement:
			if setErr := setDuckDBScriptVariable(ctx, tx, statement, value, variables); setErr != nil {
				return result, setErr
			}
		default:
			plan, lowerErr := lowerDuckDBSyntax(statement, child, variables)
			if lowerErr != nil {
				return result, lowerErr
			}
			if last {
				result, err = final(ctx, tx, plan)
				if err != nil {
					return result, fmt.Errorf("execute GoogleSQL script statement %d: %w", index, err)
				}
			} else if err := executeAndDiscardDuckDBStatement(ctx, tx, plan); err != nil {
				return result, fmt.Errorf("execute GoogleSQL script statement %d: %w", index, err)
			}
		}
	}

	for index := len(createdTables) - 1; index >= 0; index-- {
		if _, err := tx.ExecContext(ctx, "DROP TABLE "+quoteIdentifier(createdTables[index])); err != nil {
			return result, fmt.Errorf("drop GoogleSQL script variable %d: %w", index, err)
		}
	}
	return result, nil
}

func declareDuckDBScriptVariables(
	ctx context.Context,
	tx *sql.Tx,
	statement semantic.Statement,
	declaration *queryast.DeclareStatement,
	variables map[string]string,
	sequence uint64,
	statementIndex int,
) (map[string]string, error) {
	created := make(map[string]string)
	for variableIndex, variable := range declaration.Variables() {
		name := strings.ToLower(variable.Value())
		if _, exists := variables[name]; exists {
			return nil, fmt.Errorf("%w: GoogleSQL script variable is already declared", domain.ErrInvalidQuery)
		}
		table := fmt.Sprintf("__bqemu_script_variable_%d_%d_%d", sequence, statementIndex, variableIndex)
		renderer, err := newDuckDBStatementRenderer(statement, declaration, variables)
		if err != nil {
			return nil, err
		}
		var value string
		if defaultValue := declaration.DefaultValue(); defaultValue != nil {
			value, err = renderer.renderExpression(defaultValue)
		} else {
			value, err = renderer.renderType(declaration.Type())
			if err == nil {
				value = "CAST(NULL AS " + value + ")"
			}
		}
		if err != nil {
			return nil, err
		}
		create := "CREATE TEMP TABLE " + quoteIdentifier(table) + " AS SELECT " + value + " AS " + quoteIdentifier("value")
		if _, err := tx.ExecContext(ctx, create, renderer.arguments...); err != nil {
			return nil, fmt.Errorf("initialize GoogleSQL script variable: %w", err)
		}
		created[name] = table
	}
	return created, nil
}

func setDuckDBScriptVariable(
	ctx context.Context,
	tx *sql.Tx,
	statement semantic.Statement,
	assignment *queryast.SetStatement,
	variables map[string]string,
) error {
	target := assignment.Target().Segments()
	if len(target) != 1 {
		return fmt.Errorf("%w: GoogleSQL script assignment target is invalid", domain.ErrInvalidQuery)
	}
	table, exists := variables[strings.ToLower(target[0])]
	if !exists {
		return fmt.Errorf("%w: GoogleSQL script assignment target is not declared", domain.ErrInvalidQuery)
	}
	renderer, err := newDuckDBStatementRenderer(statement, assignment, variables)
	if err != nil {
		return err
	}
	value, err := renderer.renderExpression(assignment.Value())
	if err != nil {
		return err
	}
	update := "UPDATE " + quoteIdentifier(table) + " SET " + quoteIdentifier("value") + " = " + value
	if _, err := tx.ExecContext(ctx, update, renderer.arguments...); err != nil {
		return fmt.Errorf("assign GoogleSQL script variable: %w", err)
	}
	return nil
}

func executeAndDiscardDuckDBStatement(
	ctx context.Context,
	runner duckDBStatementRunner,
	plan duckDBStatementPlan,
) error {
	if err := validateDuckDBStatementPreconditions(ctx, runner, plan); err != nil {
		return err
	}
	if !plan.returnsRows() {
		_, err := runner.ExecContext(ctx, plan.statementSQL(), plan.bindArguments()...)
		return err
	}
	rows, err := runner.QueryContext(ctx, plan.statementSQL(), plan.bindArguments()...)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([]any, len(columns))
	destinations := make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return err
		}
	}
	return rows.Err()
}
