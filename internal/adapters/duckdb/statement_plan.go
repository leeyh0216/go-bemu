package duckdb

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

// duckDBStatementPlan is the adapter-private output of AST lowering. The SQL
// stored here is generated DuckDB SQL, never client-supplied GoogleSQL.
type duckDBStatementPlan struct {
	statement           string
	arguments           []any
	producesRows        bool
	analysisFingerprint string
}

var statementAnalysisFingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func newDuckDBStatementPlan(
	statement string,
	arguments []any,
	producesRows bool,
	analysisFingerprint string,
) (duckDBStatementPlan, error) {
	if strings.TrimSpace(statement) == "" {
		return duckDBStatementPlan{}, fmt.Errorf("%w: generated DuckDB statement is empty", domain.ErrInvalidQuery)
	}
	if !statementAnalysisFingerprintPattern.MatchString(analysisFingerprint) {
		return duckDBStatementPlan{}, fmt.Errorf("%w: semantic analysis fingerprint is invalid", domain.ErrPrecondition)
	}
	return duckDBStatementPlan{
		statement:           statement,
		arguments:           cloneStatementArguments(arguments),
		producesRows:        producesRows,
		analysisFingerprint: analysisFingerprint,
	}, nil
}

func (plan duckDBStatementPlan) statementSQL() string { return plan.statement }

func (plan duckDBStatementPlan) bindArguments() []any {
	return cloneStatementArguments(plan.arguments)
}

func (plan duckDBStatementPlan) returnsRows() bool { return plan.producesRows }

func (plan duckDBStatementPlan) semanticAnalysisFingerprint() string {
	return plan.analysisFingerprint
}

func cloneStatementArguments(arguments []any) []any {
	cloned := make([]any, len(arguments))
	for index, argument := range arguments {
		switch value := argument.(type) {
		case []byte:
			cloned[index] = append([]byte(nil), value...)
		default:
			cloned[index] = value
		}
	}
	return cloned
}

// renderPhysicalTable accepts only a catalog-resolved canonical reference.
// Resolving one-, two-, or three-part GoogleSQL paths belongs to the analyzer;
// doing it again here would create a second source of default-dataset rules.
func renderPhysicalTable(reference domain.TableReference) (string, error) {
	if reference.ProjectID == "" || reference.DatasetID == "" || reference.TableID == "" {
		return "", fmt.Errorf("%w: canonical table reference is incomplete", domain.ErrInvalidQuery)
	}
	return quoteIdentifier(physicalSchema(reference.ProjectID, reference.DatasetID)) + "." +
		quoteIdentifier(reference.TableID), nil
}
