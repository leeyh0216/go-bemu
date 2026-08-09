package v0442

import (
	"context"
	"fmt"
	"reflect"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

const SourceCommit = "719817782a214b8ca72be520870013a3e0253d92"

type Analyzer struct {
	fallback ports.QueryAnalyzer
	gateway  ports.GoogleSQLGateway
}

var (
	_ ports.QueryAnalyzer                  = (*Analyzer)(nil)
	_ ports.QueryOperationAnalyzer         = (*Analyzer)(nil)
	_ ports.AnalyzedQueryOperationAnalyzer = (*Analyzer)(nil)
)

func NewAnalyzer(fallback ports.QueryAnalyzer) (*Analyzer, error) {
	if interfaceIsNil(fallback) {
		return nil, fmt.Errorf("%w: fallback query analyzer is required", domain.ErrPrecondition)
	}
	return &Analyzer{fallback: fallback}, nil
}

// WithGoogleSQLGateway installs the sole syntax boundary for this versioned
// connector adapter.
func (analyzer *Analyzer) WithGoogleSQLGateway(gateway ports.GoogleSQLGateway) error {
	if analyzer == nil || interfaceIsNil(gateway) {
		return fmt.Errorf("%w: GoogleSQL gateway is required", domain.ErrPrecondition)
	}
	analyzer.gateway = gateway
	return nil
}

// AnalyzeStatementOperation is the production connector boundary. The
// GoogleSQL gateway has already parsed and semantically bound the request, so
// this adapter only matches the owned immutable representation.
func (analyzer *Analyzer) AnalyzeStatementOperation(
	ctx context.Context,
	statement semantic.Statement,
	request ports.QueryRequest,
) (ports.QueryOperation, bool, error) {
	if err := ctx.Err(); err != nil {
		return ports.QueryOperation{}, false, err
	}
	operation, matched, err := matchAnalyzedStaticOverwrite(statement, request)
	if matched || err != nil {
		return operation, matched, err
	}
	return matchAnalyzedDynamicTimeOverwrite(statement, request)
}

func (analyzer *Analyzer) AnalyzeQuery(ctx context.Context, request ports.QueryRequest) (ports.QueryAnalysis, error) {
	operation, matched, err := analyzer.AnalyzeQueryOperation(ctx, request)
	if matched {
		if err != nil {
			return ports.QueryAnalysis{}, err
		}
		return ports.QueryAnalysis{
			ReferencedTables: []domain.TableReference{operation.Destination(), operation.Source()},
			MutationTargets:  []domain.TableReference{operation.Destination()},
		}, nil
	}
	return analyzer.fallback.AnalyzeQuery(ctx, request)
}

// AnalyzeQueryOperation returns matched=true for connector-profile candidates,
// including drifted templates. Candidates never fall through to generic SQL.
func (analyzer *Analyzer) AnalyzeQueryOperation(
	ctx context.Context,
	request ports.QueryRequest,
) (ports.QueryOperation, bool, error) {
	if analyzer.gateway != nil {
		statement, err := analyzer.gateway.Analyze(ctx, request)
		if err != nil {
			return ports.QueryOperation{}, false, err
		}
		return analyzer.AnalyzeStatementOperation(ctx, statement, request)
	}
	return ports.QueryOperation{}, false, fmt.Errorf(
		"%w: GoogleSQL gateway is required for connector operation analysis", domain.ErrPrecondition,
	)
}

func (analyzer *Analyzer) VerifyQueryOperation(request ports.QueryRequest, operation ports.QueryOperation) error {
	if analyzer == nil || analyzer.gateway == nil {
		return fmt.Errorf("%w: GoogleSQL gateway is required for connector operation verification", domain.ErrPrecondition)
	}
	statement, err := analyzer.gateway.Analyze(context.Background(), request)
	if err != nil {
		return err
	}
	expected, matched, err := analyzer.AnalyzeStatementOperation(context.Background(), statement, request)
	if err != nil || !matched {
		return fmt.Errorf("%w: request no longer matches a connector AST profile", domain.ErrPrecondition)
	}
	expectedProfile, err := queryOperationProfile(expected.Kind())
	if err != nil {
		return err
	}
	if err := expected.ValidateBinding(request, expectedProfile); err != nil {
		return fmt.Errorf("%w: pinned connector parser produced an invalid proof", domain.ErrPrecondition)
	}
	if operation.SemanticFingerprint() != expected.SemanticFingerprint() ||
		operation.BindingFingerprint() != expected.BindingFingerprint() {
		return fmt.Errorf("%w: semantic query operation payload differs from the analyzed AST result", domain.ErrPrecondition)
	}
	return operation.ValidateBinding(request, expectedProfile)
}

func queryOperationProfile(kind ports.QueryOperationKind) (string, error) {
	switch kind {
	case ports.QueryOperationSparkStaticOverwrite:
		return StaticOverwriteProfile, nil
	case ports.QueryOperationSparkDynamicTimeOverwrite:
		return DynamicTimeOverwriteProfile, nil
	default:
		return "", fmt.Errorf("%w: semantic operation kind is outside connector profile 0.44.2", domain.ErrPrecondition)
	}
}

func interfaceIsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
