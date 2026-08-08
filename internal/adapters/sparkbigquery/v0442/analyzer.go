package v0442

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const SourceCommit = "719817782a214b8ca72be520870013a3e0253d92"

type Analyzer struct {
	fallback ports.QueryAnalyzer
}

var (
	_ ports.QueryAnalyzer          = (*Analyzer)(nil)
	_ ports.QueryOperationAnalyzer = (*Analyzer)(nil)
)

func NewAnalyzer(fallback ports.QueryAnalyzer) (*Analyzer, error) {
	if interfaceIsNil(fallback) {
		return nil, fmt.Errorf("%w: fallback query analyzer is required", domain.ErrPrecondition)
	}
	return &Analyzer{fallback: fallback}, nil
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
	operation, matched, err := parsePinnedQueryOperation(request)
	if !matched {
		return ports.QueryOperation{}, false, nil
	}
	if err != nil {
		analyzer.logRejection(ctx, request, err)
		return ports.QueryOperation{}, true, err
	}
	slog.InfoContext(ctx, "connector query operation analyzed",
		"event", "boundary.exit", "boundary", "sparkbigquery.v0442.query_operation_analysis",
		"operation", operation.Kind(), "model_version", operation.ProfileID(),
		"query_bytes", len(request.SQL), "query_digest", operation.SQLFingerprint(),
		"request_fingerprint", operation.RequestFingerprint(),
		"semantic_fingerprint", operation.SemanticFingerprint(),
		"binding_fingerprint", operation.BindingFingerprint(),
	)
	return operation, true, nil
}

func (analyzer *Analyzer) VerifyQueryOperation(request ports.QueryRequest, operation ports.QueryOperation) error {
	expected, matched, err := parsePinnedQueryOperation(request)
	if err != nil || !matched {
		return fmt.Errorf("%w: request no longer matches a pinned connector query profile", domain.ErrPrecondition)
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
		return fmt.Errorf("%w: semantic query operation payload differs from the pinned parser result", domain.ErrPrecondition)
	}
	return operation.ValidateBinding(request, expectedProfile)
}

func parsePinnedQueryOperation(request ports.QueryRequest) (ports.QueryOperation, bool, error) {
	operation, matched, err := parseSparkDynamicTimeOverwrite(request)
	if !matched {
		operation, matched, err = parseStaticOverwrite(request)
	}
	return operation, matched, err
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

func (analyzer *Analyzer) logRejection(ctx context.Context, request ports.QueryRequest, err error) {
	profile := StaticOverwriteProfile
	operation := "connector-static-overwrite"
	if leadingStatementKeyword(request.SQL) == "DECLARE" {
		profile = DynamicTimeOverwriteProfile
		operation = "connector-dynamic-partition-overwrite"
		if errors.Is(err, domain.ErrUnsupported) && containsGap(err.Error(), domain.GapSparkDynamicRangePartitionOverwriteV1) {
			profile = DynamicRangeOverwriteProfile
		}
	}
	attrs := []any{
		"event", "boundary.reject", "boundary", "sparkbigquery.v0442.query_operation_analysis",
		"operation", operation, "model_version", profile,
		"query", request.SQL,
		"query_bytes", len(request.SQL), "query_digest", observability.Digest([]byte(request.SQL)),
		"source_commit", SourceCommit,
		"fix_hint", "compare the pinned connector producer before updating this versioned profile",
	}
	var drift *dynamicOverwriteShapeError
	if errors.As(err, &drift) {
		attrs = append(attrs, "capability", domain.CapabilitySparkDynamicTimePartitionOverwriteV1,
			"gap", domain.GapQueryScriptsUnsupportedV1, "token_index", drift.TokenIndex,
			"expected_shape", drift.ExpectedShape)
	} else if profile == DynamicRangeOverwriteProfile {
		attrs = append(attrs, "gap", domain.GapSparkDynamicRangePartitionOverwriteV1,
			"token_index", -1, "expected_shape", "supported time-partition overwrite profile")
	}
	attrs = append(attrs, observability.ErrorAttrs(err)...)
	slog.WarnContext(ctx, "connector query operation rejected", attrs...)
}

func containsGap(message, gap string) bool {
	for index := 0; index+len(gap) <= len(message); index++ {
		if message[index:index+len(gap)] == gap {
			return true
		}
	}
	return false
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
