package application

import (
	"context"

	authdomain "github.com/leeyh0216/go-bemu/internal/auth/domain"
)

type authenticationContextKey struct{}

type authenticationContext struct {
	principal authdomain.Principal
	decision  authdomain.Decision
}

func withAuthentication(ctx context.Context, principal authdomain.Principal, decision authdomain.Decision) context.Context {
	return context.WithValue(ctx, authenticationContextKey{}, authenticationContext{principal: principal, decision: decision})
}

func PrincipalFromContext(ctx context.Context) (authdomain.Principal, bool) {
	authentication, ok := ctx.Value(authenticationContextKey{}).(authenticationContext)
	return authentication.principal, ok && authentication.principal.Valid()
}

func DecisionFromContext(ctx context.Context) (authdomain.Decision, bool) {
	authentication, ok := ctx.Value(authenticationContextKey{}).(authenticationContext)
	return authentication.decision, ok && authentication.decision.Allowed()
}
