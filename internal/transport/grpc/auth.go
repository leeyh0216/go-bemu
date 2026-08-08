package grpcserver

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authapp "github.com/leeyh0216/go-bemu/internal/auth/application"
)

const grpcAuthenticationFailureMessage = "request is not authenticated"

// The generated Google clients attach OAuth access tokens as authorization
// metadata. Service-account, authorized-user ADC, and external-account WIF are
// token acquisition paths and therefore converge at this one bearer boundary.
// Only the standard gRPC health service is public; server reflection is a
// discovery surface and remains protected whenever the selected policy does.
//
// Official sources:
//   - https://grpc.io/docs/guides/metadata/
//   - https://grpc.io/docs/guides/auth/
//   - https://github.com/grpc/grpc/blob/master/doc/health-checking.md
//   - https://cloud.google.com/docs/authentication/application-default-credentials
func authenticationUnaryServerInterceptor(authentication *authapp.Service) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if authentication == nil || publicGRPCAuthenticationMethod(info.FullMethod) {
			return handler(ctx, request)
		}
		authenticatedContext, _, err := authentication.Authenticate(ctx, authorizationMetadata(ctx))
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, grpcAuthenticationFailureMessage)
		}
		return handler(authenticatedContext, request)
	}
}

func authenticationStreamServerInterceptor(authentication *authapp.Service) grpc.StreamServerInterceptor {
	return func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if authentication == nil || publicGRPCAuthenticationMethod(info.FullMethod) {
			return handler(service, stream)
		}
		authenticatedContext, _, err := authentication.Authenticate(stream.Context(), authorizationMetadata(stream.Context()))
		if err != nil {
			return status.Error(codes.Unauthenticated, grpcAuthenticationFailureMessage)
		}
		return handler(service, &authenticatedServerStream{
			ServerStream: stream, context: authenticatedContext,
		})
	}
}

func authorizationMetadata(ctx context.Context) []string {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	return incoming.Get("authorization")
}

func publicGRPCAuthenticationMethod(fullMethod string) bool {
	switch fullMethod {
	case "/grpc.health.v1.Health/Check", "/grpc.health.v1.Health/List", "/grpc.health.v1.Health/Watch":
		return true
	default:
		return false
	}
}

type authenticatedServerStream struct {
	grpc.ServerStream
	context context.Context
}

func (stream *authenticatedServerStream) Context() context.Context { return stream.context }
