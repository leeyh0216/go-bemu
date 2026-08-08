// Package ports declares replaceable authentication dependencies.
//
// The public transports depend on TokenVerifier rather than a token file or an
// identity provider implementation. This keeps future OAuth/STS and persistent
// token adapters outside the application boundary.
package ports
