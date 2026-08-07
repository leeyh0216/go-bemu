// Package admin exposes BQEMU-owned, opt-in process diagnostics on a separate
// listener. These endpoints are not part of the BigQuery REST surface.
//
// Runtime sources:
//   - https://pkg.go.dev/runtime#Stack
//   - https://pkg.go.dev/runtime#ReadMemStats
//   - https://pkg.go.dev/runtime/debug#ReadBuildInfo
//   - https://pkg.go.dev/net/http#Server
//
// The handler never logs Authorization values or stack text. Boundary logs use
// only operation, size, digest, duration, and truncation metadata.
package admin
