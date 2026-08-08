// Package admin exposes BQEMU-owned, opt-in process diagnostics on a separate
// listener. These endpoints are not part of the BigQuery REST surface.
//
// Runtime sources:
//   - https://pkg.go.dev/runtime#Stack
//   - https://pkg.go.dev/runtime#ReadMemStats
//   - https://pkg.go.dev/runtime/debug#ReadBuildInfo
//   - https://pkg.go.dev/net/http#Server
//
// Diagnostic payloads and authorization failures can be emitted in logs. Run
// the listener on a protected network and restrict access to its log sink.
package admin
