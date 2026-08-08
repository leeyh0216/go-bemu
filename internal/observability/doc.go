// Package observability provides protocol-boundary and side-effect logging.
// Diagnostic records retain request metadata, SQL, protocol messages, payloads,
// and original errors. Operators must apply destination access, retention, and
// transport controls that are appropriate for the data handled by the emulator.
//
// Protocol provenance:
//   - gRPC server interceptors: https://grpc.io/docs/guides/interceptors/
//   - gRPC metadata: https://grpc.io/docs/guides/metadata/
//   - W3C Trace Context: https://www.w3.org/TR/trace-context/
//   - Storage API protobuf: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
//   - Cloud Logging audit guidance: https://cloud.google.com/logging/docs/audit/best-practices
package observability
