// Package observability provides protocol-boundary and side-effect logging with
// safe defaults. Request bodies and row data are represented by byte counts and
// SHA-256 digests unless the explicitly unsafe development switch is enabled;
// credential-shaped values remain redacted in both modes.
//
// Protocol provenance:
//   - gRPC server interceptors: https://grpc.io/docs/guides/interceptors/
//   - gRPC metadata: https://grpc.io/docs/guides/metadata/
//   - W3C Trace Context: https://www.w3.org/TR/trace-context/
//   - Storage API protobuf: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
package observability
