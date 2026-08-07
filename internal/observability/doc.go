// Package observability provides protocol-boundary and side-effect logging with
// a fail-closed data contract. Request/response bodies, SQL, row data, protobuf
// JSON, error text, and credentials are never emitted. Opaque inputs are
// represented by shape, byte count, item count, and SHA-256 digest in every log
// level and configuration mode. The legacy unsafePayloads setting remains
// parse-compatible but cannot relax this contract.
//
// Protocol provenance:
//   - gRPC server interceptors: https://grpc.io/docs/guides/interceptors/
//   - gRPC metadata: https://grpc.io/docs/guides/metadata/
//   - W3C Trace Context: https://www.w3.org/TR/trace-context/
//   - Storage API protobuf: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
//   - Cloud Logging audit guidance: https://cloud.google.com/logging/docs/audit/best-practices
package observability
