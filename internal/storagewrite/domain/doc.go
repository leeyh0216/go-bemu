// Package domain models the BigQuery Storage Write stream ledger without any
// dependency on gRPC, protobuf, or a concrete database.
//
// The central invariants are taken from the official BigQuery Write API:
//   - Write stream lifecycle: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream
//   - Append offset contract: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows
//   - Atomic pending-stream commit: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams
package domain
