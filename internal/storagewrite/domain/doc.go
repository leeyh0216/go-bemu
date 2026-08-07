// Package domain models the BigQuery Storage Write stream ledger without any
// dependency on gRPC, protobuf, or a concrete database.
//
// The central invariants are taken from the official BigQuery Write API:
//   - Write stream lifecycle: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream
//   - Append offset contract: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows
//   - Atomic pending-stream commit: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams
//
// Spark connector 0.44.2's exact-once direct writer creates PENDING streams,
// appends ProtoRows with offsets, finalizes each task stream, and atomically
// commits them on the driver:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java
package domain
