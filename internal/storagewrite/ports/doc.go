// Package ports defines replaceable Storage Write outbound dependencies.
//
// The application owns stream lifecycle and offset ordering. A coordinator is
// responsible only for durable side effects and must preserve the atomic commit
// guarantee of BatchCommitWriteStreams:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams
package ports
