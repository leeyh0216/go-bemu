// Package application orchestrates immutable Storage Read sessions and logical
// streams without depending on protobuf or a specific database.
//
// Protocol sources:
//   - CreateReadSession semantics: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.CreateReadSession
//   - Session expiration: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession
package application
