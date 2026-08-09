// Package contract owns the canonical public REST and RPC operation manifest,
// its annotation validation, and deterministic runtime route generation.
// Source-reviewed golden fixtures are compared by contract unit tests;
// integration runtime traffic is retained as per-run evidence and is not
// automatically passed to CompareGolden.
//
// Protocol provenance:
//   - BigQuery Storage RPC: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
//   - BigQuery REST v2: https://cloud.google.com/bigquery/docs/reference/rest/v2
package contract
