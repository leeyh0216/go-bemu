// Package domain contains the protocol-independent Storage Read model.
//
// Protocol sources:
//   - Read sessions and streams: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession
//   - ReadRows offsets and response row counts: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsrequest
//
// Protobuf values deliberately do not cross into this package. The gRPC
// adapter owns wire conversion so a future protocol revision can be isolated
// from snapshot storage and session orchestration.
package domain
