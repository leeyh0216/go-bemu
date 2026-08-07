// Package contract owns versioned, executable descriptions of the protocol
// sequences used by supported BigQuery clients and connectors. Profiles are
// deliberately exact-version keyed: an unknown consumer version must fail
// selection instead of inheriting a nearby profile whose wire behavior may have
// drifted.
//
// Protocol provenance:
//   - Spark connector 0.44.2 source: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2
//   - Python client 3.43.0 release: https://pypi.org/project/google-cloud-bigquery/3.43.0/
//   - BigQuery Storage RPC: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
//   - BigQuery REST v2: https://cloud.google.com/bigquery/docs/reference/rest/v2
//
// Golden files are source-derived canonical fixtures, not packet captures. A
// later end-to-end harness records normalized traffic and compares it with these
// fixtures using CompareGolden, so failures identify both the connector stage
// and a wire-level diff.
package contract
