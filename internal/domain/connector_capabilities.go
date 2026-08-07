package domain

// Versioned connector capability identifiers are stable diagnostics for
// contract drift and intentionally unsupported sibling templates.
// Source templates:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L905
const (
	CapabilitySparkDynamicTimePartitionOverwriteV1 = "query.connector.dynamic-time-partition-overwrite-v1"
	GapSparkDynamicRangePartitionOverwriteV1       = "query.connector.dynamic-range-partition-overwrite.unsupported-v1"
)
