package v0442

// Profile identifiers name the upstream connector query shapes recognized by
// this adapter. Syntax matching is performed exclusively over the owned
// GoogleSQL AST in analyzed_static.go and analyzed_dynamic.go.
const (
	StaticOverwriteProfile       = "spark-bigquery-connector-0.44.2/static-overwrite"
	DynamicTimeOverwriteProfile  = "spark-bigquery-connector-0.44.2/dynamic-time-partition-overwrite"
	DynamicRangeOverwriteProfile = "spark-bigquery-connector-0.44.2/dynamic-range-partition-overwrite"
)
