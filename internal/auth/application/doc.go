// Package application authenticates transport-neutral Authorization metadata.
//
// REST and gRPC adapters pass every Authorization field value to Service. The
// application applies the same RFC 6750 parsing rules at both boundaries and
// delegates credential acceptance to a replaceable ports.TokenVerifier.
//
// Protocol and client sources:
//   - Bearer header syntax: https://www.rfc-editor.org/rfc/rfc6750.html#section-2.1
//   - Invalid-token response semantics: https://www.rfc-editor.org/rfc/rfc6750.html#section-3.1
//   - gRPC authentication: https://grpc.io/docs/guides/authentication/
//   - Spark connector 0.44.2 credential selection: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryCredentialsSupplier.java
package application
