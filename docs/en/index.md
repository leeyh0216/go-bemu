<!-- doc-id: docs-index -->
<!-- lang: en -->

[English](index.md) | [한국어](../ko/index.md)

# User Documentation

These documents are for people running BQEMU from an application, CLI, or
connector. BigQuery resources follow the public [BigQuery REST API
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: start -->
## Start Here

- [Getting started](getting-started.md): run Compose, create resources, send the
  first query, add fake GCS, and connect from a development container.
- [Client credentials and TLS](client-credentials-and-tls.md): generate local
  service-account, authorized-user, WIF, direct-token, certificate, and
  truststore fixtures.

<!-- section: clients -->
## Client Guides

- [Python BigQuery client](clients/python-bigquery.md)
- [`bq` CLI](clients/bq-cli.md)
- [PySpark and Scala Spark](clients/spark-bigquery-connector.md)

These user guides explain host, Compose-network, and development-container
endpoints. Version-pinned evidence remains in the integration test manifests.

<!-- section: compatibility -->
## Compatibility References

- [Compatibility](compatibility.md): status definitions and links to the
  generated API/RPC table.
- [API and RPC compatibility](api-rpc-compatibility.md): generated method,
  endpoint, condition, test, and issue rows.
- [Consumer compatibility](consumer-compatibility.md): generated client
  versions, runtime artifacts, and scenario selectors.

<!-- section: maintainers -->
## Maintainer Documentation

Architecture, adapter contracts, implementation notes, runbooks, and version
onboarding are kept in the separate [maintainer index](maintainers/index.md).
