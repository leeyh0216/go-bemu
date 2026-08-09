<!-- doc-id: docs-index -->
<!-- lang: en -->

[English](index.md) | [한국어](../ko/index.md)

# go-bemu Documentation

This is the user path for running BQEMU. It answers where to connect, which
resources to create, and whether a feature is usable. Public resource shapes
follow the [BigQuery REST API reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: start -->
## Start

- [Getting started](getting-started.md): run Compose, choose the correct
  endpoint, create a project/dataset/table, and run a query.
- [Configuration](configuration.md): configure the service and bootstrap
  multiple projects and datasets before it becomes ready.

<!-- section: use -->
## Use The Service

- [Local credentials and TLS](client-credentials-and-tls.md): optional local
  TLS and credential fixtures for callers that require them.
- [Compatibility](compatibility.md): concise use-now, limited, and unavailable
  behavior.
- [API and RPC reference](api-rpc-compatibility.md): exact generated public
  method inventory. Use it when a caller depends on a particular field or RPC.

<!-- section: maintain -->
## Maintain The Service

Implementation, integration-test evidence, architecture, CI, releases, and
contribution instructions are kept in the [maintainer documentation](maintainers/index.md).
They are not prerequisites for using the local service.
