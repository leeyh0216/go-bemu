<!-- doc-id: readme -->
<!-- lang: en -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu` is a local BigQuery-compatible service for application and protocol
tests. It exposes BigQuery REST and Storage gRPC endpoints, persists local
catalog state, and uses an external GCS-compatible service for Parquet load
jobs. Its public resource shapes follow the [BigQuery REST API
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: start -->
## Start

```bash
docker compose up --build -d --wait
curl --fail http://localhost:9050/readyz
```

The default Compose project starts BQEMU and the required fake GCS service.
State is retained in the `bqemu-data` volume. Remove it with
`docker compose down --volumes` when the local test state is no longer needed.

<!-- section: connect -->
## Connect

| Calling process | REST endpoint | Storage gRPC endpoint |
| --- | --- | --- |
| Host running Compose | `http://localhost:9050` | `localhost:9060` |
| Sibling Compose service | `http://bqemu:9050` | `bqemu:9060` |
| Development container with BQEMU on the host | `http://host.docker.internal:9050` | `host.docker.internal:9060` |

Set the endpoint in the calling process. Do not use `localhost` from a
container unless BQEMU runs in that same container.

<!-- section: docs -->
## Documentation

- [Getting started](docs/en/getting-started.md): create resources, run a query,
  and use the required fake GCS service.
- [Configuration](docs/en/configuration.md): endpoint topology, bootstrap
  projects and datasets, persistence, TLS, and runtime limits.
- [What works](docs/en/compatibility.md): use-now, limited, and unavailable
  behavior in one page.
- [API and RPC reference](docs/en/api-rpc-compatibility.md): generated exact
  method and endpoint inventory.
- [Maintainer documentation](docs/en/maintainers/index.md): implementation,
  CI, release, and contribution material.
