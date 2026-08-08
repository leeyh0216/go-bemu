<!-- doc-id: readme -->
<!-- lang: en -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu` provides a local BigQuery-compatible endpoint for application and
connector tests. Run BQEMU beside your test process, create an emulator project,
and point the client at the local REST or Storage gRPC endpoint.

The exact API and RPC surface is listed in [Compatibility](docs/en/compatibility.md).
BigQuery request and response resources follow the public [BigQuery API
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: quick-start -->
## Docker Compose

```bash
docker compose up --build -d --wait

curl --fail -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project"}'
```

To run the image published from the default branch instead of building it:

```bash
export BQEMU_IMAGE=ghcr.io/leeyh0216/go-bemu:edge
docker compose pull bqemu
docker compose up --no-build -d --wait
```

The Compose project keeps data in the `bqemu-data` volume. Remove the volume
with `docker compose down --volumes` when the test state is no longer needed.

<!-- section: endpoints -->
## Endpoints

| Service | Default endpoint |
| --- | --- |
| BigQuery REST and health | `http://localhost:9050` |
| BigQuery Storage gRPC | `localhost:9060` |

Containerized clients use the BQEMU service name or
`host.docker.internal`, depending on where Compose runs. TLS uses the same
ports and changes the REST scheme to HTTPS.

<!-- section: next-steps -->
## Next Steps

- [Getting started](docs/en/getting-started.md): create a dataset and table,
  run the first query, enable TLS, and connect from a development container.
- [Reviewed integration examples](tests/integration/docs/en/index.md):
  version-pinned process and runtime setups verified by CI.
- [Client credentials and TLS](docs/en/client-credentials-and-tls.md)
- [Compatibility](docs/en/compatibility.md): API/RPC support by operation ID.
- [Documentation index](docs/en/index.md)
