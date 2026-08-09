<!-- doc-id: getting-started -->
<!-- lang: en -->

[English](getting-started.md) | [한국어](../ko/getting-started.md)

# Getting Started

This guide starts BQEMU, creates the minimum catalog resources, and runs one
query. It uses the public [BigQuery REST API
reference](https://cloud.google.com/bigquery/docs/reference/rest) resource shapes.

<!-- section: run -->
## Start BQEMU

From the repository root:

```bash
docker compose up --build -d --wait
curl --fail http://localhost:9050/readyz
```

The default Compose project starts the required fake GCS service too. The
`bqemu-data` volume contains local metadata and engine data. Keep it for a
restart, or remove it with `docker compose down --volumes` for a clean state.

<!-- section: endpoints -->
## Use The Right Endpoint

| Calling process | REST | Storage gRPC |
| --- | --- | --- |
| Host running Compose | `http://localhost:9050` | `localhost:9060` |
| Sibling Compose service | `http://bqemu:9050` | `bqemu:9060` |
| Development container, BQEMU on host | `http://host.docker.internal:9050` | `host.docker.internal:9060` |

REST-only callers need the REST endpoint. Storage callers need both endpoints.
For a Linux development container, add the usual
`host.docker.internal:host-gateway` host mapping when BQEMU runs on the host.

<!-- section: resources -->
## Create Resources

The default configuration already creates `local-project` and the `analytics`
dataset. To create a separate test project, dataset, and table:

```bash
curl --fail -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project","friendlyName":"Local tests"}'

curl --fail -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/datasets \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"projectId":"test-project","datasetId":"analytics"},"location":"US"}'

curl --fail -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/datasets/analytics/tables \
  -H 'Content-Type: application/json' \
  -d '{"tableReference":{"projectId":"test-project","datasetId":"events"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"label","type":"STRING"}]}}'
```

For persistent startup resources, declare them under `bootstrap.projects` in
the configuration file instead. The service reconciles them before readiness;
see [Configuration](configuration.md#bootstrap-resources).

<!-- section: query -->
## Run A Query

```bash
curl --fail -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1 AS answer","useLegacySql":false,"location":"US"}'
```

The response has the BigQuery `QueryResponse` shape. Check [Compatibility](compatibility.md)
before relying on a query feature outside the documented supported subset.

<!-- section: gcs-load -->
## Load Parquet Through Fake GCS

Load jobs accept `gs://` source URIs only. BQEMU uses `load.gcsEndpoint`; an
uploader uses its own endpoint for the same fake GCS service.

| Process | Setting or endpoint in the default Compose setup |
| --- | --- |
| BQEMU load worker | `load.gcsEndpoint: http://fake-gcs:4443` |
| Host uploader | `http://localhost:4443` |
| Sibling Compose uploader | `http://fake-gcs:4443` |
| Development-container uploader to host Compose | `http://host.docker.internal:4443` |

Parquet is the only supported load format. A caller can also use the BigQuery
media-upload endpoints; BQEMU stores completed media in the configured fake
GCS service and then submits the same `gs://` Parquet load path. Media sessions
are process-local and restart-invalid. The required configuration and limits
are in [Configuration](configuration.md#load-jobs).

<!-- section: tls -->
## Optional TLS And Credentials

Generate local certificate and credential fixtures when the calling process
requires them:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
docker compose -f compose.yaml -f compose.tls.yaml up --build -d --wait
```

Use a DNS name that matches the caller's endpoint. For example, generate with
`--tls-dns-name bqemu` for a sibling Compose service. See [Local credentials
and TLS](client-credentials-and-tls.md) for the file contract.

<!-- section: stop -->
## Stop

```bash
docker compose down
```

Add `--volumes` when the next run must start without previous local state.
