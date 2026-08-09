<!-- doc-id: getting-started -->
<!-- lang: en -->

[English](getting-started.md) | [한국어](../ko/getting-started.md)

# Getting Started

This guide starts BQEMU, creates the resources used by local clients, and runs
one query. See [Compatibility](compatibility.md) before depending on a specific
field or method. Request resources follow the [BigQuery REST API
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: run -->
## Start BQEMU

From the repository root:

```bash
docker compose up --build -d --wait
curl --fail http://localhost:9050/readyz
export BQEMU_REST=http://localhost:9050
```

Compose stores the catalog and table data in the `bqemu-data` volume. Keeping
that volume across restarts is the user's responsibility. The SQLite state and
DuckDB files are one generation: retain or replace both together. BQEMU refuses
to become ready when only one file, or two different generations, are restored.

<!-- section: endpoints -->
## Choose The Endpoint

| Client location | REST | Storage gRPC | fake GCS uploader | TLS CA path |
| --- | --- | --- | --- | --- |
| Host running Compose | `http://localhost:9050` | `localhost:9060` | `http://localhost:4443` | `.bqemu-auth/ca.pem` |
| Sibling Compose service / Dev Container on the Compose network | `http://bqemu:9050` | `bqemu:9060` | `http://fake-gcs:4443` | mounted `/run/bqemu-auth/ca.pem` |
| Development container, BQEMU on host | `http://host.docker.internal:9050` | `host.docker.internal:9060` | `http://host.docker.internal:4443` | mounted host CA path |

REST callers use only the REST endpoint. Storage Read and Storage Write callers
also use the gRPC endpoint.

For the host quick start below, set `BQEMU_REST=http://localhost:9050`. A
containerized caller must instead set it to the matching non-localhost REST
address from the table; `localhost` inside that container is the container,
not BQEMU.

<!-- section: resources -->
## Create A Project, Dataset, And Table

BQEMU projects are emulator resources, so create one before calling BigQuery
v2 methods:

```bash
curl --fail -X POST "$BQEMU_REST/bqemu/v1/projects" \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project","friendlyName":"Local tests"}'

curl --fail -X POST \
  "$BQEMU_REST/bigquery/v2/projects/test-project/datasets" \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"projectId":"test-project","datasetId":"analytics"},"location":"US"}'

curl --fail -X POST \
  "$BQEMU_REST/bigquery/v2/projects/test-project/datasets/analytics/tables" \
  -H 'Content-Type: application/json' \
  -d '{"tableReference":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"label","type":"STRING"}]}}'
```

These requests use `bqemu.projects.create`, `bigquery.datasets.insert`, and
`bigquery.tables.insert`.

<!-- section: query -->
## Run The First Query

```bash
curl --fail -X POST \
  "$BQEMU_REST/bigquery/v2/projects/test-project/queries" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1 AS answer","useLegacySql":false,"location":"US"}'
```

This calls `bigquery.jobs.query`. The response is a BigQuery `QueryResponse`
shape with a job reference, schema, completion state, and encoded rows.

<!-- section: integrations -->
## Configure A Calling Process

Choose the REST and gRPC addresses from the endpoint table according to where
the calling process runs. Version-pinned setup examples, TLS settings,
operation IDs, and observed request sequences are maintained in the [integration
test guides](../../tests/integration/docs/en/index.md).

<!-- section: external-gcs -->
## Enable Parquet Load

BQEMU requires a GCS-compatible JSON endpoint for Parquet load jobs. The default
Compose project starts a fake GCS service and points BQEMU at it:

```bash
docker compose up --build -d --wait
curl --fail http://localhost:4443/storage/v1/b
```

The two endpoint settings serve different callers:

| Caller | Setting | Compose value |
| --- | --- | --- |
| BQEMU load worker | `BQEMU_LOAD_GCS_ENDPOINT` / `load.gcsEndpoint` | `http://fake-gcs:4443` |
| Process uploading Parquet objects | Uploader-specific endpoint | Select the fake-GCS address for its location from the endpoint table |

The producing process uploads temporary Parquet objects to fake GCS. BQEMU
lists matching objects when necessary, downloads object media, and commits the
load. Direct Storage Write does not use GCS. Tested uploader configurations are
kept in the [integration test guides](../../tests/integration/docs/en/index.md).

Parquet is the only supported load format. A caller can also use the BigQuery
media-upload endpoints; BQEMU stores completed media in the configured fake
GCS service and then submits the same `gs://` Parquet load path. Media sessions
are process-local and restart-invalid.

<!-- section: tls -->
## Enable TLS And Credential Fixtures

Generate a local CA, REST/gRPC server certificate, Java truststore, and client
credential files:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
mkdir -p data
export BQEMU_HOST_UID="$(id -u)"
export BQEMU_HOST_GID="$(id -g)"
docker compose -f compose.yaml -f compose.tls.yaml up --build -d --wait
```

The generated service-account, authorized-user, WIF, and direct-token files are
local fixtures. Follow [Client credentials and TLS](client-credentials-and-tls.md)
when a client must exchange a token or trust the generated CA.

<!-- section: devcontainer -->
## Connect From A Development Container

Use `host.docker.internal` when BQEMU runs on the host. On Linux, add a host
mapping for `host.docker.internal:host-gateway`. When BQEMU is a sibling Compose
service, use the service name `bqemu` instead.

Generate credential fixtures inside the container that uses them. This keeps
the absolute subject-token path in `wif.json` valid. For sibling-service TLS,
generate the certificate with `--tls-dns-name bqemu` and mount `.bqemu-auth`
read-only.

<!-- section: stop -->
## Stop BQEMU

```bash
docker compose down
```

To delete the persisted test state as well:

```bash
docker compose down --volumes
```
