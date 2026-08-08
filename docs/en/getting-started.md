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
```

Compose stores the catalog and table data in the `bqemu-data` volume. Keeping
that volume across restarts is the user's responsibility.

<!-- section: endpoints -->
## Choose The Endpoint

| Client location | REST | Storage gRPC |
| --- | --- | --- |
| Host running Compose | `http://localhost:9050` | `localhost:9060` |
| Sibling Compose service | `http://bqemu:9050` | `bqemu:9060` |
| Development container, BQEMU on host | `http://host.docker.internal:9050` | `host.docker.internal:9060` |

REST clients use only the REST endpoint. Spark reads and direct writes require
both endpoints.

<!-- section: resources -->
## Create A Project, Dataset, And Table

BQEMU projects are emulator resources, so create one before calling BigQuery
v2 methods:

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
  -d '{"tableReference":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"label","type":"STRING"}]}}'
```

These requests use `bqemu.projects.create`, `bigquery.datasets.insert`, and
`bigquery.tables.insert`.

<!-- section: query -->
## Run The First Query

```bash
curl --fail -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1 AS answer","useLegacySql":false,"location":"US"}'
```

This calls `bigquery.jobs.query`. The response is a BigQuery `QueryResponse`
shape with a job reference, schema, completion state, and encoded rows.

<!-- section: clients -->
## Connect Another Process

Select the guide for the process sending requests:

- [Python BigQuery client](clients/python-bigquery.md)
- [`bq` CLI](clients/bq-cli.md)
- [PySpark and Scala Spark](clients/spark-bigquery-connector.md)

Each guide distinguishes host, sibling Compose service, and development
container endpoints. For TLS, configure the process trust store with the
generated CA and use a server name covered by the certificate.

<!-- section: external-gcs -->
## Use Parquet Load

The BQEMU binary does not embed an object-store server. The required fake-GCS
service is part of the default Compose project and BQEMU resolves every load
source through that service:

```bash
docker compose up --build -d --wait
curl --fail http://localhost:4443/storage/v1/b
```

Choose the endpoint from the process that runs BQEMU:

| BQEMU process location | `load.gcsEndpoint` value |
| --- | --- |
| Host running Compose | `http://127.0.0.1:4443` |
| `bqemu` service in the supplied Compose project | `http://fake-gcs:4443` |
| Development container attached to the same Compose network | `http://fake-gcs:4443` |
| Development container reaching Compose through its host | `http://host.docker.internal:4443` |

The checked-in host configuration uses the loopback endpoint; Compose overrides
it with the `fake-gcs` service DNS name. Load requests accept only `gs://`
object URIs. Local paths, `file://`, and other URI schemes are rejected before a
job is stored or an object-store request is made.

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
