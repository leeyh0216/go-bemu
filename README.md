<!-- doc-id: readme -->
<!-- lang: en -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu` is an experimental local BigQuery emulator for application and
connector tests. It exposes a BigQuery v2 REST endpoint and the BigQuery
Storage Read/Write gRPC services. DuckDB stores and executes physical data;
BQEMU owns the BigQuery-facing model and compatibility behavior.

It is not a production database, IAM implementation, or a complete BigQuery
replacement. Start with the supported paths below, and treat every other
BigQuery feature as unsupported until it is listed in the compatibility
contract.

The compatibility contract follows the [BigQuery REST v2
reference](https://cloud.google.com/bigquery/docs/reference/rest), the
[Storage RPC reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc),
and the pinned [Spark BigQuery connector `0.44.2`
source](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2).

<!-- section: status -->
## What You Can Use

The exercised public paths currently include:

- project, dataset, and table lifecycle through BigQuery v2 REST;
- synchronous `jobs.query`, process-local polling through `jobs.get` and
  `getQueryResults`, and a limited DuckDB-compatible SQL boundary;
- table schema updates that add top-level or nested fields, including fields in
  repeated records;
- catalog-synchronized `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD COLUMN`,
  and `ALTER TABLE RENAME COLUMN` statements through the query API;
- Storage Read sessions with Arrow or Avro rows, stream offsets, projection
  validation, and supported row restrictions;
- Storage Write `ProtoRows` through the default and `PENDING` streams;
- Spark connector `0.44.2` read paths and unpartitioned direct static
  overwrite that are covered by the repository contract;
- optional Parquet load jobs from the deliberately configured fake-GCS or
  `file://` adapters; and
- optional TLS for both REST and Storage gRPC.

Important limits:

- GoogleSQL is not implemented as a complete language. `ALTER COLUMN SET DATA
  TYPE`, `DROP COLUMN`, `TRUNCATE`, general `MERGE`, scripts, and many
  expressions are not available yet.
- Copy/extract jobs, non-Parquet loads, CDC, Arrow Storage Write rows, and
  several Storage Read/Write RPCs are not available. `tabledata.insertAll`
  supports its atomic profile (typed JSON rows and retry `insertId`); partial
  rows and template tables remain unsupported.
- No IAM, OAuth authorization, quota, billing, regional placement, or Google
  control-plane behavior is emulated.
- Project, dataset, table, and schema metadata are stored in BQEMU-owned
  SQLite state, while DuckDB stores physical tables and rows. Query and load
  job history is still process-local, and cross-store crash reconciliation is
  not complete yet.

See [Compatibility](docs/en/compatibility.md) for the precise, testable
contract and [Architecture](docs/en/architecture.md) for responsibility
boundaries.

<!-- section: quick-start -->
## Start With Docker Compose

Docker Compose is the simplest way to run the emulator locally. It builds the
image from this checkout, exposes the public APIs, creates a named `/data`
volume, and waits for `/readyz`.

```bash
git clone https://github.com/leeyh0216/go-bemu.git
cd go-bemu
docker compose up --build --wait
curl --fail --silent --show-error http://localhost:9050/readyz
```

Endpoints on the host:

| Surface | Address | Use |
| --- | --- | --- |
| BigQuery REST and health | `http://localhost:9050` | BigQuery v2 API, `/healthz`, `/readyz` |
| BigQuery Storage gRPC | `localhost:9060` | Storage Read and Storage Write clients |
| Admin | `127.0.0.1:9051` | Disabled by default; keep loopback-only |

`/healthz` confirms that the process is alive. `/readyz` confirms that the
required runtime dependencies are ready. Use `/readyz` for application startup
checks and test fixtures.

Stop the service without deleting its named data volume:

```bash
docker compose down
```

To remove the volume and start with an empty emulator, run:

```bash
docker compose down --volumes
```

`make docker-up`, `make docker-logs`, and `make docker-down` provide the same
workflow from this checkout.

<!-- section: image -->
## Use a Published GHCR Image

The container package name is `ghcr.io/leeyh0216/go-bemu`. The publishing
workflow creates multi-architecture Linux images for `amd64` and `arm64` with
these tags:

| Source | Published tags |
| --- | --- |
| Push to `main` | `edge`, `sha-<full-commit-sha>` |
| SemVer Git tag `vX.Y.Z` | `X.Y.Z`, `X.Y`, `latest`, `sha-<full-commit-sha>`; also `X` when `X > 0` |

`edge` follows `main`. `latest` follows the latest SemVer release, not `main`.
Use a release tag for interactive local work, then resolve it to a digest before
using it in shared CI or a long-lived test environment.

GHCR packages are private on first publication unless the package owner changes
their visibility in GitHub package settings. For a private package, authenticate
with a classic personal access token that has `read:packages` before pulling.
Anonymous pulls work only after the owner makes the package public.

```bash
export GHCR_TOKEN=... # classic token with read:packages when the package is private
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io --username <github-user> --password-stdin

export BQEMU_IMAGE=ghcr.io/leeyh0216/go-bemu:0.1.0
docker pull "$BQEMU_IMAGE"

# Prefer the digest reported by your approved image inventory or `docker inspect`.
export BQEMU_IMAGE=ghcr.io/leeyh0216/go-bemu@sha256:<digest>
docker compose up --no-build --wait bqemu
```

The checked-in Compose file reads `BQEMU_IMAGE`. With a digest-pinned value it
uses the published image instead of building the local default `go-bemu:dev`
when `--no-build` is present. Do not automate against the moving `edge` or
`latest` tags.

The same workflow publishes the shared console as
`ghcr.io/leeyh0216/bqemu-console` with matching tags. The compatibility lab
uses that image only through its optional `ui` profile.

<!-- section: compose -->
## Use It From Another Compose Service

Services in the same Compose project use Docker DNS, not `localhost`. Connect
to `http://bqemu:9050` for REST and `bqemu:9060` for Storage gRPC. Add the
following application service to a Compose file that includes this repository's
`compose.yaml`, or add equivalent settings to your existing project:

```yaml
services:
  app:
    build: .
    depends_on:
      bqemu:
        condition: service_healthy
    environment:
      BIGQUERY_REST_ENDPOINT: http://bqemu:9050
      BIGQUERY_STORAGE_GRPC_ENDPOINT: bqemu:9060
```

The checked-in `bqemu` service uses a named `bqemu-data` volume at `/data`.
For a host-visible directory instead, use a Compose override:

```yaml
services:
  bqemu:
    volumes:
      - ./bqemu-data:/data
```

Do not mount an individual database file. Mount the whole `/data` directory so
SQLite sidecar files and DuckDB data stay together. The persistent layout uses
`/data/bqemu-state.sqlite` for BQEMU metadata and `/data/bqemu.duckdb` for
physical rows. Keep the directory as one backup and restore unit.

<!-- section: dev-container -->
## Use It From a Dev Container

The repository does not require a Dev Container definition, but a consuming
project can run its workspace and BQEMU in the same Compose network. In the
consumer repository, create `.devcontainer/compose.yaml`:

```yaml
services:
  workspace:
    image: mcr.microsoft.com/devcontainers/go:1-1.26-bookworm
    volumes:
      - ..:/workspaces/app:cached
    working_dir: /workspaces/app
    command: sleep infinity

  bqemu:
    environment:
      BQEMU_PUBLIC_URL: http://bqemu:9050
```

Create `.devcontainer/devcontainer.json` next to it. The first Compose file in
the list is the `compose.yaml` that defines the `bqemu` service; use a relative
path to the checked-out BQEMU repository or copy that service into your own
Compose file.

```json
{
  "name": "app-with-bqemu",
  "dockerComposeFile": ["../../go-bemu/compose.yaml", "compose.yaml"],
  "service": "workspace",
  "workspaceFolder": "/workspaces/app",
  "shutdownAction": "stopCompose"
}
```

Inside the Dev Container, configure clients with `http://bqemu:9050` and
`bqemu:9060`. On the host, use `http://localhost:9050` and `localhost:9060`.
Wait for `http://bqemu:9050/readyz` before starting an integration test.

<!-- section: bootstrap -->
## Create a Project, Dataset, and Table

BQEMU projects are local emulator resources. Create one before calling the
BigQuery v2 dataset and table APIs:

```bash
export BQEMU_REST_ENDPOINT=http://localhost:9050
export BQEMU_PROJECT=demo-project

curl --fail --silent --show-error -X POST "$BQEMU_REST_ENDPOINT/bqemu/v1/projects" \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"demo-project"}'

curl --fail --silent --show-error -X POST \
  "$BQEMU_REST_ENDPOINT/bigquery/v2/projects/$BQEMU_PROJECT/datasets" \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"datasetId":"analytics"},"location":"US"}'

curl --fail --silent --show-error -X POST \
  "$BQEMU_REST_ENDPOINT/bigquery/v2/projects/$BQEMU_PROJECT/datasets/analytics/tables" \
  -H 'Content-Type: application/json' \
  -d '{
    "tableReference":{"tableId":"events"},
    "schema":{"fields":[
      {"name":"event_id","type":"INT64","mode":"REQUIRED"},
      {"name":"name","type":"STRING","mode":"NULLABLE"}
    ]}
  }'
```

Submitting a limited query uses the normal BigQuery v2 resource path:

```bash
curl --fail --silent --show-error -X POST \
  "$BQEMU_REST_ENDPOINT/bigquery/v2/projects/$BQEMU_PROJECT/queries" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT event_id, name FROM `demo-project.analytics.events`","useLegacySql":false}'
```

<!-- section: clients -->
## Configure Clients

BQEMU does not authorize bearer tokens. Some Google client libraries still
require a credentials object before they make a request. Use a local fixture or
an anonymous credential as appropriate for the client; the server does not
validate the token value.

### Python BigQuery Client

The official Python client accepts anonymous credentials and an explicit REST
endpoint:

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

client = bigquery.Client(
    project="demo-project",
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:9050"),
)
table = client.get_table("demo-project.analytics.events")
```

### bq CLI

The `bq` CLI validates its own option set before issuing a request. Supply any
non-empty local token and disable the active gcloud configuration:

```bash
bq --api=http://localhost:9050 \
  --project_id=demo-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-bqemu-token \
  ls
```

### Spark BigQuery Connector

Use separate HTTP and Storage gRPC options. From a container in the same
Compose network, use `bqemu` as the hostname:

```python
df = (
    spark.read.format("bigquery")
    .option("table", "demo-project.analytics.events")
    .option("parentProject", "demo-project")
    .option("billingProject", "demo-project")
    .option("project", "demo-project")
    .option("bigQueryHttpEndpoint", "http://bqemu:9050")
    .option("bigQueryStorageGrpcEndpoint", "bqemu:9060")
    .option("gcpAccessToken", "local-bqemu-token")
    .load()
)
```

For a Spark process on the host, use `http://localhost:9050` and
`localhost:9060`. Connector support is version-specific; the current tested
contract is `0.44.2`.

<!-- section: credentials -->
## Local Credential Files

The emulator intentionally has no authentication or IAM subsystem. It accepts
requests without `Authorization` and never issues or validates Google access
tokens. Credential files exist only to satisfy client-side validation and token
acquisition flows.

Generate a complete local fixture directory and run the issuer in another
terminal:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
go run ./cmd/bqemu-auth-fixture serve \
  --manifest .bqemu-auth/manifest.json \
  --listen 127.0.0.1:9052
```

When you consume only the GHCR image, extract the statically linked helper and
run it on the host. Running it on the host keeps the WIF subject-token path and
the loopback-only TLS issuer valid for host clients:

```bash
fixture_container="$(docker create "$BQEMU_IMAGE")"
docker cp "$fixture_container:/usr/local/bin/bqemu-auth-fixture" ./bqemu-auth-fixture
docker rm "$fixture_container"
chmod 0755 ./bqemu-auth-fixture

./bqemu-auth-fixture generate --output .bqemu-auth
./bqemu-auth-fixture serve \
  --manifest .bqemu-auth/manifest.json \
  --listen 127.0.0.1:9052
```

Generation writes `manifest.json`, `ca.pem`, `server.pem`, `server-key.pem`,
`service-account.json`, `authorized-user.json`, `wif.json`, and
`subject-token.txt`. The issuer serves `/healthz`, service-account and
authorized-user token exchange at <https://localhost:9052/oauth/token>, and
WIF token exchange at <https://localhost:9052/sts/token>.

Choose one client credential file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.bqemu-auth/service-account.json"
# or: authorized-user.json
# or: wif.json
export REQUESTS_CA_BUNDLE="$PWD/.bqemu-auth/ca.pem"
export SSL_CERT_FILE="$PWD/.bqemu-auth/ca.pem"
```

For Java-based clients, import `ca.pem` into the Java trust store and set
`-Djavax.net.ssl.trustStore=/path/to/truststore`. The fixture issuer is local
client test support only. It never protects BQEMU endpoints, and its issued
token is intentionally not an authorization credential for another service.
Do not use real Google credentials in local emulator configuration, Compose
files, or Dev Container mounts.

<!-- section: tls -->
## TLS

TLS protects both REST and gRPC. Generate a local certificate whose subject
alternative names cover the hostnames that clients use:

```bash
mkdir -p certs
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 30 \
  -keyout certs/server-key.pem -out certs/server.pem \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,DNS:bqemu,IP:127.0.0.1'
```

For a local process:

```bash
export BQEMU_TLS_CERT_FILE="$PWD/certs/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/certs/server-key.pem"
export BQEMU_PUBLIC_URL=https://localhost:9050
make run
```

The generated local credential fixture can supply the same certificate pair:

```bash
export BQEMU_TLS_CERT_FILE="$PWD/.bqemu-auth/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/.bqemu-auth/server-key.pem"
export BQEMU_PUBLIC_URL=https://localhost:9050
make run
```

For Compose, add a read-only certificate mount and the same environment values
in an override file:

```yaml
services:
  bqemu:
    environment:
      BQEMU_TLS_CERT_FILE: /run/bqemu-tls/server.pem
      BQEMU_TLS_KEY_FILE: /run/bqemu-tls/server-key.pem
      BQEMU_PUBLIC_URL: https://localhost:9050
    volumes:
      - ./certs:/run/bqemu-tls:ro
```

Clients must trust the certificate issuer and connect through a SAN name. TLS
does not add token validation or IAM behavior.

<!-- section: configuration -->
## Configuration and Persistence

The checked-in [configuration](configs/bqemu.yaml) is the reference for all
settings. Supply a different file with `--config`, `BQEMU_CONFIG`, or supported
`BQEMU_*` environment overrides. In Compose, `BQEMU_PUBLIC_URL` should match
the address visible to the client that consumes discovery documents.

The default Compose configuration mounts `/data` as `bqemu-data`. Preserve that
volume to keep local state across container recreation. When BQEMU SQLite state
is enabled, the directory contains both canonical metadata and DuckDB data; do
not retain one without the other.

<!-- section: troubleshooting -->
## Troubleshooting

| Symptom | Check |
| --- | --- |
| Client cannot connect | Confirm `curl http://localhost:9050/readyz` succeeds and that the published HTTP port is not in use. |
| Container client calls `localhost` | Use `http://bqemu:9050` and `bqemu:9060` inside the Compose network. |
| REST works but Spark Storage calls fail | Verify the separate `bigQueryStorageGrpcEndpoint` value and port `9060`. Do not put `http://` in the gRPC endpoint. |
| TLS handshake fails | Trust the local CA and use a hostname listed in the certificate SAN. For a self-signed certificate, configure the client trust store explicitly. |
| Data unexpectedly disappears | Check the `/data` mount. `docker compose down --volumes` deliberately deletes the named volume. |
| SQL returns `invalidQuery` or unsupported behavior | Compare the statement with the limited compatibility contract; this is not full GoogleSQL. |
| A Google client attempts real OAuth | Use the repository's local credential fixture or that client's anonymous credential mode. Never point local tests at production credentials. |

<!-- section: documentation -->
## Documentation

- [Documentation index](docs/en/index.md)
- [Compatibility contract](docs/en/compatibility.md)
- [Architecture](docs/en/architecture.md)
- [Configuration and operations](docs/en/operations.md)
- [Schema evolution and CDC](docs/en/schema-evolution-cdc.md)
- [Maintainer guide](docs/en/maintainer-guide.md)
- [Contributing](CONTRIBUTING.md)

<!-- section: development -->
## Build From Source

For local development, Go 1.26+ and the C/C++ toolchain required by the DuckDB
Go driver are needed:

```bash
make setup
make check
make run
```

Run `make test` for the Go suite. The Python, `bq`, and Spark contracts have
their own pinned client prerequisites; see the maintainer guide before running
them.

<!-- section: non-goals -->
## Non-Goals

Do not use `go-bemu` for production data, performance prediction, authorization
tests, quota or billing tests, or proof of GoogleSQL equivalence. A successful
local test demonstrates only the documented emulator contract and client
version.
