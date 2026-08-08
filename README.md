<!-- doc-id: readme -->
<!-- lang: en -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu` is an experimental, from-scratch BigQuery-compatible local service in
Go. DuckDB is an outbound execution adapter; it is not the domain model. The
service currently implements limited BigQuery v2 metadata/query/load paths and
public partial Storage Read and Write gRPC data planes. It is not a production
database or a drop-in BigQuery replacement.

The compatibility contract follows the [BigQuery REST v2
reference](https://cloud.google.com/bigquery/docs/reference/rest), the
[Storage RPC reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc),
and the exact [Spark BigQuery connector `0.44.2`
source](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2).
This repository does not vendor or clone the earlier emulator; comparison notes
pin the exact [goccy BigQuery emulator
`v0.8.1` source](https://github.com/goccy/bigquery-emulator/tree/v0.8.1).

<!-- section: status -->
## Current Status

Implemented and exercised through repository tests:

- liveness plus DuckDB-backed readiness;
- emulator project lifecycle and dataset/table create, get, list, patch, update,
  and delete, including ETag preconditions and metadata pagination;
- additive top-level/nested schema changes with transactional DuckDB DDL,
  including fields inside repeated records, exercised by the official [Python
  client `3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/);
- synchronous `jobs.query` and process-local `jobs.insert` polling through
  `jobs.get`/`getQueryResults`, exercised by the official [Python client
  `3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/), including
  terminal `invalidQuery` mapping;
- project/dataset/table/query/job and additive-schema flows exercised by the
  official [`bq` CLI `2.1.31`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)
  from [Google Cloud SDK `566.0.0`](https://cloud.google.com/sdk/docs/release-notes#56600_2026-04-28);
- isolated physical DuckDB schemas and a deliberately small SQL translation
  boundary;
- a source-derived connector `0.44.2` static-overwrite token adapter from
  [`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)
  that maps the constant-false [BigQuery `MERGE`](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
  to one atomic [DuckDB `MERGE INTO`](https://duckdb.org/docs/current/sql/statements/merge_into);
- public Storage Read sessions backed by one bounded DuckDB snapshot, Arrow or
  Avro encoding, projection/restriction validation, logical stream ranges, and
  offset resume;
- public Storage Write `ProtoRows` paths for PENDING streams and the default
  stream, including exact offsets, finalization, atomic batch commit, multiple
  logical streams, weighted request admission, bounded hidden DuckDB staging,
  and a serialized DuckDB backend;
- opt-in load jobs through a bounded fake-GCS-compatible JSON adapter or an
  explicitly enabled `file://` adapter, with Parquet staging and atomic
  `WRITE_APPEND`, `WRITE_EMPTY`, and `WRITE_TRUNCATE` for an existing table;
- optional REST and gRPC TLS termination;
- strict versioned configuration, optional protected admin composition, bounded
  multi-listener shutdown, and a hardened non-root Compose profile;
- protocol profiles and redacted boundary observability.

Important limits:

- durable metadata, row insert/preview, copy/extract jobs, and full GoogleSQL are
  not implemented;
- unpartitioned direct static overwrite is verified with Spark `3.5.8` and
  connector `0.44.2`; dynamic time/range partition overwrite and general
  BigQuery `MERGE` parity are gaps;
- Storage Read remains partial: `SplitReadStream`, response compression,
  historical `snapshot_time`, restart-durable sessions, and nested-field
  projection are gaps;
- Storage Write remains partial: CDC, Arrow rows, BUFFERED and explicitly
  created COMMITTED streams, `FlushRows`, default-value expressions, and
  restart-durable pending staging are gaps;
- load remains partial: non-Parquet formats, missing-table `CREATE_IF_NEEDED`,
  schema-update options, autodetect, and multipart/resumable transfer are gaps;
- BigQuery-compatible REST and gRPC endpoints do not authenticate callers;
  client credentials and IAM are not emulated;
- canonical BigQuery metadata can outlive neither the process nor the in-memory
  repositories, even if a DuckDB file retains table data.

The detailed and testable status vocabulary is in
[Compatibility](docs/en/compatibility.md). BigQuery's own job resources and
polling contract are defined by [`jobs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs)
and [`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults).

<!-- section: architecture -->
## Architecture

```text
REST / gRPC inbound adapters
            |
            v
     application use cases
            |
            v
 domain model + outbound ports
            ^
            |
DuckDB / memory / object-store / system adapters
```

Domain and application packages do not depend on DuckDB, HTTP, gRPC, or Google
DTOs. The design follows a replaceable adapter boundary; DuckDB transactions and
SQL semantics remain DuckDB behavior unless an application use case explicitly
adds BigQuery semantics. See [Architecture](docs/en/architecture.md) and
[ADR-0001](docs/en/adr/0001-duckdb-behind-warehouse-port.md). The physical engine
contract is documented by [DuckDB SQL
introduction](https://duckdb.org/docs/stable/sql/introduction).

<!-- section: quick-start -->
## Quick Start

Requirements are Go 1.26+, a C/C++ toolchain for the DuckDB Go driver, and
optionally Docker. The `bq` contract additionally requires the exact CLI version
installed by Google Cloud SDK `566.0.0`.

```bash
make test
make run
```

Default endpoints:

| Surface | Address |
| --- | --- |
| BigQuery REST and health | `http://localhost:9050` |
| BigQuery Storage gRPC | `localhost:9060` |

Create the emulator-only project before using BigQuery v2 resources:

```bash
curl -sS -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project"}'

curl -sS -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/datasets \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"datasetId":"analytics"},"location":"US"}'
```

Dataset JSON is modeled after the official
[`datasets.insert`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert)
resource. Unsupported request fields may be decoded but do not gain semantics.

<!-- section: query-example -->
## Query Example

After creating a table, submit Standard SQL through `jobs.query`:

```bash
curl -sS -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"SELECT * FROM `test-project.analytics.inventory` ORDER BY id",
    "useLegacySql":false
  }'
```

This executes a DuckDB-compatible subset. It does not imply compatibility with
the [GoogleSQL query syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax),
functions, scripts, optimizer, billing, or distributed execution model.

<!-- section: maintainer-onboarding -->
## Maintainer Onboarding

The shortest repeatable path from clone to a verified change is:

```bash
direnv allow              # optional; checked-in .envrc contains no credentials
make check
make run
```

The checked-in `.envrc` loads safe defaults from `.envrc.example` and then an
optional ignored `.envrc.local`; only the local file may contain machine-specific
non-production overrides, and neither file should contain credentials. Without
direnv, run `make check` and `make run` directly; Make supplies the documented
defaults.
Then follow the ordered [maintainer guide](docs/en/maintainer-guide.md): read the
architecture, run one public request, run the first focused test, add a protocol
or client version through the compatibility pipeline, and diagnose drift from a
structured report. The service contracts remain the official [BigQuery REST
reference](https://cloud.google.com/bigquery/docs/reference/rest) and [Storage
RPC reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).

<!-- section: tls -->
## TLS

Set both files to enable TLS on REST and gRPC:

```bash
export BQEMU_TLS_CERT_FILE="$PWD/certs/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/certs/server-key.pem"
export BQEMU_PUBLIC_URL="https://localhost:9050"
make run
```

The client must trust the issuing CA and connect with a hostname in the
certificate SAN. TLS only protects transport; it does not add the token
acquisition or IAM semantics described by [Google Cloud
authentication](https://cloud.google.com/docs/authentication).

<!-- section: documentation -->
## Documentation

- [Documentation index](docs/en/index.md)
- [Architecture](docs/en/architecture.md)
- [BigQuery and connector internals](docs/en/bigquery-internals.md)
- [Compatibility contract](docs/en/compatibility.md)
- [Schema evolution and CDC](docs/en/schema-evolution-cdc.md)
- [Maintainer guide and runbooks](docs/en/maintainer-guide.md)
- [Configuration and operations](docs/en/operations.md)
- [Architecture decisions](docs/en/adr/)
- [Contributing](CONTRIBUTING.md)

All maintainer documentation has an English and Korean counterpart. Repository
tests reject missing counterparts, section drift, unpaired primary-source links,
and mutable upstream `master`/`main` source links.

<!-- section: development -->
## Development

```bash
make format
make test
make vet
make build
make python-test
make bq-test
```

Consumers build this repository directly; they do not clone or rebuild another
emulator. Protocol code uses the official generated Google Storage API package,
whose canonical methods and messages are listed in the [Storage RPC
reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1).

<!-- section: non-goals -->
## Non-goals

Do not use `go-bemu` for performance prediction, IAM validation, quota or billing
tests, regional placement, production durability, or proof of GoogleSQL
equivalence. A local compatibility result is evidence only for the explicitly
listed contract and version.
