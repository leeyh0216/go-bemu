<!-- doc-id: client-interactions -->
<!-- lang: en -->

[English](client-interactions.md) | [한국어](../ko/client-interactions.md)

# Client Interaction Guide

Use this page to choose the BQEMU endpoint and the public API surface a client
will exercise. Host processes use `http://localhost:9050` and `localhost:9060`;
services in the same Compose network use `http://bqemu:9050` and `bqemu:9060`.

<!-- section: matrix -->
## Client Matrix

| Client | Configure | BQEMU interaction | Start here |
| --- | --- | --- | --- |
| Python `google-cloud-bigquery` | `ClientOptions(api_endpoint=...)`, anonymous/local credentials | REST `datasets`, `tables`, `jobs.query`, `jobs.*`, `tabledata.list`, `tabledata.insertAll`, and Parquet media upload where supported | `tests/integration/` |
| Spark BigQuery connector | `httpTransport`, `bigQueryStorageGrpcEndpoint`, project/dataset | REST jobs/catalog plus Storage Read/Write gRPC | `tests/spark/` |
| Trino | Use BQEMU only through a connector/catalog that supports BigQuery REST and Storage endpoints; no Trino-specific server branch exists | The same REST and Storage APIs; validate its connector independently | Compatibility table before use |
| AWS SDK / boto3 | Configure only an S3-compatible fake-GCS/object-store endpoint when using indirect Parquet loads | Object source resolution plus BigQuery load-job REST; AWS APIs are not emulated | Load compatibility and object-store configuration |
| `bq` CLI | explicit REST endpoint and local credentials | REST catalog and query APIs | `tests/bqcli/` |

<!-- section: workflow -->
## Repeatable Workflow

1. Start `docker compose up --build --wait`.
2. Create the emulator project at `POST /bqemu/v1/projects`, then dataset/table
   resources through BigQuery v2 REST.
3. Set host or Compose-network endpoints exactly as shown above.
4. Use the [Compatibility](compatibility.md) table to confirm the requested
   operation is supported before treating a client failure as an emulator bug.

No BQEMU product path branches on a client name. Client-specific directories
contain only executable examples and integration evidence.

The public boundary is the official [BigQuery REST v2
reference](https://cloud.google.com/bigquery/docs/reference/rest) and
[BigQuery Storage RPC reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).
