# Client Compose Labs

Each directory starts BQEMU together with one client-facing tool image. Run a
lab from its directory:

```bash
docker compose -f ../../../compose.yaml -f compose.yaml up --build --wait
```

The client containers use the shared Compose-network names
`http://bqemu:9050` (REST) and `bqemu:9060` (Storage gRPC). The host uses the
published `http://localhost:9050` and `localhost:9060` endpoints instead.
Stop a lab with the same two Compose files and `down`.

- `python/` supplies the REST and Storage endpoint environment for a Python
  BigQuery client.
- `spark/` supplies the Spark BigQuery connector endpoint environment.
- `trino/` supplies a network-ready Trino shell. Install and configure a
  connector that supports the documented BigQuery endpoints separately.
- `aws/` supplies an AWS CLI shell for inspecting an external S3-compatible
  object store used by indirect load tests; AWS APIs are not emulated.

These labs deliberately add no client-specific BQEMU service or code path.
