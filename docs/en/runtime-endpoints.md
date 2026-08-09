<!-- doc-id: runtime-endpoints -->
<!-- lang: en -->

[English](runtime-endpoints.md) | [한국어](../ko/runtime-endpoints.md)

# Runtime Endpoints And Client Labs

<!-- section: endpoint-matrix -->
## Choose Endpoints By Execution Location

Use the addresses in this table exactly as written. `bqemu` is the service DNS
name on the Compose network; it is not resolvable from the host.

| Caller location | BigQuery REST | Storage Read/Write gRPC | External fake GCS for load jobs |
| --- | --- | --- | --- |
| Host process with `docker compose up` | `http://localhost:9050` | `localhost:9060` | `http://127.0.0.1:4443` |
| Sibling service in the same Compose project | `http://bqemu:9050` | `bqemu:9060` | `http://fake-gcs:4443` |
| Development container that joins the Compose network | `http://bqemu:9050` | `bqemu:9060` | `http://fake-gcs:4443` |
| Development container connecting to a host-run BQEMU | `http://host.docker.internal:9050` | `host.docker.internal:9060` | the host-published fake-GCS address |

The default Compose project starts fake GCS before BQEMU. Start both with:

```bash
docker compose up --build --wait
```

Load input is always a `gs://` object URI; see [Compatibility](compatibility.md)
before relying on a load format or source shape.

<!-- section: tls-paths -->
## TLS And Credential Paths

The host CA file is a host filesystem path, for example
`$PWD/.bqemu-auth/ca.pem`. A container cannot read that path unless it is
mounted into the container. Mount the directory read-only and point the
client-specific trust setting at the mount path, such as `/certs/ca.pem` for
Python or `/certs/truststore.p12` for a Java client.

The endpoint remains location-specific when TLS is enabled: use `https` for
the REST address in the table, and retain the corresponding host name for
Storage gRPC. Generate the local issuer, certificates, and truststore using
the [client credentials and TLS guide](client-credentials-and-tls.md).

<!-- section: client-labs -->
## Start A Client Lab

The Compose overrides in [`examples/clients/`](../../examples/clients/README.md)
make the in-network addresses observable without changing BQEMU. From one of
the lab directories, run:

```bash
docker compose -f ../../../compose.yaml -f compose.yaml up --build --wait
```

Choose `python`, `spark`, `trino`, or `aws`. The images intentionally expose
only their environment and a shell/readiness command. Configure each real
client with its documented endpoint options:

| Client | BQEMU API boundary | Follow-up guide |
| --- | --- | --- |
| Python BigQuery | REST, plus Storage gRPC when its API uses it | [Python BigQuery client](clients/python-bigquery.md) |
| Spark BigQuery connector | REST plus Storage Read/Write gRPC | [PySpark and Scala Spark](clients/spark-bigquery-connector.md) |
| Trino | A separately configured connector that supports BigQuery REST and Storage | [Compatibility](compatibility.md) |
| AWS CLI / SDK | External object store for supported indirect loads; not AWS APIs | [Load and object-storage support](compatibility.md#load-object-storage-and-public-access) |

BQEMU has no client-name-dependent runtime branch. The authoritative public
boundaries are the [BigQuery REST v2 reference](https://cloud.google.com/bigquery/docs/reference/rest)
and the [BigQuery Storage RPC reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).
