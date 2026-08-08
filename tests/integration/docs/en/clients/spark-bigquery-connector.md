<!-- doc-id: clients/spark-bigquery-connector -->
<!-- lang: en -->

[English](spark-bigquery-connector.md) | [한국어](../../ko/clients/spark-bigquery-connector.md)

# PySpark And Scala Spark

The required runtime is Spark `3.5.8`, Scala `2.12.18` (binary `2.12`), Java
`17`, and Spark BigQuery Connector `0.44.2`. Connector-specific behavior is
bound to the reviewed [connector source
revision](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92).

<!-- section: endpoints -->
## REST And Storage Endpoints

Pass both endpoints for table reads and direct writes:

```text
parentProject=test-project
billingProject=test-project
project=test-project
bigQueryHttpEndpoint=http://localhost:9050
bigQueryStorageGrpcEndpoint=localhost:9060
gcpAccessToken=local-test-token
```

`bigQueryHttpEndpoint` carries table metadata and job requests.
`bigQueryStorageGrpcEndpoint` carries Storage Read and direct Storage Write
RPCs. A sibling Compose service uses `bqemu:9050` and `bqemu:9060`; a
development container connecting to the host uses `host.docker.internal`.

<!-- section: credentials -->
## Credentials And TLS

Use `gcpAccessToken` for the shortest local setup. To exercise client credential
parsing, replace it with an absolute `credentialsFile` path to a generated
service-account, authorized-user, or WIF JSON file.

For TLS, generate the repository fixtures and add the PKCS12 truststore before
starting PySpark or `spark-shell`:

```bash
export AUTH_DIR="$PWD/.bqemu-auth"
export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=$AUTH_DIR/truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=PKCS12"
```

Use HTTPS on `localhost` port `9050` for REST and keep `localhost:9060` for the
gRPC option. JSON credentials that exchange a token also need the issuer and proxy
from [Client credentials and TLS](../../../../../docs/en/client-credentials-and-tls.md).

<!-- section: pyspark -->
## PySpark Read

Start PySpark with the connector JAR, create a `SparkSession`, and load a fully
qualified table:

```python
options = {
    "parentProject": "test-project",
    "billingProject": "test-project",
    "project": "test-project",
    "bigQueryHttpEndpoint": "http://localhost:9050",
    "bigQueryStorageGrpcEndpoint": "localhost:9060",
    "gcpAccessToken": "local-test-token",
}

rows = (
    spark.read.format("bigquery")
    .options(**options)
    .option("readDataFormat", "ARROW")
    .load("test-project.analytics.events")
    .collect()
)
```

<!-- section: scala -->
## Scala Spark Read

The Scala entrypoint uses the same options:

```scala
val options = Map(
  "parentProject" -> "test-project",
  "billingProject" -> "test-project",
  "project" -> "test-project",
  "bigQueryHttpEndpoint" -> "http://localhost:9050",
  "bigQueryStorageGrpcEndpoint" -> "localhost:9060",
  "gcpAccessToken" -> "local-test-token"
)

val rows = spark.read
  .format("bigquery")
  .options(options)
  .option("readDataFormat", "ARROW")
  .load("test-project.analytics.events")
  .collect()
```

Create the project, dataset, and table by following [Getting
started](../../../../../docs/en/getting-started.md).

<!-- section: direct -->
## Direct Read And Write Calls

The normalized cases are
`spark-pyspark-3.5.8-connector-0.44.2` and
`spark-scala-3.5.8-connector-0.44.2`. Their public scenario IDs are
`spark-pyspark-public-edge` and `spark-scala-public-edge`.

| Flow | Operation order |
| --- | --- |
| Table read | `bigquery.tables.get` -> `grpc.bigquery-read.create-read-session` -> one or more `grpc.bigquery-read.read-rows` streams |
| Pending direct append | `grpc.bigquery-write.create-write-stream` -> one or more `grpc.bigquery-write.append-rows` calls -> `grpc.bigquery-write.finalize-write-stream` -> `grpc.bigquery-write.batch-commit-write-streams` |
| Static direct overwrite | `bigquery.tables.insert` creates a temporary table; pending-stream operations write it; `bigquery.jobs.insert`, `bigquery.jobs.get`, and `bigquery.jobs.getQueryResults` execute and observe the overwrite query; `bigquery.tables.delete` removes the temporary table. |

Storage Read uses Arrow or Avro responses. Direct Storage Write sends
`ProtoRows`; it does not use GCS. The Storage RPC messages follow the public
[BigQuery Storage RPC reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).
The PySpark scenario can also call `grpc.bigquery-write.get-write-stream` to
inspect committed stream state.

<!-- section: indirect -->
## Indirect Parquet Write

Start the optional fake GCS Compose profile:

```bash
docker compose -f compose.yaml -f compose.load.yaml up --build -d --wait
```

Include the Spark BigQuery Connector and Hadoop GCS Connector JARs, then add the
Hadoop endpoint settings:

```python
spark = (
    SparkSession.builder
    .config("spark.jars", "/opt/jars/spark-bigquery.jar,/opt/jars/gcs-connector.jar")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "false")
    .config("spark.hadoop.fs.gs.auth.null.enable", "true")
    .config("spark.hadoop.fs.gs.storage.root.url", "http://localhost:4443")
    .config("spark.hadoop.fs.gs.storage.service.path", "storage/v1/")
    .getOrCreate()
)

(frame.write.format("bigquery")
 .options(**options)
 .option("writeMethod", "indirect")
 .option("intermediateFormat", "parquet")
 .option("temporaryGcsBucket", "bqemu-temporary")
 .mode("append")
 .save("test-project.analytics.events"))
```

The Compose configuration passes `http://fake-gcs:4443` to BQEMU through
`BQEMU_LOAD_GCS_ENDPOINT`. Spark uses `http://localhost:4443` because it runs in
the host network namespace. These values address the same fake GCS service from
different callers.

The `spark-pyspark-indirect-load` and `spark-scala-indirect-load` scenarios run
this sequence:

1. Spark writes temporary Parquet objects through the Hadoop GCS Connector.
2. The BigQuery connector calls `bigquery.tables.get` for the destination.
3. It submits one `bigquery.jobs.insert` load job with exact `gs://` source URIs.
4. BQEMU lists objects when the URI requires expansion and downloads object
   metadata and media through its outbound GCS JSON adapter.
5. BQEMU applies the load transaction, and the connector polls
   `bigquery.jobs.get` until the job is terminal.
6. Spark removes temporary objects through the Hadoop GCS Connector.

The fake service implements the subset of the public [Cloud Storage JSON
API](https://cloud.google.com/storage/docs/json_api/v1/objects) used by this
flow. It is not embedded in BQEMU.

<!-- section: shapes -->
## Request And Response Shapes

| Operation | Request | Response used by the connector |
| --- | --- | --- |
| `bigquery.tables.get` | REST table path | Table metadata and schema |
| `grpc.bigquery-read.create-read-session` | Parent project, table, data format, selected fields, row restriction, stream count | ReadSession schema and stream names |
| `grpc.bigquery-read.read-rows` | Read stream name and offset | Arrow or Avro row batches with row counts |
| `grpc.bigquery-write.create-write-stream` | Parent table and `PENDING` stream type | WriteStream name and state |
| `grpc.bigquery-write.append-rows` | Stream name, writer schema, offset, and `ProtoRows` | Append offset or row errors |
| `grpc.bigquery-write.finalize-write-stream` | Stream name | Final row count |
| `grpc.bigquery-write.batch-commit-write-streams` | Parent table and finalized stream names | Commit time and stream errors |
| `bigquery.jobs.insert` for indirect load | Job with Parquet `configuration.load`, source URIs, destination, and write disposition | Job resource and reference |

The exact accepted fields and support level are maintained in
[Compatibility](../../../../../docs/en/compatibility.md). Runtime versions, artifacts, and scenario
selectors are generated in [Consumer compatibility](../consumer-compatibility.md).

<!-- section: related -->
## Related Work

Behavior outside these flows is tracked in the compatibility documents and
issues [#5](https://github.com/leeyh0216/go-bemu/issues/5),
[#6](https://github.com/leeyh0216/go-bemu/issues/6),
[#7](https://github.com/leeyh0216/go-bemu/issues/7), and
[#8](https://github.com/leeyh0216/go-bemu/issues/8).
