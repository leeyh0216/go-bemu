<!-- doc-id: getting-started -->
<!-- lang: en -->

[English](getting-started.md) | [한국어](../ko/getting-started.md)

# Getting Started

This guide starts BQEMU for local client and connector tests. For the exact
implemented API surface, see [Compatibility](compatibility.md). For credential
files and local certificates, see [Local client credentials and TLS](client-credentials-and-tls.md).

<!-- section: compose -->
## Docker Compose

Start BQEMU with its default REST and Storage gRPC listeners:

```bash
docker compose up --build -d --wait
curl --fail http://localhost:9050/readyz
```

The named `bqemu-data` volume owns `/data`. Keep or replace that volume as part
of your test environment policy. `docker compose down --volumes` removes it.

Create a local project before creating BigQuery resources:

```bash
curl --fail -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project"}'
```

<!-- section: external-gcs -->
## Parquet Load And External GCS

BQEMU does not contain a GCS server. The optional load adapter reads `gs://`
objects from the configured GCS JSON API endpoint. The following command starts
one digest-pinned fake GCS service beside BQEMU:

```bash
docker compose -f compose.yaml -f compose.load.yaml up --build -d --wait
curl --fail http://localhost:9050/readyz
curl --fail http://localhost:4443/storage/v1/b
```

The two configuration boundaries are independent:

| Caller | Endpoint setting | Compose value |
| --- | --- | --- |
| BQEMU load worker | `load.gcsEndpoint` or `BQEMU_LOAD_GCS_ENDPOINT` | `http://fake-gcs:4443` |
| Spark Hadoop GCS Connector | `fs.gs.storage.root.url` | `http://localhost:4443` |

They resolve to the same Compose service from different network namespaces.
The seeded bucket is `bqemu-temporary`. Direct Storage Write does not use GCS.

For Spark indirect writes, include the reviewed shaded Hadoop GCS Connector and
set both the Spark BigQuery Connector and Hadoop options:

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

(df.write.format("bigquery")
 .option("table", "test-project.analytics.events")
 .option("project", "test-project")
 .option("parentProject", "test-project")
 .option("bigQueryHttpEndpoint", "http://localhost:9050")
 .option("gcpAccessToken", "local-test-token")
 .option("writeMethod", "indirect")
 .option("intermediateFormat", "parquet")
 .option("temporaryGcsBucket", "bqemu-temporary")
 .mode("append")
 .save())
```

The fake GCS setting above is only for isolated local tests. The public GCS JSON
API contract is documented by [Cloud Storage JSON API](https://cloud.google.com/storage/docs/json_api/v1/objects).

<!-- section: python -->
## Python BigQuery Client

The client requires a credential object even though BQEMU does not authenticate
public requests:

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

client = bigquery.Client(
    project="test-project",
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:9050"),
)
```

Use the generated credential fixtures instead when a library insists on a
service-account, authorized-user, WIF, or access-token file. The commands are in
[Local client credentials and TLS](client-credentials-and-tls.md).

<!-- section: bq -->
## bq CLI

Point `bq` at the REST listener and provide a local placeholder token:

```bash
bq \
  --api=http://localhost:9050 \
  --project_id=test-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-test-token \
  ls
```

For Parquet load, upload objects to the external fake GCS first and then run:

```bash
bq --api=http://localhost:9050 \
  --project_id=test-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-test-token \
  load --source_format=PARQUET \
  test-project:analytics.events \
  'gs://bqemu-temporary/input/*.parquet'
```

<!-- section: spark -->
## PySpark And Scala Spark

Both entrypoints use the same connector options. REST metadata and jobs go to
`bigQueryHttpEndpoint`; Storage Read and direct Storage Write go to
`bigQueryStorageGrpcEndpoint`:

```text
bigQueryHttpEndpoint=http://localhost:9050
bigQueryStorageGrpcEndpoint=localhost:9060
gcpAccessToken=local-test-token
parentProject=test-project
project=test-project
```

The required runtime contract is Spark `3.5.8`, Scala `2.12`, and Spark BigQuery
Connector `0.44.2`. PySpark and Scala `spark-shell` are tested as separate
entrypoints. Indirect writes additionally need the Hadoop GCS settings from the
previous section. The connector behavior is bound to the reviewed
[0.44.2 source revision](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92).

<!-- section: tls -->
## TLS

Generate local certificates and client credential files, then start the TLS
override described in [Local client credentials and TLS](client-credentials-and-tls.md).
Use the generated CA for Python and `bq`, and the generated PKCS12 truststore for
Spark. The endpoint hostname must be present in the certificate SAN.

<!-- section: devcontainer -->
## Development Container

When the client runs in a development container and BQEMU runs on the host, use
`host.docker.internal` instead of `localhost`:

```text
http://host.docker.internal:9050
host.docker.internal:9060
http://host.docker.internal:4443
```

On Linux, add `--add-host=host.docker.internal:host-gateway` or the equivalent
Compose `extra_hosts` entry. Mount the generated CA, truststore, and credential
directory read-only. Do not copy generated secrets into an image layer.

<!-- section: shutdown -->
## Stop The Environment

Keep the BQEMU data volume:

```bash
docker compose -f compose.yaml -f compose.load.yaml down
```

Remove BQEMU data as well:

```bash
docker compose -f compose.yaml -f compose.load.yaml down --volumes
```
