<!-- doc-id: clients/python-bigquery -->
<!-- lang: en -->

[English](python-bigquery.md) | [한국어](../../ko/clients/python-bigquery.md)

# Python BigQuery Client

Set an explicit REST endpoint when constructing the client. The API follows the
official [Python BigQuery reference](https://cloud.google.com/python/docs/reference/bigquery/latest).

<!-- section: endpoint -->
## Required Client Options

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

Set `api_endpoint` to `http://bqemu:9050` from a sibling Compose service or to
`http://host.docker.internal:9050` from a development container that reaches
host Compose. A project ID alone does not redirect requests.

<!-- section: tls -->
## TLS

For HTTPS, set the endpoint to `https://...:9050` and trust the generated CA:

```bash
export REQUESTS_CA_BUNDLE="$PWD/.bqemu-auth/ca.pem"
export SSL_CERT_FILE="$REQUESTS_CA_BUNDLE"
```

Credential-file and TLS fixture setup is described in [Local credentials and
TLS](../../../../../docs/en/client-credentials-and-tls.md).

<!-- section: query -->
## Minimal Query

```python
job = client.query("SELECT 1 AS answer", location="US")
rows = list(job.result())
assert rows[0]["answer"] == 1
client.close()
```

Create the emulator project first with [Getting started](../../../../../docs/en/getting-started.md).

<!-- section: load -->
## Parquet Load

URI loads use `gs://` Parquet input. Configure the uploader and BQEMU with
addresses for the same fake GCS service; see [Getting started](../../../../../docs/en/getting-started.md#load-parquet-through-fake-gcs).
Other load formats and local paths are unavailable.

<!-- section: validation -->
## Validation

This setup is validated with `google-cloud-bigquery` `3.43.0`. Exact scenario
IDs and request traces belong to the integration-test evidence, not to client
configuration.
