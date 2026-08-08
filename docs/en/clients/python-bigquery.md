<!-- doc-id: clients/python-bigquery -->
<!-- lang: en -->

[English](python-bigquery.md) | [한국어](../../ko/clients/python-bigquery.md)

# Python BigQuery Client

Use `google-cloud-bigquery` `3.43.0` with Python `3.13`. The client API is
documented in the official [Python BigQuery reference](https://cloud.google.com/python/docs/reference/bigquery/latest).

<!-- section: endpoint -->
## REST Endpoint

The Python client uses the REST listener. Pass the endpoint when constructing
the client; setting only a project ID does not redirect requests.

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

Use `http://bqemu:9050` from a sibling Compose service or
`http://host.docker.internal:9050` from a development container when BQEMU runs
on the host.

<!-- section: credentials -->
## Credentials And TLS

BQEMU does not authenticate public BigQuery requests, but the Python library
still requires a credential object. `AnonymousCredentials` is sufficient for a
plain local endpoint.

For credential parsing or HTTPS tests, generate the repository fixtures and run
their local issuer. Then use one of `service-account.json`,
`authorized-user.json`, `wif.json`, or `access-token.txt` as described in
[Client credentials and TLS](../client-credentials-and-tls.md).

```bash
export REQUESTS_CA_BUNDLE="$PWD/.bqemu-auth/ca.pem"
export SSL_CERT_FILE="$REQUESTS_CA_BUNDLE"
```

Use HTTPS on `localhost` port `9050` with these variables. JSON files that
exchange a token also need the local issuer and proxy from the fixture guide.

<!-- section: example -->
## Minimal Query

```python
job = client.query(
    "SELECT 1 AS answer",
    location="US",
)
rows = list(job.result())
assert rows[0]["answer"] == 1
client.close()
```

Create the emulator project first by following [Getting started](../getting-started.md).

<!-- section: operations -->
## API Call Order

The normalized consumer case is
`google-cloud-bigquery-python-3.43.0`. It exercises these scenario IDs:

| Scenario ID | Client flow and operation order |
| --- | --- |
| `python-metadata` | Dataset methods call `bigquery.datasets.insert`, `bigquery.datasets.list`, `bigquery.datasets.get`, `bigquery.datasets.patch`, and `bigquery.datasets.delete`. Table methods call the matching `bigquery.tables.insert`, `bigquery.tables.list`, `bigquery.tables.get`, `bigquery.tables.patch`, and `bigquery.tables.delete` operations. |
| `python-query` | `client.query()` sends `bigquery.jobs.insert` and reads pages with `bigquery.jobs.getQueryResults`. `get_job()` calls `bigquery.jobs.get`; `query_and_wait()` starts with `bigquery.jobs.query`; `list_jobs()` calls `bigquery.jobs.list`. |
| `python-tabledata` | `list_rows()` calls `bigquery.tabledata.list` and follows `pageToken` pages. |
| `python-indirect-load` | `load_table_from_uri()` sends `bigquery.jobs.insert`, then polls `bigquery.jobs.get` until the load job is terminal. |

The manifest operation IDs describe the calls accepted by the tested flow.
Optional polling and pagination determine how many times a request is repeated.

<!-- section: shapes -->
## Request And Response Shapes

| Operation | Request | Response used by the client |
| --- | --- | --- |
| `bigquery.datasets.insert` | `POST /bigquery/v2/projects/{projectId}/datasets` with a Dataset resource | Dataset resource |
| `bigquery.tables.insert` | `POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables` with a Table resource | Table resource |
| `bigquery.jobs.insert` | `POST /bigquery/v2/projects/{projectId}/jobs` with `configuration.query` or `configuration.load` | Job resource and `jobReference` |
| `bigquery.jobs.get` | Job ID and location in path/query parameters | Job resource with `status.state` and errors |
| `bigquery.jobs.getQueryResults` | Job ID, location, and page controls | Schema, `jobComplete`, rows, and next page token |
| `bigquery.tabledata.list` | Table path and page controls | BigQuery `f`/`v` row encoding and next page token |

The exact accepted fields and support level are maintained in
[Compatibility](../compatibility.md). Client versions and scenario selectors are
generated in [Consumer compatibility](../consumer-compatibility.md).

<!-- section: related -->
## Related Work

Behavior outside the documented operations is tracked by
[open issues](https://github.com/leeyh0216/go-bemu/issues) and the compatibility
documents rather than this client guide.
