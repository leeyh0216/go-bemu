<!-- doc-id: clients/bq-cli -->
<!-- lang: en -->

[English](bq-cli.md) | [한국어](../ko/bq-cli.md)

# bq CLI

Use `bq` `2.1.31` from Google Cloud SDK `566.0.0`. Command syntax is documented
in the official [`bq` CLI reference](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference).

<!-- section: endpoint -->
## REST Endpoint

Pass the BQEMU REST address to every command with `--api`. Keep the local test
configuration independent from the user's normal Google Cloud configuration.

```bash
export CLOUDSDK_CONFIG="$(mktemp -d)"
export CLOUDSDK_CORE_DISABLE_PROMPTS=1

bq \
  --api=http://localhost:9050 \
  --project_id=test-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-test-token \
  --format=json \
  ls --projects
```

Use `http://bqemu:9050` from a sibling Compose service or
`http://host.docker.internal:9050` from a development container.

<!-- section: credentials -->
## Credentials And TLS

The direct token shown above is sufficient because BQEMU does not validate
public bearer tokens. To test a credential file, generate the repository
fixtures, start the local issuer, and set one of the generated JSON files:

```bash
export AUTH_DIR="$PWD/.bqemu-auth"
export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$AUTH_DIR/service-account.json"
export CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE="$AUTH_DIR/ca.pem"
export REQUESTS_CA_BUNDLE="$AUTH_DIR/ca.pem"
```

Then use HTTPS on `localhost` port `9050` and add
`--ca_certificates_file="$AUTH_DIR/ca.pem"`. Authorized-user and WIF files need
the issuer and restricted proxy settings from [Client credentials and
TLS](../../../../docs/en/client-credentials-and-tls.md). A direct access token does not need the
issuer.

<!-- section: example -->
## Minimal Commands

Assuming the emulator project already exists:

```bash
bq --api=http://localhost:9050 --project_id=test-project \
  --use_gcloud_config=false --oauth_access_token=local-test-token \
  mk --dataset --location=US test-project:analytics

bq --api=http://localhost:9050 --project_id=test-project \
  --use_gcloud_config=false --oauth_access_token=local-test-token \
  mk --table test-project:analytics.events id:INTEGER,label:STRING

bq --api=http://localhost:9050 --project_id=test-project \
  --use_gcloud_config=false --oauth_access_token=local-test-token \
  --format=json query --use_legacy_sql=false 'SELECT 1 AS answer'
```

The project creation command is in [Getting started](../../../../docs/en/getting-started.md).

<!-- section: operations -->
## API Call Order

The normalized consumer case is `bq-cli-2.1.31`.

| Scenario ID | CLI flow and operation order |
| --- | --- |
| `bq-metadata` | The CLI reads `bqemu.discovery.get`. `ls --projects` calls `bigquery.projects.list`; dataset commands call `bigquery.datasets.insert`, `bigquery.datasets.get`, `bigquery.datasets.patch`, and `bigquery.datasets.delete`; table commands call `bigquery.tables.insert`, `bigquery.tables.get`, `bigquery.tables.patch`, `bigquery.tables.list`, and `bigquery.tables.delete`. |
| `bq-query` | `bq query` sends `bigquery.jobs.insert`, then reads results with `bigquery.jobs.getQueryResults`; commands that inspect a job call `bigquery.jobs.get`, and `bq ls --jobs` calls `bigquery.jobs.list`. |
| `bq-indirect-load` | `bq load` sends `bigquery.jobs.insert` with a load configuration and polls `bigquery.jobs.get` until the job is terminal. |

Discovery can be requested again by a new CLI process. Polling and pagination
can repeat the corresponding GET operation.

<!-- section: shapes -->
## Request And Response Shapes

| CLI command | Public request | Response consumed by `bq` |
| --- | --- | --- |
| Initial command | `GET /$discovery/rest` | BigQuery v2 Discovery document |
| `mk --dataset` | Dataset resource to `bigquery.datasets.insert` | Dataset resource |
| `mk --table` | Table resource and schema to `bigquery.tables.insert` | Table resource |
| `query` | Job with `configuration.query` to `bigquery.jobs.insert` | Job reference followed by query-result pages |
| `load` | Job with `configuration.load.sourceUris` and Parquet format | Job resource followed by job status |
| `ls --jobs` | Project, location, and page controls to `bigquery.jobs.list` | Job summaries and next page token |

The CLI renders BigQuery JSON resources into command-specific output. Use
`--format=json` when tests need a stable machine-readable result.

The exact accepted fields and support level are maintained in
[Compatibility](../../../../docs/en/compatibility.md). The selected CLI and Cloud SDK versions
are generated in [Consumer compatibility](../../../../docs/en/consumer-compatibility.md).

<!-- section: related -->
## Related Work

Behavior outside the documented operations is tracked by
[open issues](https://github.com/leeyh0216/go-bemu/issues) and the compatibility
documents rather than this client guide.
