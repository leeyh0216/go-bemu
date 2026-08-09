<!-- doc-id: clients/bq-cli -->
<!-- lang: en -->

[English](bq-cli.md) | [한국어](../../ko/clients/bq-cli.md)

# bq CLI

Configure the CLI to use BQEMU's REST endpoint. Command syntax follows the
official [`bq` CLI reference](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference).

<!-- section: endpoint -->
## Required Overrides

Pass these settings to each command, or wrap them in a local shell function:

```bash
bq \
  --api=http://localhost:9050 \
  --project_id=test-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-test-token \
  <command>
```

| Override | Why it matters |
| --- | --- |
| `--api` | Redirects BigQuery REST requests to BQEMU. |
| `--project_id` | Selects the emulator project used when a command omits a project. |
| `--use_gcloud_config=false` | Prevents a local Cloud SDK profile from changing the selected project or endpoint. |
| `--oauth_access_token` | Satisfies the CLI's local credential requirement. BQEMU does not authorize public requests. |

Use `http://bqemu:9050` from a sibling Compose service and
`http://host.docker.internal:9050` from a development container that reaches
host Compose. Do not use `localhost` from a different container.

<!-- section: tls -->
## TLS

For HTTPS, set the endpoint to `https://...:9050` and add the generated CA:

```bash
export AUTH_DIR="$PWD/.bqemu-auth"
export CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE="$AUTH_DIR/ca.pem"

bq --api=https://localhost:9050 --ca_certificates_file="$AUTH_DIR/ca.pem" \
  --project_id=test-project --use_gcloud_config=false \
  --oauth_access_token=local-test-token <command>
```

Generate local TLS files with [Local credentials and TLS](../../../../../docs/en/client-credentials-and-tls.md).

<!-- section: commands -->
## Common Commands

Create the emulator project first with [Getting started](../../../../../docs/en/getting-started.md).

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

Use `--format=json` when a local test must inspect a stable machine-readable
response.

<!-- section: load -->
## Parquet Load

`load` receives `gs://` Parquet input. Configure the uploader and BQEMU with
addresses that reach the same fake GCS service; see [Getting started](../../../../../docs/en/getting-started.md#load-parquet-through-fake-gcs).
Other load formats and local paths are unavailable.

<!-- section: validation -->
## Validation

This configuration is validated with `bq` `2.1.31` from Google Cloud SDK
`566.0.0`. Exact operation traces, scenario IDs, and artifact provenance are
test-framework evidence, not CLI setup requirements.
