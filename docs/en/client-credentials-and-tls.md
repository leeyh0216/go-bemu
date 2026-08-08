<!-- doc-id: client-credentials-and-tls -->
<!-- lang: en -->

[English](client-credentials-and-tls.md) | [한국어](../ko/client-credentials-and-tls.md)

# Local Client Credentials and TLS

<!-- section: boundary -->
## What These Files Do

BQEMU accepts BigQuery-compatible REST and gRPC requests without authenticating
or authorizing the caller. A credential generated here only satisfies the
client-side checks performed by strict Google clients. BQEMU does not validate
the resulting access token and does not emulate IAM, OAuth consent, or a Google
identity.

`admin.tokenFile` has a separate purpose. It protects only the optional BQEMU
administration listener.

The helper implements local forms of [Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
and [Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation).
Use it only for disposable local tests. It is not an identity provider.

<!-- section: generate -->
## Generate TLS and Credential Files

Generation requires Go 1.26+ and a JDK `keytool` on `PATH`. Use `--keytool` to
select another executable. The command checks this dependency before creating
the output directory.

Run one command from the repository root:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
```

The command creates the following files.

| File | Client use | Unix mode |
| --- | --- | --- |
| `manifest.json` | Absolute paths, issuer addresses, proxy address, trust-store password | `0600` |
| `ca.pem` | CA trust for Python, `bq`, curl, and BQEMU health checks | `0644` |
| `server.pem` | TLS certificate for BQEMU and the local issuer | `0644` |
| `server-key.pem` | TLS private key | `0600` |
| `service-account.json` | Local JWT bearer exchange | `0600` |
| `authorized-user.json` | Local OAuth refresh exchange | `0600` |
| `wif.json` | File-sourced external-account STS exchange | `0600` |
| `subject-token.txt` | Subject token referenced by `wif.json` | `0600` |
| `access-token.txt` | Direct token alternative with no exchange | `0600` |
| `truststore.p12` | Java and Spark PKCS12 trust store | `0600` |

The output directory is `0700`. `wif.json` contains the absolute path of
`subject-token.txt`, so generate the files in their final location. Existing
paths and symbolic links are rejected. Add `--force` only when replacing an
entire disposable set.

The default addresses are `https://localhost:9052` for token exchange and
`http://127.0.0.1:9053` for the local CONNECT proxy. Override them together when
the ports are occupied:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth --base-url https://localhost:19052 --proxy-url http://127.0.0.1:19053
```

Use a repeatable `--tls-dns-name` when a client reaches BQEMU by a container
service name:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth --tls-dns-name bqemu
```

The certificate also contains the two Google OAuth host names needed by the
local proxy. Trust `ca.pem` only in the test processes shown below. Do not add
this CA to a machine-wide or browser trust store.

A published BQEMU image contains the helper but not a JDK. Extract the helper
and run it on a host or development container that has `keytool`:

```bash
fixture_container="$(docker create "$BQEMU_IMAGE")"
docker cp "$fixture_container:/usr/local/bin/bqemu-auth-fixture" ./bqemu-auth-fixture
docker rm "$fixture_container"
chmod 0755 ./bqemu-auth-fixture
./bqemu-auth-fixture generate --output .bqemu-auth
```

<!-- section: issuer -->
## Run Token Exchange

Start the issuer before using any JSON credential profile:

```bash
go run ./cmd/bqemu-auth-fixture serve --manifest .bqemu-auth/manifest.json
```

The same process starts two loopback listeners:

| Listener or endpoint | Purpose |
| --- | --- |
| `https://localhost:9052/oauth/token` | OAuth refresh and JWT bearer grants |
| `/token` and `/o/oauth2/token` | Strict-client aliases for the same grants |
| `https://localhost:9052/sts/token` | [RFC 8693](https://www.rfc-editor.org/rfc/rfc8693.html) token exchange |
| `https://localhost:9052/introspect` | WIF token introspection |
| `https://localhost:9052/healthz` | Issuer readiness |
| `http://127.0.0.1:9053` | CONNECT proxy for fixed Google OAuth token origins |

Some official clients ignore the `token_uri` in an authorized-user file or use
a fixed Google OAuth audience. The proxy accepts CONNECT only for
`oauth2.googleapis.com:443` and `accounts.google.com:443`, then routes that TLS
connection to the local issuer. It cannot forward arbitrary Internet traffic.

Set these variables for Python and `bq` processes that use a JSON credential:

```bash
export AUTH_DIR="$PWD/.bqemu-auth"
export REQUESTS_CA_BUNDLE="$AUTH_DIR/ca.pem"
export SSL_CERT_FILE="$AUTH_DIR/ca.pem"
export HTTPS_PROXY=http://127.0.0.1:9053
export https_proxy="$HTTPS_PROXY"
export NO_PROXY=localhost,127.0.0.1,::1
export no_proxy="$NO_PROXY"
```

The issuer keeps only token digests in memory, issues one-hour tokens, and loses
all state when stopped. Logs contain method, path, status, and duration only.
They do not contain request bodies, assertions, credentials, subject tokens, or
access tokens.

<!-- section: clients -->
## Choose a Profile

Use one of four independent profiles.

| Profile | File or option | Exchange |
| --- | --- | --- |
| Service account | `service-account.json` | Signed JWT bearer grant |
| Authorized user | `authorized-user.json` | Refresh-token grant |
| WIF external account | `wif.json` | File subject token and STS |
| Direct access token | `access-token.txt` | None |

The direct token is the smallest setup because BQEMU accepts any bearer value.
The three JSON files are useful when a test must exercise the credential-loading
behavior of the real client. The REST and Storage gRPC endpoints must still be
set separately.

<!-- section: python -->
## Python 3.43.0

Install and use the official [google-cloud-bigquery Python
client](https://cloud.google.com/python/docs/reference/bigquery/latest). Load a
generated JSON file without adding scopes, and pass the project and endpoint
explicitly. This prevents a WIF credential from attempting project discovery.

```python
from google.auth import load_credentials_from_file
from google.cloud import bigquery

credentials, _ = load_credentials_from_file(
    ".bqemu-auth/wif.json",
)
client = bigquery.Client(
    project="test-project",
    credentials=credentials,
    client_options={"api_endpoint": "https://localhost:9050"},
)
print([item.dataset_id for item in client.list_datasets()])
client.close()
```

Replace `wif.json` with `service-account.json` or `authorized-user.json` to use
another exchange. For a direct token:

```python
from pathlib import Path
from google.cloud import bigquery
from google.oauth2.credentials import Credentials

token = Path(".bqemu-auth/access-token.txt").read_text().strip()
client = bigquery.Client(
    project="test-project",
    credentials=Credentials(token=token),
    client_options={"api_endpoint": "https://localhost:9050"},
)
print([item.dataset_id for item in client.list_datasets()])
client.close()
```

The `REQUESTS_CA_BUNDLE`, proxy, and `NO_PROXY` variables from the previous
section must be present in the Python process.

<!-- section: bq -->
## bq CLI 2.1.31

Use an isolated Cloud SDK configuration and the official
[`bq` CLI](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference).
This prevents local test credentials from changing a normal gcloud profile.

```bash
export CLOUDSDK_CONFIG="$(mktemp -d)"
export CLOUDSDK_CORE_DISABLE_PROMPTS=1
export CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK=true
export CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE="$AUTH_DIR/ca.pem"
export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$AUTH_DIR/service-account.json"

bq --api=https://localhost:9050 --project_id=test-project --ca_certificates_file="$AUTH_DIR/ca.pem" --format=json ls
```

Change the override to `authorized-user.json` or `wif.json` for the other
credential profiles. Use the token file without starting the issuer:

```bash
bq --api=https://localhost:9050 --project_id=test-project --ca_certificates_file="$AUTH_DIR/ca.pem" --oauth_access_token="$(tr -d '\r\n' < "$AUTH_DIR/access-token.txt")" --format=json ls
```

Remove the temporary `CLOUDSDK_CONFIG` directory after the test.

<!-- section: spark -->
## PySpark and Scala Spark

The supported contract is Spark `3.5.8` with the [Spark BigQuery connector
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2).
The generator already creates the PKCS12 trust store. Configure the JVM trust
store and the loopback proxy before starting PySpark or `spark-shell`:

```bash
export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=$AUTH_DIR/truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=PKCS12 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=9053 -Dhttp.nonProxyHosts=localhost|127.*"
export SPARK_LOCAL_IP=127.0.0.1
```

A PySpark read using a JSON credential is:

```python
df = (
    spark.read.format("bigquery")
    .option("parentProject", "test-project")
    .option("billingProject", "test-project")
    .option("project", "test-project")
    .option("bigQueryHttpEndpoint", "https://localhost:9050")
    .option("bigQueryStorageGrpcEndpoint", "localhost:9060")
    .option("credentialsFile", "/absolute/path/.bqemu-auth/service-account.json")
    .load("test-project.analytics.events")
)
df.show()
```

Replace `credentialsFile` with an authorized-user or WIF path. For the direct
token profile, remove `credentialsFile` and set `gcpAccessToken` to the trimmed
contents of `access-token.txt`.

The same reader options apply to the Scala `DataFrameReader` API. For example,
`spark.read.format("bigquery").option("credentialsFile", path).load(table)`
uses the JSON profile. Keep both endpoint options: metadata queries use HTTPS,
while table reads use the Storage gRPC endpoint.

The Java BigQuery SDK version pulled transitively by the connector is an
implementation detail. `google-cloud-bigquery 2.60.0` is not a separate
compatibility or test axis.

<!-- section: bqemu-tls -->
## Run BQEMU with TLS

Use the generated certificate for both BQEMU REST and Storage gRPC:

```bash
export BQEMU_TLS_CERT_FILE="$AUTH_DIR/server.pem"
export BQEMU_TLS_KEY_FILE="$AUTH_DIR/server-key.pem"
export BQEMU_PUBLIC_URL=https://localhost:9050
go run ./cmd/emulator
```

Clients must trust `ca.pem` and connect with a name in the certificate SAN.
TLS protects transport only. It does not make BQEMU inspect an
`Authorization` header.

<!-- section: compose -->
## Docker Compose

Generate the files on the host first. The TLS override runs the container with
the host UID and GID so it can read the `0700` directory and `0600` key without
weakening their permissions. It also bind-mounts `data` because the named
volume is owned by the image user.

```bash
command -v keytool
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
mkdir -p data
export BQEMU_HOST_UID="$(id -u)"
export BQEMU_HOST_GID="$(id -g)"
docker compose -f compose.yaml -f compose.tls.yaml up -d --build --wait
```

The override mounts `.bqemu-auth` read-only at `/run/bqemu-auth`. The public
addresses remain `https://localhost:9050` and `localhost:9060`. Run the token
issuer on the host when the client also runs on the host.

Read-only bind-mount behavior is described in the [Docker
documentation](https://docs.docker.com/engine/storage/bind-mounts/#use-a-read-only-bind-mount).
Stop the service with:

```bash
docker compose -f compose.yaml -f compose.tls.yaml down --remove-orphans
```

<!-- section: devcontainer -->
## Development Containers

Generate the files inside the development container when the client also runs
there. This keeps the absolute `subject-token.txt` path in `wif.json` valid and
places the loopback issuer in the same network namespace as the client.

When BQEMU is a sibling Compose service named `bqemu`, include that DNS name:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth --tls-dns-name bqemu
go run ./cmd/bqemu-auth-fixture serve --manifest .bqemu-auth/manifest.json
```

Use `https://bqemu:9050` and `bqemu:9060` as the connector endpoints, add
`bqemu` to `NO_PROXY`, and continue to use `localhost:9052` for token exchange.
The development container must provide Go 1.26+ and `keytool`. Do not generate
the files on one filesystem path and then move them into the container.

<!-- section: verification -->
## Verify the Supported Clients

The repository contract installs pinned Python and Spark dependencies, verifies
the reviewed connector checksum, starts a TLS BQEMU and issuer, and runs every
profile through real client processes:

```bash
make auth-client-setup
make auth-client-test
```

The `bq` executable must already be version `2.1.31` on `PATH`. The contract
checks exactly:

- google-cloud-bigquery Python `3.43.0`;
- `bq` `2.1.31`;
- PySpark and Scala Spark `3.5.8`;
- Spark BigQuery connector `0.44.2`.

Diagnostics contain operation names, exit status, byte counts, and SHA-256
digests. Generated secrets and raw client output are not printed.

<!-- section: cleanup -->
## Rotate and Remove the Files

Stop the issuer and remove the generated files when the test ends:

```bash
rm -rf .bqemu-auth
```

A new generation creates new keys and tokens. Regenerate instead of editing
individual JSON endpoints: the service-account JWT audience, WIF subject-token
path, certificate names, and manifest must remain consistent.
