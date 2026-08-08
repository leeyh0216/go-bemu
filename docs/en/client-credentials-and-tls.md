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
| `ca.pem` | CA trust for HTTP clients, curl, and BQEMU health checks | `0644` |
| `server.pem` | TLS certificate for BQEMU and the local issuer | `0644` |
| `server-key.pem` | TLS private key | `0600` |
| `service-account.json` | Local JWT bearer exchange | `0600` |
| `authorized-user.json` | Local OAuth refresh exchange | `0600` |
| `wif.json` | File-sourced external-account STS exchange | `0600` |
| `subject-token.txt` | Subject token referenced by `wif.json` | `0600` |
| `access-token.txt` | Direct token alternative with no exchange | `0600` |
| `truststore.p12` | JVM PKCS12 trust store | `0600` |

The output directory is `0700`. `wif.json` contains the absolute path of
`subject-token.txt`, so generate the files in their final location. Existing
paths and symbolic links are rejected. Add `--force` only when replacing an
entire disposable set.

With `--force`, the helper writes a complete sibling directory, synchronizes
and validates every generated file, and then atomically exchanges that
directory with the previous generation. A failure or interruption before the
exchange leaves the previous generation unchanged. A later generation removes
marked staging directories left by an interrupted run. Atomic replacement
requires Linux or macOS and a filesystem that supports atomic directory
exchange; otherwise the command fails without changing the previous files.
Generation also holds a sibling `.bqemu-auth.lock`, so another process cannot
delete an active staging directory. On Linux and macOS this credential-free
advisory lock file may remain after the command exits. Remove it only when no
fixture generator is running.

The default addresses are `https://localhost:9052` for token exchange and
`http://127.0.0.1:9053` for the local CONNECT proxy. Override them together when
the ports are occupied:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth --base-url https://localhost:19052 --proxy-url http://127.0.0.1:19053
```

The generated manifest records both listener addresses. `serve` reads them by
default, so the custom-port example does not need separate `--listen` or
`--proxy-listen` flags. An explicit listener override must use a loopback host
and the same port recorded in the manifest. The remaining examples use the
default ports.

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

This command listens on the issuer and proxy addresses stored by `generate` in
`manifest.json`.

The same process starts two loopback listeners:

| Listener or endpoint | Purpose |
| --- | --- |
| `https://localhost:9052/oauth/token` | OAuth refresh and JWT bearer grants |
| `/token` and `/o/oauth2/token` | Strict-client aliases for the same grants |
| `https://localhost:9052/sts/token` | [RFC 8693](https://www.rfc-editor.org/rfc/rfc8693.html) token exchange |
| `https://localhost:9052/introspect` | WIF token introspection |
| `https://localhost:9052/healthz` | Issuer readiness |
| `http://127.0.0.1:9053` | CONNECT proxy for fixed Google OAuth token origins |

Some credential libraries ignore the `token_uri` in an authorized-user file or use
a fixed Google OAuth audience. The proxy accepts CONNECT only for
`oauth2.googleapis.com:443` and `accounts.google.com:443`, then routes that TLS
connection to the local issuer. It cannot forward arbitrary Internet traffic.

Set these variables for processes that use a JSON credential:

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
all state when stopped. Diagnostic logs include request and response headers and
bodies, including assertions, credentials, subject tokens, and access tokens.
The request-body capture is bounded by the issuer's 64 KiB token-request limit.

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

<!-- section: integration-guides -->
## Integration Guides

Exact executable configuration is an integration concern. The version-pinned
examples, endpoint flags, credential options, and JVM trust-store settings live
under the [integration guides](../../tests/integration/docs/en/index.md). Those
guides consume the files described here without making any executable or
version a product dependency.

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

Use `https://bqemu:9050` and `bqemu:9060` as the public endpoints, add
`bqemu` to `NO_PROXY`, and use the token-exchange address recorded in the
manifest (`localhost:9052` by default).
The development container must provide Go 1.26+ and `keytool`. Do not generate
the files on one filesystem path and then move them into the container.

<!-- section: verification -->
## Verify Integration Profiles

The integration contract installs the dependencies selected by each normalized
case, verifies declared execution artifacts, starts TLS-enabled BQEMU and the
issuer, and runs every required credential profile through the public process:

```bash
make auth-client-setup
make auth-client-test
```

The default target runs every required case in
`tests/integration/contract/consumers.normalized.json`. CI uses the same entrypoint with
`BQEMU_AUTH_CASE` set to the normalized case ID, so a failure identifies one
runtime and adapter without changing the contract runner. Set
`BQEMU_AUTH_JUNIT` to write a case-specific JUnit XML file containing the case
name, duration, error type, and original failure text. `BQEMU_AUTH_DIAGNOSTICS`
writes NDJSON events with status, byte counts, SHA-256 correlation digests, and
the retained child and background-process output.

Exact executable and runtime versions are generated in [Consumer
Compatibility](../../tests/integration/docs/en/consumer-compatibility.md). The
[integration test framework](../../tests/integration/docs/en/framework.md)
defines case selection, evidence, and CI behavior.

Diagnostics contain operation names, exit status, byte counts, SHA-256
correlation digests, and raw client/server output. Credential and token values
may therefore appear in local artifacts; control their access and retention as
you would the generated fixture directory.

<!-- section: cleanup -->
## Rotate and Remove the Files

Stop the issuer and remove the generated files when the test ends:

```bash
rm -rf .bqemu-auth
rm -f .bqemu-auth.lock
```

A new generation creates new keys and tokens. Regenerate instead of editing
individual JSON endpoints: the service-account JWT audience, WIF subject-token
path, certificate names, and manifest must remain consistent.
