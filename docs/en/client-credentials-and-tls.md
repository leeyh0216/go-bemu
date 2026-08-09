<!-- doc-id: client-credentials-and-tls -->
<!-- lang: en -->

[English](client-credentials-and-tls.md) | [한국어](../ko/client-credentials-and-tls.md)

# Local Client Credentials and TLS

<!-- section: boundary -->
## Responsibility Boundary

BQEMU does not authenticate or authorize its BigQuery-compatible REST and gRPC
requests. It accepts requests with no `Authorization` value and ignores any
credential value a client sends. `admin.tokenFile` is different: it protects
only the optional BQEMU administration listener.

Some Google client libraries still require a structurally valid credential and
perform an OAuth or STS exchange before calling an emulator. This repository
therefore includes a local credential generator and a loopback-only token
issuer. They exercise client-side [Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
and [Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation)
protocol paths. They do not add access control to BQEMU and do not emulate IAM,
OAuth consent, Google identities, or production token validation.

<!-- section: generate -->
## Generate the Files

Run the generator from the repository root:

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
```

The published image also contains a statically linked helper. GHCR-only users
can extract it and run it on the host:

```bash
fixture_container="$(docker create "$BQEMU_IMAGE")"
docker cp "$fixture_container:/usr/local/bin/bqemu-auth-fixture" ./bqemu-auth-fixture
docker rm "$fixture_container"
chmod 0755 ./bqemu-auth-fixture
```

Keep generation and serving on the host when host clients consume the files.
This preserves the absolute subject-token path embedded in `wif.json` and the
issuer's loopback-only network boundary.

The default issuer origin is <https://localhost:9052>. Use `--base-url` when a
different loopback port is required. Existing files are not replaced unless
`--force` is present.

The command prints only the manifest path and creates:

| File | Purpose | Mode on Unix |
| --- | --- | --- |
| `manifest.json` | Paths and local endpoint addresses | `0600` |
| `ca.pem` | Local CA certificate | `0644` |
| `server.pem` | Server certificate for `localhost`, `127.0.0.1`, and `::1` | `0644` |
| `server-key.pem` | Server private key | `0600` |
| `service-account.json` | Service-account client credential | `0600` |
| `authorized-user.json` | Authorized-user ADC credential | `0600` |
| `wif.json` | File-sourced external-account credential | `0600` |
| `subject-token.txt` | Subject token referenced by `wif.json` | `0600` |

The output directory is `0700`. Generated credentials are disposable local
test material. Keep the directory out of source control and do not reuse it in
a production environment.

<!-- section: issuer -->
## Run the Local Token Issuer

Start the HTTPS issuer in a separate terminal:

```bash
go run ./cmd/bqemu-auth-fixture serve \
  --manifest .bqemu-auth/manifest.json \
  --listen 127.0.0.1:9052
```

The issuer refuses a non-loopback listen address. It exposes these endpoints:

| Endpoint | Supported flow |
| --- | --- |
| `POST /oauth/token` | OAuth refresh token and JWT bearer grants |
| `POST /sts/token` | RFC 8693 token exchange for `wif.json` |
| `GET /healthz` | Issuer process health |

Request bodies, headers, credentials, signed assertions, subject tokens, and
issued access tokens are not logged. Form bodies, headers, timeouts, token
lifetimes, and in-memory work all have explicit limits. The token exchange
response follows [RFC 8693](https://www.rfc-editor.org/rfc/rfc8693.html).

<!-- section: clients -->
## Select a Client Credential

Set one generated file as Application Default Credentials:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.bqemu-auth/service-account.json"
# or: authorized-user.json
# or: wif.json
```

All three files point token acquisition at the local issuer. They do not change
the separate REST or Storage gRPC endpoint configuration used to direct a
client to BQEMU.

Python HTTP clients commonly use the following CA setting:

```bash
export REQUESTS_CA_BUNDLE="$PWD/.bqemu-auth/ca.pem"
export SSL_CERT_FILE="$PWD/.bqemu-auth/ca.pem"
```

For Java and Spark, create a disposable trust store and pass it to the JVM:

```bash
keytool -importcert -noprompt \
  -alias bqemu-local-ca \
  -file .bqemu-auth/ca.pem \
  -keystore .bqemu-auth/truststore.p12 \
  -storetype PKCS12 \
  -storepass changeit

export BQEMU_JAVA_TLS_OPTS="-Djavax.net.ssl.trustStore=$PWD/.bqemu-auth/truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit"
```

Pass `BQEMU_JAVA_TLS_OPTS` through the JVM option mechanism of the Java or Spark
process. The generated server certificate is valid for the three documented
loopback names only.

<!-- section: bqemu-tls -->
## Enable TLS on BQEMU

The same generated certificate can terminate TLS on BQEMU REST and gRPC:

```bash
export BQEMU_TLS_CERT_FILE="$PWD/.bqemu-auth/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/.bqemu-auth/server-key.pem"
go run ./cmd/emulator
```

TLS protects data in transit. It does not make BQEMU validate the generated
access tokens. Clients must trust `ca.pem` and connect as `localhost`,
`127.0.0.1`, or `::1`.

<!-- section: cleanup -->
## Cleanup

Stop the issuer and delete the entire generated directory when the test run is
finished:

```bash
rm -rf .bqemu-auth
```

Regenerate the files after changing the issuer origin. Editing individual JSON
endpoints leaves the signed assertion audience or manifest inconsistent and is
not supported.
