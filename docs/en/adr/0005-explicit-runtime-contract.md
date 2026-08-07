<!-- doc-id: adr-0005-explicit-runtime-contract -->
<!-- lang: en -->

[English](0005-explicit-runtime-contract.md) | [한국어](../../ko/adr/0005-explicit-runtime-contract.md)

# ADR-0005: Use an Explicit Runtime and Diagnostics Contract

<!-- section: status -->
## Status

Accepted for configuration, diagnostics and listener composition, the container
profile, and bounded server shutdown. Readiness drain and operation accounting
remain proposed.

<!-- section: context -->
## Context

Hidden defaults, scattered test sleeps, a public diagnostics route, or an
unbounded graceful stop make compatibility failures hard to reproduce and can
leak sensitive values. BigQuery-compatible REST and Storage RPC listeners are
public protocol surfaces, while emulator diagnostics are project-owned control
surfaces. The service list comes from the [Storage RPC
reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc), and
container isolation controls come from the [Compose service
reference](https://docs.docker.com/reference/compose-file/services/).

<!-- section: decision -->
## Decision

1. Settings merge as `compiled defaults < YAML file < mapped environment <
   repeated --set`; `--config` overrides the `BQEMU_CONFIG` file selector.
2. Diagnostics use a separate listener, disabled by default, and never share the
   BigQuery REST namespace. A non-loopback bind requires a token and server TLS.
3. Startup, request, eventual-state, and shutdown tests use named configurable
   deadlines and bounded sanitized diagnostics.
4. The release container runs non-root. Its operational profile uses read-only
   rootfs, explicit writable data volumes, bounded temporary storage, health and
   readiness probes, and one graceful-stop deadline with forced fallback.
5. No endpoint or log exposes credentials, tokens, private keys, raw SQL, or row
   payloads by default.

<!-- section: consequences -->
## Consequences

The strict versioned loader, typed generic overrides, validation, effective
configuration, source/effective fingerprints, protected bounded diagnostics,
hardened Compose profile, and file-configured composition are implemented. The
runtime applies configured HTTP/gRPC limits and shared TLS, starts admin only
when enabled, and forces gRPC stop when its shared shutdown deadline expires.
Readiness drain, outstanding-operation reporting, a dedicated second-signal path,
and split per-phase test timeouts remain incomplete. Configuration failures use
`model_version/operation/shape/fingerprint/fix_hint`; protocol drift uses
`version/operation/shape/fingerprint/fix_hint`.

<!-- section: alternatives -->
## Alternatives

A diagnostics handler on the public BigQuery listener was rejected because it
mixes emulator control APIs with the compatibility surface. Scattered literal
timeouts were rejected because CI tuning would require code changes and failures
would lack one effective configuration. Automatic secret loading was rejected;
the checked-in `.envrc` loads only non-secret defaults from `.envrc.example` and
optional machine overrides from ignored `.envrc.local`.
