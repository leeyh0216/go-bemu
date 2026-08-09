# Public Operation Contract

This directory owns the machine-checked contract for BQEMU's public REST and
gRPC surface. It is a maintainer tool, not the user compatibility guide. Users
start with [What works today](../docs/en/compatibility.md); the generated
[method reference](../docs/en/api-rpc-compatibility.md) is the exact public
surface.

## Start Here

| File | Purpose | Edit it? |
| --- | --- | --- |
| `operations.yaml` | Handwritten source of truth for every public operation. | Yes |
| `operations.normalized.json` | Canonical, deterministic form of the manifest. | No; generated |
| `operation_manifest.go` | Strict schema and validation rules. | When the contract model changes |
| `operation_annotations.go` | Links declared Go tests to literal operation IDs. | Rarely |
| `operation_generate.go` | Generates route specs and EN/KO API tables. | When a generated representation changes |
| `../internal/contractspec/operations_gen.go` | Runtime route/RPC specifications. | No; generated |

Run `make contract-check` before reviewing a change. It rejects stale
generated files, unknown operation IDs, missing test evidence, invalid
conditions, and REST/gRPC descriptor drift.

## Add Or Change An Operation

1. **Start from the protocol source.** Add an HTTPS official source to the
   top-level `sources` list if it is not already present. Do not infer a
   public method from a client implementation or an internal route.
2. **Implement and test the public boundary.** REST operations need their
   registered method/path and gRPC operations need their registered
   service/method before the manifest is changed.
3. **Edit `operations.yaml`.** Give the operation a stable dotted ID, one
   protocol shape, an allowed component, EN and KO descriptions, support and
   verification levels, sources, limitations, and its Go test IDs.
4. **Annotate every declared Go test.** In the test function, use a literal
   ID so the compiler can read it statically:

   ```go
   import "github.com/leeyh0216/go-bemu/internal/contracttest"

   func TestExample(t *testing.T) {
       contracttest.Operation(t, "bigquery.example.get")
       // Exercise the public route or RPC here.
   }
   ```

   The test ID in YAML is `go:path/without/_test.go:TestExample`. A declared
   ID without this annotation, or an annotation for an unknown ID, fails
   `make contract-check`.
5. **Regenerate and inspect the review surface.** Run
   `make contract-generate`, then review the normalized JSON, generated Go
   route specs, and both generated API/RPC tables. Never hand-edit them.
6. **Run focused behavior tests plus `make contract-check`.** Generated route
   registration, Discovery, gRPC descriptors, and annotations are all checked
   from the same manifest.

External compatibility suites may prove end-to-end behavior, but they do not
replace the product Go test evidence that this manifest validates.

## Required Classification

`support` describes the BQEMU operation, not the entire upstream API:

| Value | Meaning |
| --- | --- |
| `implemented` | The documented operation behavior is available and has public-boundary evidence. |
| `partial` | The operation is callable, but `supportedInput` and `limitations` bound what it does. |
| `registered` | The route or RPC descriptor is present, but its behavior is not implemented. |
| `unsupported` | The operation is intentionally not exposed as supported; it has no execution test evidence. |

`verification` is the strongest required evidence: `transport` proves the
public REST/gRPC boundary, `application` proves an application path, `unit`
proves a local contract, and `none` is allowed only for unsupported entries.
Do not upgrade support or verification to make a table look better; add the
missing boundary test instead.

Every limitation must be explicit:

- `none`: no known limitation beyond the stated input.
- `by-design`: an intentional emulator boundary; list its approved scope.
- `tracked`: a missing behavior with one or more GitHub issues.
- `mixed`: both an intentional boundary and tracked missing behavior.

Conditions are currently limited to `admin.enabled`, `ui.enabled`,
`storage.read.enabled`, and `storage.write.enabled`. A condition needs EN/KO
text and test evidence included in the operation's own `tests` list.

## Rules That Prevent Drift

- The production transport imports generated `internal/contractspec`, never
  the compiler package `contract`.
- One REST listener/method/path and one gRPC service/method pair may map to one
  operation only.
- Operation IDs and test annotations are literal, stable identifiers. Do not
  build them dynamically.
- `operations.normalized.json`, `operations_gen.go`, and the EN/KO API tables
  change only through `make contract-generate`.
- Keep user-facing descriptions short. Detailed implementation discussion
  belongs in an issue, ADR, or maintainer document.

## Common Commands

```bash
make contract-check       # validate source, annotations, descriptors, and generated files
make contract-generate    # rewrite deterministic generated artifacts
go test ./contract        # schema/compiler behavior only
go test ./internal/transport/rest ./internal/transport/grpc
```

For the surrounding design, see the [development workflow](../docs/en/maintainers/development-workflow.md),
[maintainer documentation hub](../docs/en/maintainers/index.md), and [CI reporting
policy](../docs/en/maintainers/ci-reporting.md).
