<!-- doc-id: adr-0005-explicit-runtime-contract -->
<!-- lang: ko -->

[English](../../en/adr/0005-explicit-runtime-contract.md) | [한국어](0005-explicit-runtime-contract.md)

# ADR-0005: 명시적 Runtime/Diagnostics 계약 사용

<!-- section: status -->
## 상태

Configuration, diagnostics/listener composition, container profile, bounded
server shutdown에는 accepted다. Readiness drain과 operation accounting은 proposed다.

<!-- section: context -->
## 배경

숨겨진 default, 흩어진 test sleep, public diagnostics route, bounded deadline이
없는 graceful stop은 compatibility failure 재현을 어렵게 하고 민감한 값을
노출할 수 있다. BigQuery-compatible REST와 Storage RPC listener는 public
protocol surface지만 emulator diagnostics는 project-owned control surface다.
Service 목록은 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)를 기준으로
하고 container isolation control은 [Compose service
레퍼런스](https://docs.docker.com/reference/compose-file/services/)를 기준으로 한다.

<!-- section: decision -->
## 결정

1. 설정은 `compiled defaults < YAML file < mapped environment < repeated --set`
   순서로 merge하며 `--config`가 `BQEMU_CONFIG` file selector를 덮어쓴다.
2. Diagnostics는 default로 disabled인 별도 listener를 사용하고 BigQuery REST
   namespace를 공유하지 않는다. Non-loopback bind에는 token과 server TLS가 필요하다.
3. Startup, request, eventual-state, shutdown test는 이름이 있고 설정 가능한
   deadline 및 bounded sanitized diagnostics를 사용한다.
4. Release container는 non-root로 실행한다. Operational profile은 read-only
   rootfs, explicit writable data volume, bounded temporary storage,
   health/readiness probe, forced fallback이 있는 하나의 graceful-stop deadline을
   사용한다.
5. 어떤 endpoint/log도 어느 mode에서든 credential, token, private key, raw SQL,
   row payload, HTTP body, protobuf JSON, error text를 노출하지 않는다. Opaque
   value는 shape/count/length/SHA-256 summary를 사용한다. 부분 redaction은 안전한
   경계를 증명할 수 없으므로 legacy `unsafePayloads` input은 deprecated no-op으로
   유지한다. [Cloud Logging audit
   guidance](https://cloud.google.com/logging/docs/audit/best-practices)를 따른다.

<!-- section: consequences -->
## 결과

Strict versioned loader, typed generic override, validation, effective
configuration, source/effective fingerprint, 보호되고 bounded한 diagnostics,
hardened Compose profile, file-configured composition은 구현되어 있다. Runtime은
configured HTTP/gRPC limit와 shared TLS를 적용하고 enabled일 때만 admin을 시작하며
shared shutdown deadline 만료 시 gRPC를 강제 중지한다.
Resource-close phase는 QueryService admission/cancellation/drain을 소유하고 Storage
Read, Storage Write, DuckDB teardown보다 먼저 실행한다.
`logging.unsafePayloads`를 설정한 legacy configuration은 계속 parse되고 같은 effective
model을 생성하지만 true 값은 deprecation event만 남기며 payload-safe logging을
바꾸지 않는다. Readiness drain,
outstanding-operation reporting, 두 번째 signal 전용 path, phase별 split test
timeout은 아직 불완전하다. Configuration failure는
`model_version/operation/shape/fingerprint/fix_hint`를 사용하고 protocol drift는
`version/operation/shape/fingerprint/fix_hint`를 사용한다.

<!-- section: alternatives -->
## 대안

Public BigQuery listener의 diagnostics handler는 emulator control API와
compatibility surface를 섞으므로 거부했다. 흩어진 literal timeout은 CI tuning에
code change가 필요하고 failure가 하나의 effective configuration을 제시하지 못해
거부했다. Automatic secret loading도 거부한다. Checked-in `.envrc`는
`.envrc.example`의 non-secret default와 ignore되는 `.envrc.local`의 optional
machine override만 load한다.
