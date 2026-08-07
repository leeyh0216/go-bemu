<!-- doc-id: operations -->
<!-- lang: ko -->

[English](../en/operations.md) | [한국어](operations.md)

# 설정과 운영 Runbook

<!-- section: configuration -->
## 설정 계약

구현된 versioned loader는 낮은 순서에서 높은 순서로 다음 precedence를 사용한다.

```text
compiled defaults < YAML file < mapped environment variables < repeated --set path=value
```

`BQEMU_CONFIG`가 optional file을 선택하고 `--config`가 이 selector를 덮어쓴다.
모든 runtime leaf는 `config.bqemu.dev/v1alpha1` model에 존재하며 typed `--set`으로
override할 수 있다. 자주 쓰는 setting에는 이름 있는 `BQEMU_*` environment
mapping도 있다. 완전한 Docker-oriented example은
[`configs/bqemu.yaml`](../../configs/bqemu.yaml)이다.

| Layer | Selector | 계약 |
| --- | --- | --- |
| compiled defaults | 없음 | 완전하고 유효한 in-memory model |
| file | `BQEMU_CONFIG` 또는 `--config` | YAML document 하나, 최대 1 MiB |
| environment | 문서화된 `BQEMU_*` mapping | 비어 있지 않은 scalar override |
| CLI | 반복 가능한 `--set path=value` | 모든 leaf의 typed override |

Composition root는 default, HTTP/gRPC/TLS limit, database/temp path, shutdown
budget, logging, admin, UI, 두 Storage service, opt-in load field를 사용한다.
`storage.read.maxSnapshotBytes`는 각 materialization 전에 예약하고 adapter의 실제
retained byte로 정산한다. Live session과 in-flight reservation 합계는
`maxTotalSnapshotBytes`를 넘을 수 없다.
`query.operationTimeout`(default `2m`, 환경 변수
`BQEMU_QUERY_OPERATION_TIMEOUT`)은 동기 query와 service가 소유하는 비동기 query 실행 모두의 server
hard ceiling이다. `query.anonymousResultTtl`(default `24h`, 환경 변수
`BQEMU_QUERY_ANONYMOUS_RESULT_TTL`)은 생성된 result table 만료를 제어한다. 두 값은
양수여야 하고 configuration file 또는 `--set`으로 바꿀 수 있다. Protocol 근거는 공식
[`jobTimeoutMs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.job_timeout_ms)와
[anonymous cached-result lifetime](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored)이다.
`query.compensationTimeout`(default `30s`, 환경 변수
`BQEMU_QUERY_COMPENSATION_TIMEOUT`)은 metadata publication 실패 뒤 physical cleanup을
별도로 제한한다. 취소된 request와는 분리하지만 deadline 없이 실행하지 않는다.
`tableData.operationTimeout`(default `30s`, 환경 변수
`BQEMU_TABLE_DATA_OPERATION_TIMEOUT`)은
[`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)
뒤의 DuckDB count-and-page transaction을 제한한다. `tableData.maxPageRows`(default
`10000`, 환경 변수 `BQEMU_TABLE_DATA_MAX_PAGE_ROWS`)는 caller가 더 많은 row를 요청해도
한 response를 제한하며 1과 BigQuery의 100,000-row response quota 사이여야 한다. 두
값 모두 file-first이며 typed `--set` override를 제공한다.
Memory snapshot은 encoded row byte를,
spill file은 각 row의 8-byte frame prefix까지 계산한다.
`storage.write.maxInFlightBytes*`는 serialized DuckDB coordinator를 기다리는 decoded
request를 제한하고, `maxStagedBytes*`는 숨김 DuckDB PENDING table에 보관된
deterministic serialized-row charge를 제한한다. Configured append size는 stream별
limit 이하이고, stream별 limit은 global limit 이하여야 한다. Staged-byte 계산은
안정적이고 이식 가능한 논리적 크기이며 DuckDB의 실제 file/page 크기는 아니다.
`storage.write.queueWaitTimeout`(default `5s`)은 byte admission 이후 serialized
coordinator에 들어갈 때까지의 대기를 제한하고,
`storage.write.operationTimeout`(default `30s`)은 수락된 operation의 serialized
queue 체류와 backend 실행을 합친 전체 시간을 제한한다. Timer는 queue admission
성공 뒤에만 시작하므로 queue 포화는 독립적으로 `RESOURCE_EXHAUSTED` 경계가 된다.
각각 `BQEMU_STORAGE_WRITE_QUEUE_WAIT_TIMEOUT`,
`BQEMU_STORAGE_WRITE_OPERATION_TIMEOUT`에 mapping된다. Queue 포화는 재시도 가능한
gRPC `RESOURCE_EXHAUSTED`, server operation deadline은 `DEADLINE_EXCEEDED`로
보고하며 caller의 더 이른 deadline이 우선한다. PENDING append가 응답 전에 staging된
경우 Finalize 전에 같은 offset/schema/payload receipt로 재전송해야 하며 이 retry는
idempotent하다.
DEFAULT stream은 공식 at-least-once ambiguity를 유지한다. 공식 제한은 전체 request에
적용되지만 pinned Java 3.22.1 client는 `ProtoData` 크기로 batch를 구성한다.
Compatibility 설정 `maxAppendRequestBytes`는 이 client-visible payload를 모델링하고
transient admission은 전체 `AppendRowsRequest`를 계산한다. Startup 시에는
`server.grpc.maxReceiveMessageBytes`가 설정된 payload와 file-configured
`maxAppendEnvelopeBytes`(default 64 KiB)를 수용하는지도 검증한다. 환경 변수는
`BQEMU_STORAGE_WRITE_MAX_APPEND_ENVELOPE_BYTES`다. 이 규칙은 공식
[`AppendRows` request와 retry 계약](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows)을
따른다.
정확한 pinned client source는
[`google-cloud-bigquerystorage` 3.22.1 source artifact](https://repo.maven.apache.org/maven2/com/google/cloud/google-cloud-bigquerystorage/3.22.1/google-cloud-bigquerystorage-3.22.1-sources.jar)에 고정한다.
`maxConcurrentAppendRequests`(default `16`, 환경 변수
`BQEMU_STORAGE_WRITE_MAX_CONCURRENT_APPEND_REQUESTS`)는 gRPC `Recv` 전에
획득하여 bidi stream 전체에서 protobuf decode, clone, digest의 동시 메모리를
weighted coordinator admission 이전부터 제한한다.
`load.enabled`에는 absolute `load.gcsEndpoint`가 필요하고
`load.allowFileSources`는 default false이며 object/list/download limit을 적용한다.
Authentication과 runtime contract-profile negotiation은 아직 composition되지 않는다.
유효한 setting은 각 Partial capability를 넘어서는 주장이 아니다.

HTTP edge는 `identity`와 `gzip` request body를 허용한다.
`server.http.maxCompressedRequestBytes`는 gzip decode 전 wire에서 읽는 byte를
제한하고, `server.http.maxUncompressedRequestBytes`는 decoded byte를 독립적으로
제한한다. 두 값의 default는 2 MiB다. 각각
`BQEMU_HTTP_MAX_COMPRESSED_REQUEST_BYTES`,
`BQEMU_HTTP_MAX_UNCOMPRESSED_REQUEST_BYTES`에 mapping되며 typed `--set`으로도
설정할 수 있다. Enforcement는 stream을 직접 읽으므로 `ContentLength`를 알 수 없는
chunked request에도 적용된다. Unsupported encoding은 `415`, malformed 또는
multiple encoding은 `400`, 두 byte budget 초과는 `413`을 반환한다. Boundary
log에는 encoding, accepted/rejected outcome, byte count, SHA-256 digest, status,
reason만 남기고 raw body와 credential은 남기지 않는다. 구현 근거는 Go의
[`Request` body 계약](https://pkg.go.dev/net/http#Request),
[`MaxBytesReader`](https://pkg.go.dev/net/http#MaxBytesReader),
[`gzip.NewReader`](https://pkg.go.dev/compress/gzip#NewReader), 공식
[`tables.insert` method](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert),
고정된 connector의 [`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)다.

Unknown YAML field, multiple document, ambiguous numeric duration, unknown
override path, invalid cross-field 조합은 listener 시작 전에 실패한다. Error에는
`stage`, `operation`, `model_version`, `field`, `shape`, `fingerprint`, `fix_hint`가
있다. `--print-effective-config`는 merged model을 검증하고 출력하며 source-file 및
effective-model SHA-256 fingerprint로 drift를 재현할 수 있게 한다. Schema는
[YAML 1.2.2 명세](https://yaml.org/spec/1.2.2/)를 따른다.

Secret byte를 YAML이나 environment variable에 넣지 않는다. TLS key, static
token, remote admin token은 mounted file path로 참조한다. Effective configuration은
이 reference path를 포함할 수 있지만 file content를 읽거나 출력하지 않는다.
Output은 operational metadata로 다룬다. TLS는 전송만 보호하며 [Google Cloud
인증](https://cloud.google.com/docs/authentication)을 구현하지 않는다.

<!-- section: logging-safety -->
## Payload-safe Logging 계약

`logging.unsafePayloads`와 `BQEMU_LOG_UNSAFE_PAYLOADS`는
`config.bqemu.dev/v1alpha1`의 deprecated compatibility input이다. 기존 file,
environment, `--set logging.unsafePayloads=true`는 계속 load되지만 값은 no-op이며
structured deprecation event를 남긴다. Raw payload logging을 활성화할 수 없다. Key
제거에는 향후 configuration API version이 필요하지만 동작 변경에는 필요하지 않다.

이 invariant는 JSON/text output, 모든 log level, legacy setting의 두 값에 모두
적용된다. 공식 [Cloud Logging structured-log
model](https://cloud.google.com/logging/docs/structured-logging)과 [audit-log security
guidance](https://cloud.google.com/logging/docs/audit/best-practices)를 따른다.

| Boundary | 기록하는 shape | 절대 기록하지 않는 값 |
| --- | --- | --- |
| REST | method/path, query/header **이름**, encoding, 읽은 body byte count와 SHA-256, status, duration | header/query 값, request/response body |
| Storage gRPC | RPC/protobuf full name, resource identifier, offset, item/row count, wire/schema byte count와 SHA-256 | protobuf JSON, serialized row, filter/SQL text, credential metadata 값 |
| error | Go error type, message byte count, 전체 message SHA-256 | `error`/`err.Error()` text, 그 안의 SQL, 값, path, credential |
| side effect | component, operation, pre/post state, success, duration, 안전한 identifier/count/digest | raw request/response 또는 backend payload |

`PayloadAttrs`, `ProtoAttrs`, `ErrorAttrs`가 opaque data의 유일한 변환 경계다.
호환성을 위해 남긴 `RedactText` helper도 omitted marker, byte count, digest만
반환하며 pattern 기반 부분 redaction을 security boundary로 취급하지 않는다.
SHA-256 fingerprint는 same/different 진단용이지 payload 복원용이 아니다. Storage
message shape의 근거는 공식
[`google.cloud.bigquery.storage.v1` RPC model](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1)이다.

<!-- section: local-run -->
## 로컬 실행 Runbook

```bash
direnv allow
mkdir -p data "$BQEMU_TEMP_DIRECTORY"
make check
make run
curl --fail http://localhost:9050/healthz
curl --fail http://localhost:9050/readyz
```

Direnv는 선택 사항이며 secret을 자동으로 load하지 않는다. Checked-in `.envrc`는
`.envrc.example`을 source하고, 존재하면 ignore되는 `.envrc.local`을 load한다.
Example은 `configs/bqemu.yaml`을 선택하고 container database/temp path를 host용으로
override하며 bounded Go, Python, Docker test budget을 설정한다. Machine-specific
non-production override만 `.envrc.local`에 넣는다. Listener를 시작하지 않고 merge를
확인하려면 다음을 실행한다.

```bash
go run ./cmd/emulator --print-effective-config
go run ./cmd/emulator --set logging.level=debug --print-effective-config
```

Liveness는 process가 응답함을, readiness는 warehouse ping도 성공함을 뜻한다.
gRPC는 표준 health service를 노출한다. Enabled Storage Read/Write service는
`SERVING`, disabled service는 `NOT_SERVING`을 보고하며 transport drain 전에 모든
gRPC health entry를 `NOT_SERVING`으로 바꾼다. Canonical Storage service 목록은
[Storage RPC 레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)에 있다.

<!-- section: container -->
## Container 계약

Image는 전용 non-root `bqemu` user로 실행하며 `/data`를 writable volume으로
선언하고 `/readyz`를 health-check한다. Default Compose는 repository-relative config
path를 build argument로 전달한다. Image는 이를 mode `0440`으로 stable runtime
path에 copy한다.

```yaml
build:
  context: .
  args:
    BQEMU_CONFIG_SOURCE: configs/bqemu.yaml
environment:
  BQEMU_CONFIG: /etc/bqemu/bqemu.yaml
volumes:
  - bqemu-data:/data
tmpfs:
  - /tmp/bqemu:uid=65532,gid=65532,mode=0700
```

다른 non-secret file을 선택하려면 [Docker build
context](https://docs.docker.com/build/building/context/) 내부 path를
`BQEMU_CONFIG_SOURCE=configs/<profile>.yaml`로 설정하고 rebuild한다. 이 default는
Docker Desktop host-sharing 제한을 포함한 runtime host bind를 피한다. 명시적
override로 readable file을 `/etc/bqemu/bqemu.yaml:ro`에 bind할 수도 있으며 동작은
[bind mount 문서](https://docs.docker.com/engine/storage/bind-mounts/)를 따른다.

Database는 container UID `65532`가 소유한 named volume으로만 영속화한다. Built
config에는 path와 non-secret setting만 넣는다. TLS/token file은 별도로 read-only
mount하고 content를 image layer에 copy하지 않는다.

Checked-in Compose profile은 read-only root filesystem, `no-new-privileges`,
전용 `/tmp/bqemu` tmpfs, readiness health check, configured 10초 application
budget보다 긴 15초 stop grace period를 설정한다. 모든 Linux capability drop과
명시적 CPU/memory limit은 deployment별 추가 사항으로 남아 있다. Docker의 권위
있는 control은 [read-only root
filesystem](https://docs.docker.com/reference/cli/docker/container/run/#read-only)과
[Compose service 설정](https://docs.docker.com/reference/compose-file/services/)이다.

<!-- section: shutdown -->
## Health와 Graceful Shutdown

SIGINT, SIGTERM 또는 listener failure가 발생하면 runtime은 먼저 모든 gRPC health
entry를 `NOT_SERVING`으로 바꾼다. `runtime.serverDrainTimeout`(default `5s`) context
하나가 public/admin HTTP shutdown과 gRPC `GracefulStop`을 제한하며 만료 시
`grpc.Stop`으로 강제 종료한다. 별도 `runtime.storageCloseTimeout`(default `4s`)은
공유 resource-close budget이다. 먼저 새 query를 거부하고 이미 수용한 sync/async query를
취소한 뒤 종료를 기다리고, 이어 Read snapshot cleanup, Write orphan cleanup,
coordinator close를 제한한다. Query가 이 budget 안에 DuckDB를 반환하지 않으면 active
query와 경합하지 않도록 Storage 및 DuckDB close를 건너뛰고 남은 resource는 process
teardown이 회수한다.
`runtime.shutdownTimeout`(default `10s`)은 startup 또는 early-return path의 deferred
Storage cleanup fallback이다. HTTP readiness는 drain 전에 false가 되지 않고
outstanding operation count를 보고하지 않으며 두 번째 signal 전용 즉시 종료 path도
없다. DuckDB file이 남더라도 abrupt termination은 process-local catalog, job, Read
session, Write stream, load idempotency record를 잃을 수 있다.

Test는 query admission 거부, active sync/async 취소, bounded query drain,
query-before-Storage close 순서를 검증한다. Idle shutdown, active REST request,
active gRPC stream, deadline expiry, second-signal force exit, mounted volume
restart는 계속 필요한 runtime scenario다. Storage Write
visibility 계약은 공식
[`BatchCommitWriteStreams` 계약](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams)이며
shutdown이 commit 성공을 만들어내면 안 된다.

<!-- section: timeouts -->
## 설정 가능한 Test Timeout

어떤 test도 이름 없는 sleep에 의존하지 않는다. 현재 control은 unit과 scope를
이름에 명시한다.

| Control | Format과 default | 현재 scope |
| --- | --- | --- |
| `BQEMU_GO_TEST_TIMEOUT` | Go duration, `10m` | `make test`, race test, CI package budget |
| `BQEMU_STORAGE_READ_TEST_TIMEOUT` | Go duration, `5s` | Storage Read application test context 하나 |
| `BQEMU_STORAGE_WRITE_TEST_TIMEOUT` | Go duration, `5s` | Storage Write application, adapter, public gRPC test context |
| `BQEMU_REST_TEST_TIMEOUT` | Go duration, `5s` | REST request, gzip boundary, pagination, overwrite test context |
| `BQEMU_PYTEST_TIMEOUT_SECONDS` | 양의 초, suite default `90`; direnv default `300` | 공식 Python-client build, readiness, request, shutdown budget |
| `BQEMU_BQCLI_TIMEOUT_SECONDS` | 양의 초, `300` | 각 bq CLI subprocess와 emulator readiness budget |
| `BQEMU_DOCKER_START_TIMEOUT_SECONDS` | 양의 초, `120` | `docker compose --wait` startup budget |

Python startup failure에는 local process log의 bounded tail만 포함한다. Suite-wide
Python budget은 설정 가능하지만 의도적으로 coarse로 분류한다. 향후 phase별 분리는
다음과 같다.

| Control | 목적 | 만료 시 진단 |
| --- | --- | --- |
| `BQEMU_TEST_STARTUP_TIMEOUT` | process/container readiness budget | port, health body, 마지막 sanitized log |
| `BQEMU_TEST_REQUEST_TIMEOUT` | REST/RPC operation 하나의 budget | operation, capability, request fingerprint |
| `BQEMU_TEST_EVENTUALLY_TIMEOUT` | polling/eventual-state budget | 마지막 state와 transition history |
| `BQEMU_TEST_SHUTDOWN_TIMEOUT` | graceful-stop budget | outstanding REST/RPC/job count |

Default는 하나의 test configuration module에 두고 실패 output에 출력해야 한다.
CI는 environment variable로 budget을 늘릴 수 있고 individual test는 명시적
fixture/option으로 budget을 줄일 수 있다. Timeout failure는 `version`,
`operation`, `shape`, `fingerprint`, `fix_hint`를 보고한다. 네 개 split
control은 설계 계약이며 아직 wiring되지 않았다.

<!-- section: diagnostics -->
## Diagnostics Admin Endpoint 설계

Versioned model은 `admin.enabled`, `admin.address`, `admin.tokenFile`,
`admin.readHeaderTimeout`, `admin.maxStackBytes`를 정의한다. Admin은 default로
disabled이며 주소는 `127.0.0.1:9051`이다. Composition root는 enabled일 때만 별도
listener를 시작하며 BigQuery REST namespace를 공유하지 않는다. Non-loopback
bind에는 token file과 configured server TLS가 모두 필요하고 admin listener는 이
shared TLS identity를 사용한다.

| Method와 path | 구현된 response |
| --- | --- |
| `GET /healthz` | admin liveness와 `admin.bqemu.dev/v1alpha1` |
| `GET /bqemu/v1/admin/diagnostics/runtime` | uptime, Go/build/process, goroutine, heap, GC snapshot |
| `GET /bqemu/v1/admin/diagnostics/goroutines` | SHA-256/truncation header가 있는 bounded text stack |

`tokenFile`을 설정하면 모든 route가 constant-time으로 비교하는 Bearer token을
요구한다. Trim한 token은 최소 16 byte이고 file은 최대 64 KiB다. Goroutine
output은 설계상 configuration을 제외해도 민감하다. Default cap은 4 MiB이며 log에
복사하지 않고 remote access는 반드시 보호한다.

향후 `GET /bqemu/v1/admin/config`는 model version, source/effective fingerprint,
redacted effective model을 반환할 수 있다. Authorization header, token/key
content, raw SQL, row payload, unbounded log를 포함하면 안 되며 secret file
reference는 configured/not-configured 또는 non-reversible digest로 줄인다. 이
config endpoint, capability/operation count, recent drift summary는 아직 planned며
IAM 대체재가 아니다.
