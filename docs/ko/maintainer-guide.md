<!-- doc-id: maintainer-guide -->
<!-- lang: ko -->

[English](../en/maintainer-guide.md) | [한국어](maintainer-guide.md)

# Maintainer 안내서

<!-- section: bootstrap -->
## Clone부터 Service 실행까지

Go 1.26 이상, DuckDB용 C/C++ compiler, 선택적으로 Docker와 direnv가 필요하다.
Upstream emulator clone은 필요하지 않다.

```bash
direnv allow
mkdir -p data "$BQEMU_TEMP_DIRECTORY"
make check
make run
```

Credential이 없는 checked-in `.envrc`는 `.envrc.example`을 source한 다음
ignore되는 optional `.envrc.local`을 load한다. Example은 `configs/bqemu.yaml`,
host database/temp path, bounded test budget을 선택한다. Machine-specific
non-production override만 `.envrc.local`에 넣는다. Token, private key,
credential JSON, production endpoint는 checked-in file 어느 쪽에도 넣지 않는다.
Direnv를 쓰지 않으면 같은 값을 export하거나 `--config`와 반복 `--set`을
명시적으로 전달한다. 정확한 merge/validation 규칙은 [설정과 운영](operations.md)에
있다. `GET /healthz`를 확인한 다음 root README의 emulator project를 생성한다.
REST shape는 [BigQuery REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를 기준으로 한다.

<!-- section: learning-path -->
## 학습 경로

1. [아키텍처](architecture.md), 특히 dependency와 transaction 경계를 읽는다.
2. Service와 README의 project/dataset/table/query 요청 하나를 실행한다.
3. `go test ./internal/domain -run Schema`와 같은 가장 가까운 focused test를 실행한다.
4. `internal/transport`에서 application/port를 지나 adapter까지 요청을 추적한다.
5. Storage 또는 connector 의존 계약을 바꾸기 전에 [BigQuery와 connector 내부
   동작](bigquery-internals.md)을 읽는다.
6. [호환성](compatibility.md)에서 기존 capability를 선택하거나 새로 선언한다.

Connector baseline은 정확한 [Spark BigQuery connector
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)다.

<!-- section: first-change -->
## 첫 변경 Runbook

공개 동작 하나와 negative case 하나로 시작한다. Domain invariant, fake port를
사용한 application test, adapter 동작, 공개 REST/gRPC test, compatibility row,
두 언어 문서를 추가하거나 갱신한다. 다음을 실행한다.

```bash
gofmt -w ./cmd ./internal docs/documentation_test.go
go test ./...
go vet ./...
```

Review 전 engine SQL만 assert하는 test가 없는지, unsupported field를 구현된
것처럼 조용히 무시하지 않는지, error가 capability와 actionable fix를 명명하는지
확인한다.

<!-- section: new-version -->
## Protocol 또는 Client Version 추가

`protocol profile -> adapter -> capability -> golden -> E2E` pipeline을 사용한다.
정확한 artifact/tag, REST/RPC sequence, field-presence rule, wire format, schema
mapping, retry/offset semantics, removal criteria를 기록한다. 이전 profile을
제자리에서 수정하거나 mutable branch를 연결하지 않는다. Storage operation은
[공식 RPC 레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)와
비교한다.

<!-- section: diagnose-drift -->
## Drift 진단

1. 공개 endpoint에서 재현하고 stage 및 identifier를 수집한다.
2. Credential, SQL text, row payload 없이 `version`, `operation`, `shape`,
   `fingerprint`, `fix_hint`를 출력한다.
3. Request/response를 pinned profile과 공식 계약에 비교한다.
4. Mismatch를 transport, application invariant, outbound adapter로 좁힌다.
5. 좁은 fix를 적용하기 전에 negative golden을 추가한다.
6. Focused test, full test, vet, released-client E2E lane을 실행한다.

Schema fingerprint는 deterministic digest이며 schema authority가 아니다.
Canonical type source는 [BigQuery data
type](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)이다.

<!-- section: release -->
## Release와 문서 Runbook

`make check`를 실행하고 Docker 동작이 바뀌었으면 container를 build하며,
compatibility diff를 검토하고 모든 공개 주장이 테스트한 경계를 명시하는지
확인한다. README, CONTRIBUTING, 모든 `docs/en/**` file은 marker와 source URL이
같은 한국어 counterpart를 가져야 한다. 모든 issue body도 scope, acceptance
criteria, 제외 범위, source가 동등한 `## English`와 `## 한국어` section을 가져야
한다. Acceptance criteria가 실행되기 전에는 issue를 close하지 않는다.
