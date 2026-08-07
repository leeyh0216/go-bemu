<!-- doc-id: contributing -->
<!-- lang: ko -->

[English](CONTRIBUTING.md) | [한국어](CONTRIBUTING.ko.md)

# go-bemu 기여 안내

<!-- section: scope -->
## 범위를 먼저 정의하기

BigQuery 동작을 구현하기 전에 재현할 공개 REST method, gRPC RPC/message, SQL
규칙 또는 wire format을 식별한다. 권위 있는 계약을 구현과 문서 가까이에
연결한다. [BigQuery REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest), [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc),
[GoogleSQL 레퍼런스](https://cloud.google.com/bigquery/docs/reference/standard-sql)에서
시작한다.

<!-- section: architecture -->
## 아키텍처 규칙

- `internal/domain`과 `internal/application`을 HTTP, gRPC, Google DTO, DuckDB,
  object-store SDK와 독립적으로 유지한다.
- 외부 시스템은 port 뒤에 두고 compile-time adapter assertion을 추가한다.
- transaction, offset, idempotency, visibility invariant를 제약 대상 state
  transition 바로 옆에 둔다.
- 지원하지 않는 의미는 명시적으로 거부한다. 다른 BigQuery type이나 lifecycle로
  조용히 강제 변환하지 않는다.
- Runtime setting은 versioned YAML model, default, validation, typed `--set`
  path, sample에 함께 추가한다. `defaults < file < environment < --set`을 보존하고
  hidden flag나 magic test value를 추가하지 않는다.
- Secret material은 mounted file path로 참조한다. Effective configuration, log,
  fixture, example에 secret byte를 추가하지 않는다.

<!-- section: provenance -->
## 출처 규칙

- primary source를 사용한다. connector 의존 동작은 변경 가능한 branch가 아니라
  정확한 [`0.44.2`
  tag](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
  연결한다.
- 이전 emulator와 비교할 때는 정확한 [goccy BigQuery emulator `v0.8.1`
  tag](https://github.com/goccy/bigquery-emulator/tree/v0.8.1)를 연결한다. Source를
  복사하거나 upstream build dependency로 만들지 않는다.
- golden fixture와 호환성 설명에 정확한 upstream tag 또는 commit을 연결한다.
- 계약은 바꾸어 설명한다. 불가피한 인용은 짧게 유지하고 출처를 표시한다.
- 버전에 묶인 주장에 GitHub `master`나 `main` 소스 링크를 추가하지 않는다.
- 모든 compatibility workaround의 제거 조건을 명시한다.

<!-- section: bilingual-docs -->
## 이중 언어 문서

maintainer 대상 Markdown 변경은 같은 pull request에서 두 언어 파일을 모두
갱신해야 한다.

- `README.md`와 `README.ko.md`;
- `CONTRIBUTING.md`와 `CONTRIBUTING.ko.md`;
- `docs/en/**`와 `docs/ko/**`의 동일 상대 경로.

`doc-id`와 순서가 있는 `section` marker를 동일하게 유지한다. link text를
번역해도 primary-source URL은 동일하게 유지한다. 모든 페이지의 영어/한국어
전환 링크를 보존한다.

<!-- section: tests -->
## 테스트

```bash
gofmt -w docs/documentation_test.go
go test ./...
go vet ./...
```

domain state test, fake outbound adapter를 사용하는 application test, 공개
REST/gRPC contract test, malformed/boundary case를 추가한다. DuckDB가 SQL을
받아들였다는 테스트만으로 BigQuery 의미를 재현했다고 볼 수 없다.

<!-- section: evolution-pipeline -->
## 호환성 진화 파이프라인

동작은 다음 순서로 추가한다.

```text
protocol profile -> adapter -> capability -> golden -> E2E
```

profile은 공개 client/version과 관찰된 wire contract를 고정한다. adapter에는 가장
작은 변환만 둔다. capability는 정확한 지원 수준을 명명한다. 정제된 golden은
shape를 기록하고 end-to-end test는 released client가 공개 경계를 통과함을
입증한다. Drift report에는 `version`, `operation`, `shape`, `fingerprint`,
`fix_hint`가 반드시 포함되어야 한다. 이것은 [BigQuery API
계약](https://cloud.google.com/bigquery/docs/reference)과의 비교를 대체하지 않는다.

<!-- section: issues -->
## 이중 언어 이슈

유지보수 이슈를 포함한 모든 issue body는 동등한 `## English`와 `## 한국어`
section을 가진다. scope, acceptance criteria, 제외 범위, dependency, primary
source를 동일하게 유지하며 title은 영어만 사용해도 된다. 기존 이슈를 수정할
때도 같은 작업에서 두 section을 함께 갱신한다. Connector 근거는 정확한
[`0.44.2` source](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
가리켜야 한다.

<!-- section: change-description -->
## 변경 설명

지원 connector/client version, capability ID, 권위 있는 source, 관찰된 차이,
선택한 경계, failure 동작, 남은 제한을 명시한다. 모든 acceptance criterion이
실제로 충족되기 전에는 `refs #N`을 사용한다.
