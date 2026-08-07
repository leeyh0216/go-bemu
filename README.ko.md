<!-- doc-id: readme -->
<!-- lang: ko -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu`는 Go로 처음부터 구현하는 실험적 BigQuery 호환 로컬 서비스다.
DuckDB는 외부 실행 어댑터이며 도메인 모델 자체가 아니다. 현재 제한된
BigQuery v2 메타데이터/쿼리/load 경로와 공개 Storage Read/Write gRPC data plane의
부분집합을 구현한다. 프로덕션 데이터베이스나 BigQuery의 완전한 대체재가 아니다.

호환성 계약은 [BigQuery REST v2
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest), [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc), 정확한
[Spark BigQuery connector `0.44.2`
소스](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
기준으로 한다.
이 저장소는 이전 emulator를 vendor하거나 clone하지 않는다. 비교 설명은 정확한
[goccy BigQuery emulator `v0.8.1`
source](https://github.com/goccy/bigquery-emulator/tree/v0.8.1)에 고정한다.

<!-- section: status -->
## 현재 상태

저장소 테스트로 실행되는 범위는 다음과 같다.

- liveness와 DuckDB 기반 readiness;
- emulator project 수명 주기와 ETag precondition 및 metadata pagination을
  포함한 dataset/table 생성, 조회, 목록, patch, update, 삭제;
- repeated record 내부 field까지 포함한 additive top-level/nested schema 변경,
  transactional DuckDB DDL, 공식 [Python client
  `3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/) 실행;
- 공식 [Python client
  `3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/)으로 실행한 동기
  `jobs.query`, process-local `jobs.insert`의 `jobs.get`/`getQueryResults` polling,
  terminal `invalidQuery` mapping;
- [Google Cloud SDK `566.0.0`](https://cloud.google.com/sdk/docs/release-notes#56600_2026-04-28)의
  공식 [`bq` CLI `2.1.31`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)로
  실행한 project/dataset/table/query/job 및 additive-schema flow;
- 격리된 DuckDB 물리 schema와 의도적으로 작은 SQL 변환 경계;
- [`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)에서
  파생한 connector `0.44.2` static-overwrite token adapter. Constant-false [BigQuery
  `MERGE`](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)를
  하나의 atomic [DuckDB `MERGE
  INTO`](https://duckdb.org/docs/current/sql/statements/merge_into)로 변환한다;
- 하나의 bounded DuckDB snapshot, Arrow/Avro encoding, projection/restriction
  validation, logical stream range, offset resume를 사용하는 공개 Storage Read session;
- exact offset, finalize, atomic batch commit, 여러 logical stream, weighted request
  admission, bounded hidden DuckDB staging, serialized DuckDB backend를 포함하는
  PENDING/default stream 공개 Storage Write `ProtoRows` 경로;
- bounded fake-GCS-compatible JSON adapter 또는 명시적으로 활성화한 `file://`
  adapter, Parquet staging, 기존 table 대상 atomic `WRITE_APPEND`, `WRITE_EMPTY`,
  `WRITE_TRUNCATE`를 사용하는 opt-in load job;
- 선택적 REST/gRPC TLS termination;
- strict versioned configuration, optional protected admin composition, bounded
  multi-listener shutdown, hardened non-root Compose profile;
- 프로토콜 profile과 민감정보를 제외한 경계 관측성.

중요한 제한은 다음과 같다.

- 영속 metadata, row insert/preview, copy/extract job, 전체 GoogleSQL은 구현하지 않았다.
- Unpartitioned direct static overwrite는 Spark `3.5.8`과 connector `0.44.2`로
  검증했다. Dynamic time/range partition overwrite와 일반 BigQuery `MERGE`
  parity는 gap이다.
- Storage Read는 partial이다. `SplitReadStream`, response compression, historical
  `snapshot_time`, restart-durable session, nested-field projection은 gap이다.
- Storage Write는 partial이다. CDC, Arrow row, BUFFERED 및 명시적으로 생성하는
  COMMITTED stream, `FlushRows`, default-value expression, restart-durable pending
  staging은 gap이다.
- load는 partial이다. Parquet 이외 format, table이 없을 때 `CREATE_IF_NEEDED`,
  schema-update option, autodetect, multipart/resumable transfer는 gap이다.
- 인증은 비활성 상태이며 IAM을 에뮬레이션하지 않는다.
- DuckDB 파일에 table data가 남아도 canonical BigQuery metadata는 메모리
  repository와 함께 프로세스 종료 시 사라진다.

세부적이고 테스트 가능한 상태 용어는 [호환성](docs/ko/compatibility.md)에
정리되어 있다. BigQuery의 job resource와 polling 계약은 공식
[`jobs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs) 및
[`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults)에
정의되어 있다.

<!-- section: architecture -->
## 아키텍처

```text
REST / gRPC inbound adapters
            |
            v
     application use cases
            |
            v
 domain model + outbound ports
            ^
            |
DuckDB / memory / object-store / system adapters
```

domain과 application package는 DuckDB, HTTP, gRPC, Google DTO에 의존하지
않는다. DuckDB transaction과 SQL 의미는 application use case가 BigQuery
의미를 명시적으로 추가하지 않는 한 DuckDB 동작이다. 자세한 내용은
[아키텍처](docs/ko/architecture.md)와
[ADR-0001](docs/ko/adr/0001-duckdb-behind-warehouse-port.md)을 참고한다. 물리
엔진의 계약은 [DuckDB SQL
소개](https://duckdb.org/docs/stable/sql/introduction)에 정의되어 있다.

<!-- section: quick-start -->
## 빠른 시작

Go 1.26 이상, DuckDB Go driver용 C/C++ toolchain, 선택적으로 Docker가
필요하다. `bq` 계약에는 Google Cloud SDK `566.0.0`이 설치하는 정확한 CLI
버전도 필요하다.

```bash
make test
make run
```

기본 endpoint는 다음과 같다.

| Surface | Address |
| --- | --- |
| BigQuery REST와 health | `http://localhost:9050` |
| BigQuery Storage gRPC | `localhost:9060` |

BigQuery v2 resource를 사용하기 전에 emulator 전용 project를 생성한다.

```bash
curl -sS -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project"}'

curl -sS -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/datasets \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"datasetId":"analytics"},"location":"US"}'
```

Dataset JSON은 공식
[`datasets.insert`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert)
resource를 기준으로 한다. 지원하지 않는 request field가 decode되더라도 해당
의미가 구현되었다는 뜻은 아니다.

<!-- section: query-example -->
## 쿼리 예제

table을 만든 뒤 `jobs.query`로 Standard SQL을 제출한다.

```bash
curl -sS -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"SELECT * FROM `test-project.analytics.inventory` ORDER BY id",
    "useLegacySql":false
  }'
```

이는 DuckDB와 호환되는 부분집합만 실행한다. [GoogleSQL query
syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax),
함수, script, optimizer, billing, 분산 실행 모델과 호환된다는 뜻이 아니다.

<!-- section: maintainer-onboarding -->
## Maintainer 시작 절차

clone 후 검증된 변경까지 가는 가장 짧고 재현 가능한 경로는 다음과 같다.

```bash
direnv allow              # 선택 사항이며 checked-in .envrc에는 credential이 없다
make check
make run
```

Checked-in `.envrc`는 `.envrc.example`의 안전한 default를 load한 다음 ignore되는
`.envrc.local`을 선택적으로 load한다. Machine-specific non-production override는
local file에만 두며 어느 file에도 credential을 넣지 않는다. Direnv를 쓰지 않으면
`make check`와 `make run`을 직접 실행하며 Make가 문서화된 default를 제공한다.
그다음 순서가 정해진 [maintainer 안내서](docs/ko/maintainer-guide.md)를 따른다.
아키텍처를 읽고, 공개 요청 하나를 실행하고, 첫 focused test를 수행한 다음,
호환성 파이프라인으로 protocol/client version을 추가하고 구조화된 보고서로 drift를
진단한다. 서비스 계약은 공식 [BigQuery REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)와 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)를 기준으로 한다.

<!-- section: tls -->
## TLS

REST와 gRPC에서 TLS를 활성화하려면 두 파일을 모두 설정한다.

```bash
export BQEMU_TLS_CERT_FILE="$PWD/certs/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/certs/server-key.pem"
export BQEMU_PUBLIC_URL="https://localhost:9050"
make run
```

클라이언트는 발급 CA를 신뢰해야 하며 certificate SAN에 포함된 hostname으로
접속해야 한다. TLS는 전송만 보호하며 [Google Cloud
인증](https://cloud.google.com/docs/authentication)의 token 획득이나 IAM 의미를
추가하지 않는다.

<!-- section: documentation -->
## 문서

- [문서 색인](docs/ko/index.md)
- [아키텍처](docs/ko/architecture.md)
- [BigQuery와 connector 내부 동작](docs/ko/bigquery-internals.md)
- [호환성 계약](docs/ko/compatibility.md)
- [Schema evolution과 CDC](docs/ko/schema-evolution-cdc.md)
- [Maintainer 안내와 runbook](docs/ko/maintainer-guide.md)
- [설정과 운영](docs/ko/operations.md)
- [아키텍처 결정](docs/ko/adr/)
- [기여 안내](CONTRIBUTING.ko.md)

모든 maintainer 문서는 영어와 한국어 쌍을 가진다. 저장소 테스트는 누락된
상대 문서, section 구조 불일치, primary-source 링크 불일치, 변경 가능한
upstream `master`/`main` 소스 링크를 거부한다.

<!-- section: development -->
## 개발

```bash
make format
make test
make vet
make build
make python-test
make bq-test
```

소비자는 이 저장소를 직접 빌드하며 다른 emulator를 clone하거나 다시 빌드하지
않는다. 프로토콜 코드는 Google이 생성한 공식 Storage API package를 사용하며,
canonical method와 message는 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1)에
정의되어 있다.

<!-- section: non-goals -->
## 비목표

`go-bemu`를 성능 예측, IAM 검증, quota/billing 테스트, 지역 배치, 프로덕션
내구성 또는 GoogleSQL 동등성의 증거로 사용하면 안 된다. 로컬 호환성 결과는
명시적으로 나열된 계약과 버전에 대해서만 증거가 된다.
