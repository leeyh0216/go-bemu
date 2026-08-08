<!-- doc-id: readme -->
<!-- lang: ko -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu`는 Go로 새로 구현한 실험용 BigQuery 호환 로컬 서비스입니다.
DuckDB는 쿼리 실행과 데이터 저장에 사용하는 외부 엔진이며, 도메인 모델에는
포함되지 않습니다. 현재 BigQuery v2 메타데이터와 쿼리·로드 API, 공개 Storage
Read/Write gRPC의 일부를 지원합니다. 운영 환경용 데이터베이스나 BigQuery의 완전한
대체품은 아닙니다.

호환성은 [BigQuery REST v2
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest), [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc), 특정 버전의
[Spark BigQuery 커넥터 `0.44.2`
소스](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
기준으로 판단합니다.
이 저장소에는 기존 에뮬레이터의 코드를 포함하거나 복제하지 않습니다. 비교 설명은
[goccy BigQuery 에뮬레이터 `v0.8.1`
소스](https://github.com/goccy/bigquery-emulator/tree/v0.8.1)를 기준으로 작성합니다.

<!-- section: status -->
## 현재 상태

자동화 테스트로 확인한 기능은 다음과 같습니다.

- 프로세스 상태와 DuckDB 연결 준비 상태를 확인할 수 있습니다.
- 에뮬레이터 프로젝트의 수명 주기를 지원합니다. ETag 사전 조건과 메타데이터
  페이지 나누기를 포함하여 데이터 세트와 테이블을 생성, 조회, 수정, 삭제할 수
  있습니다.
- 최상위 및 중첩 스키마에 필드를 추가할 수 있습니다. 반복 레코드 내부 필드도
  추가할 수 있으며, DuckDB DDL은 트랜잭션으로 실행합니다. 공식 [Python 클라이언트
  `3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/)으로 이 동작을
  확인했습니다.
- 공식 [Python 클라이언트
  `3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/)으로 동기
  `jobs.query`를 실행할 수 있습니다. 프로세스 메모리에 저장되는 `jobs.insert` 작업은
  `jobs.get`과 `getQueryResults`로 조회할 수 있으며, 최종 `invalidQuery` 오류도
  변환합니다.
- [Google Cloud SDK `566.0.0`](https://cloud.google.com/sdk/docs/release-notes#56600_2026-04-28)의
  공식 [`bq` CLI `2.1.31`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)로
  프로젝트, 데이터 세트, 테이블, 쿼리, 작업을 처리할 수 있습니다. 필드 추가 흐름도
  확인했습니다.
- 프로젝트와 데이터 세트마다 DuckDB 스키마를 분리합니다. SQL 변환 범위는
  제한적입니다.
- [`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)가
  생성하는 커넥터 `0.44.2`의 정적 덮어쓰기 SQL을 인식합니다. 조건이 항상 거짓인
  [BigQuery `MERGE`](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)를
  원자적인 [DuckDB `MERGE
  INTO`](https://duckdb.org/docs/current/sql/statements/merge_into) 하나로 변환합니다.
- 공개 Storage Read 세션은 크기에 상한을 둔 DuckDB 스냅샷 하나를 사용합니다.
  Arrow와 Avro 인코딩, 열 선택과 행 제한 검사, 논리 스트림 범위, 오프셋 재개를
  지원합니다.
- 공개 Storage Write `ProtoRows` 경로는 `PENDING` 스트림과 기본 스트림을 지원합니다.
  오프셋 검사, 스트림 종료, 원자적 일괄 커밋, 여러 논리 스트림을 처리합니다. 요청
  수와 숨김 DuckDB 준비 영역의 크기에 상한을 두며, DuckDB 쓰기는 직렬화합니다.
- 선택적으로 로드 작업을 활성화할 수 있습니다. 요청 크기에 상한을 둔 fake GCS 호환
  JSON 어댑터 또는 별도로 활성화한 `file://` 어댑터에서 Parquet 파일을 준비합니다.
  기존 테이블에는 `WRITE_APPEND`, `WRITE_EMPTY`, `WRITE_TRUNCATE`를 원자적으로
  적용합니다.
- REST와 gRPC에서 TLS를 선택적으로 사용할 수 있습니다.
- 버전이 있는 설정 모델을 엄격히 검사합니다. 관리용 리스너를 선택적으로 보호할 수
  있고, 여러 리스너의 종료 시간에 상한을 둡니다. Compose 프로필은 루트 권한 없이
  실행되도록 보안 설정을 적용합니다.
- 프로토콜 기준 버전과 공개 API 경계의 상태를 관측할 수 있습니다. 민감정보는
  관측값에서 제외합니다.

중요한 제한은 다음과 같습니다.

- 영속 메타데이터, 행 삽입과 미리 보기, 복사와 추출 작업, 전체 GoogleSQL은 구현하지
  않았습니다.
- 파티션이 없는 직접 정적 덮어쓰기는 Spark `3.5.8`과 커넥터 `0.44.2`로
  검증했습니다. 시간 및 범위 파티션의 동적 덮어쓰기와 일반 BigQuery `MERGE`의 동작
  일치 여부는 아직 검증하지 않았습니다.
- Storage Read는 일부 기능만 지원합니다. `SplitReadStream`, 응답 압축, 과거 시점의
  `snapshot_time`, 재시작 후 세션 복구, 중첩 필드 선택은 지원하지 않습니다.
- Storage Write는 일부 기능만 지원합니다. CDC, Arrow 행, `BUFFERED` 스트림,
  명시적으로 생성하는 `COMMITTED` 스트림, `FlushRows`, 기본값 표현식, 재시작 후
  `PENDING` 준비 영역 복구는 지원하지 않습니다.
- 로드 작업은 일부 기능만 지원합니다. Parquet 이외의 형식, 테이블이 없을 때의
  `CREATE_IF_NEEDED`, 스키마 변경 옵션, 자동 감지, 멀티파트 및 재개 가능한 전송은
  지원하지 않습니다.
- 로컬 Bearer 인증은 비활성화 모드, 형식과 존재 여부만 확인하는 모드, 크기에 상한을
  둔 정적 토큰 모드를 제공합니다. 인증 정보 획득과 IAM은 에뮬레이션하지 않습니다.
- 테이블 데이터는 DuckDB 파일에 남을 수 있습니다. 그러나 기준 BigQuery 메타데이터는
  메모리 저장소에 있으므로 프로세스가 종료되면 사라집니다.

테스트 가능한 지원 상태는 [호환성](docs/ko/compatibility.md)에 정리되어 있습니다.
BigQuery 작업 리소스와 결과 조회 방식은 공식
[`jobs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs) 및
[`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults)에
정의되어 있습니다.

<!-- section: architecture -->
## 아키텍처

```text
REST / gRPC 인바운드 어댑터
            |
            v
     애플리케이션 유스 케이스
            |
            v
 도메인 모델 + 아웃바운드 포트
            ^
            |
DuckDB / 메모리 / 객체 저장소 / 시스템 어댑터
```

도메인과 애플리케이션 패키지는 DuckDB, HTTP, gRPC, Google DTO에 의존하지 않습니다.
애플리케이션 유스 케이스에서 BigQuery 의미를 별도로 정의하지 않은 동작은 DuckDB의
트랜잭션과 SQL 규칙을 따릅니다. 자세한 내용은
[아키텍처](docs/ko/architecture.md)와
[ADR-0001](docs/ko/adr/0001-duckdb-behind-warehouse-port.md)을 참고합니다. 물리
엔진의 계약은 [DuckDB SQL
소개](https://duckdb.org/docs/stable/sql/introduction)에 정의되어 있습니다.

<!-- section: quick-start -->
## 빠른 시작

Go 1.26 이상과 DuckDB Go 드라이버를 빌드할 C/C++ 도구 모음이 필요합니다. Docker는
선택 사항입니다. `bq` 호환성을 검사하려면 Google Cloud SDK `566.0.0`이 설치하는
CLI 버전도 필요합니다.

```bash
make test
make run
```

기본 접속 주소는 다음과 같습니다.

| 기능 | 주소 |
| --- | --- |
| BigQuery REST와 상태 확인 | `http://localhost:9050` |
| BigQuery Storage gRPC | `localhost:9060` |

BigQuery v2 리소스를 사용하기 전에 에뮬레이터 전용 프로젝트를 생성합니다.

```bash
curl -sS -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project"}'

curl -sS -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/datasets \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"datasetId":"analytics"},"location":"US"}'
```

데이터 세트 JSON은 공식
[`datasets.insert`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert)
리소스를 기준으로 합니다. 요청에 포함된 필드를 해석할 수 있더라도 해당 필드의
동작까지 지원한다는 의미는 아닙니다.

<!-- section: query-example -->
## 쿼리 예제

테이블을 만든 뒤 `jobs.query`로 GoogleSQL 쿼리를 제출합니다.

```bash
curl -sS -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"SELECT * FROM `test-project.analytics.inventory` ORDER BY id",
    "useLegacySql":false
  }'
```

이 경로에서는 DuckDB와 호환되는 일부 구문만 실행합니다. [GoogleSQL 쿼리
구문](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax),
함수, 스크립트, 최적화 도구, 비용 청구, 분산 실행 모델 전체와 호환되는 것은
아닙니다.

<!-- section: maintainer-onboarding -->
## 유지보수 담당자 시작 절차

저장소를 복제한 뒤 변경을 검증하는 기본 절차는 다음과 같습니다.

```bash
direnv allow              # 선택 사항이며 저장소의 .envrc에는 인증 정보가 없습니다
make check
make run
```

저장소의 `.envrc`에는 민감정보가 없으며, `.envrc.example`의 안전한 기본값을
불러옵니다. 파일이 있으면 Git에서 제외한 `.envrc.local`도 불러옵니다. 컴퓨터마다
다른 개발용 설정은 이 로컬 파일에만 두고, 어떤 파일에도 인증 정보를 넣지 않습니다.

Direnv를 사용하지 않으면 `make check`와 `make run`을 직접 실행할 수 있습니다.
Make가 문서화된 기본값을 제공합니다. 그다음 [유지보수 담당자
안내서](docs/ko/maintainer-guide.md)의 순서를 따릅니다. 아키텍처를 읽고 공개 요청과
관련 테스트를 실행한 뒤, 프로토콜과 클라이언트 버전에 따른 호환성 차이를 구조화된
보고서로 진단합니다. 서비스 동작은 공식 [BigQuery REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)와 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)를 기준으로
판단합니다.

<!-- section: tls -->
## TLS

REST와 gRPC에서 TLS를 사용하려면 인증서와 키 파일을 모두 설정합니다.

```bash
export BQEMU_TLS_CERT_FILE="$PWD/certs/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/certs/server-key.pem"
export BQEMU_PUBLIC_URL="https://localhost:9050"
make run
```

클라이언트는 인증서를 발급한 CA를 신뢰해야 합니다. 인증서의 SAN에 포함된 호스트
이름으로 접속해야 합니다. TLS는 전송 구간만 보호하며 [Google Cloud
인증](https://cloud.google.com/docs/authentication)의 토큰 획득이나 IAM 동작을
추가하지 않습니다.

<!-- section: documentation -->
## 문서

- [문서 색인](docs/ko/index.md)
- [아키텍처](docs/ko/architecture.md)
- [BigQuery와 커넥터 내부 동작](docs/ko/bigquery-internals.md)
- [호환성 계약](docs/ko/compatibility.md)
- [스키마 변경과 CDC](docs/ko/schema-evolution-cdc.md)
- [유지보수 안내와 작업 절차](docs/ko/maintainer-guide.md)
- [설정과 운영](docs/ko/operations.md)
- [아키텍처 결정](docs/ko/adr/)
- [기여 안내](CONTRIBUTING.ko.md)

모든 유지보수 문서는 영어판과 한국어판을 함께 제공합니다. 저장소 테스트는 대응하는
문서가 없거나 절 구조와 주요 출처 링크가 다른 경우 실패합니다. 변경될 수 있는 상위
저장소의 `master` 또는 `main` 링크도 허용하지 않습니다.

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

사용자는 이 저장소를 직접 빌드합니다. 다른 에뮬레이터를 복제하거나 다시 빌드하지
않습니다. 프로토콜 코드는 Google이 생성한 공식 Storage API 패키지를 사용합니다.
기준 메서드와 메시지는 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1)에
정의되어 있습니다.

<!-- section: non-goals -->
## 비목표

`go-bemu`를 성능 예측, IAM 검증, 할당량과 비용 청구 테스트, 지역 배치, 운영 환경의
내구성 또는 GoogleSQL 동등성의 근거로 사용하면 안 됩니다. 로컬 호환성 결과는 이
문서에 나열한 동작과 버전에 대해서만 유효합니다.
