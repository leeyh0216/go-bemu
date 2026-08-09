<!-- doc-id: architecture -->
<!-- lang: ko -->

[English](../en/architecture.md) | [한국어](architecture.md)

# 아키텍처

<!-- section: goals -->
## 목표와 비목표

이 서비스는 로컬 클라이언트에서 관찰할 수 있는 BigQuery 계약을 재현합니다.
실행 엔진, 메타데이터 저장소, 객체 저장소, 인증 식별 정보, 시계, ID 생성기는
교체할 수 있어야 합니다.

계약의 기준은 [BigQuery REST
API](https://cloud.google.com/bigquery/docs/reference/rest)와 [Storage
RPC](https://cloud.google.com/bigquery/docs/reference/storage/rpc)입니다. DuckDB 고유
동작은 계약의 기준으로 삼지 않습니다.

Dremel, Colossus, 슬롯, 할당량, 과금, 지역 배치, 운영 환경 수준의 가용성은
재현하지 않습니다.

<!-- section: dependency-rule -->
## 의존성 규칙

```text
transport/rest, transport/grpc  ->  application  ->  domain + ports
                                                  ^
adapters/duckdb, memory, objectstore, system  ----|
```

화살표는 소스 코드의 의존 방향을 나타냅니다.

도메인 값에는 Google JSON, protobuf, SQL 연결 객체, 프레임워크 유형을 넣지
않습니다. 애플리케이션 서비스는 포트를 조정합니다. 보상 작업과 상태 전이도
애플리케이션 계층이 담당합니다.

전송 계층은 공개 전송 형식을 도메인 입력과 출력으로 변환합니다. 어댑터는 외부
시스템과 실제로 상호작용합니다.

<!-- section: package-ownership -->
## 패키지 소유권

| 패키지 | 담당 | 포함하면 안 되는 것 |
| --- | --- | --- |
| `internal/domain` | 식별자, 기준 스키마, 작업 상태, 도메인 오류 | HTTP, protobuf, DuckDB |
| `internal/application` | 사용 사례, 실행 순서, 보상 작업 | 라우팅, 생성된 전송 형식, SQL 문법 |
| `internal/ports` | 계층 간 입출력 계약 | 구체 클라이언트 |
| `internal/adapters/duckdb` | 저장소 객체 이름, 유형 변환, SQL 실행 | REST 리소스, 작업 수명 주기 |
| `internal/adapters/memory` | 프로세스 내부 저장소 | 쿼리 의미 |
| `internal/transport/*` | 공개 REST/gRPC 경계 | 데이터베이스 의존성 |
| `cmd/emulator` | 객체 조립과 수명 주기 | 업무 규칙 |

컴파일 시점 검증 구문은 어댑터가 포트를 구현했는지 확인합니다. 애플리케이션
테스트에서도 DuckDB를 시험용 구현으로 바꿀 수 있어야 합니다. 컴파일 성공만으로
실제 동작까지 교체 가능하다고 볼 수는 없습니다.

<!-- section: control-data-planes -->
## 제어 영역과 데이터 영역

**BigQuery 계약:** REST 리소스는 메타데이터와 작업을 만듭니다. Storage Read/Write
RPC는 대량의 행 데이터를 이동합니다.

읽기 세션은 테이블 스냅샷을 여러 스트림으로 나눕니다. 이 동작은
[`CreateReadSession`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.CreateReadSession)에
정의되어 있습니다. 쓰기 스트림 유형은 데이터가 보이는 시점과 커밋 동작을
결정합니다. 자세한 내용은 [Storage Write
API](https://cloud.google.com/bigquery/docs/write-api)에 설명되어 있습니다.

**현재 구현:** 공개 제어 영역은 REST 메타데이터, 쿼리 작업, Parquet 적재 작업으로
구성됩니다.

공개 Storage Read는 크기 제한을 둔 DuckDB 스냅샷 하나를 구체화합니다. 결과는
Arrow 또는 Avro로 인코딩합니다. 각 스트림은 정해진 논리 범위와 재개 가능한
오프셋을 가집니다.

공개 Storage Write는 `PENDING` 스트림과 기본 스트림에서 `ProtoRows`를 받습니다.
오프셋을 검증하고 `PENDING` 스트림을 확정한 뒤, 검증된 그룹을 직렬화된 DuckDB
트랜잭션 하나로 커밋합니다.

두 데이터 영역은 부분 지원(`Partial`) 상태입니다. 읽기에는 스트림 분할, 압축,
과거 스냅샷, 재시작 뒤 스냅샷 바이트 복구 기능이 없습니다. 쓰기에는 CDC,
`ArrowRows`, `BUFFERED` 스트림, 명시적 `COMMITTED` 스트림, `FlushRows`가 없습니다.

<!-- section: catalog-physical-model -->
## 카탈로그와 저장 모델

기준 리소스에는 BigQuery 프로젝트, 데이터 세트, 테이블, 필드, 파티션, 클러스터링
메타데이터가 들어 있습니다. DuckDB 어댑터는 `project.dataset.table`을 16진수로
인코딩한 저장소 스키마와 인용 처리한 테이블 식별자로 변환합니다. DuckDB
카탈로그와 SQL 동작은 [DuckDB CREATE
SCHEMA](https://duckdb.org/docs/stable/sql/statements/create_schema)와 [식별자
규칙](https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers)에 정의되어
있습니다.

현재 메타데이터 저장소와 엔진 카탈로그는 분리되어 있습니다. 생성 작업은 저장소
DDL을 먼저 실행한 뒤 메타데이터를 저장합니다. 메타데이터 저장에 실패하면 앞선
DDL을 보상합니다.

삭제 작업은 저장소 객체를 먼저 지운 뒤 메타데이터를 지웁니다. 두 단계 사이에서
프로세스가 중단되면 상태가 어긋날 수 있습니다. 재시작 후 일관성이나 원자적
카탈로그를 보장하려면 영속 메타데이터와 하나의 트랜잭션 경계가 필요합니다.

생성 쿼리 결과는 설정한 materialization 데이터 세트가 있으면 이를 사용하고, 없으면
프로젝트와 위치별로 에뮬레이터가 소유하는 숨김 데이터 세트 하나를 사용합니다. 각
작업의 테이블 식별자는 충돌하기 어렵게 생성합니다. 메타데이터를 반영할 때 파일
설정으로 정한 24시간 만료 시각을 기록하며, 대상 소유권과 만료 시각은 SQLite에
저장되어 재시작 뒤에도 유지됩니다.

`CatalogService`는 한 프로세스 안에서 저장소 객체와 메타데이터 리소스 변경을
직렬화합니다. 변경 경계에서는 만료 여부를 다시 확인합니다. `tables.get`,
`tables.list`, Storage Read의 테이블 확인 과정에서 만료된 테이블을 정리합니다. 이때
저장소 객체를 먼저 지운 뒤 메타데이터를 지웁니다.

이 동작은 BigQuery의 [익명 결과
저장소](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored)를
재현합니다. 다만 영속적인 백그라운드 만료 서비스까지 제공하지는 않습니다.

`CatalogService`에는 정리 고루틴과 `Close` 단계가 없습니다. 각 API 경계에서 필요한
지연 정리를 동기적으로 끝냅니다. 숨김 데이터 세트를 ID로 직접 지정하면 일반
콘텐츠 삭제 검사를 적용합니다. `datasets.list`에서는 `all=true`일 때만 숨김 데이터
세트를 반환합니다.

<!-- section: query-jobs -->
## 쿼리 작업 수명 주기

```text
PENDING -> RUNNING -> DONE(result)
                   -> DONE(errorResult)
```

BigQuery는 성공한 작업과 실패한 작업을 모두 `DONE`으로 표시합니다. 클라이언트는
공식 [JobStatus
리소스](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatus)에
따라 `status.errorResult`를 확인합니다.

SQLite는 작업 상태, 설정, 결과 스키마와 생성 대상 소유권을 저장합니다. 결과 행
payload는 현재 프로세스 메모리에만 두며, 재시작 뒤에는 빈 성공 결과를 만들지 않고
사용할 수 없다고 응답합니다. 쿼리 작업은
`(project, location, jobId)`와 기준 설정 지문값으로 식별합니다. 이미 사용한 ID는
설정이 같아도 `409 duplicate`를 반환합니다. 설정 지문값은 SQL을 기록하지 않고도
기존 설정과 새 설정이 같은지 구분하며, 로그에는 SQL 원문도 함께 기록합니다.

이 동작은 BigQuery의 공식 재시도 방식에 맞춥니다. 자세한 내용은 [신뢰성
지침](https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_job_insertions)을
참고합니다.

`jobs.insert`는 서비스가 소유한 백그라운드 고루틴에서 실행합니다. 실행 시간은 파일에
설정한 `query.operationTimeout`을 넘을 수 없습니다.

종료를 시작하면 새 쿼리를 받지 않습니다. 이미 받은 동기·비동기 작업은 취소합니다.
서비스가 유휴 상태가 된 뒤에 Storage 서비스와 DuckDB를 닫습니다.

모든 쿼리 결과 행은 여전히 Go 메모리에 남습니다. 작업자 수를 제한하는 접수 제어,
영속적인 종료 상태, 결과 보존 정책, 공개
[`jobs.cancel`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel) API는
아직 지원하지 않습니다. 쿼리 작업과 적재 작업 사이의 ID 중복 방지에는
`query.jobs.cross-repository-identity-v1`이 남아 있습니다. 종료 상태 저장 복구에는
`query.terminal-persistence-v1`이 남아 있습니다.

REST 요청 객체는 알려진 미지원 쿼리 제어 항목이 전달되었는지 보존합니다. 해당
항목은 `query.options.unsupported-v1`로 실행 전에 거부합니다. 진단에는 필드 이름만
포함합니다. 매개변수 값, 라벨, SQL, 행은 포함하지 않습니다. 이 경계는 공식
[`QueryRequest`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest)와
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)
필드 집합을 따릅니다.

REST 테이블 조회는 별도 `TableDataReader` 출력 포트를 거칩니다. 애플리케이션은
카탈로그 변경 경계에서 유효한 기준 메타데이터와 지연 만료 여부를 확인합니다.

파일에 설정한 작업 제한 시간은 컨텍스트를 확인하는 요청 접수 전에 시작합니다.
이 시간에는 TTL 확인과 DuckDB 트랜잭션도 포함됩니다.

DuckDB는 같은 트랜잭션에서 정확한 전체 행 수를 구합니다. 설정된 행 수까지만
읽으면서 기준 표현의 크기를 순차적으로 계산합니다. DuckDB가 반환하는 JSON에는 공개
`f/v` 행에 없는 필드 이름도 들어 있으므로 응답 크기 판정에 사용하지 않습니다.

애플리케이션은 교체 가능한 어댑터가 반환한 값을 응답 전에 다시 검증합니다. 전체 행
수는 음수가 아니어야 합니다. 적용된 행 수 제한과 페이지 범위, 기준 표현의 바이트
상한도 확인합니다.

스키마를 따르는 중첩 `f/v` JSON 표현은 REST 계층만 만듭니다. 압축하지 않은 응답은
외피와 토큰을 포함해 정확한 크기 제한을 적용합니다. 이미 받은 행 조각은 전체
내용을 한 번 더 복사하지 않고 전송합니다.

일반 페이지는 10,000,000바이트를 넘기기 직전에 다음 행을 받지 않습니다. 첫 번째
행만 100,000,000바이트의 최종 응답 상한까지 허용합니다. 이 결정적인 로컬 계산은
Cloud 내부 표현을 기준으로 한 공식 [페이지
제한](https://cloud.google.com/bigquery/docs/paging-results#api-limits)보다 엄격합니다.

리소스 범위의 불투명 토큰은 실제로 내보낸 행 수만큼 이동합니다. 이 방식은 DuckDB
값을 전송 계층 코드에 노출하지 않으면서 공식
[`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)
경계를 따릅니다.

실행 상한은 공식
[`jobTimeoutMs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.job_timeout_ms)와
같이 작업 실행 시간을 제한하려는 목적을 따릅니다. 요청별 `timeoutMs`의 정확한
동작은 별도 호환성 미지원 항목입니다.

커넥터가 요구하는 `configuration.query.priority`와 `configuration.labels`는 스케줄러
정책이 아니라 도메인 데이터입니다. 우선순위는 열거형 값인지 검증합니다. 라벨은 빈
맵을 포함해 검증하고 그대로 다시 반환합니다. 두 값 모두 설정 지문값에 포함합니다.
로그에는 우선순위, 라벨 수, 정렬한 라벨 키의 지문값만 남깁니다. 라벨 값은 남기지
않습니다.

지원하는 쿼리 범위에서는 작업을 만들기 전에 하나의 `GoogleSQLGateway` 포트를
호출합니다.

```text
GoogleSQL 요청
  -> 공식 parse와 semantic analysis
  -> 불변 AST와 기준 relation/type binding
  -> 원본/기본/대상 데이터 세트 위치 검증
  -> 익명 대상 생성(행을 반환하고 대상을 생략한 경우)
  -> JobRepository.CreateOrGet
  -> StatementExecutor 또는 StatementMaterializer
  -> 카탈로그 반영
```

애플리케이션은 DuckDB 구문 분석기를 의존하지 않습니다. Gateway가 외부 parser
handle을 소유하고 BQEMU semantic statement만 반환합니다. DuckDB 어댑터는 이
statement를 방문하며 사용자 SQL을 받거나 다시 해석하지 않습니다. 구조화 로그에는
statement kind, analysis fingerprint, destination policy, row count,
transaction 결과를 기록합니다.

이 동작은 BigQuery의 [위치
추론](https://cloud.google.com/bigquery/docs/locations#specify_locations)과
`JobConfigurationQuery`의 [자동 생성 대상
계약](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)을
따릅니다.

<!-- section: transactions -->
## 트랜잭션과 공개 시점

엔진의 명령문 트랜잭션이 곧바로 BigQuery 작업 트랜잭션이 되는 것은 아닙니다.
메타데이터와 저장소 DDL은 여전히 서로 다른 저장소에 있습니다.

명시적 쿼리 대상은 DuckDB 임시 테이블에서 한 번만 계산합니다. `WRITE_EMPTY`,
`WRITE_APPEND`, 정확히 같은 스키마를 요구하는 `WRITE_TRUNCATE`는 같은 트랜잭션에서
적용합니다. 기준은
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)입니다.

새 테이블 메타데이터는 CTAS 커밋이 끝난 뒤 공개합니다. 메타데이터 반영에 실패하면
생성한 저장소 테이블을 삭제합니다. 익명 대상은 저장소에 숨김 데이터 세트를 먼저
만든 뒤 메타데이터에 반영합니다. 이후에는 같은 CTAS 및 보상 절차를 사용합니다.

캐시 재사용, 영속적인 TTL 정리, 스키마를 바꾸는 잘라내기는 아직 지원하지 않습니다.

Parquet 적재는 임시 테이블을 검증합니다. 대상 쓰기 방식은 DuckDB 트랜잭션 하나에서
적용합니다.

Storage Write는 이름이 지정된 모든 `PENDING` 스트림을 먼저 검증합니다. 직렬화된
조정기가 DuckDB 트랜잭션 하나로 검증된 스트림을 적용합니다. 이 동작은
[`BatchCommitWriteStreams`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams)로
정의된 원자적 그룹 계약을 따릅니다.

쿼리와 로드 작업 메타데이터, Storage Write 수명 주기와 receipt 원장은 SQLite에
저장합니다. 쿼리 결과 행과 Storage Read 스냅샷 바이트는 프로세스 메모리에만 두며
재시작 뒤에는 사용할 수 없습니다. 객체 다운로드는 의도적으로 적재 커밋 밖에서
수행합니다.

<!-- section: sql-boundary -->
## SQL 문법 경계

지원하는 모든 `SELECT`, DML, script child, catalog DDL statement는 하나의 공식
GoogleSQL parse/analyze gateway와 불변 semantic AST를 사용합니다.

문법 기준은 [GoogleSQL 어휘
구조](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)와
[쿼리 문법](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)입니다.
알 수 없거나 지원하지 않는 형식은 비슷한 SQL로 추정하거나 DuckDB SQL로 재시도하지
않고 명시적으로 거부해야 합니다.

애플리케이션은 `CREATE TABLE`, `DROP TABLE`, `TRUNCATE TABLE`, 최상위 `ADD`, `RENAME`,
`DROP COLUMN`, `ALTER COLUMN SET DATA TYPE`을 타입 있는 엔진 계획과 기준 카탈로그
서비스를 통해 실행합니다. 지원하지 않는 DDL은 변경 전에
`query.ddl.catalog-sync-v1`로 거부합니다. 엔진 변경과 SQLite 공개 사이에서 프로세스가
중단된 경우의 복구는 #26에 남아 있습니다.

여러 명령문 입력은 script root로 표현합니다. 지원 범위는 `DECLARE`, `SET`,
query/DML child를 분석하고 script variable을 binding한 뒤 하나의 엔진 transaction에서
순서대로 실행합니다. Control flow, temporary routine, dynamic SQL, exception block은
지원하지 않으며 실행 전에 거부합니다. 항상 거짓인 교체를 포함한 `MERGE`도 같은 AST
visitor 경로를 사용합니다. 지원 expression과 action을 넘어서는 partition별 의미
일치는 #8에 남아 있습니다.

<!-- section: runtime-security -->
## 실행 환경, TLS, 공개 접근

프로세스는 저장 엔진 하나와 BQEMU 전용 SQLite 상태 저장소를 구성합니다. SQLite는
기준 카탈로그, 쿼리·로드 작업 메타데이터, Storage Read 수명 주기 메타데이터, Storage
Write 수명 주기와 receipt 원장을 소유합니다. 그 주위에 시스템 시계와 ID 어댑터,
애플리케이션 서비스, 공개 REST/gRPC 수신기를 구성합니다. 관리용 수신기는 선택 사항입니다.

인증서 한 쌍으로 공개 수신기와 활성화된 관리 수신기에 TLS를 적용할 수 있습니다.

BigQuery 호환 REST와 gRPC 엔드포인트는 호출자를 인증하거나 인가하지 않습니다.
`Authorization` 값이 없거나, 임의 값이거나, 형식이 잘못되었거나, 만료된 형태여도
동일한 프로토콜 핸들러로 전달합니다. 공개 실행 환경은 인증 정보를 해석하거나 호출자
신원을 요청 컨텍스트에 넣지 않습니다. 경계 로그는 전달받은 메타데이터와 전송
데이터를 인증 상태가 아닌 진단 맥락으로 기록합니다.

TLS는 전송 구간을 보호할 뿐 호출자 신원을 추가하지 않습니다. 클라이언트의 토큰 획득
절차는 에뮬레이터 실행 환경 밖의 책임입니다. `admin.tokenFile`은 별도의 진단용
수신기만 보호하며, 공개 BigQuery 요청의 인증 정책으로 사용하지 않습니다.

<!-- section: observability -->
## 지원 범위와 관측성

경계 로그에는 작업, 상태, 식별자, 개수, 지연 시간뿐 아니라 Authorization과
메타데이터 값, SQL, 행 데이터, Protobuf 메시지, HTTP 본문, 원본 오류 맥락이
들어갑니다. 크기 제한은 메모리와 로그가 끝없이 커지는 것을 막으며, 해시는 원본을
대체하는 대신 상관관계를 찾는 보조 필드로 유지합니다. 전송 데이터 기록을 켜고 끄는
설정은 없습니다. 진단 로그의 접근, 보관, 외부 전송은 배포 환경에서 관리합니다.

지원 범위 프로필은 버전별로 관찰한 결과입니다. 기능 협상 수단이 아니며 모든 실행
절차의 성공을 보장하지도 않습니다.

<!-- section: replacement-roadmap -->
## 교체 로드맵

1. 기준 메타데이터, 작업, 읽기 세션, 쓰기·적재 원장을 트랜잭션을 지원하는 시스템
   테이블에 영속화합니다.
2. 버전을 고정한 정적 덮어쓰기 입력 구조는 일반화하지 않습니다. 넓은 범위의 정규식
   SQL 변환은 구조 기반 어댑터로 교체합니다.
3. 현재 바이트, 행, 세션 상한은 유지합니다. Storage Read에 스트림 분할, 압축, 과거
   스냅샷, 영속 세션 복구를 추가합니다.
4. Storage Write에 `ArrowRows`, `BUFFERED` 스트림, 명시적 `COMMITTED` 스트림,
   `FlushRows`, 기본값 식, CDC, 영속적인 대기 스트림 복구를 추가합니다.
5. 완료되지 않은 media-upload 세션을 재시작 뒤에도 복구할 수 있게 하고 GCS 준비
   객체의 제한된 정리를 추가합니다. Parquet는 계속 유일하게 지원하는 load 형식입니다.
6. 현재의 재시작 가능 메타데이터, 저장소 객체 우선 정리, 메타데이터 삭제 재시도
   원칙을 유지하면서 실행량 상한이 있는 선택형 백그라운드 만료 정리를 추가합니다.

이 변경은 의존성 규칙을 지켜야 합니다. DuckDB는 애플리케이션 API가 아니며 교체
가능한 구현으로 남아야 합니다.
