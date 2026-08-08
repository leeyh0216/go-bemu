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

**현재 구현:** 공개 제어 영역은 REST 메타데이터와 쿼리, 선택적으로 활성화하는
Parquet 적재 작업으로 구성됩니다.

공개 Storage Read는 크기 제한을 둔 DuckDB 스냅샷 하나를 구체화합니다. 결과는
Arrow 또는 Avro로 인코딩합니다. 각 스트림은 정해진 논리 범위와 재개 가능한
오프셋을 가집니다.

공개 Storage Write는 `PENDING` 스트림과 기본 스트림에서 `ProtoRows`를 받습니다.
오프셋을 검증하고 `PENDING` 스트림을 확정한 뒤, 검증된 그룹을 직렬화된 DuckDB
트랜잭션 하나로 커밋합니다.

두 데이터 영역은 부분 지원(`Partial`) 상태입니다. 읽기에는 스트림 분할, 압축,
과거 스냅샷, 재시작 복구, 중첩 필드 선택 기능이 없습니다. 쓰기에는 CDC,
`ArrowRows`, `BUFFERED` 스트림, 명시적 `COMMITTED` 스트림, `FlushRows`, 영속
준비 영역이 없습니다.

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

익명 쿼리 결과는 프로젝트와 위치별로 에뮬레이터가 소유하는 숨김 데이터 세트
하나를 사용합니다. 각 작업의 테이블 식별자는 충돌하기 어렵게 생성합니다.
메타데이터를 반영할 때 파일 설정으로 정한 24시간 만료 시각을 기록합니다.

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

현재 저장소는 작업 상태와 구체화한 결과를 메모리에 보관합니다. 쿼리 작업은
`(project, location, jobId)`와 기준 설정 지문값으로 식별합니다. 이미 사용한 ID는
설정이 같아도 `409 duplicate`를 반환합니다. 설정 지문값은 SQL을 기록하지 않고도
기존 설정과 새 설정이 같은지 구분합니다.

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

지원하는 쿼리 범위에서는 작업을 만들기 전에 명시적으로 `QueryAnalyzer` 포트를
호출합니다.

```text
GoogleSQL 요청
  -> 구조 기반 관계 분석
  -> 원본/기본/대상 데이터 세트 위치 검증
  -> 익명 대상 생성(행을 반환하고 대상을 생략한 경우)
  -> JobRepository.CreateOrGet
  -> DuckDB 결과 구체화
  -> 카탈로그 반영
```

애플리케이션은 DuckDB 구문 분석기를 의존하지 않습니다. DuckDB 어댑터는 참조한
테이블 식별자와 `producesRows`만 반환합니다. 로그에는 SQL 길이와 요약 해시,
명령문 유형, 모델 버전, 개수만 남깁니다. SQL 원문은 남기지 않습니다.

이 동작은 BigQuery의 [위치
추론](https://cloud.google.com/bigquery/docs/locations#specify_locations)과
`JobConfigurationQuery`의 [자동 생성 대상
계약](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)을
따릅니다. 구체화 데이터 세트를 지정하지 않은 커넥터 `0.44.2`는 완료된 작업의
`destinationTable`을 읽습니다. 해당 코드는
[`TempTableBuilder`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L1150-L1240)에
있습니다.

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

이 트랜잭션만으로 프로세스 내부 작업 저장소나 스트림 원장이 재시작 후에도 유지되는
것은 아닙니다. 객체 다운로드는 의도적으로 적재 커밋 밖에서 수행합니다.

<!-- section: sql-boundary -->
## SQL 문법 경계

백틱 참조 변환은 현재 임시 어댑터가 담당합니다. 어휘 분석기는 관계가 나오는 위치,
인용된 열, 문자열, 주석을 구분합니다. 스크립트, 테이블 데코레이터, 함수 인수, 인용하지
않은 모든 경로까지 처리하는 완전한 구문 분석기는 아닙니다.

일반적인 호환성을 제공하려면 GoogleSQL 구문 분석기와 의미 분석 어댑터가 필요합니다.
문법 기준은 [GoogleSQL 어휘
구조](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)와
[쿼리 문법](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)입니다.
알 수 없거나 지원하지 않는 형식은 비슷한 SQL로 추정하지 않고 명시적으로
거부해야 합니다.

분석기는 카탈로그를 바꾸는 DDL을 표시합니다. 애플리케이션은
`query.ddl.catalog-sync-v1`에 따라 `CREATE`, `ALTER`, `DROP`, `TRUNCATE`를 작업 생성과
엔진 실행 전에 거부합니다. DDL을 지원하려면 DuckDB에서 직접 실행해서는 안 됩니다.
기준 카탈로그와 저장소 변경을 원자적으로 조정하는 포트가 필요합니다.

같은 경계에서는 명령문 하나와 선택적인 마지막 세미콜론만 허용합니다. 리터럴과
주석을 구분하는 검사기는 모든 [여러 명령문
쿼리](https://cloud.google.com/bigquery/docs/multi-statement-queries)를
`query.scripts.unsupported-v1`로 작업 생성과 엔진 변경 전에 거부합니다.

스크립트를 완전히 지원하려면 명령문별 의미 분석, 변수, 제어 흐름, 임시 객체, 작업
단위 트랜잭션 의미가 필요합니다. 해석하지 못한 스크립트를 DuckDB에 그대로 넘기는
우회 방식은 허용하지 않습니다.

검증된 정적 비파티션 덮어쓰기는 의도적으로 입력 구조와 버전을 제한합니다. 출시된
커넥터 `0.44.2`를 사용하는 공개 API E2E는 토큰 파서로
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)의
구현에서 파생한 커넥터 `0.44.2` 구조를 인식합니다. 조건이 항상 거짓인 [BigQuery
`MERGE` 계약](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)을
적용하고 원자적인 [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into) 하나를 실행합니다.

동적 시간·범위 파티션 덮어쓰기나 임의의 `MERGE`로 일반화하지 않습니다. 두 기능은
명시적인 미지원 항목입니다.

<!-- section: runtime-security -->
## 실행 환경, TLS, 인증 정보

프로세스는 웨어하우스 하나와 프로세스 내부 카탈로그·작업 저장소를 구성합니다.
시스템 시계와 ID 어댑터, 애플리케이션 서비스, 공개 REST/gRPC 수신기도 구성합니다.
관리용 수신기는 선택 사항입니다.

인증서 한 쌍으로 공개 수신기와 활성화된 관리 수신기에 TLS를 적용할 수 있습니다.
객체 조립 지점은 웨어하우스에 변경을 가하기 전에 인증 애플리케이션 서비스를
만듭니다. 이 서비스는 교체 가능한 검증 포트를 감쌉니다.

REST와 gRPC 경계는 `disabled`, 문법만 확인하는 `bearer-present`, 파일에서 읽고 크기
상한을 적용하는 `static` 어댑터를 같은 서비스로 사용합니다. 관측성 처리는 가장
바깥에 둡니다. 인증은 HTTP 본문 디코딩, gRPC `RecvMsg`, 라우팅, 애플리케이션 변경
작업보다 먼저 실행합니다.

정적 스냅샷은 변경할 수 없으며 원자적으로 교체합니다. 시작할 때 잘못된 설정을
발견하면 프로세스를 종료합니다. 다시 읽은 설정이 잘못되면 모든 요청을 거부합니다.
다음에 올바른 설정을 읽으면 복구합니다.

공개 API에서는 REST 상태 확인과 준비 상태 확인, gRPC 상태 확인 서비스만 인증 없이
제공합니다. REST 탐색 API와 gRPC 리플렉션은 보호합니다. Bearer 파서는 [RFC
6750](https://www.rfc-editor.org/rfc/rfc6750#section-2.1)을 따릅니다.

전송 보안, 토큰 획득, 인증, 인가는 서로 다른 지원 범위입니다. 서비스 계정,
사용자 인증 ADC, 외부 계정 WIF는
[Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)와
[Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation)에 따라
Bearer 토큰을 얻을 수 있습니다. 서비스는 [Google Cloud
인증](https://cloud.google.com/docs/authentication)의 Google 서명 검증, 토큰 교환,
IAM 인가를 재현하지 않습니다.

<!-- section: observability -->
## 지원 범위와 관측성

경계 로그에는 작업, 상태, 식별자, 개수, 지연 시간, 요약 해시가 들어갑니다.
Authorization 값, 인증 정보, 토큰, SQL 원문, 행 내용, protobuf JSON, HTTP 본문, 오류
문구는 로그 형식과 수준, 설정 모드에 관계없이 제외합니다.

더 이상 사용하지 않는 `logging.unsafePayloads` 입력은 설정 해석 호환성만 유지합니다.
이 값을 설정해도 동작은 바뀌지 않습니다.

관측성 어댑터는 불투명 값을 구조 요약, 바이트·항목 수, 전체 값의 SHA-256으로만
기록합니다. 알 수 없는 값은 안전하다고 확인될 때까지 기록하지 않습니다. 이 원칙은
[Cloud Logging 감사
지침](https://cloud.google.com/logging/docs/audit/best-practices)을 따릅니다. 정규식
치환만으로 알 수 없는 프로토콜 값이 안전하다고 판단하지 않습니다.

지원 범위 프로필은 버전별로 관찰한 결과입니다. 기능 협상 수단이 아니며 모든 실행
절차의 성공을 보장하지도 않습니다.

<!-- section: replacement-roadmap -->
## 교체 로드맵

1. 기준 메타데이터, 작업, 읽기 세션, 쓰기·적재 원장을 트랜잭션을 지원하는 시스템
   테이블에 영속화합니다.
2. 버전을 고정한 정적 덮어쓰기 입력 구조는 일반화하지 않습니다. 넓은 범위의 정규식
   SQL 변환은 구조 기반 어댑터로 교체합니다.
3. 현재 바이트, 행, 세션 상한은 유지합니다. Storage Read에 스트림 분할, 압축, 과거
   스냅샷, 중첩 필드 선택, 영속 세션 복구를 추가합니다.
4. Storage Write에 `ArrowRows`, `BUFFERED` 스트림, 명시적 `COMMITTED` 스트림,
   `FlushRows`, 기본값 식, CDC, 영속적인 대기 스트림 복구를 추가합니다.
5. 준비 영역의 크기 상한은 유지합니다. 적재 포트에 없는 테이블 생성, 스키마 변경
   옵션, Parquet 이외 형식, 멀티파트·재개 가능 전송을 추가합니다.
6. 저장소 객체를 먼저 정리하고 메타데이터 작업을 재시도할 수 있는 원칙은
   유지합니다. 익명 결과의 소유권과 만료를 영속화하고 실행량에 상한을 둔 백그라운드
   정리 작업을 추가합니다.

이 변경은 의존성 규칙을 지켜야 합니다. DuckDB는 애플리케이션 API가 아니며 교체
가능한 구현으로 남아야 합니다.
