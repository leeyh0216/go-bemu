<!-- doc-id: google-sql-boundary -->
<!-- lang: ko -->

[English](../en/google-sql-boundary.md) | [한국어](google-sql-boundary.md)

# GoogleSQL 경계와 지원 범위 안내서

<!-- section: boundary -->
## 전체 파서가 아닌 제한된 경계

BQEMU는 완전한 GoogleSQL 파서를 구현하지 않았으며 그렇게 주장하지도 않습니다. 현재는
의도적으로 범위를 좁힌 네 가지 경로를 사용합니다.

1. 쿼리 또는 DML 문장 하나를 받아 지원하는 백틱 식별자를 DuckDB용으로 바꾸는 어휘
   분석기가 있습니다.
2. Spark 커넥터 `0.44.2`의 정적 덮어쓰기 `MERGE` 하나를 처리하는 토큰 파서가
   있습니다.
3. 출처 버전을 고정한 Spark 동적 시간 파티션 덮어쓰기 스크립트를 의미 명령으로 바꾸는
   파서가 있습니다.
4. 카탈로그와 함께 변경하는 작은 DDL 범위를 애플리케이션 파서가 처리합니다.

언어의 기준은 [GoogleSQL 어휘
구조](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)와
[쿼리
문법](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)입니다.
DuckDB에서 어떤 문장이 실행되었다는 사실은 그 요청 하나에 대한 결과입니다. 주변
문법이나 GoogleSQL의 의미까지 지원한다는 뜻은 아닙니다.

<!-- section: admission -->
## 문장 승인 범위

일반 경로는 첫 키워드가 `SELECT`, `WITH`, `VALUES`, `INSERT`, `UPDATE`, `DELETE`,
`MERGE`인 문장 하나만 받습니다. 마지막 세미콜론 하나는 허용합니다. 따옴표와 주석을
구분하는 검사를 거쳐 추가 문장이 있으면 작업이나 엔진을 변경하기 전에 거부합니다.

`CREATE`, `ALTER`, `DROP`, `TRUNCATE`는 카탈로그 변경으로 분류합니다. 일반 DuckDB
실행 경로로 보내지 않습니다. 애플리케이션 DDL 파서는 아래에 적은 문장만 받으며,
`TRUNCATE`는 지원하지 않습니다. 여러 문장으로 이루어진 형식 중에서는 버전을 고정한
동적 시간 파티션 덮어쓰기만 지원합니다. 그 밖의 [여러 문장
쿼리](https://cloud.google.com/bigquery/docs/multi-statement-queries)는 지원하지
않습니다.

앞부분의 공백과 `--`, `#`, `/* ... */` 주석을 인식합니다. 문자열, 블록 주석 또는
백틱 식별자가 올바르게 끝나지 않으면 변환 전에 실패합니다. 이 분석기는 문법 및 의미
분석기를 대신하지 않습니다.

<!-- section: translations -->
## 구현한 변환

| 입력 유형 | 구현한 변환 | 보장하지 않는 동작 |
| --- | --- | --- |
| 승인된 일반 문장 | 문자열과 주석은 그대로 두고, 백틱으로 감싼 열, 별칭과 CTE 식별자를 DuckDB의 큰따옴표 식별자로 바꿉니다. | 함수, 연산자, 리터럴, 형 변환, null, 정렬 규칙과 평가 순서는 변환하지 않습니다. |
| `FROM`, `JOIN`, `MERGE`, `INTO`, `UPDATE`, `USING`, `TABLE` 뒤의 백틱 릴레이션과 같은 깊이의 쉼표 목록 | 세 부분 `project.dataset.table`, 두 부분 `dataset.table`, 기본 데이터 세트를 사용하는 한 부분 테이블을 인코딩한 물리 스키마와 테이블 이름으로 바꿉니다. | 따옴표가 없는 경로, 데코레이터, 와일드카드 테이블과 모든 중첩 문법 위치를 처리하지는 않습니다. |
| Spark `0.44.2` 정적 덮어쓰기 | 상수 거짓 조건의 `MERGE` 전체를 파싱하고 `INSERT ROW`를 DuckDB `INSERT BY NAME`으로 바꾼 뒤 `MERGE INTO` 하나를 원자적으로 실행합니다. | 일반 BigQuery `MERGE`와 같은 동작을 보장하지 않습니다. |
| Spark `0.44.2` 동적 시간 파티션 덮어쓰기 | 커넥터 스크립트 전체를 의미 명령으로 파싱하고 기준 원본·대상 스키마와 파티션 메타데이터를 확인합니다. DuckDB 트랜잭션 하나에서 해당 파티션을 지우고 원본 행을 넣습니다. | 스크립트 원문을 변환하지 않으며 임의의 스크립트도 받지 않습니다. |
| 지원 DDL | SQL을 의미 명령으로 파싱해 `CatalogService`를 호출합니다. 애플리케이션이 물리 저장소와 SQLite 메타데이터를 함께 조정합니다. | DDL 원문을 불투명한 DuckDB SQL로 넘기지 않습니다. |

정적 덮어쓰기 형식은 정확한
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)
생성 코드와 상수 거짓 조건을 사용하는 [BigQuery `MERGE`
계약](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)에
맞춥니다. 물리 실행에는 [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into)를 사용합니다. 이 형식과
비슷하지만 토큰이 다른 요청은 일반 SQL로 보내지 않고 거부합니다.

동적 형식은 기준 최상위 단일 파티션 필드에 적용한 `DATE_TRUNC` 또는
`TIMESTAMP_TRUNC`를 받습니다. 단위는 `HOUR`, `DAY`, `MONTH`, `YEAR` 중 하나입니다.
기준 메타데이터에는 호환되는 `DATE`, `TIMESTAMP` 또는 `DATETIME` 필드가 있어야
합니다. 원본 필드의 자료형, 모드, 중첩 이름과 순서도 대상과 일치해야 합니다. 범위
파티션 덮어쓰기는 지원하지 않습니다.

<!-- section: ddl -->
## 의미 기반 DDL 지원 범위

지원 범위는 [GoogleSQL 데이터 정의
언어](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language)보다
의도적으로 좁습니다.

```sql
CREATE TABLE table_reference (
  column_name scalar_type [NOT NULL]
  [, ...]
)

DROP TABLE table_reference

ALTER TABLE table_reference ADD COLUMN column_name scalar_type [NOT NULL]

ALTER TABLE table_reference DROP COLUMN column_name

ALTER TABLE table_reference RENAME COLUMN old_name TO new_name

ALTER TABLE table_reference ALTER COLUMN column_name SET DATA TYPE scalar_type
```

`table_reference`는 세 부분으로 쓸 수 있습니다. 두 부분으로 쓰면 요청 프로젝트를
사용하고, 한 부분으로 쓰면 기본 프로젝트와 데이터 세트를 사용합니다. 일반 식별자와
백틱 식별자를 모두 받습니다. 지원하는 네 `ALTER TABLE` 문장에서는 `COLUMN`을 반드시
써야 합니다.

DDL 열은 최상위 스칼라만 지원합니다. 사용할 수 있는 자료형 이름은
`BOOL`/`BOOLEAN`, `INT64`/`INTEGER`, `FLOAT64`/`FLOAT`, `NUMERIC`,
`BIGNUMERIC`, `STRING`, `BYTES`, `DATE`, `DATETIME`, `TIME`, `TIMESTAMP`,
`JSON`입니다. 10진수 문법은 `NUMERIC(p[,s])` 또는 `BIGNUMERIC(p[,s])`이며 Spark
공통 정밀도 제한과 기본값을 적용합니다. `GEOGRAPHY`, 중첩 `STRUCT` 선언과 반복 열은
SQL DDL에서 지원하지 않습니다. REST로 만든 스키마는 저장소 포트를 통해 기본 구조체와
리스트를 사용할 수 있으므로 이 제한과 구분해야 합니다.

위에 적은 형식은 모두 카탈로그 변경 경계를 통해 실행합니다. 기준 SQLite 저장소를
사용하면 각 `ALTER TABLE` 변경은 영속 의도를 먼저 기록하고 DuckDB 트랜잭션 하나를
적용합니다. 기준 스키마 반영과 변경 기록 완료는 SQLite 트랜잭션 하나에서 처리합니다.
작업이 중단되면 시작 절차에서 기록한 변경 전후 스키마와 물리 지문을 바탕으로
복구합니다. 호환되지 않는 `SET DATA TYPE` 변환은 두 스키마를 바꾸지 않고 실패합니다.

기준 변경 기록이 없는 상태 저장소에서는 열 추가와 이름 변경에 별도 제한 시간을 둔
역방향 계획 보상을 사용할 수 있습니다. 해당 구성에서는 열 삭제와 자료형 변경을
거부합니다. `CREATE TABLE`과 `DROP TABLE`은 기존 카탈로그 변경 순서를 사용하며 테이블
스키마 변경 기록의 복구 대상에는 포함되지 않습니다.

현재 DDL 토큰 분석기는 지원 명령 바로 뒤에서 입력이 끝나야 한다고 검사합니다. DDL 뒤의
세미콜론을 포함한 추가 입력은 거부합니다. 이는 앞에서 설명한 일반 단일 문장 검사와 다른
제한입니다.

<!-- section: unsupported -->
## 현재 지원하지 않는 형식

다음 표는 주요 항목을 명시한 것이며 모든 문법을 열거한 목록은 아닙니다. 알 수 없는
문법을 자동으로 지원하는 것으로 판단하면 안 됩니다.

| 영역 | 지원하지 않는 형식 |
| --- | --- |
| DDL 수식어 | `OR REPLACE`, `TEMP`/`TEMPORARY`, `IF [NOT] EXISTS`, 여러 변경 작업 |
| 테이블 생성 원본 | `LIKE`, `COPY`, `CLONE`, `AS SELECT`, 외부 테이블, 스냅샷, 구체화된 뷰 |
| 테이블 속성 | `PARTITION BY`, `CLUSTER BY`, `OPTIONS`, 기본 표현식, 제약 조건, 정렬 규칙, 정책 태그, 행 접근 정책 |
| 스키마 변경 | 중첩 및 반복 SQL 선언, 테이블 이름 변경, 열 기본값 및 옵션, 모드 변경, 여러 변경 작업 |
| 쿼리 언어 | 이름 또는 위치 기반 매개변수, 프로시저, UDF, 뷰, 동적 SQL, 트랜잭션, 변수, 제어 흐름, 일반 스크립트 |
| 릴레이션 문법 | 백틱으로 감싸지 않은 프로젝트·데이터 세트 경로, 테이블 데코레이터, 와일드카드 테이블, 연결, 외부 원본, 원격 함수 |
| DML 의미 | 임의의 BigQuery `MERGE` 동작 일치와 커넥터의 동적 범위 파티션 덮어쓰기 |
| 함수와 표현식 | 검증한 입력 유형에서 DuckDB와 같은 동작을 확인하지 않은 GoogleSQL 전용 함수 및 표현식 |

REST에서 알고 있지만 지원하지 않는 쿼리 옵션은 SQL에 값을 넣지 않습니다. 전송 또는
애플리케이션 경계에서 거부합니다. 우연히 실행되는 DuckDB 전용 문법도 선언한 GoogleSQL
지원 범위에는 포함하지 않습니다.

<!-- section: failures -->
## 오류와 테스트 계약

지원 문법의 형식이 잘못되면 입력 오류 또는 쿼리 오류를 반환합니다. 구현하지 않은 문법과
의미에는 미지원 오류를 반환하고, 정의된 항목에는 안정적인 지원 기능 식별자를 함께
사용합니다. 카탈로그 충돌, 없는 리소스와 오래된 기준 메타데이터는 각 도메인 오류 분류를
유지합니다. 로그에는 SQL과 백엔드 오류 원문을 남기지 않습니다. 문장 종류, 모델 버전,
바이트 수, 공개해도 되는 토큰 위치와 전체 쿼리 지문만 진단 정보로 사용합니다.

기능을 확장할 때는 허용 문법과 바로 이웃한 거부 문법을 함께 시험합니다. 따옴표와 주석,
전체 토큰 소비, 위치 및 참조 분석, 기준 메타데이터 검사, 트랜잭션 롤백과 로그의 민감정보
제거도 확인해야 합니다. 커넥터 입력 유형에는 정확한 생성 코드 버전과 변경 감지 테스트가
필요합니다. 일반 문법은 파서와 의미 분석 어댑터로 구현해야 하며, 어휘 치환 범위만 넓혀서
추가하면 안 됩니다.
