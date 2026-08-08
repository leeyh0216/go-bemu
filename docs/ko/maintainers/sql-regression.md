<!-- doc-id: maintainers/sql-regression -->
<!-- lang: ko -->

[English](../../en/maintainers/sql-regression.md) | [한국어](sql-regression.md)

# SQL 회귀 케이스

데이터 기반 SQL 스위트는 제품의 GoogleSQL 분석 경계부터 엔진 실행 경계까지 동작이
달라지는 것을 탐지합니다. 각 케이스는 카탈로그 픽스처, 실행문, 기대 결과와 선택적인
실행 후 테이블 상태를 선언합니다. 실행기는 백엔드 고유 표현이 아니라 BigQuery 기준
자료형과 값을 비교합니다.

픽스처 필드와 기대값을 추가할 때는 [GoogleSQL 자료형
레퍼런스](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)를
기준으로 사용합니다.

<!-- section: layout -->
## 케이스 구조

`internal/sqltest/testdata/cases` 아래에 디렉터리 하나를 추가합니다. 한 케이스에는
정확히 다음 네 파일만 있어야 하며, 알 수 없는 파일이나 JSON 필드는 검증에
실패합니다.

| 파일 | 계약 |
|---|---|
| `case.json` | 스키마 버전, 안정적인 케이스 ID, 기본 프로젝트와 데이터셋, 결과 행 순서 |
| `dataset.json` | 초기 프로젝트, 데이터셋, 테이블, 재귀 스키마와 행 |
| `query.sql` | 제품의 게이트웨이와 의미 실행문 경로로 실행할 이식 가능한 GoogleSQL |
| `expected.json` | 기대 행, 영향받은 행 수, 안정적인 오류와 선택적인 테이블 사후 조건 |

실행문이 결정적인 순서를 정의할 때만 `rowOrder: ordered`를 사용합니다. 행 구성은
중요하지만 순서는 중요하지 않으면 `unordered`, 행 결과가 없으면 `none`을 사용합니다.

픽스처 테이블에는 `timePartitioning`(`type`, `field`, 선택적인 `expirationMs`) 또는
`rangePartitioning`(`field`, `range.start`, `range.end`, `range.interval`) 중 하나를
선언할 수 있습니다. 로더는 없거나 호환되지 않는 파티션 필드를 거부하며 한 테이블에
두 파티션 방식을 함께 허용하지 않습니다.

<!-- section: values -->
## 자료형이 있는 값

기대 스키마에는 기준 필드 자료형, 모드, 정밀도, 소수 자릿수, 반올림 모드와 재귀
필드를 사용합니다. 값은 다음과 같이 표현합니다.

| 자료형 | JSON 표현 |
|---|---|
| `INT64`, `FLOAT64`, `BOOL`, `STRING` | JSON 스칼라 |
| `NUMERIC`, `BIGNUMERIC` | 정확한 값을 보존하는 10진수 문자열 |
| `BYTES` | Base64 문자열 |
| `DATE`, `DATETIME`, `TIME`, `TIMESTAMP` | 정규 ISO 문자열 |
| `RECORD` | 하위 필드 이름을 키로 쓰는 객체 |
| `REPEATED` | 원소 표현의 JSON 배열 |

`kind: rows`에는 `schema`와 `rows`를 모두 선언합니다. `kind: affected`에는
`affectedRows`를 선언합니다. `kind: error`에는 `error.phase`를 `analyze` 또는
`execute`로 선언하고 안정적인 오류 코드를 사용합니다. 변경이나 실패 이후의 카탈로그와
행 상태를 증명해야 할 때는 `tables`를 추가합니다.

<!-- section: authoring -->
## 작성 규칙

케이스는 특정 클라이언트, CLI, 커넥터, 버전이나 저장 엔진에 의존하면 안 됩니다.
호출자 템플릿, 백엔드 SQL 문법 또는 실행 환경별 준비 절차를 넣지 않습니다. 미지원
문법은 그 동작 자체가 의도된 제품 계약일 때만 분석 또는 실행 오류 기대값으로
표현합니다.

한 케이스는 한 동작을 검증하도록 작성합니다. 변경 케이스에는 테이블 사후 조건을
추가하고, 선제 거부 케이스에는 의도하지 않은 변경이 없었다는 사후 조건을 추가합니다.
비교기는 스키마, 행, 오류 또는 테이블 상태의 첫 차이를 필드 경로와 함께 출력합니다.

<!-- section: commands -->
## 케이스 실행

구현 중에는 단일 케이스만 실행합니다.

```sh
go test ./internal/sqltest -run '^TestGoogleSQLRegressionCases/projection-filter$' -count=1
```

SQL 경계를 변경해 제출하기 전에는 전체 SQL 회귀 작업을 실행합니다.

```sh
make ci-test-sql-regression
```

CI는 이 검사를 독립적인 필수 작업으로 실행합니다. SQL 회귀 작업이 실패하거나
건너뛰어지면 이미지 게시를 막습니다.
