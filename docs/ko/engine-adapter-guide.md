<!-- doc-id: engine-adapter-guide -->
<!-- lang: ko -->

[English](../en/engine-adapter-guide.md) | [한국어](engine-adapter-guide.md)

# 저장 엔진 어댑터 구현 안내서

<!-- section: purpose -->
## 적용 범위

이 문서는 새 저장 엔진을 BQEMU에 연결할 때 지켜야 하는 내부 계약을 설명합니다.
엔진 어댑터는 BigQuery 논리 모델을 저장하고 실행하는 수단입니다. 기준 메타데이터를
소유하거나 공개 API의 의미를 결정해서는 안 됩니다.

현재 구현된 계약은 스키마 계획, 로드 계획, 쿼리 실행, Storage Read/Write용 좁은
포트로 나뉩니다. 하나의 거대한 엔진 인터페이스를 애플리케이션에 전달하지 않습니다.

<!-- section: dependency -->
## 의존성 방향

도메인과 애플리케이션은 DuckDB를 비롯한 구체 구현을 가져오지 않습니다. 각 기능을
사용하는 패키지가 필요한 포트를 소유합니다.

- 카탈로그 포트는 [`internal/ports/catalog.go`](../../internal/ports/catalog.go)에
  있습니다.
- 로드 포트는 [`internal/loadjob/ports`](../../internal/loadjob/ports)에 있습니다.
- Storage Read 포트는
  [`internal/storageread/ports`](../../internal/storageread/ports)에 있습니다.
- Storage Write 포트는
  [`internal/storagewrite/ports`](../../internal/storagewrite/ports)에 있습니다.
- 공통 엔진 값과 계획 계약은 [`internal/engine`](../../internal/engine)에 있습니다.

어댑터는 이 포트를 구현할 수 있지만, 반대 방향의 의존성을 만들면 안 됩니다. 포트에
엔진 SQL, 실제 자료형 이름, DSN, 로컬 파일 경로를 추가하지 않습니다. 애플리케이션이
구체 어댑터 내부 필드나 연결 객체에 접근하는 방식도 허용하지 않습니다.

<!-- section: capabilities -->
## 기능 선언

엔진은 시작할 때 `engine.Capabilities`의 변경할 수 없는 사본을 만듭니다. 이 값에는
엔진 ID와 버전, `Decimal` 정밀도와 스케일, `STRUCT`와 `LIST`의 최대 깊이, 트랜잭션,
원자적 교체, 물리 상태 검사, DDL 범위를 선언합니다.

선언하지 않은 기능은 지원하지 않는 것으로 처리합니다. 예를 들어 단일 테이블
트랜잭션을 선언하지 않은 엔진은 로드 계획을 만들 수 없습니다. `WRITE_TRUNCATE`를
계획하려면 테이블 단위 원자적 교체도 선언해야 합니다.

기능 값에 실제 자료형이나 SQL 문자열을 넣지 않습니다. 구체 엔진의 자료형 대응은
어댑터 내부에 두고, 공개 계획에는 그 대응 결과의 지문만 사용합니다.

<!-- section: composition -->
## 실행 환경 구성

구체 어댑터 생성은 실행 파일의 조립 코드에서만 수행합니다. 현재 조립 계약은
[`cmd/emulator/engine_runtime.go`](../../cmd/emulator/engine_runtime.go)에 있습니다.

`engineRuntime`은 모든 필수 포트와 수명 주기 객체가 존재하는지 한 번 확인합니다.
조립이 끝나면 카탈로그, 쿼리, 로드, Storage Read/Write와 같은 좁은 포트로 즉시
나누어 각 서비스에 주입합니다. 애플리케이션에 `engineRuntime` 전체를 전달하거나
실행 중에 필요한 구현을 찾는 저장소로 사용하지 않습니다.

종료가 필요한 Storage Write 구현은 애플리케이션용 `Coordinator`와 조립 코드용
`ManagedCoordinator`를 분리합니다. 다른 기능도 같은 원칙으로 수명 주기 제어를
소비자 포트에서 분리합니다.

<!-- section: schema-plan -->
## 스키마 계획과 실행

스키마 변경은 다음 순서로 처리합니다.

1. 애플리케이션이 실제 요청에서 `engine.SchemaIntent`를 만듭니다.
2. 어댑터의 `PlanSchema`가 논리 스키마와 기능 범위를 확인합니다.
3. 어댑터 전용 검증기는 부작용 없이 자료형을 표현할 수 있는지 확인합니다.
4. `engine.SchemaPlan`이 엔진 ID, 기능 지문, 논리 입력과 계획 발급자를 결합합니다.
5. 실행 메서드는 실제 인수로 `SchemaIntent`를 다시 만듭니다.
6. 실행 메서드는 SQL을 만들기 전에 `ValidateBinding`을 호출합니다.

`SchemaPlan`은 실행 권한을 나타내는 짧은 수명의 값입니다. SQL이나 실제 자료형을
포함하지 않으며 저장소에 기록하지 않습니다. 다른 엔진이나 다른 계획 발급자가 만든
계획, 변경된 기능 값, 변경된 스키마 입력은 실행 전에 거부합니다.

새 카탈로그 쓰기 경로는 `CreatePlannedTable` 또는
`ApplyPlannedSchemaAdditions`처럼 계획이 필수인 포트만 호출해야 합니다. 계획 없이
실행하는 편의 메서드는 애플리케이션 소유 포트에 추가하지 않습니다. 어댑터 내부
호환과 시험용으로만 유지합니다.

<!-- section: load-plan -->
## 로드 계획과 실행

로드 작업은 스키마와 원본 객체를 각각 결합합니다. 현재 처리 순서는 다음과 같습니다.

1. 대상 테이블의 `SchemaPlan`을 만듭니다.
2. 객체 저장소에서 URI에 해당하는 객체 메타데이터를 확인합니다.
3. `URI`, `generation`, `ETag`와 선언된 크기로 객체 지문을 만듭니다.
4. `LoadPlanRequest`로 `LoadPlan`을 만듭니다.
5. 원본을 제한된 임시 디렉터리에 내려받고 실제 바이트 수를 확인합니다.
6. `ExecuteLoad`가 계획, 객체 지문과 크기를 다시 확인한 뒤 트랜잭션을 시작합니다.

`LoadPlan`에는 원본 URI나 로컬 경로를 저장하지 않습니다. `ResolvedObject`에는
지문과 크기만 들어갑니다. 내려받은 `LocalObject`는 같은 지문과 실제 크기를 함께
전달하여 계획한 객체와 실행 파일을 연결합니다.

어댑터 전용 로드 검증은 파일을 내려받기 전에 끝나야 합니다. Parquet의 실제 컬럼
구조처럼 파일을 열어야 알 수 있는 정보만 실행 트랜잭션 안에서 검사합니다. 스키마,
객체, 엔진 기능 또는 어댑터 대응이 바뀌면 물리 쓰기를 시작하지 않고 거부합니다.

<!-- section: query-storage -->
## 쿼리와 Storage 포트

SQL은 GoogleSQL gateway에서 한 번만 해석합니다. Gateway는 relation과 expression
type이 canonical catalog metadata에 이미 결합된 불변의 engine-neutral semantic
statement를 반환합니다. 엔진 어댑터는 이 statement를 방문해 비공개 물리 plan을
만들며, 원문 SQL을 tokenize 또는 재파싱하거나 미해결 table path를 추론해서는 안
됩니다.

Storage Read와 Storage Write 구현도 소비자가 소유한 `resolver`와 `factory` 포트를
구현합니다. 새 엔진의 구체 타입이 `cmd/emulator`의 조립 함수 밖으로 넘어가지 않도록
합니다.

<!-- section: errors -->
## 오류 계약

계획 단계 오류는 안정적인 분류를 사용합니다. 잘못된 논리 입력은 `ErrInvalid`, 엔진이
표현할 수 없는 기능은 `ErrUnsupported`, 오래되었거나 다른 실행 환경에 묶인 계획은
`ErrPrecondition`으로 판정합니다.

어댑터 원본 오류를 계획 오류의 원인으로 감싸 실제 테이블 이름, 엔진 SQL, 파일 경로
등 백엔드 맥락을 오류 문자열과 `errors.Unwrap`에서 확인할 수 있게 합니다. 취소와
기한 초과도 그대로 보존하면서 안정적인 오류 코드와 기능 식별자를 함께 제공합니다.

<!-- section: implementation -->
## 새 어댑터 구현 순서

1. 변경할 수 없는 `engine.Capabilities`를 정의합니다.
2. 부작용이 없는 `SchemaAdapterPlanner`와 `LoadAdapterPlanner`를 구현합니다.
3. 카탈로그, 쿼리, 테이블 데이터, Storage Read/Write의 필요한 포트만 구현합니다.
4. 실행 메서드에서 계획 바인딩을 확인한 뒤에만 SQL, 트랜잭션, 파일 접근을 시작합니다.
5. `cmd/emulator`의 조립 코드에서 모든 포트를 명시적으로 연결합니다.
6. [`internal/enginetest`](../../internal/enginetest)의 계획 적합성 검사를 실행합니다.
7. 어댑터 패키지에서 실제 트랜잭션, 롤백과 자료형 대응을 별도로 시험합니다.

DuckDB 예시는 [`internal/adapters/duckdb`](../../internal/adapters/duckdb)에 있습니다.
시험용 구현은 [`internal/enginetest/fake.go`](../../internal/enginetest/fake.go)에
있습니다. DuckDB 트랜잭션 동작은 [공식 트랜잭션
문서](https://duckdb.org/docs/stable/sql/statements/transactions)를 기준으로
확인합니다. 시험용 구현을 애플리케이션 코드에 특별 취급하지 않습니다.

<!-- section: verification -->
## 검증 명령

먼저 새 어댑터에서 `enginetest.RunPlanningConformance`를 호출하는 테스트를 추가합니다.
그다음 변경한 어댑터와 소비자 패키지에 경쟁 조건 검사를 실행합니다.

```bash
go test ./internal/enginetest ./internal/adapters/<engine>
go test -race ./internal/engine ./internal/loadjob/ports ./internal/adapters/<engine>
make check
```

검토할 때에는 계획 없이 실행할 수 있는 공개 포트가 생기지 않았는지 확인합니다.
애플리케이션이 구체 엔진 패키지를 가져오지 않는지도 확인합니다. 마지막으로 계획이나
오류에 SQL, 실제 자료형, URI, 로컬 경로가 포함되지 않았는지 검사합니다.

엔진 내부에 적용 세대나 표식을 저장하는 경우에도 이것을 기준 메타데이터로
사용하지 않습니다. 해당 값은 물리 적용 결과를 확인하는 영수증입니다. BigQuery
논리 메타데이터의 원본은 BQEMU의 메타데이터 저장소입니다.
