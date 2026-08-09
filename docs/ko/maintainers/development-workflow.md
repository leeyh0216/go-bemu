<!-- doc-id: maintainers/development-workflow -->
<!-- lang: ko -->

[English](../../en/maintainers/development-workflow.md) | [한국어](development-workflow.md)

# 기여 프레임워크

이 문서는 [기여 안내](../../../CONTRIBUTING.ko.md)의 상세 문서입니다. 동작 하나를
구현에서 공개 계약, 생성물, CI 근거까지 가져가는 프레임워크를 설명합니다. 개발자는
소유 경계에서 한 번만 판단하고, 나머지 근거는 결정적으로 생성하는 것이 목표입니다.
공개 동작의 기준은 여전히 [BigQuery REST API
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)입니다.

<!-- section: choose-boundary -->
## 1. 소유 경계를 고릅니다

| 변경 | 시작 위치 | 시작하면 안 되는 위치 |
| --- | --- | --- |
| 도메인 규칙 또는 엔진 동작 | `internal/domain`, `internal/application`, 소유 어댑터 | REST/gRPC handler |
| 공개 REST 또는 gRPC 메서드 | 소유 application 경로, transport test, 그 뒤 `contract/operations.yaml` | 생성된 route spec, API 표 |
| GoogleSQL 동작 | `internal/querylang`, GoogleSQL gateway, 엔진 visitor | raw SQL 문자열 재작성 |
| 영속 상태 조회 | `internal/adapters/sqlite/queries/*.sql`, sqlc 설정 | repository method 안에서 SQL 직접 조립 |
| 외부 호출자 동작 | `tests/integration/<family>` | 제품 runtime package |

먼저 집중 unit/application 테스트와 소유 경계 구현을 만듭니다. 호출자가 볼 수 있는
동작은 공개 transport test가 필요합니다.

<!-- section: public-contract -->
## 2. 공개 Operation을 기록합니다

REST 또는 gRPC 동작은 [`contract/operations.yaml`](../../../contract/operations.yaml)에
entry 하나를 추가하거나 바꾸고, 선언한 모든 Go transport/application test에 literal
`contracttest.Operation(t, "...")` annotation을 넣습니다. 자세한 규칙과 예시는
[`contract/README.ko.md`](../../../contract/README.ko.md)에 있습니다.

```bash
make contract-generate
make contract-check
```

`contract-generate`는 다음 결정적 생성물을 만듭니다. 검토에는 포함하지만 직접
수정하지 않습니다.

| 생성 파일 | 용도 |
| --- | --- |
| `contract/operations.normalized.json` | 정규화한 기계 판독 공개 표면 |
| `internal/contractspec/operations_gen.go` | 실행 환경 REST/RPC route spec |
| `docs/en/api-rpc-compatibility.md` | 정확한 영문 API/RPC 인벤토리 |
| `docs/ko/api-rpc-compatibility.md` | 정확한 한글 API/RPC 인벤토리 |

`contract-check`는 선언만 하고 annotation하지 않은 테스트, 없는 operation annotation,
route/descriptor drift, 오래된 생성물도 거부합니다.

<!-- section: integration -->
## 3. 통합 동작을 추가합니다

외부 호출자는 `tests/integration`에 둡니다. runtime에 맞는 위치에 테스트를 만들고
테스트 함수에 literal operation annotation을 붙입니다. integration compiler는 선언한
test/evidence link와 공개 operation ID가 실제인지 검사합니다.

runner/runtime/version/provenance 선언의 수기 원본은
`tests/integration/contract/consumers.yaml`과 versioned case YAML입니다.

```bash
make integration-contract-generate
make integration-contract-check
```

명령은 정규 실행 matrix와 EN/KO 통합 호환성 표를 생성합니다. 생성물은 직접 수정하지
않습니다.

현재 scenario selector와 순서/cardinality expectation은 annotation만으로 추론할 수 없는
행동 assertion이므로 명시적으로 둡니다. 중복된 scenario operation/evidence 목록은
compiler가 검사하며, 후속 annotation-derived projection이 test source에서 알 수 있는
목록을 제거할 예정입니다. 그 전에도 테스트와 매니페스트가 일치하지 않으면
`integration-contract-check`가 실패합니다.

<!-- section: sql-state -->
## 4. SQL과 상태 resource를 추가합니다

SQL 회귀 case는 `internal/sqltest/testdata/cases/<case-id>/` 아래의 `dataset.json`,
`case.json`, `expected.json`을 사용합니다. harness는 공개 job과 같은 GoogleSQL gateway와
엔진 경로를 실행합니다. 큰 쿼리 결과를 Go test에 넣지 말고 집중 case를 추가합니다.

SQLite repository query는 `internal/adapters/sqlite/queries/*.sql`에 둡니다. 바꾼 뒤에는
다음을 실행합니다.

```bash
make sqlc-generate
make sqlc-check
```

생성된 Go adapter는 직접 수정하지 않습니다. `sqlc-check`는 체크인한 생성 source가 SQL
resource와 일치하는지 검사합니다.

<!-- section: verify -->
## 5. 맞는 순서로 검증합니다

로컬에서는 집중 범위만 실행하고, 비용이 큰 종단간 matrix는 CI에서 실행합니다.

```bash
# 바꾼 package만 예시로 실행합니다.
go test ./contract
go test ./internal/transport/rest

# 해당 원본을 바꿨을 때 필요한 검사입니다.
make contract-check
make integration-contract-check
make sqlc-check

# 문서 또는 CI report 변경 검사입니다.
go test ./docs ./tests/integration/cipolicy
make ci-report-test
```

source, test, 생성물을 함께 커밋합니다. CI는 커밋한 runner가 JUnit을 만든 뒤에만 Job
Summary 표와 내려받는 JUnit HTML을 만듭니다. [CI 리포트](ci-reporting.md)를 참고하세요.

<!-- section: review -->
## 검토 확인 목록

1. 변경에 소유 domain/application/adapter 경계가 하나인가?
2. 호출자에게 보이는 동작마다 공개 operation entry와 literal test annotation이 있는가?
3. 생성물은 직접 고치지 않고 다시 생성했는가?
4. 집중 테스트는 새 동작을, compiler check는 metadata 동기화를 증명하는가?
5. 사용자용 지원 범위와 설정 레퍼런스는 여전히 정확한가?
