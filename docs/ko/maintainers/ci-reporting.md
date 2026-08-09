<!-- doc-id: maintainers/ci-reporting -->
<!-- lang: ko -->

[English](../../en/maintainers/ci-reporting.md) | [한국어](ci-reporting.md)

# CI 테스트 리포트

CI는 먼저 판단 결과를 보여 주고 그다음 상세 증거를 제공합니다. workflow는 공식 [GitHub
Actions job summary 계약](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#adding-a-job-summary)과
공개 API 구현 기준인 [BigQuery REST API 레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를
따릅니다.

<!-- section: decision -->
## 먼저 읽을 내용

모든 통합 job은 GitHub Actions Summary에 test 수, passed, failed, errors, skipped,
JUnit producer 누락 여부를 짧은 표로 기록합니다. release-blocking aggregate job은
모든 필수 job 결과와 publish 차단 여부를 두 번째 표로 기록합니다.

artifact를 열기 전에 이 summary를 읽습니다. JUnit 출력이 없는 실패 job은 빈 성공
리포트처럼 보이지 않고 누락으로 표시합니다.

<!-- section: artifacts -->
## Artifact 결정

| Artifact | 생성 시점 | 내용 | 목적 |
| --- | --- | --- | --- |
| `test-report-*` | 모든 통합 실행 | `index.html`, `summary.md`, JUnit XML, 구조화된 증거와 mismatch data | `index.html`을 내려받아 읽기 쉬운 suite와 failure report를 봅니다. |
| `failure-diagnostics-*` | 실행이 실패했을 때만 | 해당 workflow가 선택한 process, emulator, JVM, load diagnostics | 이미 확인한 test failure의 원인을 분석합니다. |
| report artifact 없음 | JUnit producer가 없는 static 또는 Go-only job | job outcome과 aggregate summary는 Actions에 남음 | 상세 test coverage처럼 보이는 빈 XML artifact를 만들지 않습니다. |

renderer는 선언한 JUnit 경로만 읽습니다. artifact directory 전체를 재귀적으로 업로드하지
않습니다. 따라서 새 test runner를 추가할 때는 JUnit을 만들고 `test-report-*`에 참여할지,
아니면 Actions outcome만 두는 이유를 문서화할지 명시적으로 결정합니다.

<!-- section: inspect -->
## 실패 확인 절차

1. 실패한 job의 Summary에서 실패 suite 또는 누락된 JUnit producer를 확인합니다.
2. 같은 `test-report-*` artifact를 내려받아 브라우저에서 `index.html`을 엽니다. JUnit
   원본의 failure와 error text가 포함됩니다.
3. process 또는 service detail이 필요할 때만 `failure-diagnostics-*`를 내려받습니다.
4. 가장 작은 owning boundary를 고치고 runner의 JUnit case를 추가하거나 갱신해 다음
   summary에서도 같은 신호가 보이게 합니다.

reporting script는 standard-library regression test를 가집니다. 프로토콜과 통합 비교
로직과는 의도적으로 분리되어 있습니다.
