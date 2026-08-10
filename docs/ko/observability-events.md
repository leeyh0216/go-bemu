<!-- doc-id: observability-events -->
<!-- lang: ko -->

[English](../en/observability-events.md) | [한국어](observability-events.md)

# 관측 이벤트 계약

<!-- section: event-vocabulary -->

런타임은 작은 동기식 이벤트 어휘를 구조화 로그로 기록합니다. bqemu에는
EventBus나 outbox가 없으므로 전달 API가 아닙니다.

| 이벤트 | 필수 문맥 |
| --- | --- |
| `boundary.enter` | `request_id`, `trace_id`, 경계 메타데이터 |
| `boundary.exit` | `request_id`, `trace_id`, 결과 메타데이터 |
| `boundary.reject` | `request_id`, `trace_id`, 거절 메타데이터 |
| `side_effect.before` | `request_id`, `trace_id`, component와 operation |
| `side_effect.after` | `request_id`, `trace_id`, component와 operation |
| `side_effect.error` | `request_id`, `trace_id`, 오류 메타데이터 |
| `domain.transition` | aggregate, from, to, reason, correlation ID |

이 표는 `internal/observability.EventKind` descriptor에서 생성되며 계약
테스트가 체크인된 카탈로그와 descriptor의 일치를 보장합니다.
주변 요청 계약은 [BigQuery REST API
참조](https://cloud.google.com/bigquery/docs/reference/rest)를 따릅니다.
