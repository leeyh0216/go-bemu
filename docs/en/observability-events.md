# Observability event contract

The runtime emits a small synchronous event vocabulary. These are structured
log records, not a delivery API: bqemu has no EventBus or outbox.

| Event | Required context |
| --- | --- |
| `boundary.enter` | `request_id`, `trace_id`, boundary metadata |
| `boundary.exit` | `request_id`, `trace_id`, outcome metadata |
| `boundary.reject` | `request_id`, `trace_id`, rejection metadata |
| `side_effect.before` | `request_id`, `trace_id`, component and operation |
| `side_effect.after` | `request_id`, `trace_id`, component and operation |
| `side_effect.error` | `request_id`, `trace_id`, error metadata |
| `domain.transition` | aggregate, from, to, reason, correlation ID |

This table is generated from the `internal/observability.EventKind` descriptor;
the contract test keeps the checked-in catalog aligned with that descriptor.
