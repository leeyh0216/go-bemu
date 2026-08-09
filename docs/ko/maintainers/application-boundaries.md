<!-- doc-id: application-boundaries -->
<!-- lang: ko -->

[English](../../en/maintainers/application-boundaries.md) | [한국어](application-boundaries.md)

<!-- section: ownership -->
# 애플리케이션 경계

애플리케이션 handler는 한 번에 한 가지 보이는 정책을 소유합니다. 즉 catalog metadata와 physical compensation, query admission/execution/materialization, 또는 Storage Read/Write 상태 전이입니다. transport package는 가장 작은 로컬 use-case interface에 의존하며 concrete application service를 요구하지 않습니다.
공개 경계는 [BigQuery REST API](https://cloud.google.com/bigquery/docs/reference/rest)를 따릅니다.

```text
REST / gRPC -> transport-local use-case interface -> application handler -> consumer-owned port -> adapter
```

`internal/application`은 adapter나 transport package를 import하거나 SQLite/DuckDB 구현 타입을 노출하면 안 됩니다. package boundary test가 이 방향을 강제하며, `cmd/emulator` 조립부가 typed port를 제공합니다.
