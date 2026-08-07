<!-- doc-id: adr-0003-public-api-edge -->
<!-- lang: ko -->

[English](../../en/adr/0003-public-api-edge.md) | [한국어](0003-public-api-edge.md)

# ADR-0003: 하나의 Public API Edge를 유지한다

<!-- section: status -->
## 상태

승인됨.

<!-- section: context -->
## 배경

SDK, `bq`, Spark, contract test, optional console은 같은 resource와 error를 관찰해야
한다. UI 전용 private API는 호환성 작업을 우회하며 공개 client가 실패하는데도
성공을 보고할 수 있다. 공개 resource shape는 [BigQuery REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest/v2)에 정의되어 있다.

<!-- section: decision -->
## 결정

모든 operational client는 `/bigquery/v2`를 사용한다. Emulator 전용 project
lifecycle, capability discovery, reset, 향후 seed/tracing operation은 명확한
`/emulator/v1` namespace를 사용하며 state를 변경할 수 있으면 비활성화하거나
보호해야 한다. Console discovery는 link를 제공할 수 있지만 business operation을
복제하면 안 된다.

<!-- section: consequences -->
## 결과

Public-edge test가 모든 client에 도움이 된다. Admin feature는 조용히 BigQuery
extension이 될 수 없다. Optional static UI serving은 API startup과 독립적이며 UI
asset이 없어도 REST와 gRPC가 동작해야 한다.

<!-- section: alternatives -->
## 대안

Console 전용 backend는 두 계약을 만들므로 거부했다. Admin operation을 BigQuery
v2 path 아래 숨기는 방식은 공식 client가 BigQuery가 아닌 의미를 우연히 발견할
수 있으므로 거부했다.
