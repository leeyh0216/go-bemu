<!-- doc-id: adr-0002-official-storage-protobuf -->
<!-- lang: ko -->

[English](../../en/adr/0002-official-storage-protobuf.md) | [한국어](0002-official-storage-protobuf.md)

# ADR-0002: 공식 Storage API Protobuf Type을 사용한다

<!-- section: status -->
## 상태

승인됨.

<!-- section: context -->
## 배경

Storage Read/Write 호환성은 정확한 service, oneof, wrapper presence, field number,
streaming cardinality에 의존한다. 손으로 만든 유사 DTO는 compile되더라도 호환되지
않는 wire byte를 만들 수 있다. 권위 있는 계약은 공식 [BigQuery Storage v1 RPC
package](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1)다.

<!-- section: decision -->
## 결정

Google이 생성한 `storagepb` server interface를 등록하고 구현한다. Protobuf value는
gRPC transport adapter에 두고 그 경계에서 domain 또는 application input으로
변환한다. Golden test는 Go object equality만이 아니라 serialized
Arrow/Avro/Proto field를 검증해야 한다.

<!-- section: consequences -->
## 결과

RPC method name과 message evolution은 upstream API package를 따른다.
Registration과 application/protobuf slice는 production adapter보다 먼저 반영될 수
있다. 완전한 snapshot/encoder adapter를 composition하고 public edge에서 테스트할
때까지 service health는 `NOT_SERVING`, runtime method는 `UNIMPLEMENTED`로
유지한다. 공식 type을 사용한다고 BigQuery state 의미가 자동으로 생기지는 않는다.

<!-- section: alternatives -->
## 대안

`.proto` file이나 생성 Go code 복사는 두 번째 schema authority를 만들므로
거부했다. Generic byte proxy는 message-level invariant를 안전하게 강제하지 못해
거부했다.
