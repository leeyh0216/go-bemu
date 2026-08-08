<!-- doc-id: adr-0002-official-storage-protobuf -->
<!-- lang: ko -->

[English](../../en/adr/0002-official-storage-protobuf.md) | [한국어](0002-official-storage-protobuf.md)

# ADR-0002: 공식 Storage API Protobuf 타입을 사용합니다

<!-- section: status -->
## 상태

승인했습니다.

<!-- section: context -->
## 배경

Storage Read/Write 호환성은 정확한 서비스, `oneof`, 래퍼 필드 존재 여부, 필드
번호, 요청과 응답의 스트리밍 여부에 따라 결정됩니다. 직접 만든 유사 DTO는
컴파일되더라도 호환되지 않는 전송 바이트를 만들 수 있습니다. 공식 계약은
[BigQuery Storage v1 RPC
패키지](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1)입니다.

<!-- section: decision -->
## 결정

Google이 생성한 `storagepb` 서버 인터페이스를 등록하고 구현합니다. Protobuf 값은
gRPC 전송 어댑터 안에서만 사용합니다. 이 경계에서 도메인 또는 애플리케이션
입력으로 변환합니다. 검증 기준값을 사용하는 테스트에서는 Go 객체가 같은지만
비교하지 않습니다. 직렬화한 Arrow/Avro/Proto 필드도 검증해야 합니다.

<!-- section: consequences -->
## 결과

RPC 메서드 이름과 메시지 변경은 공식 API 패키지를 따릅니다. 서비스 등록 코드와
애플리케이션/Protobuf 연동 일부는 운영용 어댑터보다 먼저 반영할 수 있습니다.
완전한 스냅샷/인코더 어댑터를 연결하고 공개 API 경계에서 검증하기 전까지 서비스
상태는 `NOT_SERVING`으로 유지합니다. 실제 RPC 메서드는 `UNIMPLEMENTED`를
반환합니다. 공식 타입만 사용한다고 BigQuery 상태의 의미가 자동으로 구현되지는
않습니다.

<!-- section: alternatives -->
## 대안

`.proto` 파일이나 생성된 Go 코드를 복사하면 스키마의 기준이 둘로 나뉩니다.
따라서 이 방식은 채택하지 않았습니다. 범용 바이트 프록시는 메시지 수준의 불변
조건을 안전하게 강제할 수 없으므로 채택하지 않았습니다.
