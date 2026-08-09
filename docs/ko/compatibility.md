<!-- doc-id: compatibility -->
<!-- lang: ko -->

[English](../en/compatibility.md) | [한국어](compatibility.md)

# 현재 지원 범위

이 페이지는 BQEMU가 로컬 테스트에 맞는지 판단하는 기준입니다. **지원**은 지금 명시한
동작을 사용할 수 있다는 뜻입니다. **제한됨**은 표에 적은 경계 안에서만 동작한다는
뜻입니다. **미지원**은 요청을 거부하거나 RPC가 `UNIMPLEMENTED`를 반환한다는 뜻이므로
테스트가 이에 의존하면 안 됩니다. 공개 형태는 [BigQuery REST API
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를 따릅니다.

<!-- section: use-now -->
## 지금 사용 가능

| 영역 | 상태 | 의존할 수 있는 동작 |
| --- | --- | --- |
| 에뮬레이터 프로젝트, 데이터세트, 테이블 | 지원 | 문서화한 메타데이터 부분집합을 생성, 조회, 목록, 수정, 삭제하며 중첩/반복 필드를 포함한 스키마를 사용합니다. |
| 테이블 행과 쿼리 job | 제한됨 | 문서화한 GoogleSQL 부분집합 실행, 쿼리 job polling, 행 페이지 처리, 명시적 또는 생성 결과 목적지 사용 |
| Storage Read | 제한됨 | live read session 생성과 Arrow 또는 Avro row batch 읽기 |
| Storage Write | 제한됨 | default 또는 PENDING stream으로 ProtoRows 추가, PENDING stream finalize, 검증한 그룹 batch commit |
| Parquet load job | 제한됨 | 설정한 GCS 호환 서비스를 통해 `gs://` Parquet 객체를 읽고 지원하는 write disposition과 schema update 적용 |
| 로컬 상태 보존 | 제한됨 | 카탈로그와 job 메타데이터는 재시작 뒤에도 남지만 쿼리 결과 행과 Storage Read snapshot byte는 남지 않습니다. |
| TLS | 지원 | 인증서와 key로 REST와 gRPC TLS를 활성화할 수 있습니다. |

접속 주소는 [시작하기](getting-started.md), 시작 리소스와 GCS 설정은 [설정](configuration.md)을
참고하세요.

<!-- section: limits -->
## 중요한 제한

| 영역 | 경계 |
| --- | --- |
| GoogleSQL | 구현한 AST 부분집합만 실행합니다. 미지원 문법은 엔진을 호출하기 전에 실패합니다. |
| View | 미지원 |
| 재시작 뒤 쿼리 결과 | job 메타데이터는 남지만 메모리에 있던 비어 있지 않은 결과는 재시작 뒤 사용할 수 없습니다. |
| Storage Read | split RPC, compression, historical snapshot, 재시작 뒤 snapshot byte 복원은 없습니다. |
| Storage Write | ArrowRows, CDC, BUFFERED/명시적 COMMITTED stream, `FlushRows`는 없습니다. |
| Load 원본과 형식 | `gs://`와 Parquet만 지원합니다. 로컬 경로, Avro, ORC, CSV, NDJSON은 지원하지 않습니다. |
| Load 동작 | autodetect와 미지원 schema/layout 변경은 없습니다. multipart와 resumable media upload는 Parquet만 받습니다. resumable upload는 전체 크기를 모르는 청크와 최종 상태 조회를 받으며, 완료되지 않은 세션은 프로세스 로컬이므로 재시작 뒤 유효하지 않습니다. 완료된 media 객체는 설정한 GCS 호환 bucket에 남습니다. |
| Control plane | IAM authorization, 운영 identity service, copy job, extract job, job cancellation endpoint는 없습니다. |

지원하는 요청에 미지원 옵션이 있으면 BQEMU는 그 옵션을 조용히 무시하지 않고 거부합니다.

<!-- section: reference -->
## 정확한 API와 RPC 레퍼런스

특정 경로, RPC, 옵션, 응답 필드에 의존하는 호출자는 자동 생성 [API 및 RPC 호환성 표](api-rpc-compatibility.md)를
확인합니다. Storage 서비스 비활성화 조건과 등록만 되고 구현되지 않은 RPC도 여기에
명시합니다. 구현 세부와 통합 증거는 사용자 선행 문서가 아닌 유지보수 자료입니다.
