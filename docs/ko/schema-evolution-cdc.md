<!-- doc-id: schema-evolution-cdc -->
<!-- lang: ko -->

[English](../en/schema-evolution-cdc.md) | [한국어](schema-evolution-cdc.md)

# 스키마 변경과 변경 데이터 캡처

<!-- section: schema-contract -->
## 스키마 계약

BigQuery는 일부 온라인 스키마 변경을 허용합니다. 기존 스키마를 임의로 교체하는
동작은 정의하지 않습니다.

최상위 필드나 중첩 필드를 추가할 때 새 필드는 `NULLABLE` 또는 `REPEATED`여야
합니다. 기존 필드의 식별자, 순서, 유형, 모드는 유지해야 합니다. 기준 계약은 [테이블
스키마 관리](https://cloud.google.com/bigquery/docs/managing-table-schemas)와 문서 안의
[중첩 필드 변경 절차](https://cloud.google.com/bigquery/docs/managing-table-schemas#add_a_nested_column_to_a_record_column)입니다.

`go-bemu`는 공개 REST 경계에서 `CAP-SCHEMA-ADDITIVE-V1`을 검증 완료 상태로
제공합니다. 최상위, 레코드 내부, 반복 레코드 내부의 마지막 위치에 필드를 재귀적으로
추가할 수 있습니다. 새 필드는 `NULLABLE` 또는 `REPEATED`여야 합니다.

필드 삭제, 이름 변경, 순서 변경, 유형 변경, 모드 변경, 새 `REQUIRED` 필드는
거부합니다. 기존 행에서 새 null 허용 필드의 값은 null입니다. 이 지원 범위는
의도적으로 좁습니다. BigQuery가 허용하는 모든 유형 확대나 null 허용 전환을
구현했다는 뜻은 아닙니다.

<!-- section: rest-schema-updates -->
## REST 스키마 변경

BigQuery는 부분 변경에 `tables.patch` 사용을 권장합니다. `tables.update`는 리소스
전체를 교체합니다. 공식 요청의 의미는
[`tables.patch`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch)와
[`tables.update`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/update)에
정의되어 있습니다.

어댑터는 생략한 속성과 명시적인 JSON `null`을 구분해야 합니다. 기준 메타데이터를
공개하기 전에 저장소 DDL을 적용해야 합니다. 실패에 대비한 보상 작업이나 하나의
트랜잭션 경계도 필요합니다.

`CAP-REST-METADATA-PATCH-V1`은 데이터 세트와 테이블의 PATCH 및 PUT을 검증 완료
상태로 제공합니다. 라벨, 설명, 만료 시각, 기본 만료 시간을 변경할 수 있습니다.
`If-Match` 검증에 실패하면 HTTP 412를 반환합니다.

`CAP-SCHEMA-ADDITIVE-V1`은 REST 원본 요청과 실제 에뮬레이터 프로세스를 대상으로
검증했습니다. 공식 [Python 클라이언트
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/)도 사용했습니다.
DuckDB 어댑터는 모든 저장소 필드 추가를 명시적 트랜잭션에서 적용합니다. 기존 행의
null 처리도 시험합니다.

기준 메타데이터는 여전히 프로세스 내부에만 있습니다. 프로세스가 중단되면 메모리
카탈로그와 DuckDB 파일을 원자적으로 맞출 수 없습니다. DuckDB에서 직접 수행한
변경은 기준 BigQuery 메타데이터 변경으로 보지 않습니다.

DDL 변환, 적재·쿼리의 `schemaUpdateOptions`, Storage Write 스키마 알림은 별도
미지원 항목입니다.

<!-- section: load-schema-updates -->
## 적재·쿼리 작업의 스키마 변경

적재 작업과 쿼리 작업은 `schemaUpdateOptions`로 제한된 스키마 변경을 요청할 수
있습니다. 이 옵션은 필드 추가나 null 허용 전환을 지정합니다. 전송 필드와 쓰기 방식의
상호작용은
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad)와
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)에
정의되어 있습니다.

이 처리에는 준비 영역이 필요합니다. 대상의 현재 스키마를 기준으로 검증해야 합니다.
데이터와 메타데이터를 원자적으로 공개하고 작업 오류를 영속화해야 합니다. JSON
옵션을 받는 것만으로 이 기능을 구현했다고 볼 수는 없습니다.

현재 선택형 Parquet 적재는 기존 테이블을 기준으로 유형 변환을 검증합니다. 쓰기
방식도 원자적으로 적용합니다. `schemaUpdateOptions`, 대상 생성, 자동 감지는
거부합니다. 따라서 적재 작업을 통한 스키마 변경은 지원하지 않습니다.

<!-- section: write-schema-updates -->
## Storage Write 스키마 변경

`AppendRows`는 연결의 첫 요청에서 작성자 스키마를 제공합니다. 대상 스키마가 바뀌면
응답에서 `updated_schema`를 받을 수 있습니다. Google은 이 동작을 [스키마 변경
감지](https://cloud.google.com/bigquery/docs/write-api#schema_update_detection)로
문서화합니다. 기준 메시지는
[`AppendRows` RPC](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows)에
정의되어 있습니다.

현재 공개 쓰기 서비스는 부분 지원(`Partial`) 상태입니다. `ProtoRows` 추가 요청의
작성자 스키마 지문값을 보관합니다. 실행 중인 프로세스에서는 `PENDING` 스트림의
정확한 오프셋을 유지합니다.

대상 스키마 버전을 영속적으로 추적하지는 않습니다. `updated_schema`도 반환하지
않습니다. 호환되지 않는 변경, 스키마 알림, 재시작 후 오프셋 복구는 지원하지
않습니다.

<!-- section: cdc-contract -->
## BigQuery CDC 계약

BigQuery CDC는 Storage Write 적재 모드입니다. SQL `MERGE`를 다시 쓰는 기능이
아닙니다.

테이블에는 기본 키를 선언해야 합니다. 각 행은 `_CHANGE_TYPE`에 `UPSERT` 또는
`DELETE`를 담아 보냅니다. `_CHANGE_SEQUENCE_NUMBER`는 서로 경쟁하는 변경의 순서를
선택적으로 결정합니다.

BigQuery는 백그라운드에서 변경을 적용합니다. 따라서 변경이 보이는 시점과
`max_staleness`도 관찰할 수 있는 계약입니다. 의사 열 이름, 순서 번호 형식과 정렬,
삭제 데이터 요구 사항을 포함한 기준 규칙은 [BigQuery 변경 데이터
캡처](https://cloud.google.com/bigquery/docs/change-data-capture)에 있습니다.

현재 에뮬레이터에는 기본 키 제약 메타데이터가 없습니다. CDC 변경 대기열, 적용
기준점, 백그라운드 적용 작업, 지연 허용 모델, CDC 측정값도 없습니다. 따라서 CDC는
**지원하지 않습니다**.

`UPSERT`를 DuckDB `INSERT OR REPLACE`로 즉시 처리하면 안 됩니다. 이 방식은 순서,
중복, 삭제, 공개 시점 오류를 숨깁니다.

<!-- section: stream-ledger -->
## 필요한 스트림·CDC 원장

최소한의 올바른 설계에서도 행 추가 접수와 CDC 적용을 분리해야 합니다.

| 원장 | 필요한 상태 |
| --- | --- |
| 쓰기 스트림 | 스트림 유형과 상태, 테이블, 스키마 버전과 지문값, 다음 오프셋, 수락한 데이터의 요약 해시, 확정된 행 수 |
| CDC 변경 | 기본 키 요약 해시, 변경 유형, 해석한 순서 튜플, 행 추가 식별자, 수신 시각, 적용 상태와 오류 |
| 테이블 적용 | 적용 기준점, 마지막 성공 시각, 대기 중인 변경 수, 지연 허용 정책 |

CDC 적용 순서를 판단하기 전에 `BatchCommitWriteStreams`가 대기 스트림의 행을
원자적으로 공개해야 합니다. Write API 일괄 처리 계약은 [대기 스트림을 사용한 일괄
적재](https://cloud.google.com/bigquery/docs/write-api-batch)에 정의되어 있습니다.

이 원장은 포트 뒤에 있어야 합니다. DuckDB 테이블은 원장을 저장하는 어댑터 구현 중
하나일 뿐입니다. 상태 전이 API 자체가 아닙니다.

<!-- section: flink-profile -->
## Flink 커넥터 1.2.0 클라이언트 프로필

공식 GoogleCloudDataproc Flink 커넥터 `1.2.0`은 [Spark 커넥터
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)와
별도의 클라이언트 프로필입니다. 두 커넥터의 작업·체크포인트 모델과 Storage RPC
호출 순서는 서로 바꿔 쓸 수 없습니다.

계획한 프로필은 [출시된 Maven
디렉터리](https://repo1.maven.org/maven2/com/google/cloud/flink/flink-1.17-connector-bigquery/1.2.0/)에서
`com.google.cloud.flink:flink-1.17-connector-bigquery:1.2.0`을 찾아야 합니다. 파일의
URL, 크기, SHA-256을 기록해야 합니다. 원본 저장소를 복제하거나 직접 빌드해서는 안
됩니다. 정확한 버전은 태그가 지정된
[`pom.xml`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/pom.xml)로
확인합니다.

프로필 작업에는 크기 제한이 있는 원본 읽기를 명시합니다. 기본 스트림의 한 번 이상
쓰기와 체크포인트를 사용하는 버퍼 스트림 쓰기도 구분합니다. 스키마 불일치, CDC
`UPSERT`와 `DELETE`도 각각 별도 작업으로 정의합니다.

커넥터 코드는
[`BigQueryCdcSchemaProvider.java`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQueryCdcSchemaProvider.java)에서
CDC 의사 열을 추가합니다.
[`BigQueryExactlyOnceSink.java`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/BigQueryExactlyOnceSink.java)에서
체크포인트를 사용하는 작성기를 구성합니다.

이 원본 링크는 클라이언트가 기대하는 동작을 설명합니다. 에뮬레이터가 해당 동작을
지원한다는 뜻은 아닙니다. 공개 Storage Read와 `ProtoRows`의 `PENDING`·기본 쓰기
범위는 부분 지원(`Partial`)입니다. Flink `1.2.0` E2E 검증을 마친 작업은 없습니다.
버퍼·체크포인트 쓰기, 스키마 알림, CDC는 명시적인 미지원 항목입니다.

<!-- section: evolution-pipeline -->
## 모듈식 검증 절차

모든 스키마·CDC 동작은 다음 순서로 검증합니다.

```text
프로토콜 프로필 -> 어댑터 -> 지원 범위 -> 기준 결과 -> E2E
```

프로필은 클라이언트와 프로토콜 버전을 식별합니다. 어댑터는 알려진 입력 구조만
변환합니다. 지원 범위에는 지원, 부분 지원, 미지원 상태를 기록합니다.

기준 시험 데이터에는 성공 및 실패 구조와 해당 원본 진단 맥락을 함께 포함해야
합니다. E2E는 출시된 클라이언트로 공개 REST/gRPC API를
통과해야 합니다. DuckDB 단위 테스트가 성공했다는 이유로 어느 단계도 생략할 수
없습니다.

<!-- section: drift-report -->
## 불일치 보고서

모든 불일치 보고서에는 다음과 같은 안정된 필드를 포함해야 합니다.

```text
version=<클라이언트/프로토콜 버전>
operation=<REST 메서드, RPC 또는 SQL 템플릿>
shape=<JSON/protobuf/스키마 요약>
fingerprint=<결정적 요약 해시>
fix_hint=<다음 조치 경계>
```

`fingerprint`는 기준 스키마나 데이터 구조에서 계산하며 원본 진단 맥락과 함께
제공할 수 있습니다.

`version`과 `operation`은 적용할 프로필을 선택합니다. `shape`와 `fingerprint`는
불일치가 발생한 위치를 좁힙니다. `fix_hint`는 수정할 어댑터, 지원 범위, 기준 결과,
E2E 단계 중 하나를 가리킵니다.

<!-- section: test-gates -->
## 승격 검증

검증 완료 스키마 테스트는 최상위, 중첩, 반복 레코드 필드 추가를 다룹니다. 데이터가
있는 테이블에서 새 필드가 null인지도 확인합니다. 파괴적인 변경 거부, 트랜잭션 중
저장소 오류, 오래된 ETag, Python 클라이언트 E2E도 검증합니다.

재시작 후 상태 조정, DDL, 적재·쿼리 스키마 변경, Storage Write 스키마 알림은 아직
지원하지 않습니다.

향후 CDC 검증에는 순서가 뒤바뀌거나 중복된 순서 값이 필요합니다. `UPSERT`와
`DELETE`, 누락된 키, 잘못된 의사 열도 시험해야 합니다. 재연결과 오프셋 재전송, 여러
스트림, 커밋 공개 시점, 적용 지연, 실패 복구도 검증해야 합니다.

승격할 때도 [BigQuery 데이터
유형](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)과 결과
유형을 비교해야 합니다.
