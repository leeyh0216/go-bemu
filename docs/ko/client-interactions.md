<!-- doc-id: client-interactions -->
<!-- lang: ko -->

[English](../en/client-interactions.md) | [한국어](client-interactions.md)

# 클라이언트 상호작용 안내

이 문서는 클라이언트가 사용할 BQEMU endpoint와 공개 API 경계를 고르는 기준입니다. 호스트 프로세스는 `http://localhost:9050`, `localhost:9060`을 사용하고, 같은 Compose network의 서비스는 `http://bqemu:9050`, `bqemu:9060`을 사용합니다.

<!-- section: matrix -->
## 클라이언트 표

| 클라이언트 | 설정 | BQEMU 상호작용 | 시작 위치 |
| --- | --- | --- | --- |
| Python `google-cloud-bigquery` | `ClientOptions(api_endpoint=...)`, anonymous/local credentials | REST `datasets`, `tables`, `jobs.query`, `jobs.*`, `tabledata.list`, `tabledata.insertAll`, 지원 범위의 Parquet media upload | `tests/integration/` |
| Spark BigQuery connector | `httpTransport`, `bigQueryStorageGrpcEndpoint`, project/dataset | REST jobs/catalog 및 Storage Read/Write gRPC | `tests/spark/` |
| Trino | BigQuery REST와 Storage endpoint를 지원하는 connector/catalog로만 연결하며 Trino 전용 server branch는 없습니다 | 동일 REST와 Storage API, connector 자체 검증 필요 | 사용 전 compatibility 표 |
| AWS SDK / boto3 | indirect Parquet load에만 S3-compatible fake-GCS/object-store endpoint 설정 | object source resolution과 BigQuery load-job REST; AWS API 자체는 에뮬레이션하지 않습니다 | load compatibility와 object-store 설정 |
| `bq` CLI | 명시적 REST endpoint와 local credentials | REST catalog와 query API | `tests/bqcli/` |

<!-- section: workflow -->
## 반복 가능한 흐름

1. `docker compose up --build --wait`를 실행합니다.
2. `POST /bqemu/v1/projects`로 emulator project를 만든 뒤 BigQuery v2 REST로 dataset/table을 만듭니다.
3. 호스트 또는 Compose-network endpoint를 위 표대로 정확히 설정합니다.
4. client 실패를 emulator 버그로 보기 전 [호환성](compatibility.md) 표에서 operation 지원 여부를 확인합니다.

BQEMU 제품 경로는 client 이름으로 분기하지 않습니다. client별 디렉터리는 실행 예제와 integration evidence만 담습니다.

공개 경계는 공식 [BigQuery REST v2
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)와 [BigQuery Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)입니다.
