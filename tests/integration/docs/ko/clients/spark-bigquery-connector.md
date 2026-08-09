<!-- doc-id: clients/spark-bigquery-connector -->
<!-- lang: ko -->

[English](../../en/clients/spark-bigquery-connector.md) | [한국어](spark-bigquery-connector.md)

# PySpark와 Scala Spark

이 안내는 Spark `3.5.8`, Scala `2.12.18`(바이너리 `2.12`), Java `17`, Spark BigQuery
Connector `0.44.2` 조합으로 검증했습니다. 커넥터에 따라 달라지는 동작은 검토한 [커넥터 소스
리비전](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92)을
기준으로 합니다.

<!-- section: endpoints -->
## REST와 Storage 접속 주소

테이블을 읽거나 직접 쓰려면 두 주소를 모두 전달합니다.

```text
parentProject=test-project
billingProject=test-project
project=test-project
bigQueryHttpEndpoint=http://localhost:9050
bigQueryStorageGrpcEndpoint=localhost:9060
gcpAccessToken=local-test-token
```

`bigQueryHttpEndpoint`는 테이블 메타데이터와 job 요청에 사용합니다.
`bigQueryStorageGrpcEndpoint`는 Storage Read와 직접 Storage Write RPC에 사용합니다.
같은 Compose의 다른 서비스에서는 `bqemu:9050`과 `bqemu:9060`을 사용합니다.
BQEMU를 호스트에서 실행하고 개발 컨테이너에서 접속한다면
`host.docker.internal`을 사용합니다.

<!-- section: credentials -->
## 인증 정보와 TLS

가장 간단한 로컬 설정은 `gcpAccessToken`입니다. 클라이언트의 인증 파일 해석을
시험하려면 이 옵션을 제거하고, 생성된 service account, authorized user, WIF JSON
파일의 절대 경로를 `credentialsFile`에 전달합니다.

TLS를 사용한다면 이 저장소의 생성 도구로 파일을 만든 뒤 PySpark나 `spark-shell`을
시작하기 전에 PKCS12 truststore를 지정합니다.

```bash
export AUTH_DIR="$PWD/.bqemu-auth"
export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=$AUTH_DIR/truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=PKCS12"
```

REST에는 `localhost`의 `9050` 포트로 HTTPS 연결을 사용하고 gRPC 옵션에는
`localhost:9060`을 그대로 사용합니다. Token을 교환하는 JSON 인증 정보에는
[클라이언트 인증 파일과 TLS](../../../../../docs/ko/client-credentials-and-tls.md)에 설명된 발급 서버와
프록시도 필요합니다.

<!-- section: pyspark -->
## PySpark 읽기

커넥터 JAR를 포함해 PySpark를 시작하고 `SparkSession`을 만든 뒤, 전체 테이블 이름을
지정합니다.

```python
options = {
    "parentProject": "test-project",
    "billingProject": "test-project",
    "project": "test-project",
    "bigQueryHttpEndpoint": "http://localhost:9050",
    "bigQueryStorageGrpcEndpoint": "localhost:9060",
    "gcpAccessToken": "local-test-token",
}

rows = (
    spark.read.format("bigquery")
    .options(**options)
    .option("readDataFormat", "ARROW")
    .load("test-project.analytics.events")
    .collect()
)
```

<!-- section: scala -->
## Scala Spark 읽기

Scala에서도 같은 옵션을 사용합니다.

```scala
val options = Map(
  "parentProject" -> "test-project",
  "billingProject" -> "test-project",
  "project" -> "test-project",
  "bigQueryHttpEndpoint" -> "http://localhost:9050",
  "bigQueryStorageGrpcEndpoint" -> "localhost:9060",
  "gcpAccessToken" -> "local-test-token"
)

val rows = spark.read
  .format("bigquery")
  .options(options)
  .option("readDataFormat", "ARROW")
  .load("test-project.analytics.events")
  .collect()
```

프로젝트, 데이터 세트, 테이블은 [시작하기](../../../../../docs/ko/getting-started.md)의 절차로 만듭니다.

공개 통합 시나리오는 중첩 RECORD projection과 `IN`, null-safe 동등 비교,
null predicate, 중첩 `AND`/`OR`/`NOT`, starts-with/ends-with/contains 문자열
predicate, DATE/TIMESTAMP 비교의 pushdown을 검증합니다. 필드 기반 시간
파티션 filter도 포함합니다. 함수 호출, filter subquery, ingestion-time
`_PARTITIONDATE`/`_PARTITIONTIME`, 물리 파티션 pruning은 지원하지 않습니다.

<!-- section: direct -->
## 직접 읽기와 쓰기 호출

| 동작 | Operation 순서 |
| --- | --- |
| 테이블 읽기 | `bigquery.tables.get` -> `grpc.bigquery-read.create-read-session` -> 하나 이상의 `grpc.bigquery-read.read-rows` 스트림 |
| `PENDING` 직접 추가 | `grpc.bigquery-write.create-write-stream` -> 하나 이상의 `grpc.bigquery-write.append-rows` -> `grpc.bigquery-write.finalize-write-stream` -> `grpc.bigquery-write.batch-commit-write-streams` |
| 정적 직접 덮어쓰기 | `bigquery.tables.insert`로 임시 테이블을 만들고 `PENDING` 스트림 operation으로 데이터를 씁니다. `bigquery.jobs.insert`, `bigquery.jobs.get`, `bigquery.jobs.getQueryResults`로 덮어쓰기 쿼리를 실행하고 결과를 확인한 뒤 `bigquery.tables.delete`로 임시 테이블을 삭제합니다. |

Storage Read 응답은 Arrow 또는 Avro 형식입니다. 직접 Storage Write는
`ProtoRows`를 보내며 GCS를 사용하지 않습니다. Storage RPC 메시지는 공개 [BigQuery
Storage RPC 레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)를
기준으로 합니다.
PySpark scenario는 반영한 스트림 상태를 확인하기 위해
`grpc.bigquery-write.get-write-stream`을 호출할 수도 있습니다.

<!-- section: indirect -->
## Parquet 간접 쓰기

fake GCS는 기본 Compose 실행 환경에 포함됩니다. 간접 쓰기 전에 전체 stack을 시작합니다.

```bash
docker compose up --build -d --wait
```

Spark BigQuery Connector와 Hadoop GCS Connector JAR를 포함하고 Hadoop 주소를
설정합니다.

```python
spark = (
    SparkSession.builder
    .config("spark.jars", "/opt/jars/spark-bigquery.jar,/opt/jars/gcs-connector.jar")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "false")
    .config("spark.hadoop.fs.gs.auth.null.enable", "true")
    .config("spark.hadoop.fs.gs.storage.root.url", "http://localhost:4443")
    .config("spark.hadoop.fs.gs.storage.service.path", "storage/v1/")
    .getOrCreate()
)

(frame.write.format("bigquery")
 .options(**options)
 .option("writeMethod", "indirect")
 .option("intermediateFormat", "parquet")
 .option("temporaryGcsBucket", "bqemu-temporary")
 .mode("append")
 .save("test-project.analytics.events"))
```

Compose 설정은 `BQEMU_LOAD_GCS_ENDPOINT`를 통해 `http://fake-gcs:4443`을 BQEMU에
전달합니다. 호스트 네트워크에서 실행하는 Spark는 `http://localhost:4443`을
사용합니다. 두 값은 서로 다른 호출자가 같은 fake GCS 서비스를 바라보도록 합니다.

PySpark와 Scala 통합 흐름은 다음 순서로 실행됩니다.

1. Spark가 Hadoop GCS Connector를 통해 임시 Parquet 객체를 업로드합니다.
2. BigQuery 커넥터가 대상 테이블에 `bigquery.tables.get`을 호출합니다.
3. 정확한 `gs://` 원본 URI를 포함한 로드 작업 하나를 `bigquery.jobs.insert`로
   제출합니다.
4. URI를 확장해야 하면 BQEMU가 outbound GCS JSON 어댑터로 객체 목록을 조회하고,
   객체 메타데이터와 내용을 내려받습니다.
5. BQEMU가 로드를 반영하면 커넥터는 작업이 끝날 때까지 `bigquery.jobs.get`으로
   상태를 확인합니다.
6. Spark가 Hadoop GCS Connector를 통해 임시 객체를 삭제합니다.

Fake 서비스는 이 흐름에서 사용하는 공개 [Cloud Storage JSON
API](https://cloud.google.com/storage/docs/json_api/v1/objects)의 일부를 제공합니다.
BQEMU에 GCS 서버가 내장된 것은 아닙니다.

<!-- section: shapes -->
## 요청과 응답 형식

| Operation | 요청 | 커넥터가 사용하는 응답 |
| --- | --- | --- |
| `bigquery.tables.get` | REST 테이블 경로 | 테이블 메타데이터와 스키마 |
| `grpc.bigquery-read.create-read-session` | 상위 프로젝트, 테이블, 자료 형식, 선택한 필드, 행 조건, 스트림 수 | ReadSession 스키마와 스트림 이름 |
| `grpc.bigquery-read.read-rows` | Read stream 이름과 offset | 행 수를 포함한 Arrow 또는 Avro 배치 |
| `grpc.bigquery-write.create-write-stream` | 상위 테이블과 `PENDING` 스트림 타입 | WriteStream 이름과 상태 |
| `grpc.bigquery-write.append-rows` | 스트림 이름, writer 스키마, offset, `ProtoRows` | 추가된 offset 또는 행 오류 |
| `grpc.bigquery-write.finalize-write-stream` | 스트림 이름 | 최종 행 수 |
| `grpc.bigquery-write.batch-commit-write-streams` | 상위 테이블과 종료한 스트림 이름 | 반영 시각과 스트림 오류 |
| 간접 로드의 `bigquery.jobs.insert` | Parquet `configuration.load`, 원본 URI, 대상, 쓰기 방식을 포함한 Job | Job 리소스와 참조 |

필드별 처리 범위와 지원 수준은 [호환성](../../../../../docs/ko/compatibility.md)에 정리되어 있습니다.
실행 환경 버전, 아티팩트, scenario selector는 [소비자
호환성](../consumer-compatibility.md)에서 자동으로 생성합니다.

<!-- section: related -->
## 관련 작업

위 흐름에 포함되지 않은 동작은 호환성 문서와
[#5](https://github.com/leeyh0216/go-bemu/issues/5),
[#6](https://github.com/leeyh0216/go-bemu/issues/6),
[#7](https://github.com/leeyh0216/go-bemu/issues/7),
[#8](https://github.com/leeyh0216/go-bemu/issues/8)에서 관리합니다.
