<!-- doc-id: getting-started -->
<!-- lang: ko -->

# 시작하기

이 문서는 로컬 클라이언트와 커넥터 테스트를 위해 BQEMU를 실행하는 방법을 설명합니다. 정확한 API 구현 범위는 [호환성](compatibility.md)에서 확인할 수 있습니다. 인증 파일과 로컬 인증서는 [로컬 클라이언트 인증 파일과 TLS](client-credentials-and-tls.md)를 참고해 주십시오.

<!-- section: compose -->
## Docker Compose

기본 REST 및 Storage gRPC 리스너와 함께 BQEMU를 시작합니다.

```bash
docker compose up --build -d --wait
curl --fail http://localhost:9050/readyz
```

이름이 `bqemu-data`인 볼륨이 `/data`를 보관합니다. 테스트 환경 정책에 따라 이 볼륨을 유지하거나 교체해 주십시오. `docker compose down --volumes`를 실행하면 볼륨도 삭제됩니다.

BigQuery 리소스를 만들기 전에 로컬 프로젝트를 생성합니다.

```bash
curl --fail -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project"}'
```

<!-- section: external-gcs -->
## Parquet Load와 외부 GCS

BQEMU에는 GCS 서버가 포함되어 있지 않습니다. 선택형 load 어댑터는 설정한 GCS JSON API endpoint에서 `gs://` 객체를 읽습니다. 다음 명령은 digest로 고정한 fake GCS를 BQEMU와 함께 시작합니다.

```bash
docker compose -f compose.yaml -f compose.load.yaml up --build -d --wait
curl --fail http://localhost:9050/readyz
curl --fail http://localhost:4443/storage/v1/b
```

두 설정 경계는 서로 독립적입니다.

| 호출자 | Endpoint 설정 | Compose 값 |
| --- | --- | --- |
| BQEMU load worker | `load.gcsEndpoint` 또는 `BQEMU_LOAD_GCS_ENDPOINT` | `http://fake-gcs:4443` |
| Spark Hadoop GCS Connector | `fs.gs.storage.root.url` | `http://localhost:4443` |

서로 다른 네트워크 공간에서 같은 Compose 서비스를 가리킵니다. 미리 만든 버킷 이름은 `bqemu-temporary`입니다. Direct Storage Write는 GCS를 사용하지 않습니다.

Spark 간접 쓰기에는 검토된 shaded Hadoop GCS Connector를 포함하고, Spark BigQuery Connector와 Hadoop 설정을 모두 지정해야 합니다.

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

(df.write.format("bigquery")
 .option("table", "test-project.analytics.events")
 .option("project", "test-project")
 .option("parentProject", "test-project")
 .option("bigQueryHttpEndpoint", "http://localhost:9050")
 .option("gcpAccessToken", "local-test-token")
 .option("writeMethod", "indirect")
 .option("intermediateFormat", "parquet")
 .option("temporaryGcsBucket", "bqemu-temporary")
 .mode("append")
 .save())
```

위 fake GCS 설정은 격리된 로컬 테스트에만 사용해야 합니다. 공개 GCS JSON API 계약은 [Cloud Storage JSON API](https://cloud.google.com/storage/docs/json_api/v1/objects)에 설명되어 있습니다.

<!-- section: python -->
## Python BigQuery Client

BQEMU가 공개 요청을 인증하지 않더라도 클라이언트에는 credential 객체가 필요합니다.

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

client = bigquery.Client(
    project="test-project",
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:9050"),
)
```

라이브러리가 service account, authorized user, WIF 또는 access token 파일을 요구한다면 생성된 fixture를 사용해 주십시오. 명령은 [로컬 클라이언트 인증 파일과 TLS](client-credentials-and-tls.md)에 있습니다.

<!-- section: bq -->
## bq CLI

`bq`가 REST 리스너를 사용하도록 지정하고 로컬 placeholder token을 전달합니다.

```bash
bq \
  --api=http://localhost:9050 \
  --project_id=test-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-test-token \
  ls
```

Parquet load는 먼저 외부 fake GCS에 객체를 올린 뒤 실행합니다.

```bash
bq --api=http://localhost:9050 \
  --project_id=test-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-test-token \
  load --source_format=PARQUET \
  test-project:analytics.events \
  'gs://bqemu-temporary/input/*.parquet'
```

<!-- section: spark -->
## PySpark와 Scala Spark

두 진입점은 같은 커넥터 옵션을 사용합니다. REST 메타데이터와 job은 `bigQueryHttpEndpoint`로 전달하고, Storage Read와 direct Storage Write는 `bigQueryStorageGrpcEndpoint`로 전달합니다.

```text
bigQueryHttpEndpoint=http://localhost:9050
bigQueryStorageGrpcEndpoint=localhost:9060
gcpAccessToken=local-test-token
parentProject=test-project
project=test-project
```

필수 실행 계약은 Spark `3.5.8`, Scala `2.12`, Spark BigQuery Connector `0.44.2`입니다. PySpark와 Scala `spark-shell`은 별도 진입점으로 검증합니다. 간접 쓰기에는 앞 절의 Hadoop GCS 설정도 필요합니다.

<!-- section: tls -->
## TLS

로컬 인증서와 클라이언트 인증 파일을 생성한 뒤 [로컬 클라이언트 인증 파일과 TLS](client-credentials-and-tls.md)에 설명된 TLS override를 시작합니다. Python과 `bq`에는 생성된 CA를 사용하고 Spark에는 생성된 PKCS12 truststore를 사용합니다. Endpoint hostname은 인증서 SAN에 포함되어야 합니다.

<!-- section: devcontainer -->
## 개발 컨테이너

클라이언트는 개발 컨테이너에서 실행하고 BQEMU는 호스트에서 실행한다면 `localhost` 대신 `host.docker.internal`을 사용합니다.

```text
http://host.docker.internal:9050
host.docker.internal:9060
http://host.docker.internal:4443
```

Linux에서는 `--add-host=host.docker.internal:host-gateway` 또는 같은 의미의 Compose `extra_hosts`를 추가합니다. 생성한 CA, truststore, credential 디렉터리는 읽기 전용으로 mount해 주십시오. 생성된 비밀 파일을 이미지 계층에 복사해서는 안 됩니다.

<!-- section: shutdown -->
## 환경 종료

BQEMU 데이터 볼륨을 유지하면서 종료합니다.

```bash
docker compose -f compose.yaml -f compose.load.yaml down
```

BQEMU 데이터도 삭제하려면 다음 명령을 실행합니다.

```bash
docker compose -f compose.yaml -f compose.load.yaml down --volumes
```
