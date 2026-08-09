<!-- doc-id: runtime-endpoints -->
<!-- lang: ko -->

[English](../en/runtime-endpoints.md) | [한국어](runtime-endpoints.md)

# 실행 위치별 엔드포인트와 클라이언트 Lab

<!-- section: endpoint-matrix -->
## 실행 위치에 맞는 엔드포인트 선택

표의 주소를 그대로 사용합니다. `bqemu`는 Compose network의 서비스 DNS 이름이므로
호스트에서는 해석되지 않습니다.

| 호출 위치 | BigQuery REST | Storage Read/Write gRPC | load job용 외부 fake GCS |
| --- | --- | --- | --- |
| `docker compose up`을 실행한 호스트 프로세스 | `http://localhost:9050` | `localhost:9060` | `http://127.0.0.1:4443` |
| 같은 Compose 프로젝트의 sibling 서비스 | `http://bqemu:9050` | `bqemu:9060` | `http://fake-gcs:4443` |
| Compose network에 참여한 개발 컨테이너 | `http://bqemu:9050` | `bqemu:9060` | `http://fake-gcs:4443` |
| 호스트에서 실행 중인 BQEMU에 연결하는 개발 컨테이너 | `http://host.docker.internal:9050` | `host.docker.internal:9060` | 호스트에 공개한 fake-GCS 주소 |

기본 Compose 프로젝트는 BQEMU보다 먼저 fake GCS를 시작합니다.

```bash
docker compose up --build --wait
```

로드 입력은 항상 `gs://` 객체 URI입니다. load 형식과 입력 구조는
[호환성](compatibility.md)에서 먼저 확인합니다.

<!-- section: tls-paths -->
## TLS와 credential 경로

호스트 CA 파일은 `$PWD/.bqemu-auth/ca.pem`처럼 호스트 파일시스템 경로입니다.
컨테이너는 그 경로를 자동으로 읽을 수 없으므로 디렉터리를 read-only로 mount해야
합니다. 그 다음 Python에는 `/certs/ca.pem`, Java 클라이언트에는
`/certs/truststore.p12`처럼 컨테이너 mount 경로를 신뢰 설정에 지정합니다.

TLS를 켜도 endpoint의 실행 위치 규칙은 같습니다. 표의 REST 주소만 `https`로
바꾸고 Storage gRPC에는 같은 host name을 유지합니다. local issuer, certificate,
truststore 생성 절차는 [클라이언트 credential과 TLS 안내](client-credentials-and-tls.md)를
따릅니다.

<!-- section: client-labs -->
## 클라이언트 Lab 시작

[`examples/clients/`](../../examples/clients/README.md)의 Compose override는 BQEMU를
바꾸지 않고 network 내부 주소를 바로 확인하게 합니다. 각 lab 디렉터리에서 다음을
실행합니다.

```bash
docker compose -f ../../../compose.yaml -f compose.yaml up --build --wait
```

`python`, `spark`, `trino`, `aws` 중 하나를 선택합니다. image는 environment와
shell/readiness command만 제공하며, 실제 client는 문서화된 endpoint option으로
설정합니다.

| 클라이언트 | BQEMU API 경계 | 다음 안내 |
| --- | --- | --- |
| Python BigQuery | REST, API가 사용할 때 Storage gRPC | [Python BigQuery 클라이언트](clients/python-bigquery.md) |
| Spark BigQuery connector | REST와 Storage Read/Write gRPC | [PySpark와 Scala Spark](clients/spark-bigquery-connector.md) |
| Trino | BigQuery REST와 Storage를 지원하는 별도 connector | [호환성](compatibility.md) |
| AWS CLI / SDK | 지원되는 indirect load용 외부 object store이며 AWS API는 아님 | [load와 object-storage 지원](compatibility.md#load-object-storage-and-public-access) |

BQEMU runtime은 client 이름으로 분기하지 않습니다. 공개 경계는 공식 [BigQuery REST v2 레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)와 [BigQuery Storage RPC 레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)입니다.
