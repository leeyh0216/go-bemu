<!-- doc-id: consumer-compatibility -->
<!-- lang: ko -->

[English](../en/consumer-compatibility.md) | [한국어](consumer-compatibility.md)

# 소비자 호환성

<!-- section: generated-cases -->
이 문서는 `contract/consumers.normalized.json`에서 생성됩니다. 공개 동작은 [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest)를 기준으로 검증합니다. Spark 사례의 출처는 [Spark BigQuery 커넥터 0.44.2](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)입니다.

| 사례 | 실행 계열 | 상태 | 런타임 | 시나리오 |
|---|---|---|---|---|
| `bq-cli-2.1.31` | bq | required | bq 2.1.31, cloudSdk 566.0.0 | `bq-metadata`<br>`bq-query` |
| `google-cloud-bigquery-python-3.43.0` | python | required | client 3.43.0, python 3.13 | `python-metadata`<br>`python-query`<br>`python-tabledata` |
| `spark-pyspark-3.5.8-connector-0.44.2` | spark | required | connector 0.44.2, java 17, python 3.11, scalaBinary 2.12, spark 3.5.8 | `spark-pyspark-public-edge` |
| `spark-scala-3.5.8-connector-0.44.2` | spark | required | connector 0.44.2, java 17, scala 2.12.18, scalaBinary 2.12, spark 3.5.8 | `spark-scala-public-edge` |
