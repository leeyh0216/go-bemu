# 소비자 호환성

이 문서는 `contract/consumers.normalized.json`에서 생성됩니다.

| 사례 | 실행 계열 | 상태 | 런타임 | 시나리오 |
|---|---|---|---|---|
| `bq-cli-2.1.31` | bq | required | bq 2.1.31, cloudSdk 566.0.0 | `bq-metadata`<br>`bq-query` |
| `google-cloud-bigquery-python-3.43.0` | python | required | client 3.43.0, python 3.13 | `python-metadata`<br>`python-query`<br>`python-tabledata` |
| `spark-pyspark-3.5.8-connector-0.44.2` | spark | required | connector 0.44.2, java 17, python 3.11, scalaBinary 2.12, spark 3.5.8 | `spark-pyspark-public-edge` |
| `spark-scala-3.5.8-connector-0.44.2` | spark | required | connector 0.44.2, java 17, scala 2.12.18, scalaBinary 2.12, spark 3.5.8 | `spark-scala-public-edge` |
