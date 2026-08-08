<!-- doc-id: consumer-compatibility -->
<!-- lang: en -->

[English](consumer-compatibility.md) | [한국어](../ko/consumer-compatibility.md)

# Consumer Compatibility

<!-- section: generated-cases -->
This page is generated from `contract/consumers.normalized.json`. Public behavior is verified against the [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest). Spark cases use the [Spark BigQuery connector 0.44.2](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2) source.

| Case | Family | Lane | Runtime | Scenarios |
|---|---|---|---|---|
| `bq-cli-2.1.31` | bq | required | bq 2.1.31, cloudSdk 566.0.0 | `bq-metadata`<br>`bq-query` |
| `google-cloud-bigquery-python-3.43.0` | python | required | client 3.43.0, python 3.13 | `python-metadata`<br>`python-query`<br>`python-tabledata` |
| `spark-pyspark-3.5.8-connector-0.44.2` | spark | required | connector 0.44.2, java 17, python 3.11, scalaBinary 2.12, spark 3.5.8 | `spark-pyspark-public-edge` |
| `spark-scala-3.5.8-connector-0.44.2` | spark | required | connector 0.44.2, java 17, scala 2.12.18, scalaBinary 2.12, spark 3.5.8 | `spark-scala-public-edge` |
