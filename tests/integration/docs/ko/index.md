<!-- doc-id: integration-docs-index -->
<!-- lang: ko -->

[English](../en/index.md) | [한국어](index.md)

# 통합 테스트 안내

이 문서는 공개 [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest)에
대해 CI가 실행하는 버전 고정 프로세스를 설명합니다. 제품 런타임 의존성이 아닌 통합
테스트 자료입니다.

<!-- section: guides -->
## 버전별 안내

- [Python BigQuery 클라이언트](clients/python-bigquery.md)
- [`bq` CLI](clients/bq-cli.md)
- [PySpark와 Scala Spark](clients/spark-bigquery-connector.md)

<!-- section: evidence -->
## 자동 생성 증거

- [통합 테스트 프레임워크](framework.md): 매니페스트 구조, 실행기 계약, CI lane과 사례
  추가 절차입니다.
- [소비자 호환성](consumer-compatibility.md): CI가 사용하는 정확한 버전, 변경되지 않는
  아티팩트, scenario ID와 selector입니다.
