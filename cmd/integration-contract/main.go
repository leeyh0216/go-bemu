package main

import (
	"log"

	integration "github.com/leeyh0216/go-bemu/tests/integration/contract"
)

func main() {
	operations, err := integration.Compile("tests/integration", map[string][]string{
		"query-parameters":     {"bigquery.jobs.query.parameters"},
		"tabledata-insert-all": {"bigquery.tabledata.insert-all"},
		"parquet-media-upload": {"bigquery.jobs.insert.media-upload"},
		"dataset-label-filter": {"bigquery.datasets.list.filter"},
		"dataset-metadata-view": {"bigquery.datasets.get.metadata-view"},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := integration.WriteManifest("contract/generated/integration-consumer-contract.json", operations, nil); err != nil {
		log.Fatal(err)
	}
	if err := integration.WriteCompatibilityDocuments(
		"docs/en/generated/integration-consumer-contract.md",
		"docs/ko/generated/integration-consumer-contract.md",
		operations,
		nil,
	); err != nil {
		log.Fatal(err)
	}
}
