#!/usr/bin/env sh
set -eu

endpoint="${BQEMU_ENDPOINT:-http://localhost:9050}"

curl --fail --silent --show-error -X POST "$endpoint/bqemu/v1/projects" \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project"}'

curl --fail --silent --show-error -X POST \
  "$endpoint/bigquery/v2/projects/test-project/datasets" \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"datasetId":"analytics"},"location":"US"}'

curl --fail --silent --show-error -X POST \
  "$endpoint/bigquery/v2/projects/test-project/datasets/analytics/tables" \
  -H 'Content-Type: application/json' \
  -d '{"tableReference":{"tableId":"events"},"schema":{"fields":[{"name":"id","type":"INT64"},{"name":"name","type":"STRING"}]}}'

printf '\nBootstrap complete.\n'
