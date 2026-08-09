.DEFAULT_GOAL := help

BINARY ?= bin/go-bemu
AUTH_FIXTURE_BINARY ?= bin/bqemu-auth-fixture
BQEMU_CONFIG ?= configs/bqemu.yaml
BQEMU_LOCAL_DATA_DIR ?= $(CURDIR)/data
BQEMU_DATABASE_DSN ?= $(BQEMU_LOCAL_DATA_DIR)/bqemu.duckdb
BQEMU_TEMP_DIRECTORY ?= $(if $(TMPDIR),$(TMPDIR),/tmp)/bqemu
BQEMU_GO_TEST_TIMEOUT ?= 10m
BQEMU_PYTEST_TIMEOUT_SECONDS ?= 300
BQEMU_BQCLI_BIN ?= bq
BQEMU_BQCLI_TIMEOUT_SECONDS ?= 300
BQEMU_DOCKER_START_TIMEOUT_SECONDS ?= 120
BQEMU_SPARK_TEST_TIMEOUT_SECONDS ?= 600
BQEMU_SPARK_RPC_TIMEOUT_SECONDS ?= 30
BQEMU_ARTIFACT_TIMEOUT_SECONDS ?= 180
BQEMU_SPARK_VENV ?= $(CURDIR)/.artifacts/spark/venv
BQEMU_SPARK_PYTHON ?= $(BQEMU_SPARK_VENV)/bin/python
BQEMU_PYTHON_VERSION ?= 3.13
BQEMU_SPARK_PYTHON_VERSION ?= 3.11
BQEMU_AUTH_CASE ?= all
BQEMU_CONSUMER_CASE ?=
BQEMU_AUTH_JUNIT ?=
BQEMU_AUTH_DIAGNOSTICS ?=
GO_TEST_FLAGS ?=
GO_SOURCE_DIRS ?= ./cmd ./contract ./docs ./internal
IMAGE ?= go-bemu:dev
PYTHON ?= .venv/bin/python
PYTHON3 ?= python3

SQLC_VERSION ?= v1.31.1

.PHONY: help doctor docker-doctor setup python-setup auth-spark-setup auth-client-setup auth-fixtures auth-client-test auth-runner-test ci-report-test build run format format-check contract-generate contract-check integration-contract-generate integration-contract-check sqlc-generate sqlc-check consumer-runner-test test test-race python-test bq-test spark-prepare spark-contract spark-scala-contract spark-contract-setup vet check config-check github-actions-policy ci-static ci-test-all ci-test-sql-regression ci-test-core ci-test-adapters ci-test-storage-read ci-test-storage-write ci-test-transport ci-test-composition docker-build docker-up docker-down docker-logs clean

help:
	@printf '%s\n' \
	  'make doctor       Validate Go, CGO, Docker, and configuration prerequisites' \
	  'make setup        Run doctor and download Go modules' \
	  'make python-setup Create a Python 3.13 venv from the hash lock' \
	  'make auth-client-setup Install pinned Python and Spark client dependencies' \
	  'make auth-fixtures Generate local TLS, credential, token, and Java trust files' \
	  'make auth-client-test Run real TLS credential contracts for Python, bq, PySpark, and Scala Spark' \
	  'make ci-report-test Validate the CI JUnit summary and HTML renderer' \
	  'make build        Build the emulator and local credential helper' \
	  'make run          Run locally with repository data and temp directories' \
	  'make contract-generate Regenerate canonical API/RPC contract artifacts' \
	  'make contract-check Check API/RPC manifest, annotations, and generated files' \
	  'make integration-contract-generate Regenerate integration case artifacts' \
	  'make integration-contract-check Check integration cases and generated files' \
	  'make sqlc-generate Regenerate typed SQLite query adapters from SQL resources' \
	  'make sqlc-check Check SQLite generated query adapters for drift' \
	  'make check        Run formatting, bounded race tests, and vet' \
	  'make python-test  Run the official Python client real-process contract' \
	  'make bq-test      Run the exact-version official bq CLI contract' \
	  'make spark-contract Install exact Spark locks and run the released connector contract' \
	  'make spark-scala-contract Run the standalone Scala decimal public-edge contract' \
	  'make ci-static     Validate formatting, vet, configuration, and GitHub Actions policy' \
	  'make ci-test-*     Run the same functional Go test groups used by CI' \
	  'make docker-build Build the standalone non-root image' \
	  'make docker-up    Start the read-only Compose service and wait for readiness' \
	  'make docker-down  Stop the Compose service and remove transient resources'

doctor:
	BQEMU_CONFIG="$(BQEMU_CONFIG)" BQEMU_DOCTOR_REQUIRE_DOCKER=false ./scripts/doctor.sh

docker-doctor:
	BQEMU_CONFIG="$(BQEMU_CONFIG)" BQEMU_DOCTOR_REQUIRE_DOCKER=true ./scripts/doctor.sh

setup: doctor
	mkdir -p "$(BQEMU_LOCAL_DATA_DIR)" "$(BQEMU_TEMP_DIRECTORY)"
	go mod download

python-setup:
	uv venv --python "$(BQEMU_PYTHON_VERSION)" .venv
	uv pip sync --python "$(PYTHON)" --require-hashes tests/integration/python/requirements.lock

build:
	mkdir -p $(dir $(BINARY)) $(dir $(AUTH_FIXTURE_BINARY))
	CGO_ENABLED=1 go build -trimpath -o "$(BINARY)" ./cmd/emulator
	CGO_ENABLED=0 go build -trimpath -o "$(AUTH_FIXTURE_BINARY)" ./cmd/bqemu-auth-fixture

auth-client-setup: python-setup auth-spark-setup

auth-spark-setup:
	@if test -x "$(BQEMU_SPARK_PYTHON)"; then \
		test "$$("$(BQEMU_SPARK_PYTHON)" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" = "$(BQEMU_SPARK_PYTHON_VERSION)"; \
	else \
		uv venv --python "$(BQEMU_SPARK_PYTHON_VERSION)" "$(BQEMU_SPARK_VENV)"; \
	fi
	uv pip sync --python "$(BQEMU_SPARK_PYTHON)" --require-hashes tests/integration/spark/requirements.lock

auth-fixtures:
	CGO_ENABLED=0 go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth

auth-client-test:
	BQEMU_AUTH_PYTHON="$(PYTHON)" \
	BQEMU_AUTH_SPARK_PYTHON="$(BQEMU_SPARK_PYTHON)" \
	BQEMU_AUTH_BQ="$(BQEMU_BQCLI_BIN)" \
	BQEMU_AUTH_CASE="$(BQEMU_AUTH_CASE)" \
	BQEMU_AUTH_JUNIT="$(BQEMU_AUTH_JUNIT)" \
	BQEMU_AUTH_DIAGNOSTICS="$(BQEMU_AUTH_DIAGNOSTICS)" \
	BQEMU_AUTH_TEST_TIMEOUT_SECONDS="$(BQEMU_SPARK_TEST_TIMEOUT_SECONDS)" \
	"$(PYTHON3)" tests/integration/auth/run_contract.py

auth-runner-test:
	"$(PYTHON3)" -m unittest discover -s tests/integration/auth -p 'test_*.py'

ci-report-test:
	"$(PYTHON3)" -m unittest discover -s tests/integration/cireport -p 'test_*.py'

run:
	mkdir -p "$(BQEMU_LOCAL_DATA_DIR)" "$(BQEMU_TEMP_DIRECTORY)"
	BQEMU_DATABASE_DSN="$(BQEMU_DATABASE_DSN)" \
	BQEMU_TEMP_DIRECTORY="$(BQEMU_TEMP_DIRECTORY)" \
	CGO_ENABLED=1 go run ./cmd/emulator --config "$(BQEMU_CONFIG)"

format:
	gofmt -w $(GO_SOURCE_DIRS)

format-check:
	@unformatted="$$(gofmt -l $(GO_SOURCE_DIRS))"; \
	  if test -n "$$unformatted"; then \
	    files="$$(printf '%s\n' "$$unformatted" | paste -sd, -)"; \
	    printf '%s\n' "stage=format operation=gofmt shape=go-source status=failed files=$$files fix_hint=make-format" >&2; \
	    exit 1; \
	  fi

contract-generate:
	go run ./cmd/contractctl compile --root .

contract-check:
	go run ./cmd/contractctl check --root .

integration-contract-generate:
	go run ./tests/integration/cmd/integrationctl compile --root .

integration-contract-check:
	go run ./tests/integration/cmd/integrationctl check --root .

sqlc-generate:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate

sqlc-check: sqlc-generate
	git diff --exit-code -- internal/adapters/sqlite/sqlcgen
	@untracked="$$(git ls-files --others --exclude-standard -- internal/adapters/sqlite/sqlcgen)"; \
	  if test -n "$$untracked"; then \
	    printf '%s\n' "stage=sqlc operation=generate shape=untracked-output status=failed files=$$untracked fix_hint=git-add-generated-output" >&2; \
	    exit 1; \
	  fi

consumer-runner-test:
	"$(PYTHON3)" -m unittest discover -s tests/integration/framework/tests -p 'test_*.py'

test:
	CGO_ENABLED=1 go test -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./...

test-race:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./...

python-test:
	BQEMU_PYTEST_TIMEOUT_SECONDS="$(BQEMU_PYTEST_TIMEOUT_SECONDS)" \
	"$(PYTHON)" -m tests.integration.framework.consumer_runner $(if $(strip $(BQEMU_CONSUMER_CASE)),--case "$(BQEMU_CONSUMER_CASE)",--family python)

bq-test:
	@command -v "$(BQEMU_BQCLI_BIN)" >/dev/null 2>&1 || \
	  (printf '%s\n' 'stage=setup operation=find-bq shape=missing-binary fix_hint=install-case-declared-Google-Cloud-SDK' >&2; exit 1)
	BQEMU_BQCLI_TIMEOUT_SECONDS="$(BQEMU_BQCLI_TIMEOUT_SECONDS)" \
	"$(PYTHON3)" -m tests.integration.framework.consumer_runner $(if $(strip $(BQEMU_CONSUMER_CASE)),--case "$(BQEMU_CONSUMER_CASE)",--family bq)

spark-prepare:
	mkdir -p "$(CURDIR)/.artifacts/spark/diagnostics"
	@if test -x "$(BQEMU_SPARK_PYTHON)"; then \
	  test "$$("$(BQEMU_SPARK_PYTHON)" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" = "$(BQEMU_SPARK_PYTHON_VERSION)"; \
	else \
	  uv venv --python "$(BQEMU_SPARK_PYTHON_VERSION)" "$(BQEMU_SPARK_VENV)"; \
	fi
	uv pip sync --python "$(BQEMU_SPARK_PYTHON)" --require-hashes tests/integration/spark/requirements.lock

spark-contract: spark-prepare
	BQEMU_SPARK_TEST_TIMEOUT_SECONDS="$(BQEMU_SPARK_TEST_TIMEOUT_SECONDS)" \
	BQEMU_SPARK_RPC_TIMEOUT_SECONDS="$(BQEMU_SPARK_RPC_TIMEOUT_SECONDS)" \
	BQEMU_ARTIFACT_TIMEOUT_SECONDS="$(BQEMU_ARTIFACT_TIMEOUT_SECONDS)" \
	PYTHONPYCACHEPREFIX="$(CURDIR)/.artifacts/spark/pycache" \
	"$(BQEMU_SPARK_PYTHON)" -m tests.integration.framework.consumer_runner $(if $(strip $(BQEMU_CONSUMER_CASE)),--case "$(BQEMU_CONSUMER_CASE)",--family spark --all)

spark-contract-setup: spark-prepare

spark-scala-contract: spark-prepare
	BQEMU_SPARK_TEST_TIMEOUT_SECONDS="$(BQEMU_SPARK_TEST_TIMEOUT_SECONDS)" \
	BQEMU_SPARK_RPC_TIMEOUT_SECONDS="$(BQEMU_SPARK_RPC_TIMEOUT_SECONDS)" \
	BQEMU_ARTIFACT_TIMEOUT_SECONDS="$(BQEMU_ARTIFACT_TIMEOUT_SECONDS)" \
	PYTHONPYCACHEPREFIX="$(CURDIR)/.artifacts/spark/pycache" \
	"$(BQEMU_SPARK_PYTHON)" -m pytest -c tests/integration/spark/pytest.ini \
	  tests/integration/spark/test_scala_decimal_edge.py \
	  --basetemp="$(CURDIR)/.artifacts/spark/pytest-scala" \
	  --junitxml="$(CURDIR)/.artifacts/spark/diagnostics/junit-scala.xml"

vet:
	CGO_ENABLED=1 go vet ./...

github-actions-policy:
	CGO_ENABLED=1 go test ./tests/integration/cipolicy

ci-static: github-actions-policy format-check contract-check integration-contract-check sqlc-check consumer-runner-test auth-runner-test ci-report-test vet config-check

ci-test-all:
	CGO_ENABLED=1 go test -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./...

ci-test-sql-regression:
	CGO_ENABLED=1 go test -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/sqltest

ci-test-core:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) \
		./internal/admin \
		./internal/application \
		./internal/auth/... \
		./internal/domain \
		./internal/loadjob/... \
		./internal/observability \
		./internal/ports \
		./internal/tabledata

ci-test-adapters:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/adapters/...

ci-test-storage-read:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/storageread/...

ci-test-storage-write:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/storagewrite/...

ci-test-transport:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/transport/...

ci-test-composition:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) \
		./cmd/emulator \
		./contract \
		./docs \
		./internal/config

config-check:
	CGO_ENABLED=1 go run ./cmd/emulator --config "$(BQEMU_CONFIG)" --print-effective-config >/dev/null

check: format-check contract-check integration-contract-check test-race vet config-check

docker-build: docker-doctor
	docker build --tag "$(IMAGE)" .

docker-up: docker-doctor
	docker compose up -d --build --wait --wait-timeout "$(BQEMU_DOCKER_START_TIMEOUT_SECONDS)" bqemu

docker-down:
	docker compose down --remove-orphans

docker-logs:
	docker compose logs --no-color bqemu

clean:
	rm -rf bin coverage.out
