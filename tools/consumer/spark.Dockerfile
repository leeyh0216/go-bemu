# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
FROM golang:1.26.5-bookworm@sha256:6c5605ab3a9a9fb3c4eafe5b3d63cdbf3881caf113262b67862547b54a9db599 AS emulator-build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY contract ./contract
COPY internal ./internal
COPY cmd ./cmd
COPY release ./release
RUN CGO_ENABLED=1 go build -trimpath -ldflags='-s -w' -o /out/go-bemu ./cmd/emulator

FROM python:3.11-slim-bookworm@sha256:d29f48a31a8b408ed19272ca1e7b10ebae13b240a27e862d3d4217c528e2e0c3
WORKDIR /workspace
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 BQEMU_EMULATOR_BINARY=/usr/local/bin/go-bemu PYTHONPATH=/opt/pyspark
RUN apt-get update && apt-get install --no-install-recommends --yes openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
COPY tests/integration/spark/requirements.lock /tmp/requirements.lock
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements.lock
ADD --checksum=sha256:54cca0767b21b40e3953ad1d30f8601c53abf9cbda763653289cdcfcac52313c https://files.pythonhosted.org/packages/80/5a/3806f44eb47387e8af803508cdd6bbc0df784febf4dc010700be04a1ff89/pyspark-3.5.8.tar.gz /tmp/pyspark.tar.gz
ADD --checksum=sha256:85defdfd2b2376eb3abf5ca6474b51ab7e0de341c75a02f46dc9b5976f5a5c1b https://files.pythonhosted.org/packages/10/30/a58b32568f1623aaad7db22aa9eafc4c6c194b429ff35bdc55ca2726da47/py4j-0.10.9.7-py2.py3-none-any.whl /tmp/py4j-0.10.9.7-py2.py3-none-any.whl
RUN python -m pip install --no-cache-dir /tmp/py4j-0.10.9.7-py2.py3-none-any.whl \
    && mkdir -p /opt/pyspark \
    && tar -xzf /tmp/pyspark.tar.gz --strip-components=1 -C /opt/pyspark
ADD --checksum=sha256:516699b6ef6bd5208b16b79a8b9fcefad9903ad2f8871d99a7c7c4cd1fe7f23e https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.44.2/spark-bigquery-with-dependencies_2.12-0.44.2.jar /opt/connectors/spark-bigquery.jar
COPY --from=emulator-build /out/go-bemu /usr/local/bin/go-bemu
COPY tests ./tests
COPY contract/operations.normalized.json ./contract/operations.normalized.json
ENTRYPOINT ["python", "-m", "pytest"]
