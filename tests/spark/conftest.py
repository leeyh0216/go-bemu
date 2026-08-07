"""Real Spark 3.5.8 process fixtures for connector 0.44.2.

The JVM crosses the public HTTPS and TLS gRPC listeners. It never imports a Go
adapter or calls an in-process service.

Official connector source:
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import ssl
import subprocess
import time
from typing import Iterator
import urllib.error
import urllib.request
import uuid

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = (
    REPOSITORY_ROOT
    / "contract"
    / "matrices"
    / "spark-bigquery-connector-0.44.2.json"
)
ARTIFACT_LOCK_PATH = REPOSITORY_ROOT / "tests" / "spark" / "artifacts.lock.json"
STATIC_ACCESS_TOKEN = "bqemu-spark-e2e-static-token"
TRUSTSTORE_PASSWORD = "bqemu-test-only"


class CapabilityGapError(RuntimeError):
    """Raised only after a test matches the matrix's exact known failure."""


@dataclass(frozen=True)
class PublicEdge:
    http_endpoint: str
    grpc_endpoint: str
    project_id: str
    dataset_id: str
    ca_file: Path
    truststore: Path
    process: subprocess.Popen[bytes]
    log_path: Path
    jvm_log_path: Path


def _positive_timeout(name: str, default: str) -> float:
    raw = os.getenv(name, default)
    try:
        value = float(raw)
    except ValueError as error:
        raise pytest.UsageError(f"{name} must be a positive number of seconds") from error
    if value <= 0:
        raise pytest.UsageError(f"{name} must be a positive number of seconds")
    return value


def _test_timeout() -> float:
    return _positive_timeout("BQEMU_SPARK_TEST_TIMEOUT_SECONDS", "600")


def _rpc_timeout() -> float:
    return _positive_timeout("BQEMU_SPARK_RPC_TIMEOUT_SECONDS", "30")


def pytest_configure(config: pytest.Config) -> None:
    environment_override = "BQEMU_SPARK_TEST_TIMEOUT_SECONDS" in os.environ
    if hasattr(config.option, "timeout") and (
        environment_override or config.option.timeout in (None, 0)
    ):
        config.option.timeout = _test_timeout()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    with MATRIX_PATH.open("r", encoding="utf-8") as stream:
        matrix = json.load(stream)
    entries = {entry["id"]: entry for entry in matrix["entries"]}
    for item in items:
        marker = item.get_closest_marker("capability")
        if marker is None or len(marker.args) != 1:
            raise pytest.UsageError(f"{item.nodeid} must declare one capability ID")
        capability_id = marker.args[0]
        entry = entries.get(capability_id)
        if entry is None:
            raise pytest.UsageError(
                f"{item.nodeid} references unknown capability {capability_id}"
            )
        if entry["state"] in {"gap", "cloud-only"}:
            item.add_marker(
                pytest.mark.xfail(
                    strict=True,
                    raises=CapabilityGapError,
                    reason=(
                        f"{capability_id} state={entry['state']} "
                        f"issue={matrix['issueCatalog'][entry['issueRef']]['url']}"
                    ),
                )
            )


def _emit(*, operation: str, stage: str, shape: str, status: str, fix_hint: str) -> None:
    fingerprint = hashlib.sha256(
        f"{operation}\0{stage}\0{shape}\0{status}".encode("utf-8")
    ).hexdigest()
    event = {
        "version": "spark-bigquery-connector-0.44.2",
        "operation": operation,
        "stage": stage,
        "shape": shape,
        "fingerprint": f"sha256:{fingerprint}",
        "status": status,
        "fix_hint": fix_hint,
    }
    print(" ".join(f"{key}={value}" for key, value in event.items()), flush=True)
    trace_path = Path(
        os.getenv(
            "BQEMU_SPARK_TRACE_PATH",
            str(REPOSITORY_ROOT / ".artifacts" / "spark" / "diagnostics" / "trace.jsonl"),
        )
    )
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    with trace_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")


def record_capability(capability_id: str, shape: str) -> None:
    _emit(
        operation=capability_id,
        stage="public-edge-assertion",
        shape=shape,
        status="passed",
        fix_hint="none",
    )


def raise_known_gap(
    capability_id: str,
    *,
    error: Exception,
    expected_fragments: tuple[str, ...],
    stage: str,
    shape: str,
    fix_hint: str,
) -> None:
    """Convert only the reviewed failure signature into a strict matrix xfail.

    The upstream exception text is inspected in memory but never retained. An
    unrelated exception remains a hard failure instead of being mislabeled as
    the known gap.
    """

    error_text = str(error)
    if not all(fragment in error_text for fragment in expected_fragments):
        raise error
    _emit(
        operation=capability_id,
        stage=stage,
        shape=shape,
        status="failed",
        fix_hint=fix_hint,
    )
    raise CapabilityGapError(
        f"{capability_id} matched reviewed failure stage={stage} shape={shape}"
    ) from None


def _run(command: list[str], *, cwd: Path, timeout: float, stage: str) -> None:
    try:
        subprocess.run(
            command,
            cwd=cwd,
            check=True,
            timeout=timeout,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        _emit(
            operation="spark-public-edge-setup",
            stage=stage,
            shape=Path(command[0]).name,
            status="failed",
            fix_hint="inspect-redacted-ci-artifact",
        )
        # Command output can contain JVM configuration. Do not echo it here;
        # the structured stage is enough to find the separately retained log.
        output = getattr(error, "stdout", None) or getattr(error, "output", None) or b""
        _write_safe_bytes(
            bytes(output)[-131072:],
            REPOSITORY_ROOT
            / ".artifacts"
            / "spark"
            / "diagnostics"
            / f"{stage}-tail.log",
        )
        raise RuntimeError(f"bounded setup command failed at {stage}") from error


def _write_safe_bytes(payload: bytes, target: Path) -> None:
    text = payload.decode("utf-8", errors="replace")
    text = re.sub(r"(?i)(bearer\s+)[^\s,;]+", r"\1<redacted>", text)
    text = text.replace(STATIC_ACCESS_TOKEN, "<redacted-token>")
    text = re.sub(
        r"(?is)-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
        "<redacted-private-key>",
        text,
    )
    text = re.sub(
        r"(/streams/)[A-Za-z0-9._~-]+",
        r"\1<redacted-resource-name>",
        text,
    )
    text = re.sub(r"(?i)(VALUES\s*)\([^\n]+", r"\1<redacted-row-values>", text)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text[-131072:], encoding="utf-8")


def _retain_safe_tail(source: Path, name: str) -> None:
    if not source.is_file():
        return
    with source.open("rb") as stream:
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        stream.seek(max(0, size - 131072))
        payload = stream.read(131072)
    target = (
        REPOSITORY_ROOT / ".artifacts" / "spark" / "diagnostics" / name
    )
    _write_safe_bytes(payload, target)


def public_edge_log_position(edge: PublicEdge) -> int:
    """Return a byte boundary for one public-edge operation's observations."""

    return edge.log_path.stat().st_size


def observe_default_append_offsets(
    edge: PublicEdge, *, since: int
) -> tuple[int, int]:
    """Assert contiguous default-stream offsets using numeric log fields only.

    The application log records row counts and offsets around the Storage Write
    side effect. This helper deliberately discards table/stream names, payload
    digests, and all row data before returning an assertion shape.

    Storage Write default-stream offset contract:
    https://cloud.google.com/bigquery/docs/write-api#default_stream
    """

    with edge.log_path.open("rb") as stream:
        stream.seek(since)
        encoded_lines = stream.read().splitlines()
    observations: list[tuple[int, int]] = []
    for encoded in encoded_lines:
        try:
            event = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if (
            event.get("event") != "side_effect.after"
            or event.get("side_effect") != "coordinator.append_default"
            or event.get("operation") != "storage_write.append"
            or event.get("success") is not True
        ):
            continue
        offset, row_count = event.get("start_offset"), event.get("row_count")
        if not isinstance(offset, int) or not isinstance(row_count, int):
            raise AssertionError("default append observation omitted numeric shape")
        observations.append((offset, row_count))
    if not observations:
        raise AssertionError("default append produced no observed side effect")
    expected_offset = 0
    for offset, row_count in sorted(observations):
        if offset != expected_offset or row_count <= 0:
            raise AssertionError(
                "default append offset discontinuity "
                f"shape=actual:{offset},expected:{expected_offset},rows:{row_count}"
            )
        expected_offset += row_count
    return len(observations), expected_offset


def _stop_public_edge(edge: PublicEdge, timeout: float) -> None:
    if edge.process.poll() is None:
        edge.process.terminate()
        try:
            edge.process.wait(timeout=min(10.0, timeout))
        except subprocess.TimeoutExpired:
            edge.process.kill()
            edge.process.wait(timeout=min(5.0, timeout))
    _retain_safe_tail(edge.log_path, "emulator-tail.log")
    _retain_safe_tail(edge.jvm_log_path, "jvm-tail.log")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _json_request(
    edge: PublicEdge | None,
    url: str,
    method: str,
    timeout: float,
    payload: dict[str, object] | None = None,
    ca_file: Path | None = None,
    allowed_statuses: frozenset[int] = frozenset(),
) -> dict[str, object] | None:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, method=method)
    request.add_header("Authorization", "Bearer " + STATIC_ACCESS_TOKEN)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    trust = ca_file if ca_file is not None else edge.ca_file if edge is not None else None
    if trust is None:
        raise RuntimeError("TLS trust anchor is required")
    context = ssl.create_default_context(cafile=str(trust))
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            encoded = response.read()
    except urllib.error.HTTPError as error:
        encoded = error.read()
        if error.code in allowed_statuses:
            return None
        fingerprint = hashlib.sha256(encoded).hexdigest()
        raise RuntimeError(
            f"REST status={error.code} shape=error-body-length:{len(encoded)} "
            f"fingerprint=sha256:{fingerprint}"
        ) from error
    return None if not encoded else json.loads(encoded)


def _create_tls_identity(work: Path, timeout: float) -> tuple[Path, Path, Path]:
    certificate = work / "localhost.pem"
    private_key = work / "localhost-key.pem"
    truststore = work / "truststore.p12"
    _run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-keyout",
            str(private_key),
            "-out",
            str(certificate),
        ],
        cwd=work,
        timeout=timeout,
        stage="generate-tls-san",
    )
    private_key.chmod(0o600)
    _run(
        [
            "keytool",
            "-importcert",
            "-noprompt",
            "-alias",
            "bqemu-test-ca",
            "-file",
            str(certificate),
            "-keystore",
            str(truststore),
            "-storetype",
            "PKCS12",
            "-storepass",
            TRUSTSTORE_PASSWORD,
        ],
        cwd=work,
        timeout=timeout,
        stage="create-jvm-truststore",
    )
    _emit(
        operation="spark-public-edge-setup",
        stage="tls-ready",
        shape="self-signed-san-localhost",
        status="ready",
        fix_hint="none",
    )
    return certificate, private_key, truststore


@pytest.fixture(scope="session")
def test_timeout() -> float:
    return _test_timeout()


@pytest.fixture(scope="session")
def connector_jar(test_timeout: float) -> Path:
    configured = os.getenv("BQEMU_SPARK_CONNECTOR_JAR")
    if configured:
        target = Path(configured).resolve()
    else:
        _run(
            [
                os.getenv("PYTHON", os.sys.executable),
                str(REPOSITORY_ROOT / "scripts" / "fetch_spark_artifacts.py"),
            ],
            cwd=REPOSITORY_ROOT,
            timeout=_positive_timeout("BQEMU_ARTIFACT_TIMEOUT_SECONDS", "120"),
            stage="fetch-connector-jar",
        )
        with ARTIFACT_LOCK_PATH.open("r", encoding="utf-8") as stream:
            lock = json.load(stream)
        target = REPOSITORY_ROOT / ".artifacts" / "spark" / lock["artifacts"][0]["output"]
    with ARTIFACT_LOCK_PATH.open("r", encoding="utf-8") as stream:
        artifact = json.load(stream)["artifacts"][0]
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    if digest != artifact["sha256"] or target.stat().st_size != artifact["size"]:
        pytest.fail("connector JAR does not match the reviewed artifact lock")
    return target


@pytest.fixture(scope="session")
def public_edge(
    tmp_path_factory: pytest.TempPathFactory, test_timeout: float
) -> Iterator[PublicEdge]:
    work = tmp_path_factory.mktemp("spark-public-edge")
    binary = work / "go-bemu"
    _run(
        ["go", "build", "-trimpath", "-o", str(binary), "./cmd/emulator"],
        cwd=REPOSITORY_ROOT,
        timeout=test_timeout,
        stage="build-current-emulator",
    )
    certificate, private_key, truststore = _create_tls_identity(work, test_timeout)
    http_port, grpc_port = _free_port(), _free_port()
    project_id = "spark-contract-" + uuid.uuid4().hex[:8]
    dataset_id = "connector"
    http_endpoint = f"https://localhost:{http_port}"
    config = {
        "apiVersion": "config.bqemu.dev/v1alpha1",
        "kind": "BQEMUConfig",
        "defaults": {"projectId": project_id, "location": "US"},
        "server": {
            "http": {
                "address": f"127.0.0.1:{http_port}",
                "publicUrl": http_endpoint,
            },
            "grpc": {"address": f"127.0.0.1:{grpc_port}"},
            "tls": {"certFile": str(certificate), "keyFile": str(private_key)},
        },
        "database": {
            "adapter": "duckdb",
            "dsn": str(work / "spark.duckdb"),
            "tempDirectory": str(work / "tmp"),
        },
        "storage": {
            "read": {"enabled": True, "defaultStreamCount": 4, "maxStreams": 64},
            "write": {"enabled": True},
        },
        "auth": {"mode": "bearer-present"},
        "logging": {"level": "info", "format": "json", "unsafePayloads": False},
        "admin": {"enabled": False},
        "ui": {"enabled": False},
    }
    config_path = work / "bqemu.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    (work / "tmp").mkdir()
    log_path = work / "server.log"
    environment = os.environ.copy()
    # Prevent a developer's local BQEMU overrides from mutating this generated,
    # file-first test contract.
    for key in tuple(environment):
        if key.startswith("BQEMU_") and key not in {
            "BQEMU_SPARK_TEST_TIMEOUT_SECONDS",
            "BQEMU_SPARK_RPC_TIMEOUT_SECONDS",
            "BQEMU_ARTIFACT_TIMEOUT_SECONDS",
        }:
            environment.pop(key)
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [str(binary), "--config", str(config_path)],
            cwd=REPOSITORY_ROOT,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    edge = PublicEdge(
        http_endpoint=http_endpoint,
        grpc_endpoint=f"localhost:{grpc_port}",
        project_id=project_id,
        dataset_id=dataset_id,
        ca_file=certificate,
        truststore=truststore,
        process=process,
        log_path=log_path,
        jvm_log_path=work / "jvm.log",
    )
    deadline = time.monotonic() + test_timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            _json_request(
                edge,
                http_endpoint + "/readyz",
                "GET",
                min(2.0, test_timeout),
            )
            last_error = None
            break
        except (OSError, RuntimeError, urllib.error.URLError) as error:
            last_error = error
            time.sleep(0.05)
    else:
        last_error = TimeoutError("emulator readiness deadline exceeded")
    if last_error is not None or process.poll() is not None:
        _stop_public_edge(edge, test_timeout)
        _emit(
            operation="spark-public-edge-setup",
            stage="emulator-readiness",
            shape=f"process-exit:{process.returncode}",
            status="failed",
            fix_hint="inspect-redacted-ci-artifact",
        )
        pytest.fail(
            "emulator readiness failed; inspect payload-safe emulator-tail.log "
            f"shape=process-exit:{process.returncode}"
        )

    try:
        _json_request(
            edge,
            http_endpoint + "/bqemu/v1/projects",
            "POST",
            test_timeout,
            {"projectId": project_id},
            allowed_statuses=frozenset({409}),
        )
        _json_request(
            edge,
            http_endpoint + f"/bigquery/v2/projects/{project_id}/datasets",
            "POST",
            test_timeout,
            {
                "datasetReference": {"datasetId": dataset_id},
                "location": "US",
            },
        )
    except Exception:
        _emit(
            operation="spark-public-edge-setup",
            stage="seed-control-plane",
            shape="project+dataset",
            status="failed",
            fix_hint="inspect-redacted-ci-artifact",
        )
        _stop_public_edge(edge, test_timeout)
        raise
    _emit(
        operation="spark-public-edge-setup",
        stage="emulator-ready",
        shape="https+grpc-tls-random-ports",
        status="ready",
        fix_hint="none",
    )
    try:
        yield edge
    finally:
        _stop_public_edge(edge, test_timeout)


@pytest.fixture(scope="session")
def spark_session(connector_jar: Path, public_edge: PublicEdge, test_timeout: float):
    from pyspark.sql import SparkSession

    log_config = public_edge.truststore.parent / "log4j2.properties"
    log_config.write_text(
        "\n".join(
            (
                "status = error",
                "name = BQEMUSparkContract",
                "appender.file.type = File",
                "appender.file.name = ContractFile",
                f"appender.file.fileName = {public_edge.jvm_log_path}",
                "appender.file.layout.type = PatternLayout",
                "appender.file.layout.pattern = %p %c %m%n",
                "rootLogger.level = warn",
                "rootLogger.appenderRef.file.ref = ContractFile",
                "logger.connector.name = com.google.cloud.spark.bigquery",
                "logger.connector.level = info",
                "logger.connector.additivity = false",
                "logger.connector.appenderRef.file.ref = ContractFile",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    trust_options = (
        f"-Djavax.net.ssl.trustStore={public_edge.truststore} "
        f"-Djavax.net.ssl.trustStorePassword={TRUSTSTORE_PASSWORD} "
        "-Djavax.net.ssl.trustStoreType=PKCS12 "
        f"-Dlog4j.configurationFile={log_config.as_uri()}"
    )
    previous_java_options = os.environ.get("JAVA_TOOL_OPTIONS")
    previous_worker_python = os.environ.get("PYSPARK_PYTHON")
    previous_driver_python = os.environ.get("PYSPARK_DRIVER_PYTHON")
    python_executable = os.sys.executable
    os.environ["JAVA_TOOL_OPTIONS"] = " ".join(
        value for value in (previous_java_options, trust_options) if value
    )
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("bqemu-spark-connector-contract")
        .config("spark.jars", str(connector_jar))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", trust_options)
        .config("spark.executor.extraJavaOptions", trust_options)
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    if spark.version != "3.5.8":
        spark.stop()
        pytest.fail(f"Spark version drift: {spark.version}")
    _emit(
        operation="spark-public-edge-setup",
        stage="spark-ready",
        shape="local-4-scala-2.12",
        status="ready",
        fix_hint="none",
    )
    try:
        yield spark
    finally:
        spark.stop()
        if previous_java_options is None:
            os.environ.pop("JAVA_TOOL_OPTIONS", None)
        else:
            os.environ["JAVA_TOOL_OPTIONS"] = previous_java_options
        if previous_worker_python is None:
            os.environ.pop("PYSPARK_PYTHON", None)
        else:
            os.environ["PYSPARK_PYTHON"] = previous_worker_python
        if previous_driver_python is None:
            os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
        else:
            os.environ["PYSPARK_DRIVER_PYTHON"] = previous_driver_python


def connector_options(edge: PublicEdge) -> dict[str, str]:
    # Exact option definitions:
    # https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/SparkBigQueryConfig.java
    rpc_seconds = _rpc_timeout()
    return {
        "parentProject": edge.project_id,
        "billingProject": edge.project_id,
        "project": edge.project_id,
        "bigQueryHttpEndpoint": edge.http_endpoint,
        "bigQueryStorageGrpcEndpoint": edge.grpc_endpoint,
        "gcpAccessToken": STATIC_ACCESS_TOKEN,
        "createReadSessionTimeoutInSeconds": str(math.ceil(rpc_seconds)),
        "httpConnectTimeout": str(math.ceil(rpc_seconds * 1000)),
        "httpReadTimeout": str(math.ceil(rpc_seconds * 1000)),
        "httpMaxRetry": "0",
    }


def load_connector_source(
    spark_session,
    edge: PublicEdge,
    *,
    source: str,
    source_kind: str,
    wire_format: str,
    requested_streams: int,
):
    """Load a table or query through the same released-connector read edge.

    Query reads intentionally share this boundary with table reads. Once the
    anonymous destination contract is frozen, query/view/count cases can assert
    only their control-plane prefix while reusing the Storage Read assertions.

    Exact option definitions:
    https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/SparkBigQueryConfig.java
    """

    if source_kind not in {"table", "query"}:
        raise ValueError(f"unsupported connector source kind: {source_kind}")
    reader = spark_session.read.format("bigquery")
    for key, value in connector_options(edge).items():
        reader = reader.option(key, value)
    reader = (
        reader.option("readDataFormat", wire_format)
        .option("maxParallelism", str(requested_streams))
        .option("preferredMinParallelism", str(requested_streams))
    )
    if source_kind == "query":
        return reader.option("viewsEnabled", "true").option("query", source).load()
    return reader.load(source)


def create_table(
    edge: PublicEdge,
    timeout: float,
    table_id: str,
    fields: list[dict[str, str]],
) -> None:
    _json_request(
        edge,
        (
            edge.http_endpoint
            + f"/bigquery/v2/projects/{edge.project_id}/datasets/{edge.dataset_id}/tables"
        ),
        "POST",
        timeout,
        {"tableReference": {"tableId": table_id}, "schema": {"fields": fields}},
    )


def query(edge: PublicEdge, timeout: float, sql: str) -> dict[str, object]:
    result = _json_request(
        edge,
        edge.http_endpoint + f"/bigquery/v2/projects/{edge.project_id}/queries",
        "POST",
        timeout,
        {"query": sql, "useLegacySql": False},
    )
    if result is None:
        raise RuntimeError("query returned no response")
    return result
