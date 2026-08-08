#!/usr/bin/env python3
"""Real-process TLS and credential contract for supported clients.

The runner never prints commands, environment values, credential contents, or
raw child output. Process diagnostics are limited to byte counts and SHA-256
digests.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import re
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, BinaryIO
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_BQ_VERSION = "2.1.31"
EXPECTED_PYTHON_VERSION = "3.43.0"
EXPECTED_SPARK_VERSION = "3.5.8"
EXPECTED_CONNECTOR_VERSION = "0.44.2"
MAX_CAPTURE_BYTES = 1 << 20
MAX_BACKGROUND_LOG_BYTES = 16 << 20
MAX_DIAGNOSTIC_FILE_BYTES = 256 << 10
ISSUED_TOKEN_PREFIX = b"bqemu-local-issued-"
CONSUMER_CASES = ("python", "bq", "pyspark", "scala-spark")
EVENT_FIELDS = frozenset(
    {
        "boundary",
        "operation",
        "return_code",
        "stdout_bytes",
        "stdout_digest",
        "stderr_bytes",
        "stderr_digest",
        "output_bytes",
        "output_digest",
        "suite",
        "consumer_case",
        "python",
        "bq",
        "spark",
        "connector",
        "status",
        "error_type",
        "error_digest",
        "stage",
    }
)
SAFE_EVENT_TEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}")


class ContractError(RuntimeError):
    """A credential-free failure with a stable operation."""


def selected_case() -> str:
    value = os.getenv("BQEMU_AUTH_CASE", "all")
    if value != "all" and value not in CONSUMER_CASES:
        raise ContractError(
            "stage=config operation=select-consumer shape=unsupported-case "
            "fix_hint=use-a-reviewed-auth-consumer-case"
        )
    return value


def case_enabled(case: str, candidate: str) -> bool:
    return case == "all" or case == candidate


def positive_timeout() -> float:
    raw = os.getenv("BQEMU_AUTH_TEST_TIMEOUT_SECONDS", "600")
    try:
        value = float(raw)
    except ValueError as error:
        raise ContractError(
            "stage=config operation=parse-timeout fix_hint=set-positive-seconds"
        ) from error
    if value <= 0:
        raise ContractError(
            "stage=config operation=parse-timeout fix_hint=set-positive-seconds"
        )
    return value


TIMEOUT = positive_timeout()


def digest_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def junit_document(
    case: str,
    elapsed_seconds: float,
    error: Exception | None,
) -> ET.ElementTree:
    suite = ET.Element(
        "testsuite",
        {
            "name": f"bqemu-auth-{case}",
            "tests": "1",
            "failures": "0" if error is None else "1",
            "errors": "0",
            "time": f"{elapsed_seconds:.3f}",
        },
    )
    test_case = ET.SubElement(
        suite,
        "testcase",
        {
            "classname": "bqemu.auth",
            "name": case,
            "time": f"{elapsed_seconds:.3f}",
        },
    )
    if error is not None:
        encoded = f"{type(error).__name__}:{error}".encode(
            "utf-8", errors="replace"
        )
        ET.SubElement(
            test_case,
            "failure",
            {
                "type": type(error).__name__,
                "message": digest_bytes(encoded),
            },
        )
    return ET.ElementTree(suite)


def write_junit(
    case: str,
    elapsed_seconds: float,
    error: Exception | None,
) -> None:
    configured = os.getenv("BQEMU_AUTH_JUNIT", "")
    if not configured:
        return
    target = Path(configured).expanduser().absolute()
    target.parent.mkdir(parents=True, exist_ok=True)
    document = junit_document(case, elapsed_seconds, error)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=target.name + ".",
            dir=target.parent,
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            document.write(
                stream,
                encoding="utf-8",
                xml_declaration=True,
            )
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(target)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def encoded_event(fields: dict[str, Any]) -> bytes:
    valid = set(fields).issubset(EVENT_FIELDS)
    for value in fields.values():
        if isinstance(value, bool):
            valid = False
        elif isinstance(value, int):
            continue
        elif not isinstance(value, str) or SAFE_EVENT_TEXT.fullmatch(value) is None:
            valid = False
    if not valid:
        fingerprint = digest_bytes(
            json.dumps(
                fields,
                sort_keys=True,
                default=lambda value: type(value).__name__,
            ).encode(
                "utf-8", errors="replace"
            )
        )
        fields = {
            "suite": "client-credentials-and-tls",
            "status": "failed",
            "error_type": "UnsafeDiagnosticField",
            "error_digest": fingerprint,
        }
    return (
        json.dumps(fields, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("ascii")


def initialize_diagnostics() -> None:
    configured = os.getenv("BQEMU_AUTH_DIAGNOSTICS", "")
    if not configured:
        return
    target = Path(configured).expanduser().absolute()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("wb") as stream:
        os.chmod(target, 0o600)
        stream.flush()
        os.fsync(stream.fileno())


def event(**fields: Any) -> None:
    payload = encoded_event(fields)
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()
    configured = os.getenv("BQEMU_AUTH_DIAGNOSTICS", "")
    if not configured:
        return
    target = Path(configured).expanduser().absolute()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("ab") as stream:
        if stream.tell() + len(payload) > MAX_DIAGNOSTIC_FILE_BYTES:
            return
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


class StreamCapture:
    def __init__(
        self,
        secrets: tuple[bytes, ...],
        retained_limit: int = MAX_CAPTURE_BYTES,
    ) -> None:
        self.total = 0
        self.digest = hashlib.sha256()
        self.retained = bytearray()
        self.disclosed = False
        self.secrets = tuple(secret for secret in secrets if secret)
        self.overlap = b""
        self.retained_limit = retained_limit

    def consume(self, stream: BinaryIO) -> None:
        while chunk := stream.read(64 << 10):
            self.total += len(chunk)
            self.digest.update(chunk)
            searchable = self.overlap + chunk
            if any(secret in searchable for secret in self.secrets):
                self.disclosed = True
            longest = max((len(secret) for secret in self.secrets), default=1)
            self.overlap = searchable[-longest:]
            self.retained.extend(chunk)
            if len(self.retained) > self.retained_limit:
                del self.retained[: len(self.retained) - self.retained_limit]

    @property
    def fingerprint(self) -> str:
        return "sha256:" + self.digest.hexdigest()


def verify_diagnostic_capture() -> None:
    class ChunkedReader(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            return super().read(min(size, 17))

    secret_marker = b"BQEMU_AUTH_FIXED_SECRET_MARKER_DO_NOT_RETAIN"
    capture = StreamCapture((secret_marker,), retained_limit=8)
    stream = ChunkedReader(b"public-prefix" + secret_marker)
    capture.consume(stream)

    child_environment = os.environ.copy()
    child_environment["BQEMU_AUTH_INJECTED_SECRET"] = secret_marker.decode("ascii")
    injected_error: ContractError | None = None
    try:
        run_process(
            [
                sys.executable,
                "-c",
                (
                    "import os,sys; value=os.environ['BQEMU_AUTH_INJECTED_SECRET']; "
                    "print(value); print(value, file=sys.stderr); "
                    "raise RuntimeError(value)"
                ),
            ],
            "diagnostic-secret-injection",
            environment=child_environment,
            secrets=(secret_marker,),
            report=False,
        )
    except ContractError as error:
        injected_error = error

    disclosed_artifact = False
    with tempfile.TemporaryDirectory(prefix="bqemu-auth-diagnostic-self-test-") as path:
        artifact_dir = Path(path)
        junit_document(
            "python", 1.0, RuntimeError(secret_marker.decode("ascii"))
        ).write(artifact_dir / "junit-python.xml", encoding="utf-8")
        safe_failure = {
            "suite": "client-credentials-and-tls",
            "consumer_case": "python",
            "status": "failed",
            "error_type": "RuntimeError",
            "error_digest": digest_bytes(secret_marker),
        }
        (artifact_dir / "events.ndjson").write_bytes(encoded_event(safe_failure))
        for name in ("error.json", "evidence.json"):
            (artifact_dir / name).write_text(
                json.dumps(
                    {"error_type": "RuntimeError", "digest": digest_bytes(secret_marker)},
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
        disclosed_artifact = any(
            secret_marker in artifact.read_bytes()
            for artifact in artifact_dir.iterdir()
        )

    if (
        not capture.disclosed
        or len(capture.retained) > 8
        or injected_error is None
        or secret_marker in str(injected_error).encode("utf-8", errors="replace")
        or disclosed_artifact
    ):
        raise ContractError(
            "stage=security operation=diagnostic-capture shape=regression "
            "fix_hint=restore-bounded-cross-chunk-secret-scan"
        )


def run_process(
    command: list[str],
    operation: str,
    *,
    environment: dict[str, str] | None = None,
    secrets: tuple[bytes, ...] = (),
    report: bool = True,
) -> bytes:
    try:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as error:
        raise ContractError(
            f"stage=process operation={operation} shape=unavailable "
            "fix_hint=install-the-pinned-client"
        ) from error
    assert process.stdout is not None
    assert process.stderr is not None
    stdout = StreamCapture(secrets)
    stderr = StreamCapture(secrets)
    threads = (
        threading.Thread(target=stdout.consume, args=(process.stdout,), daemon=True),
        threading.Thread(target=stderr.consume, args=(process.stderr,), daemon=True),
    )
    for thread in threads:
        thread.start()
    try:
        return_code = process.wait(timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        raise ContractError(
            f"stage=process operation={operation} shape=timeout "
            "fix_hint=increase-BQEMU_AUTH_TEST_TIMEOUT_SECONDS"
        ) from None
    finally:
        for thread in threads:
            thread.join(timeout=5)
    if report:
        event(
            boundary="process",
            operation=operation,
            return_code=return_code,
            stdout_bytes=stdout.total,
            stdout_digest=stdout.fingerprint,
            stderr_bytes=stderr.total,
            stderr_digest=stderr.fingerprint,
        )
    if stdout.disclosed or stderr.disclosed:
        raise ContractError(
            f"stage=security operation={operation} shape=credential-disclosure "
            "fix_hint=remove-secret-from-client-diagnostics"
        )
    if return_code != 0:
        raise ContractError(
            f"stage=process operation={operation} shape=exit-{return_code} "
            f"stdout_digest={stdout.fingerprint} stderr_digest={stderr.fingerprint} "
            "fix_hint=inspect-local-nonretained-output"
        )
    return bytes(stdout.retained)


@dataclass
class BackgroundProcess:
    operation: str
    process: subprocess.Popen[bytes]
    output: BinaryIO
    capture: StreamCapture
    reader: threading.Thread

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.reader.join(timeout=5)
        self.output.close()
        if self.reader.is_alive():
            raise ContractError(
                f"stage=diagnostics operation={self.operation} shape=reader-timeout "
                "fix_hint=inspect-background-process-shutdown"
            )


def start_background(
    command: list[str],
    operation: str,
    secrets: tuple[bytes, ...],
    environment: dict[str, str] | None = None,
) -> BackgroundProcess:
    try:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except OSError as error:
        raise ContractError(
            f"stage=process operation={operation} shape=unavailable "
            "fix_hint=inspect-background-process-binary"
        ) from error
    assert process.stdout is not None
    capture = StreamCapture(secrets)
    reader = threading.Thread(
        target=capture.consume,
        args=(process.stdout,),
        daemon=True,
    )
    reader.start()
    return BackgroundProcess(operation, process, process.stdout, capture, reader)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def json_request(
    context: ssl.SSLContext,
    endpoint: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(endpoint + path, data=body, method=method)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(
            request,
            context=context,
            timeout=min(TIMEOUT, 10),
        ) as response:
            response_body = response.read(1 << 20)
    except (OSError, urllib.error.URLError) as error:
        raise ContractError(
            f"stage=http operation={method.lower()}-{path.split('?')[0]} "
            f"shape={type(error).__name__} fix_hint=inspect-server-digest"
        ) from None
    return {} if not response_body else json.loads(response_body)


def wait_ready(
    process: BackgroundProcess,
    context: ssl.SSLContext,
    endpoint: str,
    path: str,
) -> None:
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            break
        try:
            json_request(context, endpoint, "GET", path)
            return
        except ContractError:
            time.sleep(0.05)
    raise ContractError(
        f"stage=readiness operation={process.operation} shape=not-ready "
        "fix_hint=inspect-server-digest"
    )


def verify_connector_artifact() -> Path:
    lock_path = ROOT / "tests" / "spark" / "artifacts.lock.json"
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    if (
        lock.get("connectorVersion") != EXPECTED_CONNECTOR_VERSION
        or lock.get("sparkVersion") != EXPECTED_SPARK_VERSION
        or lock.get("scalaBinaryVersion") != "2.12"
        or len(lock.get("artifacts", [])) != 1
    ):
        raise ContractError(
            "stage=artifact operation=connector-lock shape=version-drift "
            "fix_hint=restore-the-reviewed-0.44.2-lock"
        )
    artifact = lock["artifacts"][0]
    configured = os.getenv("BQEMU_AUTH_CONNECTOR_JAR")
    path = (
        Path(configured)
        if configured
        else ROOT / ".artifacts" / "spark" / artifact["output"]
    )
    if not path.is_file():
        raise ContractError(
            "stage=artifact operation=connector-jar shape=missing "
            "fix_hint=run-scripts-fetch_spark_artifacts.py"
        )
    contents_digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1 << 20):
            contents_digest.update(chunk)
            size += len(chunk)
    if size != artifact["size"] or contents_digest.hexdigest() != artifact["sha256"]:
        raise ContractError(
            "stage=artifact operation=connector-jar shape=checksum-drift "
            "fix_hint=remove-cache-and-refetch-reviewed-artifact"
        )
    return path.resolve()


def bootstrap(
    context: ssl.SSLContext,
    endpoint: str,
    project: str,
    dataset: str,
    table: str,
) -> None:
    json_request(
        context,
        endpoint,
        "POST",
        "/bqemu/v1/projects",
        {"projectId": project},
    )
    json_request(
        context,
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{project}/datasets",
        {
            "datasetReference": {
                "projectId": project,
                "datasetId": dataset,
            },
            "location": "US",
        },
    )
    json_request(
        context,
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{project}/datasets/{dataset}/tables",
        {
            "tableReference": {
                "projectId": project,
                "datasetId": dataset,
                "tableId": table,
            },
            "schema": {
                "fields": [
                    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
                ]
            },
        },
    )
    result = json_request(
        context,
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{project}/queries",
        {
            "query": f"INSERT INTO `{project}.{dataset}.{table}` VALUES (1)",
            "useLegacySql": False,
        },
    )
    if result.get("jobComplete") is not True:
        raise ContractError(
            "stage=bootstrap operation=insert-row shape=incomplete-job "
            "fix_hint=inspect-query-contract"
        )


def child_environment(manifest: dict[str, Any]) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "REQUESTS_CA_BUNDLE": manifest["ca_certificate"],
            "SSL_CERT_FILE": manifest["ca_certificate"],
            "HTTPS_PROXY": manifest["oauth_proxy_url"],
            "https_proxy": manifest["oauth_proxy_url"],
            "NO_PROXY": "localhost,127.0.0.1,::1",
            "no_proxy": "localhost,127.0.0.1,::1",
            "CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE": manifest["ca_certificate"],
            "CLOUDSDK_CORE_DISABLE_PROMPTS": "1",
            "CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK": "true",
        }
    )
    return environment


def java_environment(
    manifest: dict[str, Any],
    spark_python: Path,
) -> dict[str, str]:
    environment = child_environment(manifest)
    proxy = urllib.parse.urlparse(manifest["oauth_proxy_url"])
    java_options = " ".join(
        (
            f"-Djavax.net.ssl.trustStore={manifest['java_truststore']}",
            f"-Djavax.net.ssl.trustStorePassword={manifest['truststore_password']}",
            "-Djavax.net.ssl.trustStoreType=PKCS12",
            f"-Dhttps.proxyHost={proxy.hostname}",
            f"-Dhttps.proxyPort={proxy.port}",
            "-Dhttp.nonProxyHosts=localhost|127.*",
        )
    )
    previous = environment.get("JAVA_TOOL_OPTIONS", "")
    environment["JAVA_TOOL_OPTIONS"] = (previous + " " + java_options).strip()
    environment["SPARK_LOCAL_IP"] = "127.0.0.1"
    environment["PYSPARK_PYTHON"] = str(spark_python)
    environment["PYSPARK_DRIVER_PYTHON"] = str(spark_python)
    return environment


def secret_values(fixture_dir: Path) -> tuple[bytes, ...]:
    service = json.loads(
        (fixture_dir / "service-account.json").read_text(encoding="utf-8")
    )
    user = json.loads(
        (fixture_dir / "authorized-user.json").read_text(encoding="utf-8")
    )
    private_key_lines = tuple(
        line.encode("ascii")
        for line in service["private_key"].splitlines()
        if len(line) >= 32 and not line.startswith("-----")
    )
    values = (
        service["private_key"].encode("utf-8"),
        *private_key_lines,
        user["client_secret"].encode("utf-8"),
        user["refresh_token"].encode("utf-8"),
        (fixture_dir / "subject-token.txt").read_bytes().strip(),
        (fixture_dir / "access-token.txt").read_bytes().strip(),
        ISSUED_TOKEN_PREFIX,
    )
    return tuple(dict.fromkeys(value for value in values if value))


def assert_dataset_output(output: bytes, dataset: str, operation: str) -> None:
    try:
        decoded = json.loads(output.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ContractError(
            f"stage=assert operation={operation} shape=invalid-json "
            "fix_hint=compare-bq-output-contract"
        ) from None
    if not any(
        item.get("datasetReference", {}).get("datasetId") == dataset
        for item in decoded
    ):
        raise ContractError(
            f"stage=assert operation={operation} shape=dataset-missing "
            "fix_hint=compare-bq-list-contract"
        )


def run_python_consumer(
    python_client: Path,
    endpoint: str,
    project: str,
    dataset: str,
    fixture_dir: Path,
    environment: dict[str, str],
    secrets: tuple[bytes, ...],
) -> None:
    output = run_process(
        [
            str(python_client),
            str(ROOT / "tests" / "auth" / "python_client.py"),
            "--endpoint",
            endpoint,
            "--project",
            project,
            "--dataset",
            dataset,
            "--fixture-dir",
            str(fixture_dir),
        ],
        "python-3.43.0",
        environment=environment,
        secrets=secrets,
    )
    if EXPECTED_PYTHON_VERSION.encode("utf-8") not in output:
        raise ContractError(
            "stage=assert operation=python-3.43.0 shape=success-marker-missing "
            "fix_hint=inspect-pinned-python-runner"
        )


def run_bq_consumer(
    bq: str,
    work: Path,
    endpoint: str,
    project: str,
    dataset: str,
    fixture_dir: Path,
    manifest: dict[str, Any],
    environment: dict[str, str],
    secrets: tuple[bytes, ...],
) -> None:
    version_output = run_process([bq, "version"], "bq-version")
    if f"BigQuery CLI {EXPECTED_BQ_VERSION}".encode() not in version_output:
        raise ContractError(
            "stage=assert operation=bq-version shape=version-drift "
            "fix_hint=install-bq-2.1.31"
        )
    bq_base = [
        bq,
        f"--api={endpoint}",
        f"--project_id={project}",
        f"--ca_certificates_file={manifest['ca_certificate']}",
        "--format=json",
    ]
    token = (fixture_dir / "access-token.txt").read_text(encoding="utf-8").strip()
    direct_output = run_process(
        bq_base + [f"--oauth_access_token={token}", "ls"],
        "bq-access-token",
        environment=environment,
        secrets=secrets,
    )
    assert_dataset_output(direct_output, dataset, "bq-access-token")
    for filename in (
        "service-account.json",
        "authorized-user.json",
        "wif.json",
    ):
        with tempfile.TemporaryDirectory(
            prefix="bqemu-cloudsdk-", dir=work
        ) as cloud_config:
            credential_environment = environment.copy()
            credential_environment.update(
                {
                    "CLOUDSDK_CONFIG": cloud_config,
                    "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE": str(
                        fixture_dir / filename
                    ),
                }
            )
            output = run_process(
                bq_base + ["ls"],
                "bq-" + filename.removesuffix(".json"),
                environment=credential_environment,
                secrets=secrets,
            )
            assert_dataset_output(
                output,
                dataset,
                "bq-" + filename.removesuffix(".json"),
            )


def run_pyspark_consumer(
    spark_python: Path,
    connector_jar: Path,
    endpoint: str,
    grpc_endpoint: str,
    project: str,
    table_name: str,
    fixture_dir: Path,
    manifest: dict[str, Any],
    secrets: tuple[bytes, ...],
) -> None:
    output = run_process(
        [
            str(spark_python),
            str(ROOT / "tests" / "auth" / "pyspark_connector.py"),
            "--connector-jar",
            str(connector_jar),
            "--http-endpoint",
            endpoint,
            "--grpc-endpoint",
            grpc_endpoint,
            "--project",
            project,
            "--table",
            table_name,
            "--fixture-dir",
            str(fixture_dir),
        ],
        "pyspark-3.5.8-connector-0.44.2",
        environment=java_environment(manifest, spark_python),
        secrets=secrets,
    )
    if b'"client":"pyspark"' not in output:
        raise ContractError(
            "stage=assert operation=pyspark shape=success-marker-missing "
            "fix_hint=inspect-pinned-spark-runner"
        )


def run_scala_consumer(
    spark_python: Path,
    spark_shell: Path,
    connector_jar: Path,
    endpoint: str,
    grpc_endpoint: str,
    project: str,
    table_name: str,
    fixture_dir: Path,
    manifest: dict[str, Any],
    secrets: tuple[bytes, ...],
) -> None:
    environment = java_environment(manifest, spark_python)
    environment.update(
        {
            "BQEMU_AUTH_PROJECT": project,
            "BQEMU_AUTH_TABLE": table_name,
            "BQEMU_AUTH_FIXTURE_DIR": str(fixture_dir),
            "BQEMU_AUTH_HTTP_ENDPOINT": endpoint,
            "BQEMU_AUTH_GRPC_ENDPOINT": grpc_endpoint,
        }
    )
    output = run_process(
        [
            str(spark_shell),
            "--master",
            "local[1]",
            "--jars",
            str(connector_jar),
            "-i",
            str(ROOT / "tests" / "auth" / "scala_connector.scala"),
        ],
        "scala-spark-3.5.8-connector-0.44.2",
        environment=environment,
        secrets=secrets,
    )
    if b'"client":"scala-spark"' not in output:
        raise ContractError(
            "stage=assert operation=scala-spark shape=success-marker-missing "
            "fix_hint=inspect-pinned-spark-runner"
        )


def main(case: str) -> int:
    initialize_diagnostics()
    verify_diagnostic_capture()
    connector_jar = (
        verify_connector_artifact()
        if case_enabled(case, "pyspark") or case_enabled(case, "scala-spark")
        else None
    )
    python_client = Path(
        os.getenv("BQEMU_AUTH_PYTHON", str(ROOT / ".venv" / "bin" / "python"))
    ).expanduser().absolute()
    spark_python_configured = Path(
        os.getenv(
            "BQEMU_AUTH_SPARK_PYTHON",
            str(ROOT / ".artifacts" / "spark" / "venv" / "bin" / "python"),
        )
    )
    spark_python = spark_python_configured.expanduser().absolute()
    spark_shell = Path(
        os.getenv(
            "BQEMU_AUTH_SPARK_SHELL",
            str(spark_python_configured.parent / "spark-shell"),
        )
    ).expanduser().absolute()
    bq = os.getenv("BQEMU_AUTH_BQ", "bq")
    required_executables: list[tuple[Path, str]] = []
    if case_enabled(case, "python"):
        required_executables.append((python_client, "python-client"))
    if case_enabled(case, "pyspark"):
        required_executables.append((spark_python, "pyspark"))
    if case_enabled(case, "scala-spark"):
        required_executables.extend(
            ((spark_python, "scala-python"), (spark_shell, "scala-spark"))
        )
    for path, operation in required_executables:
        if not path.is_file():
            raise ContractError(
                f"stage=setup operation={operation} shape=missing-executable "
                "fix_hint=install-the-pinned-client-environment"
            )

    with tempfile.TemporaryDirectory(prefix="bqemu-auth-contract-") as temporary:
        work = Path(temporary)
        emulator_binary = work / "go-bemu"
        fixture_binary = work / "bqemu-auth-fixture"
        run_process(
            ["go", "build", "-trimpath", "-o", str(emulator_binary), "./cmd/emulator"],
            "build-emulator",
        )
        run_process(
            [
                "go",
                "build",
                "-trimpath",
                "-o",
                str(fixture_binary),
                "./cmd/bqemu-auth-fixture",
            ],
            "build-auth-fixture",
        )

        issuer_port, proxy_port, http_port, grpc_port = (
            free_port(),
            free_port(),
            free_port(),
            free_port(),
        )
        fixture_dir = work / "credentials"
        run_process(
            [
                str(fixture_binary),
                "generate",
                "--output",
                str(fixture_dir),
                "--base-url",
                f"https://localhost:{issuer_port}",
                "--proxy-url",
                f"http://127.0.0.1:{proxy_port}",
            ],
            "generate-fixtures",
        )
        manifest = json.loads(
            (fixture_dir / "manifest.json").read_text(encoding="utf-8")
        )
        secrets = secret_values(fixture_dir)
        context = ssl.create_default_context(cafile=manifest["ca_certificate"])
        endpoint = f"https://localhost:{http_port}"
        grpc_endpoint = f"localhost:{grpc_port}"
        project = "bqemu-auth-contract"
        dataset = "credentials"
        table = "one_row"
        table_name = f"{project}.{dataset}.{table}"

        issuer = start_background(
            [
                str(fixture_binary),
                "serve",
                "--manifest",
                str(fixture_dir / "manifest.json"),
            ],
            "credential-issuer",
            secrets,
        )
        emulator_environment = os.environ.copy()
        emulator_environment.update(
            {
                "BQEMU_HTTP_ADDRESS": f"127.0.0.1:{http_port}",
                "BQEMU_GRPC_ADDRESS": f"127.0.0.1:{grpc_port}",
                "BQEMU_PUBLIC_URL": endpoint,
                "BQEMU_DATABASE_DSN": str(work / "contract.duckdb"),
                "BQEMU_TEMP_DIRECTORY": str(work / "tmp"),
                "BQEMU_TLS_CERT_FILE": manifest["server_certificate"],
                "BQEMU_TLS_KEY_FILE": manifest["server_key"],
                "BQEMU_UI_ENABLED": "false",
            }
        )
        emulator = start_background(
            [str(emulator_binary)],
            "emulator",
            secrets,
            emulator_environment,
        )
        try:
            wait_ready(issuer, context, manifest["base_url"], "/healthz")
            wait_ready(emulator, context, endpoint, "/readyz")
            bootstrap(context, endpoint, project, dataset, table)

            client_env = child_environment(manifest)
            if case_enabled(case, "python"):
                run_python_consumer(
                    python_client,
                    endpoint,
                    project,
                    dataset,
                    fixture_dir,
                    client_env,
                    secrets,
                )
            if case_enabled(case, "bq"):
                run_bq_consumer(
                    bq,
                    work,
                    endpoint,
                    project,
                    dataset,
                    fixture_dir,
                    manifest,
                    client_env,
                    secrets,
                )
            if case_enabled(case, "pyspark"):
                if connector_jar is None:
                    raise ContractError(
                        "stage=setup operation=pyspark shape=missing-connector "
                        "fix_hint=restore-reviewed-connector-artifact"
                    )
                run_pyspark_consumer(
                    spark_python,
                    connector_jar,
                    endpoint,
                    grpc_endpoint,
                    project,
                    table_name,
                    fixture_dir,
                    manifest,
                    secrets,
                )
            if case_enabled(case, "scala-spark"):
                if connector_jar is None:
                    raise ContractError(
                        "stage=setup operation=scala-spark shape=missing-connector "
                        "fix_hint=restore-reviewed-connector-artifact"
                    )
                run_scala_consumer(
                    spark_python,
                    spark_shell,
                    connector_jar,
                    endpoint,
                    grpc_endpoint,
                    project,
                    table_name,
                    fixture_dir,
                    manifest,
                    secrets,
                )
        finally:
            emulator.stop()
            issuer.stop()

        for background in (issuer, emulator):
            if background.capture.total > MAX_BACKGROUND_LOG_BYTES:
                raise ContractError(
                    f"stage=diagnostics operation={background.operation} "
                    "shape=log-size-limit fix_hint=reduce-test-log-volume"
                )
            if background.capture.disclosed:
                raise ContractError(
                    f"stage=security operation={background.operation} "
                    "shape=credential-disclosure fix_hint=redact-runtime-log"
                )
            event(
                boundary="background-process",
                operation=background.operation,
                output_bytes=background.capture.total,
                output_digest=background.capture.fingerprint,
            )

    event(
        suite="client-credentials-and-tls",
        consumer_case=case,
        python=EXPECTED_PYTHON_VERSION,
        bq=EXPECTED_BQ_VERSION,
        spark=EXPECTED_SPARK_VERSION,
        connector=EXPECTED_CONNECTOR_VERSION,
        status="passed",
    )
    return 0


if __name__ == "__main__":
    started = time.monotonic()
    configured_case = os.getenv("BQEMU_AUTH_CASE", "all")
    case_label = configured_case if configured_case in (*CONSUMER_CASES, "all") else "invalid"
    failure: Exception | None = None
    try:
        main(selected_case())
    except Exception as error:
        failure = error
        encoded = f"{type(error).__name__}:{error}".encode(
            "utf-8", errors="replace"
        )
        event(
            suite="client-credentials-and-tls",
            consumer_case=case_label,
            status="failed",
            error_type=type(error).__name__,
            error_digest=digest_bytes(encoded),
        )
    try:
        write_junit(case_label, time.monotonic() - started, failure)
    except Exception as error:
        encoded = f"{type(error).__name__}:{error}".encode(
            "utf-8", errors="replace"
        )
        event(
            suite="client-credentials-and-tls",
            consumer_case=case_label,
            status="failed",
            error_type=type(error).__name__,
            error_digest=digest_bytes(encoded),
            stage="junit",
        )
        failure = error
    raise SystemExit(1 if failure is not None else 0)
