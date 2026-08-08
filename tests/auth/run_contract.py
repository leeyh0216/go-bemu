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
from typing import Any, BinaryIO, Mapping
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.consumer_runtime import (  # noqa: E402
    ArtifactSpec,
    ConsumerRuntimeError,
    NormalizedConsumerCase,
    NormalizedConsumerExecution,
    check_python_dependencies,
    install_python_artifact,
    load_normalized_execution,
    materialize_artifact,
    require_execution_artifact,
    select_normalized_executions,
    verify_python_minor,
)


CONSUMER_MANIFEST = ROOT / "contract" / "consumers.normalized.json"
MAX_CAPTURE_BYTES = 1 << 20
MAX_BACKGROUND_LOG_BYTES = 16 << 20
MAX_DIAGNOSTIC_FILE_BYTES = 256 << 10
ISSUED_TOKEN_PREFIX = b"bqemu-local-issued-"
AUTH_CONSUMER_BY_ADAPTER = {
    "python-pytest-v1": "python",
    "bq-cli-v1": "bq",
    "spark-pyspark-pytest-v1": "pyspark",
    "spark-scala-shell-v1": "scala-spark",
}
REQUIRED_VERSIONS_BY_CONSUMER = {
    "python": frozenset({"python", "client"}),
    "bq": frozenset({"cloudSdk", "bq"}),
    "pyspark": frozenset(
        {"python", "spark", "connector", "scala", "scalaBinary", "java"}
    ),
    "scala-spark": frozenset(
        {"python", "spark", "connector", "scala", "scalaBinary", "java"}
    ),
}
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


@dataclass(frozen=True)
class AuthConsumerCase:
    normalized: NormalizedConsumerCase
    execution: NormalizedConsumerExecution
    consumer: str

    @property
    def case_id(self) -> str:
        return self.normalized.case_id

    @property
    def versions(self) -> Mapping[str, str]:
        return self.normalized.versions

    @property
    def bootstrap(self) -> Mapping[str, str]:
        return self.execution.bootstrap

    @property
    def artifacts(self) -> tuple[ArtifactSpec, ...]:
        return self.normalized.artifacts


def load_auth_cases(
    selected: str,
    manifest_path: Path = CONSUMER_MANIFEST,
) -> tuple[AuthConsumerCase, ...]:
    try:
        if selected == "all":
            candidates = select_normalized_executions(
                manifest_path, lane="required", execution_id="public"
            )
        else:
            candidate = load_normalized_execution(manifest_path, selected, "public")
            candidates = (candidate,) if candidate[0].lane == "required" else ()
    except ConsumerRuntimeError as error:
        raise ContractError(
            "stage=config operation=load-consumer-manifest shape=invalid "
            "fix_hint=run-make-contract-generate"
        ) from error

    cases: list[AuthConsumerCase] = []
    for normalized, execution in candidates:
        consumer = AUTH_CONSUMER_BY_ADAPTER.get(execution.runner_adapter_id)
        if consumer is None:
            raise ContractError(
                "stage=config operation=select-auth-adapter shape=unsupported "
                "fix_hint=implement-a-typed-auth-runner-adapter"
            )
        required_versions = REQUIRED_VERSIONS_BY_CONSUMER[consumer]
        if any(
            not isinstance(normalized.versions.get(name), str)
            or not normalized.versions[name]
            for name in required_versions
        ):
            raise ContractError(
                "stage=config operation=decode-runtime-versions shape=incomplete "
                "fix_hint=run-make-contract-generate"
            )
        cases.append(
            AuthConsumerCase(
                normalized=normalized,
                execution=execution,
                consumer=consumer,
            )
        )
    if not cases:
        raise ContractError(
            "stage=config operation=select-consumer shape=no-required-cases "
            "fix_hint=add-a-normalized-required-consumer-case"
        )
    return tuple(cases)


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


def error_digest(error: BaseException) -> str:
    encoded = f"{type(error).__name__}:{error}".encode(
        "utf-8", errors="replace"
    )
    return digest_bytes(encoded)


def combined_contract_error(
    stage: str,
    errors: list[BaseException],
) -> ContractError:
    fingerprints = "\x00".join(error_digest(error) for error in errors)
    return ContractError(
        f"stage={stage} operation=background-processes shape=failed "
        f"failure_count={len(errors)} failure_digest={digest_bytes(fingerprints.encode())} "
        "fix_hint=inspect-digest-only-diagnostics"
    )


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

    cleanup_trace: list[str] = []

    class ProbeCapture:
        def __init__(self, name: str, total: int, disclosed: bool) -> None:
            self.name = name
            self._total = total
            self._disclosed = disclosed

        @property
        def total(self) -> int:
            cleanup_trace.append(f"{self.name}-total")
            return self._total

        @property
        def disclosed(self) -> bool:
            cleanup_trace.append(f"{self.name}-disclosed")
            return self._disclosed

        @property
        def fingerprint(self) -> str:
            cleanup_trace.append(f"{self.name}-fingerprint")
            return digest_bytes(self.name.encode("ascii"))

    class ProbeBackground:
        def __init__(
            self,
            name: str,
            capture: ProbeCapture,
            fail_stop: bool,
        ) -> None:
            self.operation = name
            self.capture = capture
            self.fail_stop = fail_stop

        def stop(self) -> None:
            cleanup_trace.append(f"{self.operation}-stop")
            if self.fail_stop:
                raise RuntimeError(secret_marker.decode("ascii"))

    cleanup_error = stop_and_validate_backgrounds(
        (
            ProbeBackground("first", ProbeCapture("first", 1, True), True),
            ProbeBackground(
                "second",
                ProbeCapture("second", MAX_BACKGROUND_LOG_BYTES + 1, False),
                False,
            ),
        ),
        report=False,
    )
    expected_cleanup_trace = [
        "first-stop",
        "second-stop",
        "first-total",
        "first-disclosed",
        "first-fingerprint",
        "second-total",
        "second-disclosed",
        "second-fingerprint",
    ]

    if (
        not capture.disclosed
        or len(capture.retained) > 8
        or injected_error is None
        or secret_marker in str(injected_error).encode("utf-8", errors="replace")
        or disclosed_artifact
        or cleanup_error is None
        or secret_marker in str(cleanup_error).encode("utf-8", errors="replace")
        or cleanup_trace != expected_cleanup_trace
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
        failures: list[BaseException] = []
        try:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
        except Exception as error:
            failures.append(error)
            try:
                self.process.kill()
                self.process.wait(timeout=5)
            except Exception as force_error:
                failures.append(force_error)
        try:
            self.reader.join(timeout=5)
        except Exception as error:
            failures.append(error)
        try:
            self.output.close()
        except Exception as error:
            failures.append(error)
        try:
            if self.reader.is_alive():
                failures.append(
                    ContractError(
                        "stage=diagnostics operation=background-reader "
                        "shape=reader-timeout fix_hint=inspect-background-process-shutdown"
                    )
                )
        except Exception as error:
            failures.append(error)
        if failures:
            raise combined_contract_error("shutdown", failures)


def stop_and_validate_backgrounds(
    backgrounds: tuple[Any, ...],
    *,
    report: bool = True,
) -> ContractError | None:
    failures: list[BaseException] = []
    for background in backgrounds:
        try:
            background.stop()
        except Exception as error:
            failures.append(error)

    for background in backgrounds:
        operation: str | None = None
        total: int | None = None
        disclosed: bool | None = None
        fingerprint: str | None = None
        try:
            operation = background.operation
        except Exception as error:
            failures.append(error)
        try:
            total = background.capture.total
        except Exception as error:
            failures.append(error)
        try:
            disclosed = background.capture.disclosed
        except Exception as error:
            failures.append(error)
        try:
            fingerprint = background.capture.fingerprint
        except Exception as error:
            failures.append(error)

        if total is not None and total > MAX_BACKGROUND_LOG_BYTES:
            failures.append(
                ContractError(
                    "stage=diagnostics operation=background-process "
                    "shape=log-size-limit fix_hint=reduce-test-log-volume"
                )
            )
        if disclosed is True:
            failures.append(
                ContractError(
                    "stage=security operation=background-process "
                    "shape=credential-disclosure fix_hint=redact-runtime-log"
                )
            )
        if (
            report
            and operation is not None
            and total is not None
            and fingerprint is not None
        ):
            try:
                event(
                    boundary="background-process",
                    operation=operation,
                    output_bytes=total,
                    output_digest=fingerprint,
                )
            except Exception as error:
                failures.append(error)

    if failures:
        return combined_contract_error("cleanup", failures)
    return None


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


def require_case_artifact(
    case: AuthConsumerCase,
    usage: str,
) -> ArtifactSpec:
    try:
        artifact = require_execution_artifact(
            case.normalized, case.execution, usage
        )
    except ConsumerRuntimeError as error:
        raise ContractError(
            "stage=artifact operation=select-case-artifact shape=cardinality "
            "fix_hint=define-exactly-one-required-artifact-usage"
        ) from error
    if artifact.role != "execution":
        raise ContractError(
            "stage=artifact operation=decode-case-artifact shape=invalid "
            "fix_hint=run-make-contract-generate"
        )
    return artifact


def materialize_case_artifact(
    case: AuthConsumerCase,
    usage: str,
    configured_path: str = "",
) -> Path:
    artifact = require_case_artifact(case, usage)
    try:
        return materialize_artifact(
            ROOT,
            artifact,
            configured_path=configured_path,
            timeout_seconds=min(TIMEOUT, 180),
        )
    except ConsumerRuntimeError as error:
        raise ContractError(
            "stage=artifact operation=materialize-case-artifact shape=invalid-or-unavailable "
            "fix_hint=verify-the-declared-execution-artifact"
        ) from error


def install_case_python_artifact(
    case: AuthConsumerCase,
    python_executable: Path,
    usage: str,
    operation: str,
    *,
    check_dependencies: bool = True,
) -> None:
    artifact = materialize_case_artifact(case, usage)
    uv = os.getenv("BQEMU_UV_BIN", "uv")
    install_python_artifact(
        python_executable,
        artifact,
        operation,
        run_process,
        uv_executable=uv,
    )
    if check_dependencies:
        check_python_dependencies(
            python_executable,
            operation + "-dependency-check",
            run_process,
            uv_executable=uv,
        )


def verify_python_runtime(python_executable: Path, expected: str) -> None:
    try:
        verify_python_minor(
            python_executable,
            expected,
            "verify-python-runtime",
            run_process,
        )
    except ConsumerRuntimeError as error:
        raise ContractError(
            "stage=assert operation=verify-python-runtime shape=version-drift-or-invalid "
            "fix_hint=install-the-case-declared-python-runtime"
        ) from error


def prepare_case_python_runtime(
    case: AuthConsumerCase, python_executable: Path
) -> None:
    if case.consumer == "python":
        verify_python_runtime(python_executable, case.versions["python"])
        install_case_python_artifact(
            case,
            python_executable,
            "python-wheel",
            "install-python-case-artifact",
        )
        return
    if case.consumer not in ("pyspark", "scala-spark"):
        return
    expected_python = case.versions.get("python")
    if not expected_python:
        raise ContractError(
            "stage=config operation=select-python-runtime shape=missing-version "
            "fix_hint=declare-a-runtime-or-bootstrap-python-version"
        )
    verify_python_runtime(python_executable, expected_python)
    install_case_python_artifact(
        case,
        python_executable,
        "spark-python-bridge",
        "install-spark-python-bridge-artifact",
        check_dependencies=False,
    )
    install_case_python_artifact(
        case,
        python_executable,
        "spark-runtime",
        "install-spark-runtime-artifact",
    )


def verify_bq_runtime(case: AuthConsumerCase, bq: str, gcloud: str) -> None:
    bq_output = run_process([bq, "version"], "verify-bq-runtime")
    if bq_output.decode("ascii", errors="replace").strip() != (
        "This is BigQuery CLI " + case.versions["bq"]
    ):
        raise ContractError(
            "stage=assert operation=verify-bq-runtime shape=version-drift "
            "fix_hint=install-the-case-declared-cloud-sdk"
        )
    cloud_output = run_process(
        [gcloud, "version", "--format=json"], "verify-cloud-sdk-runtime"
    )
    try:
        versions = json.loads(cloud_output)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(
            "stage=assert operation=verify-cloud-sdk-runtime shape=invalid-output "
            "fix_hint=install-the-case-declared-cloud-sdk"
        ) from error
    if (
        not isinstance(versions, dict)
        or versions.get("Google Cloud SDK") != case.versions["cloudSdk"]
        or versions.get("bq") != case.versions["bq"]
    ):
        raise ContractError(
            "stage=assert operation=verify-cloud-sdk-runtime shape=version-drift "
            "fix_hint=install-the-case-declared-cloud-sdk"
        )


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
    expected_version: str,
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
            "--expected-version",
            expected_version,
        ],
        "python-client-auth",
        environment=environment,
        secrets=secrets,
    )
    if expected_version.encode("utf-8") not in output:
        raise ContractError(
            "stage=assert operation=python-client-auth shape=success-marker-missing "
            "fix_hint=inspect-pinned-python-runner"
        )


def run_bq_consumer(
    bq: str,
    expected_version: str,
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
    if f"BigQuery CLI {expected_version}".encode() not in version_output:
        raise ContractError(
            "stage=assert operation=bq-version shape=version-drift "
            "fix_hint=install-the-case-declared-bq-version"
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
    expected_spark_version: str,
    expected_connector_version: str,
    expected_scala_version: str,
    expected_scala_binary_version: str,
    expected_java_version: str,
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
            "--expected-spark-version",
            expected_spark_version,
            "--expected-connector-version",
            expected_connector_version,
            "--expected-scala-version",
            expected_scala_version,
            "--expected-scala-binary-version",
            expected_scala_binary_version,
            "--expected-java-version",
            expected_java_version,
        ],
        "pyspark-connector-auth",
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
    expected_spark_version: str,
    expected_connector_version: str,
    expected_scala_version: str,
    expected_scala_binary_version: str,
    expected_java_version: str,
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
            "BQEMU_AUTH_EXPECTED_SPARK_VERSION": expected_spark_version,
            "BQEMU_AUTH_EXPECTED_CONNECTOR_VERSION": expected_connector_version,
            "BQEMU_AUTH_EXPECTED_SCALA_VERSION": expected_scala_version,
            "BQEMU_AUTH_EXPECTED_SCALA_BINARY_VERSION": expected_scala_binary_version,
            "BQEMU_AUTH_EXPECTED_JAVA_VERSION": expected_java_version,
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
        "scala-spark-connector-auth",
        environment=environment,
        secrets=secrets,
    )
    if b'"client":"scala-spark"' not in output:
        raise ContractError(
            "stage=assert operation=scala-spark shape=success-marker-missing "
            "fix_hint=inspect-pinned-spark-runner"
        )


def main(case: AuthConsumerCase) -> int:
    connector_jar = (
        materialize_case_artifact(
            case,
            "spark-connector-dsv1-jar",
            os.getenv("BQEMU_AUTH_CONNECTOR_JAR", ""),
        )
        if case.consumer in ("pyspark", "scala-spark")
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
    gcloud = os.getenv("BQEMU_AUTH_GCLOUD", "gcloud")
    required_executables: list[tuple[Path, str]] = []
    if case.consumer == "python":
        required_executables.append((python_client, "python-client"))
    if case.consumer in ("pyspark", "scala-spark"):
        required_executables.append((spark_python, "spark-python"))
    for path, operation in required_executables:
        if not path.is_file():
            raise ContractError(
                f"stage=setup operation={operation} shape=missing-executable "
                "fix_hint=install-the-pinned-client-environment"
            )
    if case.consumer == "python":
        prepare_case_python_runtime(case, python_client)
    elif case.consumer in ("pyspark", "scala-spark"):
        prepare_case_python_runtime(case, spark_python)
        if case.consumer == "scala-spark" and not spark_shell.is_file():
            raise ContractError(
                "stage=setup operation=scala-spark shape=missing-executable "
                "fix_hint=install-the-case-declared-spark-runtime"
            )
    elif case.consumer == "bq":
        verify_bq_runtime(case, bq, gcloud)

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

        backgrounds: list[BackgroundProcess] = []
        execution_error: Exception | None = None
        try:
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
            backgrounds.append(issuer)
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
            backgrounds.append(emulator)
            wait_ready(issuer, context, manifest["base_url"], "/healthz")
            wait_ready(emulator, context, endpoint, "/readyz")
            bootstrap(context, endpoint, project, dataset, table)

            client_env = child_environment(manifest)
            if case.consumer == "python":
                run_python_consumer(
                    python_client,
                    case.versions["client"],
                    endpoint,
                    project,
                    dataset,
                    fixture_dir,
                    client_env,
                    secrets,
                )
            if case.consumer == "bq":
                run_bq_consumer(
                    bq,
                    case.versions["bq"],
                    work,
                    endpoint,
                    project,
                    dataset,
                    fixture_dir,
                    manifest,
                    client_env,
                    secrets,
                )
            if case.consumer == "pyspark":
                if connector_jar is None:
                    raise ContractError(
                        "stage=setup operation=pyspark shape=missing-connector "
                        "fix_hint=restore-reviewed-connector-artifact"
                    )
                run_pyspark_consumer(
                    spark_python,
                    connector_jar,
                    case.versions["spark"],
                    case.versions["connector"],
                    case.versions["scala"],
                    case.versions["scalaBinary"],
                    case.versions["java"],
                    endpoint,
                    grpc_endpoint,
                    project,
                    table_name,
                    fixture_dir,
                    manifest,
                    secrets,
                )
            if case.consumer == "scala-spark":
                if connector_jar is None:
                    raise ContractError(
                        "stage=setup operation=scala-spark shape=missing-connector "
                        "fix_hint=restore-reviewed-connector-artifact"
                    )
                run_scala_consumer(
                    spark_python,
                    spark_shell,
                    connector_jar,
                    case.versions["spark"],
                    case.versions["connector"],
                    case.versions["scala"],
                    case.versions["scalaBinary"],
                    case.versions["java"],
                    endpoint,
                    grpc_endpoint,
                    project,
                    table_name,
                    fixture_dir,
                    manifest,
                    secrets,
                )
        except Exception as error:
            execution_error = error

        cleanup_error = stop_and_validate_backgrounds(tuple(backgrounds))
        if execution_error is not None and cleanup_error is not None:
            raise combined_contract_error(
                "execution-and-cleanup",
                [execution_error, cleanup_error],
            ) from None
        if execution_error is not None:
            raise execution_error from None
        if cleanup_error is not None:
            raise cleanup_error from None

    version_fields: dict[str, str] = {}
    if case.consumer == "python":
        version_fields["python"] = case.versions["client"]
    elif case.consumer == "bq":
        version_fields["bq"] = case.versions["bq"]
    else:
        version_fields["spark"] = case.versions["spark"]
        version_fields["connector"] = case.versions["connector"]
    event(
        suite="client-credentials-and-tls",
        consumer_case=case.case_id,
        status="passed",
        **version_fields,
    )
    return 0


if __name__ == "__main__":
    started = time.monotonic()
    configured_case = os.getenv("BQEMU_AUTH_CASE", "all")
    case_label = (
        configured_case
        if SAFE_EVENT_TEXT.fullmatch(configured_case) is not None
        else "invalid"
    )
    failure: Exception | None = None
    try:
        initialize_diagnostics()
        verify_diagnostic_capture()
        for selected in load_auth_cases(configured_case):
            main(selected)
    except Exception as error:
        failure = error
        event(
            suite="client-credentials-and-tls",
            consumer_case=case_label,
            status="failed",
            error_type=type(error).__name__,
            error_digest=error_digest(error),
        )
    try:
        write_junit(case_label, time.monotonic() - started, failure)
    except Exception as error:
        event(
            suite="client-credentials-and-tls",
            consumer_case=case_label,
            status="failed",
            error_type=type(error).__name__,
            error_digest=error_digest(error),
            stage="junit",
        )
        failure = error
    raise SystemExit(1 if failure is not None else 0)
