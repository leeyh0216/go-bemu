"""Bounded runtimes and payload-safe diagnostics for the load E2E.

Protocol references:
  https://cloud.google.com/storage/docs/json_api/v1/objects/get
  https://cloud.google.com/storage/docs/json_api/v1/objects/list
  https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
  https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import http.client
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, BinaryIO, Mapping, Sequence, cast
import urllib.error
import urllib.parse
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]
MAX_LOCK_BYTES = 1 << 20
MAX_HTTP_RESPONSE_BYTES = 4 << 20


class ContractError(RuntimeError):
    """A stable failure signature that never contains a raw payload."""

    def __init__(
        self,
        *,
        stage: str,
        service: str,
        model_version: str,
        operation: str,
        shape: str,
        fingerprint: str,
        fix_hint: str,
    ) -> None:
        self.fields = {
            "stage": stage,
            "service": service,
            "model_version": model_version,
            "operation": operation,
            "shape": shape,
            "fingerprint": fingerprint,
            "fix_hint": fix_hint,
        }
        super().__init__(" ".join(f"{key}={value}" for key, value in self.fields.items()))


def digest(value: bytes | str | Any) -> str:
    if isinstance(value, bytes):
        encoded = value
    elif isinstance(value, str):
        encoded = value.encode("utf-8", errors="replace")
    else:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def raw_file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1 << 20):
            hasher.update(chunk)
    return hasher.hexdigest()


def file_metadata(path: Path) -> dict[str, Any]:
    try:
        size = path.stat().st_size
        fingerprint = "sha256:" + raw_file_digest(path)
    except FileNotFoundError:
        size = 0
        fingerprint = digest(b"")
    return {"bytes": size, "sha256": fingerprint}


def event(**fields: Any) -> None:
    print(
        json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=True),
        flush=True,
    )


def failure(
    *,
    stage: str,
    service: str,
    model_version: str,
    operation: str,
    shape: str,
    observed: bytes | str | Any,
    fix_hint: str,
) -> ContractError:
    return ContractError(
        stage=stage,
        service=service,
        model_version=model_version,
        operation=operation,
        shape=shape,
        fingerprint=digest(observed),
        fix_hint=fix_hint,
    )


def positive_seconds(name: str, default: str) -> float:
    raw = os.getenv(name, default)
    try:
        value = float(raw)
    except ValueError as error:
        raise failure(
            stage="configuration",
            service="load-e2e",
            model_version="bqemu-load-e2e/v1",
            operation="parse-timeout",
            shape=name,
            observed=raw,
            fix_hint="set-a-positive-number-of-seconds",
        ) from error
    if value <= 0:
        raise failure(
            stage="configuration",
            service="load-e2e",
            model_version="bqemu-load-e2e/v1",
            operation="parse-timeout",
            shape=name,
            observed=raw,
            fix_hint="set-a-positive-number-of-seconds",
        )
    return value


def positive_integer(name: str, default: str) -> int:
    raw = os.getenv(name, default)
    try:
        value = int(raw)
    except ValueError as error:
        raise failure(
            stage="configuration",
            service="load-e2e",
            model_version="bqemu-load-e2e/v1",
            operation="parse-limit",
            shape=name,
            observed=raw,
            fix_hint="set-a-positive-integer",
        ) from error
    if value <= 0:
        raise failure(
            stage="configuration",
            service="load-e2e",
            model_version="bqemu-load-e2e/v1",
            operation="parse-limit",
            shape=name,
            observed=raw,
            fix_hint="set-a-positive-integer",
        )
    return value


@dataclass(frozen=True)
class Settings:
    artifact_lock: Path
    fixture_lock: Path
    config: Path
    artifact_root: Path
    go_binary: str
    docker_binary: str
    bq_binary: str
    project: str
    dataset: str
    location: str
    artifact_timeout: float
    build_timeout: float
    fixture_timeout: float
    fake_gcs_start_timeout: float
    emulator_start_timeout: float
    client_timeout: float
    shutdown_timeout: float
    http_timeout: float
    proxy_max_response_bytes: int

    @classmethod
    def from_environment(cls) -> "Settings":
        return cls(
            artifact_lock=Path(
                os.getenv(
                    "BQEMU_LOAD_ARTIFACT_LOCK",
                    str(ROOT / "tests/load/artifacts.lock.json"),
                )
            ).resolve(),
            fixture_lock=Path(
                os.getenv(
                    "BQEMU_LOAD_FIXTURE_LOCK",
                    str(ROOT / "tests/load/fixtures.lock.json"),
                )
            ).resolve(),
            config=Path(
                os.getenv(
                    "BQEMU_LOAD_CONFIG", str(ROOT / "tests/load/bqemu.yaml")
                )
            ).resolve(),
            artifact_root=Path(
                os.getenv(
                    "BQEMU_LOAD_ARTIFACT_DIR", str(ROOT / ".artifacts/load")
                )
            ).resolve(),
            go_binary=os.getenv("BQEMU_LOAD_GO_BIN", "go"),
            docker_binary=os.getenv("BQEMU_LOAD_DOCKER_BIN", "docker"),
            bq_binary=os.getenv("BQEMU_LOAD_BQCLI_BIN", "bq"),
            project=os.getenv("BQEMU_LOAD_PROJECT", "load-e2e-project"),
            dataset=os.getenv("BQEMU_LOAD_DATASET", "load_e2e"),
            location=os.getenv("BQEMU_LOAD_LOCATION", "US"),
            artifact_timeout=positive_seconds(
                "BQEMU_LOAD_ARTIFACT_TIMEOUT_SECONDS", "180"
            ),
            build_timeout=positive_seconds(
                "BQEMU_LOAD_BUILD_TIMEOUT_SECONDS", "180"
            ),
            fixture_timeout=positive_seconds(
                "BQEMU_LOAD_FIXTURE_TIMEOUT_SECONDS", "60"
            ),
            fake_gcs_start_timeout=positive_seconds(
                "BQEMU_LOAD_FAKE_GCS_START_TIMEOUT_SECONDS", "45"
            ),
            emulator_start_timeout=positive_seconds(
                "BQEMU_LOAD_EMULATOR_START_TIMEOUT_SECONDS", "45"
            ),
            client_timeout=positive_seconds(
                "BQEMU_LOAD_CLIENT_TIMEOUT_SECONDS", "120"
            ),
            shutdown_timeout=positive_seconds(
                "BQEMU_LOAD_SHUTDOWN_TIMEOUT_SECONDS", "15"
            ),
            http_timeout=positive_seconds(
                "BQEMU_LOAD_HTTP_TIMEOUT_SECONDS", "5"
            ),
            proxy_max_response_bytes=positive_integer(
                "BQEMU_LOAD_PROXY_MAX_RESPONSE_BYTES", str(MAX_HTTP_RESPONSE_BYTES)
            ),
        )


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def read_locked_json(path: Path, expected_schema: str) -> dict[str, Any]:
    try:
        with path.open("rb") as source:
            payload = source.read(MAX_LOCK_BYTES + 1)
    except OSError as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=expected_schema,
            operation="read-lock",
            shape="unreadable-lock",
            observed=type(error).__name__,
            fix_hint="restore-the-versioned-lock-file",
        ) from error
    if len(payload) > MAX_LOCK_BYTES:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=expected_schema,
            operation="read-lock",
            shape="oversize-lock",
            observed=len(payload),
            fix_hint="reduce-lock-below-one-mebibyte",
        )
    try:
        decoded = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (ValueError, json.JSONDecodeError) as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=expected_schema,
            operation="decode-lock",
            shape="invalid-json",
            observed=type(error).__name__,
            fix_hint="repair-the-versioned-lock-file",
        ) from error
    if not isinstance(decoded, dict) or decoded.get("schemaVersion") != expected_schema:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=expected_schema,
            operation="validate-lock",
            shape="schema-version",
            observed=decoded.get("schemaVersion") if isinstance(decoded, dict) else type(decoded).__name__,
            fix_hint="update-lock-reader-and-schema-together",
        )
    return decoded


def require_exact_keys(
    value: Mapping[str, Any], expected: set[str], operation: str, model_version: str
) -> None:
    if set(value) != expected:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=model_version,
            operation=operation,
            shape="object-keys",
            observed=sorted(value),
            fix_hint="update-lock-schema-and-reader-together",
        )


def run_process(
    args: Sequence[str],
    *,
    operation: str,
    service: str,
    model_version: str,
    timeout: float,
    cwd: Path = ROOT,
    environment: Mapping[str, str] | None = None,
    expected_codes: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[bytes]:
    started = time.monotonic()
    try:
        result = subprocess.run(
            list(args),
            cwd=cwd,
            env=None if environment is None else dict(environment),
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise failure(
            stage="process",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="timeout",
            observed=timeout,
            fix_hint="increase-the-operation-specific-timeout",
        ) from error
    except OSError as error:
        raise failure(
            stage="process",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="unavailable",
            observed=type(error).__name__,
            fix_hint="install-the-pinned-runtime-or-fix-its-path",
        ) from error
    event(
        boundary="process",
        duration_ms=round((time.monotonic() - started) * 1000),
        model_version=model_version,
        operation=operation,
        return_code=result.returncode,
        service=service,
        stderr_bytes=len(result.stderr),
        stderr_sha256=digest(result.stderr),
        stdout_bytes=len(result.stdout),
        stdout_sha256=digest(result.stdout),
    )
    if result.returncode not in expected_codes:
        raise failure(
            stage="process",
            service=service,
            model_version=model_version,
            operation=operation,
            shape=f"exit-{result.returncode}",
            observed=result.stdout + b"\x00" + result.stderr,
            fix_hint="inspect-payload-safe-runtime-logs-and-locked-provenance",
        )
    return result


def validate_artifact_lock(
    settings: Settings, locked: Mapping[str, Any]
) -> dict[str, Any]:
    model = "bqemu-load-e2e-artifacts/v1"
    require_exact_keys(
        locked,
        {"schemaVersion", "fakeGCS", "pythonClient", "bqCLI"},
        "artifact-lock",
        model,
    )
    fake = cast(Mapping[str, Any], locked["fakeGCS"])
    python = cast(Mapping[str, Any], locked["pythonClient"])
    bq = cast(Mapping[str, Any], locked["bqCLI"])
    require_exact_keys(
        fake, {"image", "version", "revision", "source"}, "fake-gcs-lock", model
    )
    require_exact_keys(
        python,
        {
            "distribution",
            "version",
            "requirementsLock",
            "requirementsLockSha256",
            "contractProfile",
            "contractProfileSha256",
            "source",
        },
        "python-client-lock",
        model,
    )
    require_exact_keys(
        bq,
        {
            "version",
            "cloudSDKVersion",
            "componentManifestSha256",
            "versionFileSha256",
            "entrypointSha256",
            "contractProfile",
            "contractProfileSha256",
            "source",
        },
        "bq-cli-lock",
        model,
    )

    _validate_repository_file(
        str(python["requirementsLock"]),
        str(python["requirementsLockSha256"]),
        "python-requirements-lock",
        model,
    )
    _validate_repository_file(
        str(python["contractProfile"]),
        str(python["contractProfileSha256"]),
        "python-contract-profile",
        model,
    )
    _validate_repository_file(
        str(bq["contractProfile"]),
        str(bq["contractProfileSha256"]),
        "bq-contract-profile",
        model,
    )

    try:
        installed_python = importlib.metadata.version(str(python["distribution"]))
    except importlib.metadata.PackageNotFoundError as error:
        raise failure(
            stage="provenance",
            service="google-cloud-bigquery-python",
            model_version=str(python["version"]),
            operation="inspect-distribution",
            shape="missing-distribution",
            observed=str(python["distribution"]),
            fix_hint="sync-tests-python-requirements-lock-with-hashes",
        ) from error
    if installed_python != python["version"]:
        raise failure(
            stage="provenance",
            service="google-cloud-bigquery-python",
            model_version=str(python["version"]),
            operation="inspect-distribution",
            shape="version-drift",
            observed=installed_python,
            fix_hint="sync-tests-python-requirements-lock-with-hashes",
        )

    bq_path = shutil.which(settings.bq_binary)
    if bq_path is None:
        raise failure(
            stage="provenance",
            service="bq-cli",
            model_version=str(bq["version"]),
            operation="find-entrypoint",
            shape="missing-binary",
            observed=settings.bq_binary,
            fix_hint="install-google-cloud-sdk-566.0.0-with-bq",
        )
    resolved_bq = Path(bq_path).resolve()
    sdk_root = Path(
        os.getenv("BQEMU_LOAD_GCLOUD_SDK_ROOT", str(resolved_bq.parent.parent))
    ).resolve()
    sdk_version = _read_small_text(sdk_root / "VERSION", "cloud-sdk-version", model)
    if sdk_version != bq["cloudSDKVersion"]:
        raise failure(
            stage="provenance",
            service="bq-cli",
            model_version=str(bq["version"]),
            operation="validate-cloud-sdk",
            shape="version-drift",
            observed=sdk_version,
            fix_hint="install-google-cloud-sdk-566.0.0-with-bq",
        )
    _validate_file_digest(
        sdk_root / ".install/bq.manifest",
        str(bq["componentManifestSha256"]),
        "bq-component-manifest",
        model,
    )
    _validate_file_digest(
        sdk_root / "platform/bq/VERSION",
        str(bq["versionFileSha256"]),
        "bq-version-file",
        model,
    )
    _validate_file_digest(
        sdk_root / "platform/bq/bq.py",
        str(bq["entrypointSha256"]),
        "bq-entrypoint",
        model,
    )
    version_result = run_process(
        [bq_path, "version"],
        operation="bq-version",
        service="bq-cli",
        model_version=str(bq["version"]),
        timeout=settings.artifact_timeout,
    )
    expected_version_line = f"This is BigQuery CLI {bq['version']}".encode()
    if version_result.stdout.strip() != expected_version_line:
        raise failure(
            stage="provenance",
            service="bq-cli",
            model_version=str(bq["version"]),
            operation="bq-version",
            shape="version-output-drift",
            observed=version_result.stdout,
            fix_hint="install-google-cloud-sdk-566.0.0-with-bq",
        )

    return {
        "artifactLockSha256": "sha256:" + raw_file_digest(settings.artifact_lock),
        "pythonClientVersion": installed_python,
        "bqCLIVersion": str(bq["version"]),
        "cloudSDKVersion": sdk_version,
        "bqBinary": bq_path,
        "fakeGCSImage": str(fake["image"]),
        "fakeGCSVersion": str(fake["version"]),
        "fakeGCSRevision": str(fake["revision"]),
    }


def _validate_repository_file(
    relative: str, expected_sha256: str, operation: str, model_version: str
) -> None:
    candidate = (ROOT / relative).resolve()
    try:
        candidate.relative_to(ROOT)
    except ValueError as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=model_version,
            operation=operation,
            shape="path-outside-repository",
            observed=relative,
            fix_hint="use-a-repository-relative-lock-path",
        ) from error
    _validate_file_digest(candidate, expected_sha256, operation, model_version)


def _validate_file_digest(
    path: Path, expected_sha256: str, operation: str, model_version: str
) -> None:
    try:
        actual = raw_file_digest(path)
    except OSError as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=model_version,
            operation=operation,
            shape="missing-artifact",
            observed=type(error).__name__,
            fix_hint="restore-the-checksum-locked-artifact",
        ) from error
    if actual != expected_sha256:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=model_version,
            operation=operation,
            shape="sha256-drift",
            observed=actual,
            fix_hint="review-upstream-contract-before-updating-the-lock",
        )


def _read_small_text(path: Path, operation: str, model_version: str) -> str:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=model_version,
            operation=operation,
            shape="missing-artifact",
            observed=type(error).__name__,
            fix_hint="restore-the-checksum-locked-artifact",
        ) from error
    if len(payload) > 4096:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=model_version,
            operation=operation,
            shape="oversize-version-file",
            observed=len(payload),
            fix_hint="inspect-the-installed-artifact",
        )
    return payload.decode("utf-8", errors="strict").strip()


def ensure_fake_gcs_image(
    settings: Settings, provenance: Mapping[str, Any]
) -> dict[str, Any]:
    image = str(provenance["fakeGCSImage"])
    version = str(provenance["fakeGCSVersion"])
    revision = str(provenance["fakeGCSRevision"])
    inspect = run_process(
        [settings.docker_binary, "image", "inspect", image],
        operation="inspect-fake-gcs-image",
        service="fake-gcs-server",
        model_version=version,
        timeout=settings.artifact_timeout,
        expected_codes=(0, 1),
    )
    if inspect.returncode != 0:
        run_process(
            [settings.docker_binary, "pull", image],
            operation="pull-fake-gcs-image",
            service="fake-gcs-server",
            model_version=version,
            timeout=settings.artifact_timeout,
        )
        inspect = run_process(
            [settings.docker_binary, "image", "inspect", image],
            operation="inspect-pulled-fake-gcs-image",
            service="fake-gcs-server",
            model_version=version,
            timeout=settings.artifact_timeout,
        )
    try:
        decoded = json.loads(inspect.stdout)
        record = decoded[0]
        labels = record["Config"]["Labels"]
        repo_digests = record["RepoDigests"]
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
        raise failure(
            stage="provenance",
            service="fake-gcs-server",
            model_version=version,
            operation="inspect-image",
            shape="docker-inspect-json",
            observed=inspect.stdout,
            fix_hint="restore-the-pinned-image-or-update-the-lock",
        ) from error
    if image not in repo_digests:
        raise failure(
            stage="provenance",
            service="fake-gcs-server",
            model_version=version,
            operation="inspect-image",
            shape="repo-digest-drift",
            observed=repo_digests,
            fix_hint="pull-the-image-by-locked-digest",
        )
    expected_labels = {
        "org.opencontainers.image.version": version,
        "org.opencontainers.image.revision": revision,
    }
    if not isinstance(labels, dict) or any(
        labels.get(key) != value for key, value in expected_labels.items()
    ):
        raise failure(
            stage="provenance",
            service="fake-gcs-server",
            model_version=version,
            operation="inspect-image",
            shape="oci-label-drift",
            observed=labels,
            fix_hint="review-the-upstream-image-before-updating-the-lock",
        )
    return {
        "image": image,
        "imageId": str(record["Id"]),
        "version": version,
        "revision": revision,
    }


@dataclass(frozen=True)
class AuditEntry:
    sequence: int
    operation: str
    status: int
    response_bytes: int
    response_sha256: str
    request_sha256: str

    def evidence(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "operation": self.operation,
            "status": self.status,
            "responseBytes": self.response_bytes,
            "responseSha256": self.response_sha256,
            "requestSha256": self.request_sha256,
        }


class _AuditHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        upstream: tuple[str, int],
        timeout: float,
        max_response_bytes: int,
    ) -> None:
        super().__init__(address, _AuditHandler)
        self.upstream = upstream
        self.upstream_timeout = timeout
        self.max_response_bytes = max_response_bytes
        self.entries: list[AuditEntry] = []
        self.entries_lock = threading.Lock()

    def record(
        self, operation: str, status: int, body: bytes, request_target: str
    ) -> None:
        with self.entries_lock:
            entry = AuditEntry(
                sequence=len(self.entries) + 1,
                operation=operation,
                status=status,
                response_bytes=len(body),
                response_sha256=digest(body),
                request_sha256=digest(request_target),
            )
            self.entries.append(entry)
        event(
            boundary="gcs-json",
            model_version="fake-gcs-server-1.55.1",
            operation=operation,
            request_sha256=entry.request_sha256,
            response_bytes=entry.response_bytes,
            response_sha256=entry.response_sha256,
            sequence=entry.sequence,
            service="storage",
            status=status,
        )


class _AuditHandler(BaseHTTPRequestHandler):
    server: _AuditHTTPServer

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        server = cast(_AuditHTTPServer, self.server)
        operation = _gcs_operation(self.path)
        connection = http.client.HTTPConnection(
            server.upstream[0], server.upstream[1], timeout=server.upstream_timeout
        )
        try:
            connection.request("GET", self.path, headers={"Accept": "application/json"})
            response = connection.getresponse()
            payload = response.read(server.max_response_bytes + 1)
            if len(payload) > server.max_response_bytes:
                server.record(operation, 502, b"", self.path)
                self.send_error(502)
                return
            server.record(operation, response.status, payload, self.path)
            self.send_response(response.status)
            for name in ("Content-Type", "ETag", "Last-Modified", "x-goog-generation"):
                value = response.getheader(name)
                if value is not None:
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (OSError, http.client.HTTPException):
            server.record(operation, 502, b"", self.path)
            self.send_error(502)
        finally:
            connection.close()

    def log_message(self, format: str, *args: Any) -> None:
        return


def _gcs_operation(request_target: str) -> str:
    parsed = urllib.parse.urlsplit(request_target)
    if re.fullmatch(r"/storage/v1/b/[^/]+/o", parsed.path):
        return "objects.list"
    if re.fullmatch(r"/storage/v1/b/[^/]+/o/.+", parsed.path):
        query = urllib.parse.parse_qs(parsed.query)
        if query.get("alt") == ["media"]:
            return "objects.get.media"
        return "objects.get.metadata"
    return "unexpected"


class AuditProxy:
    def __init__(
        self,
        upstream_endpoint: str,
        timeout: float,
        max_response_bytes: int,
    ) -> None:
        parsed = urllib.parse.urlsplit(upstream_endpoint)
        if parsed.scheme != "http" or parsed.hostname is None or parsed.port is None:
            raise ValueError("audit proxy upstream must be an absolute HTTP URL")
        self._server = _AuditHTTPServer(
            ("127.0.0.1", 0),
            (parsed.hostname, parsed.port),
            timeout,
            max_response_bytes,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="bqemu-load-gcs-audit",
            daemon=True,
        )

    @property
    def endpoint(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    @property
    def entries(self) -> list[AuditEntry]:
        with self._server.entries_lock:
            return list(self._server.entries)

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: float) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            raise failure(
                stage="shutdown",
                service="gcs-audit-proxy",
                model_version="bqemu-load-e2e/v1",
                operation="join",
                shape="thread-timeout",
                observed=timeout,
                fix_hint="inspect-in-flight-object-store-requests",
            )


class FakeGCSRuntime:
    def __init__(
        self,
        settings: Settings,
        image: Mapping[str, Any],
        seed_root: Path,
        log_path: Path,
    ) -> None:
        self.settings = settings
        self.image = image
        self.seed_root = seed_root
        self.log_path = log_path
        self.container_id: str | None = None
        self.endpoint = ""

    def start(self) -> str:
        name = f"bqemu-load-e2e-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        result = run_process(
            [
                self.settings.docker_binary,
                "run",
                "--detach",
                "--rm",
                "--name",
                name,
                "--read-only",
                "--security-opt",
                "no-new-privileges:true",
                "--cap-drop",
                "ALL",
                "--tmpfs",
                "/storage:mode=0700",
                "--tmpfs",
                "/tmp:mode=0700",
                "--publish",
                "127.0.0.1::4443/tcp",
                "--volume",
                f"{self.seed_root}:/data:ro",
                str(self.image["image"]),
                "-scheme",
                "http",
                "-port",
                "4443",
                "-data",
                "/data",
                "-log-level",
                "info",
            ],
            operation="start-container",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.fake_gcs_start_timeout,
        )
        container_id = result.stdout.decode("ascii", errors="strict").strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise failure(
                stage="runtime",
                service="fake-gcs-server",
                model_version=str(self.image["version"]),
                operation="start-container",
                shape="container-id",
                observed=result.stdout,
                fix_hint="inspect-docker-runtime-output",
            )
        self.container_id = container_id
        port = run_process(
            [self.settings.docker_binary, "port", container_id, "4443/tcp"],
            operation="discover-random-port",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.fake_gcs_start_timeout,
        )
        match = re.fullmatch(
            rb"(?:127\.0\.0\.1|0\.0\.0\.0):(\d+)\s*", port.stdout
        )
        if match is None:
            raise failure(
                stage="runtime",
                service="fake-gcs-server",
                model_version=str(self.image["version"]),
                operation="discover-random-port",
                shape="docker-port-output",
                observed=port.stdout,
                fix_hint="inspect-docker-port-publication",
            )
        self.endpoint = f"http://127.0.0.1:{int(match.group(1))}"
        deadline = time.monotonic() + self.settings.fake_gcs_start_timeout
        last_shape = "not-ready"
        while time.monotonic() < deadline:
            try:
                request_json(
                    self.endpoint,
                    "GET",
                    "/storage/v1/b",
                    operation="fake-gcs-readiness",
                    service="storage",
                    model_version=str(self.image["version"]),
                    timeout=min(self.settings.http_timeout, max(0.1, deadline - time.monotonic())),
                )
                return self.endpoint
            except ContractError as error:
                last_shape = error.fields["shape"]
                time.sleep(0.05)
        raise failure(
            stage="readiness",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            operation="storage-buckets-list",
            shape=last_shape,
            observed=self._safe_log_fingerprint(),
            fix_hint="inspect-payload-safe-fake-gcs-log",
        )

    def stop(self) -> None:
        if self.container_id is None:
            return
        logs = run_process(
            [self.settings.docker_binary, "logs", "--tail", "2000", self.container_id],
            operation="capture-logs",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.shutdown_timeout,
            expected_codes=(0, 1),
        )
        self.log_path.write_bytes(logs.stdout + logs.stderr)
        os.chmod(self.log_path, 0o600)
        run_process(
            [self.settings.docker_binary, "rm", "--force", self.container_id],
            operation="remove-container",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.shutdown_timeout,
            expected_codes=(0, 1),
        )
        self.container_id = None
        event(
            boundary="diagnostic",
            model_version=str(self.image["version"]),
            operation="fake-gcs-log",
            service="fake-gcs-server",
            **file_metadata(self.log_path),
        )

    def _safe_log_fingerprint(self) -> dict[str, Any]:
        if self.container_id is None:
            return {"container": "absent"}
        result = run_process(
            [self.settings.docker_binary, "logs", "--tail", "200", self.container_id],
            operation="readiness-diagnostics",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.shutdown_timeout,
            expected_codes=(0, 1),
        )
        return {
            "stdoutBytes": len(result.stdout),
            "stdoutSha256": digest(result.stdout),
            "stderrBytes": len(result.stderr),
            "stderrSha256": digest(result.stderr),
        }


class _ReservedPort:
    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("127.0.0.1", 0))
        self.port = int(self.socket.getsockname()[1])

    def release(self) -> None:
        self.socket.close()


class EmulatorRuntime:
    def __init__(
        self,
        settings: Settings,
        binary: Path,
        work: Path,
        gcs_endpoint: str,
        log_path: Path,
    ) -> None:
        self.settings = settings
        self.binary = binary
        self.work = work
        self.gcs_endpoint = gcs_endpoint
        self.log_path = log_path
        self.process: subprocess.Popen[bytes] | None = None
        self.log: BinaryIO | None = None
        self.endpoint = ""

    def start(self) -> str:
        http_port, grpc_port = _ReservedPort(), _ReservedPort()
        self.endpoint = f"http://127.0.0.1:{http_port.port}"
        temporary = self.work / "tmp"
        home = self.work / "home"
        temporary.mkdir(mode=0o700)
        home.mkdir(mode=0o700)
        environment = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(home),
            "TMPDIR": str(temporary),
            "BQEMU_PROJECT": self.settings.project,
            "BQEMU_LOCATION": self.settings.location,
            "BQEMU_HTTP_ADDRESS": f"127.0.0.1:{http_port.port}",
            "BQEMU_GRPC_ADDRESS": f"127.0.0.1:{grpc_port.port}",
            "BQEMU_PUBLIC_URL": self.endpoint,
            "BQEMU_DATABASE_DSN": str(self.work / "load-e2e.duckdb"),
            "BQEMU_TEMP_DIRECTORY": str(temporary),
            "BQEMU_LOAD_ENABLED": "true",
            "BQEMU_LOAD_GCS_ENDPOINT": self.gcs_endpoint,
            "BQEMU_LOAD_ALLOW_FILE_SOURCES": "false",
        }
        self.log = self.log_path.open("wb")
        http_port.release()
        grpc_port.release()
        try:
            self.process = subprocess.Popen(
                [str(self.binary), "--config", str(self.settings.config)],
                cwd=ROOT,
                env=environment,
                stdout=self.log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as error:
            self.log.close()
            self.log = None
            raise failure(
                stage="runtime",
                service="bqemu",
                model_version="current-source",
                operation="start-emulator",
                shape="process-unavailable",
                observed=type(error).__name__,
                fix_hint="inspect-current-source-build-and-runtime-libraries",
            ) from error
        deadline = time.monotonic() + self.settings.emulator_start_timeout
        last_shape = "not-ready"
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                last_shape = f"exit-{self.process.returncode}"
                break
            try:
                request_json(
                    self.endpoint,
                    "GET",
                    "/readyz",
                    operation="emulator-readiness",
                    service="bigquery",
                    model_version="current-source",
                    timeout=min(self.settings.http_timeout, max(0.1, deadline - time.monotonic())),
                )
                return self.endpoint
            except ContractError as error:
                last_shape = error.fields["shape"]
                time.sleep(0.05)
        self.stop()
        raise failure(
            stage="readiness",
            service="bqemu",
            model_version="current-source",
            operation="http-readyz",
            shape=last_shape,
            observed=file_metadata(self.log_path),
            fix_hint="inspect-payload-safe-emulator-log",
        )

    def stop(self) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=self.settings.shutdown_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                try:
                    process.wait(timeout=self.settings.shutdown_timeout)
                except subprocess.TimeoutExpired as error:
                    raise failure(
                        stage="shutdown",
                        service="bqemu",
                        model_version="current-source",
                        operation="wait-process",
                        shape="kill-timeout",
                        observed=self.settings.shutdown_timeout,
                        fix_hint="inspect-runtime-shutdown-lifecycle",
                    ) from error
        self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None
        if self.log_path.exists():
            os.chmod(self.log_path, 0o600)
            event(
                boundary="diagnostic",
                model_version="current-source",
                operation="emulator-log",
                service="bqemu",
                **file_metadata(self.log_path),
            )


def request_json(
    endpoint: str,
    method: str,
    path: str,
    *,
    operation: str,
    service: str,
    model_version: str,
    timeout: float,
    payload: Mapping[str, Any] | None = None,
) -> Any:
    encoded = (
        None
        if payload is None
        else json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    )
    request = urllib.request.Request(endpoint + path, data=encoded, method=method)
    if encoded is not None:
        request.add_header("Content-Type", "application/json")
    started = time.monotonic()
    status = 0
    response_payload = b""
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            response_payload = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as error:
        status = error.code
        response_payload = error.read(MAX_HTTP_RESPONSE_BYTES + 1)
        event(
            boundary="http",
            duration_ms=round((time.monotonic() - started) * 1000),
            method=method,
            model_version=model_version,
            operation=operation,
            request_bytes=0 if encoded is None else len(encoded),
            request_sha256=digest(b"" if encoded is None else encoded),
            response_bytes=len(response_payload),
            response_sha256=digest(response_payload),
            service=service,
            status=status,
        )
        raise failure(
            stage="http",
            service=service,
            model_version=model_version,
            operation=operation,
            shape=f"status-{status}",
            observed=response_payload,
            fix_hint="compare-the-pinned-wire-contract",
        ) from error
    except (OSError, urllib.error.URLError) as error:
        raise failure(
            stage="http",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="transport-error",
            observed=type(error).__name__,
            fix_hint="inspect-runtime-readiness-and-timeout",
        ) from error
    event(
        boundary="http",
        duration_ms=round((time.monotonic() - started) * 1000),
        method=method,
        model_version=model_version,
        operation=operation,
        request_bytes=0 if encoded is None else len(encoded),
        request_sha256=digest(b"" if encoded is None else encoded),
        response_bytes=len(response_payload),
        response_sha256=digest(response_payload),
        service=service,
        status=status,
    )
    if len(response_payload) > MAX_HTTP_RESPONSE_BYTES:
        raise failure(
            stage="http",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="oversize-response",
            observed=len(response_payload),
            fix_hint="inspect-response-pagination-and-limits",
        )
    if not response_payload:
        return None
    try:
        return json.loads(response_payload, object_pairs_hook=_reject_duplicate_keys)
    except (ValueError, json.JSONDecodeError) as error:
        raise failure(
            stage="http",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="invalid-json",
            observed=response_payload,
            fix_hint="compare-the-pinned-wire-contract",
        ) from error


def source_fingerprint() -> dict[str, Any]:
    paths = [ROOT / "go.mod", ROOT / "go.sum"]
    for directory in (ROOT / "cmd", ROOT / "internal"):
        paths.extend(directory.rglob("*.go"))
    paths = sorted({path.resolve() for path in paths})
    hasher = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(ROOT).as_posix().encode()
        hasher.update(len(relative).to_bytes(4, "big"))
        hasher.update(relative)
        payload = path.read_bytes()
        hasher.update(len(payload).to_bytes(8, "big"))
        hasher.update(payload)
    return {"fileCount": len(paths), "sha256": "sha256:" + hasher.hexdigest()}


def write_evidence(path: Path, evidence: Mapping[str, Any]) -> dict[str, Any]:
    encoded = (
        json.dumps(evidence, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("ascii")
    path.write_bytes(encoded)
    os.chmod(path, 0o600)
    metadata = file_metadata(path)
    event(
        boundary="evidence",
        model_version="bqemu-load-e2e/v1",
        operation="write-evidence",
        service="load-e2e",
        **metadata,
    )
    return metadata
