#!/usr/bin/env python3
"""Fetch exact Spark test artifacts without cloning or building upstream.

Artifact coordinates come from Maven Central and are checked byte-for-byte
against the reviewed lock file before an atomic rename.

Official artifact:
https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.44.2/
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import urllib.request


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LOCKS = (REPOSITORY_ROOT / "tests" / "integration" / "spark" / "artifacts.lock.json",)
DEFAULT_OUTPUT = REPOSITORY_ROOT / ".artifacts" / "spark"


def _positive_seconds(name: str, default: str) -> float:
    raw = os.getenv(name, default)
    try:
        value = float(raw)
    except ValueError as error:
        raise SystemExit(f"{name} must be a positive number of seconds") from error
    if value <= 0:
        raise SystemExit(f"{name} must be a positive number of seconds")
    return value


def _event(
    *,
    version: str,
    stage: str,
    shape: str,
    fingerprint: str,
    status: str,
    fix_hint: str,
) -> None:
    # URL query strings and response bodies are deliberately absent. These
    # fields are safe to retain in CI drift reports.
    print(
        " ".join(
            (
                f"version=spark-bigquery-connector-{version}",
                "operation=fetch-spark-artifact",
                f"stage={stage}",
                f"shape={shape}",
                f"fingerprint={fingerprint}",
                f"status={status}",
                f"fix_hint={fix_hint}",
            )
        ),
        flush=True,
    )


def _digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _validate_existing(path: Path, expected_hash: str, expected_size: int) -> bool:
    if not path.is_file():
        return False
    actual_hash, actual_size = _digest(path)
    return actual_hash == expected_hash and actual_size == expected_size


def _fetch(
    artifact: dict[str, object], output: Path, timeout: float, version: str
) -> Path:
    target = output / str(artifact["output"])
    expected_hash = str(artifact["sha256"])
    expected_size = int(artifact["size"])
    if _validate_existing(target, expected_hash, expected_size):
        _event(
            version=version,
            stage="cache-check",
            shape=str(artifact.get("kind", "artifact")),
            fingerprint=f"sha256:{expected_hash}",
            status="hit",
            fix_hint="none",
        )
        return target

    output.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(str(artifact["url"]), headers={"User-Agent": "bqemu-contract-fetch/1"})
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=target.name + ".", dir=output, delete=False) as stream:
            temporary = Path(stream.name)
            digest = hashlib.sha256()
            size = 0
            with urllib.request.urlopen(request, timeout=timeout) as response:
                while chunk := response.read(1024 * 1024):
                    size += len(chunk)
                    if size > expected_size:
                        raise ValueError("artifact exceeds locked byte size")
                    digest.update(chunk)
                    stream.write(chunk)
            actual_hash = digest.hexdigest()
        if size != expected_size or actual_hash != expected_hash:
            raise ValueError(
                f"artifact mismatch size={size} fingerprint=sha256:{actual_hash}"
            )
        temporary.replace(target)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

    _event(
        version=version,
        stage="checksum",
        shape=str(artifact.get("kind", "artifact")),
        fingerprint=f"sha256:{expected_hash}",
        status="verified",
        fix_hint="none",
    )
    return target


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--lock",
        action="append",
        type=Path,
        help="Exact lock to fetch; repeat for multiple locks (defaults to the supported connector lock).",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    arguments = parser.parse_args()
    timeout = _positive_seconds("BQEMU_ARTIFACT_TIMEOUT_SECONDS", "120")

    targets: list[Path] = []
    for lock_path in arguments.lock or DEFAULT_LOCKS:
        with lock_path.open("r", encoding="utf-8") as stream:
            lock = json.load(stream)
        connector_version = lock.get("connectorVersion")
        source_commit = lock.get("sourceCommit")
        common_binding_valid = (
            isinstance(connector_version, str)
            and bool(connector_version)
            and isinstance(source_commit, str)
            and re.fullmatch(r"[0-9a-f]{40}", source_commit) is not None
        )
        dsv1_binding_valid = (
            lock.get("schemaVersion") == "1"
            and isinstance(lock.get("sparkVersion"), str)
            and bool(lock.get("sparkVersion"))
            and isinstance(lock.get("scalaBinaryVersion"), str)
            and bool(lock.get("scalaBinaryVersion"))
            and "artifactVariant" not in lock
            and "artifactBuild" not in lock
            and "testRuntime" not in lock
            and "executionClasspathPolicy" not in lock
        )
        if not common_binding_valid or not dsv1_binding_valid:
            raise SystemExit("unreviewed Spark artifact lock version")
        targets.extend(
            _fetch(artifact, arguments.output, timeout, connector_version)
            for artifact in lock["artifacts"]
        )
        source = lock.get("artifactBuild", {}).get("source")
        if source is not None:
            targets.append(_fetch(source, arguments.output, timeout, connector_version))
    for target in targets:
        print(target)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        _event(
            version="lock-validation",
            stage="download",
            shape=type(error).__name__,
            fingerprint="sha256:none",
            status="failed",
            fix_hint="verify-network-or-refresh-reviewed-lock",
        )
        raise
