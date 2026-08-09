"""Fail-closed build and classpath contracts for the DSv2 overlay.

The released connector is never rebuilt. The build consumes its exact Maven
JAR and emits one reviewed class whose streaming hooks delegate to the already
released batch commit/abort implementations.

Official contracts:
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/context/DataSourceWriterContext.java#L38-L50
https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/sql/connector/write/streaming/StreamingWrite.html
https://repo.maven.apache.org/maven2/org/javassist/javassist/3.30.2-GA/
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

import pytest

from artifact_variants import (
    ArtifactClasspathError,
    DSV2_OVERLAY_VARIANT,
    DSV2_RAW_VARIANT,
    OverlayClasspathError,
    enforce_overlay_classpath,
    enforce_overlay_pair,
)
from conftest import REPOSITORY_ROOT, _positive_timeout, record_capability


CAPABILITY = "SBQ-DSV2-OVERLAY-ARTIFACT-GUARD-V1"
BUILD_SCRIPT = REPOSITORY_ROOT / "tools" / "dsv2-overlay" / "build.py"
BUILD_LOCK = REPOSITORY_ROOT / "tools" / "dsv2-overlay" / "overlay.lock.json"
OUTPUT_SHA256 = "1e4d3705834745aa662442eb41f4aa99e6f7a1a89aa51b8aae1eb93c7c6c5bd3"


def _build_timeout() -> float:
    return _positive_timeout("BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS", "120")


def _invoke_builder(
    *, input_jar: Path, output: Path, lock: Path = BUILD_LOCK, timezone: str = "UTC"
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["TZ"] = timezone
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS"] = str(_build_timeout())
    return subprocess.run(
        [
            os.sys.executable,
            str(BUILD_SCRIPT),
            "--lock",
            str(lock),
            "--input",
            str(input_jar),
            "--output",
            str(output),
            "--timeout-seconds",
            str(_build_timeout()),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=False,
        timeout=_build_timeout(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


@pytest.mark.capability(CAPABILITY)
@pytest.mark.parametrize(
    ("case", "expected_shape"),
    (
        ("zero", "overlay-count:0"),
        ("two", "overlay-count:2"),
        ("mixed", "mixed-base-and-overlay:2"),
        ("hash", "overlay-jar:size:"),
    ),
)
def test_overlay_classpath_fails_closed(
    case: str,
    expected_shape: str,
    dsv2_connector_jar: Path,
    dsv2_overlay_jar: Path,
    tmp_path: Path,
) -> None:
    if case == "zero":
        classpath: list[Path] = []
    elif case == "two":
        classpath = [dsv2_overlay_jar, dsv2_overlay_jar]
    elif case == "mixed":
        classpath = [dsv2_overlay_jar, dsv2_connector_jar]
    else:
        drifted = tmp_path / "overlay.jar"
        drifted.write_bytes(b"checksum-drift")
        classpath = [drifted]

    with pytest.raises(OverlayClasspathError) as raised:
        enforce_overlay_classpath(classpath, repository_root=REPOSITORY_ROOT)
    diagnostic = str(raised.value)
    for fragment in (
        "version=0.44.2",
        "operation=spark-overlay-classpath",
        "fingerprint=sha256:",
        "fix_hint=",
        expected_shape,
    ):
        assert fragment in diagnostic
    assert re.search(r"fingerprint=sha256:[a-f0-9]{64}(?:\s|$)", diagnostic)
    for path in classpath:
        assert str(path) not in diagnostic
    record_capability(CAPABILITY, f"negative-classpath:{case}")


@pytest.mark.capability(CAPABILITY)
@pytest.mark.parametrize(
    ("case", "mutate", "expected_stage"),
    (
        (
            "version",
            lambda lock: lock.__setitem__("connectorVersion", "0.44.3"),
            "lock-version",
        ),
        (
            "class",
            lambda lock: lock["targetClass"].__setitem__("name", "drift.Class"),
            "target-class-binding",
        ),
        (
            "batch-descriptor",
            lambda lock: lock["targetClass"]["requiredMethods"][0].__setitem__(
                "descriptor", "()V"
            ),
            "method-descriptor",
        ),
        (
            "batch-bytecode",
            lambda lock: lock["targetClass"]["requiredMethods"][0].__setitem__(
                "codeSha256", "0" * 64
            ),
            "method-descriptor",
        ),
        (
            "hook-descriptor",
            lambda lock: lock["patch"]["methods"][0].__setitem__(
                "descriptor", "()V"
            ),
            "patched-method-descriptor",
        ),
        (
            "mode-guard",
            lambda lock: lock["patch"]["commitGuard"].__setitem__(
                "writeAtLeastOnce", True
            ),
            "commit-guard-lock",
        ),
    ),
)
def test_overlay_builder_rejects_lock_drift_before_javac(
    case: str,
    mutate: object,
    expected_stage: str,
    dsv2_connector_jar: Path,
    tmp_path: Path,
) -> None:
    lock = json.loads(BUILD_LOCK.read_text(encoding="utf-8"))
    mutate(lock)
    drifted_lock = tmp_path / f"{case}.json"
    drifted_lock.write_text(
        json.dumps(lock, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    result = _invoke_builder(
        input_jar=dsv2_connector_jar,
        output=tmp_path / "must-not-exist.jar",
        lock=drifted_lock,
    )
    assert result.returncode != 0
    assert f"stage={expected_stage}" in result.stdout
    assert "fingerprint=sha256:" in result.stdout
    assert str(drifted_lock) not in result.stdout
    assert str(dsv2_connector_jar) not in result.stdout
    assert not (tmp_path / "must-not-exist.jar").exists()
    record_capability(CAPABILITY, f"negative-lock:{case}")


def _commit_guard() -> dict[str, object]:
    lock = json.loads(BUILD_LOCK.read_text(encoding="utf-8"))
    guard = lock["patch"]["commitGuard"]
    assert guard == {
        "writeAtLeastOnce": False,
        "tableToWriteDeleteOnAbort": False,
        "failure": "java.lang.IllegalStateException",
    }
    commit = next(
        method for method in lock["patch"]["methods"] if method["delegate"] == "commit"
    )
    assert commit["codeBytes"] == 34
    assert commit["codeSha256"] == (
        "6174e9886129f75e6e0ecb894887605ab3ce4d430e2c9c76ae3eec79d615e8e1"
    )
    return guard


@pytest.mark.capability(CAPABILITY)
def test_overlay_commit_guard_accepts_exact_pre_existing_append_only() -> None:
    guard = _commit_guard()
    assert guard["writeAtLeastOnce"] is False
    assert guard["tableToWriteDeleteOnAbort"] is False
    record_capability(
        CAPABILITY,
        "mode-guard:direct-exact-pre-existing-table-append-only failure:fixed-type",
    )


@pytest.mark.capability(CAPABILITY)
@pytest.mark.parametrize(
    ("mode", "write_at_least_once", "delete_on_abort"),
    (
        ("at-least-once", True, True),
        ("new-table", False, True),
        ("overwrite", False, True),
    ),
)
def test_overlay_commit_guard_rejects_unsupported_static_shapes(
    mode: str,
    write_at_least_once: bool,
    delete_on_abort: bool,
) -> None:
    guard = _commit_guard()
    rejected = write_at_least_once or delete_on_abort
    assert rejected is True
    assert guard["failure"] == "java.lang.IllegalStateException"
    record_capability(CAPABILITY, f"mode-guard-rejected:{mode}")


@pytest.mark.capability(CAPABILITY)
def test_overlay_build_is_byte_identical_across_host_timezones(
    dsv2_connector_jar: Path,
    tmp_path: Path,
) -> None:
    outputs: list[Path] = []
    for timezone in ("UTC", "Asia/Seoul"):
        output = tmp_path / (timezone.replace("/", "-") + ".jar")
        result = _invoke_builder(
            input_jar=dsv2_connector_jar, output=output, timezone=timezone
        )
        assert result.returncode == 0
        assert "status=verified" in result.stdout
        enforce_overlay_classpath([output], repository_root=REPOSITORY_ROOT)
        outputs.append(output)

    payloads = [path.read_bytes() for path in outputs]
    assert payloads[0] == payloads[1]
    assert hashlib.sha256(payloads[0]).hexdigest() == OUTPUT_SHA256
    record_capability(CAPABILITY, "deterministic-build:timezone-independent")


@pytest.mark.capability(CAPABILITY)
def test_overlay_pair_is_exact_and_overlay_first(
    dsv2_connector_jar: Path,
    dsv2_overlay_jar: Path,
) -> None:
    pair = enforce_overlay_pair(
        base_paths=[dsv2_connector_jar],
        overlay_paths=[dsv2_overlay_jar],
        repository_root=REPOSITORY_ROOT,
    )
    assert pair.base.spec.variant == DSV2_RAW_VARIANT
    assert pair.overlay.spec.variant == DSV2_OVERLAY_VARIANT
    assert pair.runtime_classpath == (dsv2_overlay_jar, dsv2_connector_jar)
    record_capability(CAPABILITY, "pair:base-1,overlay-1 order:overlay-first")


@pytest.mark.capability(CAPABILITY)
@pytest.mark.parametrize(
    ("case", "expected_shape"),
    (
        ("base-zero", "connector-count:0"),
        ("base-two", "connector-count:2"),
        ("base-mixed", "mixed-variants:2"),
        ("base-hash", "maven-jar:size:"),
    ),
)
def test_overlay_pair_rejects_invalid_base_selection(
    case: str,
    expected_shape: str,
    connector_jar: Path,
    dsv2_connector_jar: Path,
    dsv2_overlay_jar: Path,
    tmp_path: Path,
) -> None:
    if case == "base-zero":
        base_paths: list[Path] = []
    elif case == "base-two":
        base_paths = [dsv2_connector_jar, dsv2_connector_jar]
    elif case == "base-mixed":
        base_paths = [connector_jar, dsv2_connector_jar]
    else:
        drifted = tmp_path / "base.jar"
        drifted.write_bytes(b"checksum-drift")
        base_paths = [drifted]

    with pytest.raises(ArtifactClasspathError) as raised:
        enforce_overlay_pair(
            base_paths=base_paths,
            overlay_paths=[dsv2_overlay_jar],
            repository_root=REPOSITORY_ROOT,
        )
    diagnostic = str(raised.value)
    assert expected_shape in diagnostic
    assert "fingerprint=sha256:" in diagnostic
    for path in base_paths:
        assert str(path) not in diagnostic
    record_capability(CAPABILITY, f"negative-pair:{case}")
