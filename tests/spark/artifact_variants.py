"""Fail-closed Spark connector artifact selection for real-process tests.

The fetch cache may contain both DSv1 and DSv2 artifacts, but one Spark JVM
must receive exactly one connector variant. The released JARs register the
same short name and carry overlapping resources, so a mixed classpath is not a
supported fallback.

Official artifacts and DSv2 provider source:
https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.44.2/
https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-3.5-bigquery/0.44.2/
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-dsv2/spark-3.5-bigquery-lib/src/main/java/com/google/cloud/spark/bigquery/v2/Spark35BigQueryTableProvider.java
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from zipfile import BadZipFile, ZipFile


CONNECTOR_VERSION = "0.44.2"
DSV1_VARIANT = "dsv1-with-dependencies-2.12"
DSV2_RAW_VARIANT = "dsv2-spark-3.5-raw"
DSV2_PROVIDER = "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider"
SERVICE_ENTRY = "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister"
VERSION_ENTRY = "spark-bigquery-connector.properties"


@dataclass(frozen=True)
class ArtifactSpec:
    variant: str
    output: str
    size: int
    sha256: str
    provider: str


@dataclass(frozen=True)
class SelectedArtifact:
    path: Path
    spec: ArtifactSpec


class ArtifactClasspathError(RuntimeError):
    """Payload-safe connector classpath drift diagnostic."""

    def __init__(self, *, stage: str, shape: str, fingerprint: str, fix_hint: str):
        self.version = CONNECTOR_VERSION
        self.operation = "spark-connector-classpath"
        self.stage = stage
        self.shape = shape
        self.fingerprint = fingerprint
        self.fix_hint = fix_hint
        super().__init__(
            " ".join(
                (
                    f"version={self.version}",
                    f"operation={self.operation}",
                    f"stage={self.stage}",
                    f"shape={self.shape}",
                    f"fingerprint={self.fingerprint}",
                    f"fix_hint={self.fix_hint}",
                )
            )
        )


def _safe_fingerprint(*parts: str) -> str:
    encoded = "\0".join(parts).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
    except OSError as error:
        raise ArtifactClasspathError(
            stage="artifact-read",
            shape=type(error).__name__,
            fingerprint=_safe_fingerprint("artifact-read", type(error).__name__),
            fix_hint="fetch-the-reviewed-variant-lock",
        ) from None
    return digest.hexdigest(), size


def load_artifact_specs(repository_root: Path) -> dict[str, ArtifactSpec]:
    locks = (
        (repository_root / "tests" / "spark" / "artifacts.lock.json", DSV1_VARIANT),
        (
            repository_root / "tests" / "spark" / "artifacts-dsv2.lock.json",
            DSV2_RAW_VARIANT,
        ),
    )
    specs: dict[str, ArtifactSpec] = {}
    for lock_path, default_variant in locks:
        with lock_path.open("r", encoding="utf-8") as stream:
            lock = json.load(stream)
        if lock.get("connectorVersion") != CONNECTOR_VERSION:
            raise ArtifactClasspathError(
                stage="lock-version",
                shape="connector-version-drift",
                fingerprint=_safe_fingerprint("lock-version", str(lock.get("connectorVersion"))),
                fix_hint="review-and-refresh-the-variant-lock",
            )
        artifacts = lock.get("artifacts", [])
        if len(artifacts) != 1:
            raise ArtifactClasspathError(
                stage="lock-shape",
                shape=f"connector-artifacts:{len(artifacts)}",
                fingerprint=_safe_fingerprint("lock-shape", str(len(artifacts))),
                fix_hint="keep-one-connector-artifact-per-variant-lock",
            )
        artifact = artifacts[0]
        variant = str(lock.get("artifactVariant") or default_variant)
        provider = str(
            artifact.get("providerClass")
            or "com.google.cloud.spark.bigquery.Scala212BigQueryRelationProvider"
        )
        specs[variant] = ArtifactSpec(
            variant=variant,
            output=str(artifact["output"]),
            size=int(artifact["size"]),
            sha256=str(artifact["sha256"]),
            provider=provider,
        )
    return specs


def enforce_connector_classpath(
    paths: list[Path], *, expected_variant: str, repository_root: Path
) -> SelectedArtifact:
    """Return the sole exact connector or fail without exposing local paths."""

    specs = load_artifact_specs(repository_root)
    if expected_variant not in specs:
        raise ArtifactClasspathError(
            stage="variant-selection",
            shape="unknown-expected-variant",
            fingerprint=_safe_fingerprint("variant-selection", expected_variant),
            fix_hint="select-a-reviewed-variant-id",
        )
    if len(paths) == 0:
        raise ArtifactClasspathError(
            stage="connector-count",
            shape="connector-count:0",
            fingerprint=_safe_fingerprint("connector-count", "0"),
            fix_hint="select-exactly-one-reviewed-connector-jar",
        )

    observations: list[tuple[Path, str, int, ArtifactSpec | None]] = []
    for path in paths:
        digest, size = _digest(path)
        matched = next(
            (
                spec
                for spec in specs.values()
                if digest == spec.sha256 and size == spec.size
            ),
            None,
        )
        observations.append((path, digest, size, matched))

    if len(observations) != 1:
        variants = {item[3].variant for item in observations if item[3] is not None}
        shape = (
            f"mixed-variants:{len(variants)}"
            if len(variants) > 1
            else f"connector-count:{len(observations)}"
        )
        digest_shape = ",".join(sorted(item[1] for item in observations))
        raise ArtifactClasspathError(
            stage="connector-count",
            shape=shape,
            fingerprint=_safe_fingerprint("connector-count", digest_shape),
            fix_hint="launch-each-connector-variant-in-an-isolated-spark-process",
        )

    path, digest, size, matched = observations[0]
    expected = specs[expected_variant]
    if matched is None or digest != expected.sha256 or size != expected.size:
        raise ArtifactClasspathError(
            stage="artifact-hash",
            shape=f"maven-jar:size:{size}",
            fingerprint="sha256:" + digest,
            fix_hint="delete-cache-and-fetch-the-reviewed-variant-lock",
        )
    if matched.variant != expected_variant:
        raise ArtifactClasspathError(
            stage="variant-selection",
            shape="recognized-but-wrong-variant",
            fingerprint="sha256:" + digest,
            fix_hint="launch-the-requested-variant-in-an-isolated-spark-process",
        )

    _validate_jar_identity(path, matched)
    return SelectedArtifact(path=path, spec=matched)


def _validate_jar_identity(path: Path, spec: ArtifactSpec) -> None:
    try:
        with ZipFile(path) as archive:
            providers = tuple(
                line.strip()
                for line in archive.read(SERVICE_ENTRY).decode("utf-8").splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            )
            properties = archive.read(VERSION_ENTRY).decode("utf-8").splitlines()
    except (BadZipFile, KeyError, UnicodeDecodeError):
        raise ArtifactClasspathError(
            stage="jar-identity",
            shape="missing-or-invalid-provider-metadata",
            fingerprint="sha256:" + spec.sha256,
            fix_hint="fetch-the-reviewed-maven-jar",
        ) from None
    versions = {
        key.strip(): value.strip()
        for line in properties
        if "=" in line
        for key, value in (line.split("=", 1),)
    }
    if providers != (spec.provider,) or versions.get("connector.version") != CONNECTOR_VERSION:
        raise ArtifactClasspathError(
            stage="jar-identity",
            shape=f"providers:{len(providers)},version-match:{versions.get('connector.version') == CONNECTOR_VERSION}",
            fingerprint="sha256:" + spec.sha256,
            fix_hint="review-provider-and-version-resource-drift",
        )
