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
DSV2_OVERLAY_VARIANT = "dsv2-spark-3.5-streaming-visibility-overlay"
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


@dataclass(frozen=True)
class OverlayArtifactSpec:
    variant: str
    output: str
    size: int
    sha256: str
    class_entry: str
    class_size: int
    class_sha256: str


@dataclass(frozen=True)
class SelectedOverlay:
    path: Path
    spec: OverlayArtifactSpec


@dataclass(frozen=True)
class OverlayClasspathPair:
    base: SelectedArtifact
    overlay: SelectedOverlay
    runtime_classpath: tuple[Path, Path]


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


class OverlayClasspathError(RuntimeError):
    """Payload-safe one-class overlay identity diagnostic."""

    def __init__(self, *, stage: str, shape: str, fingerprint: str, fix_hint: str):
        self.version = CONNECTOR_VERSION
        self.operation = "spark-overlay-classpath"
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


def load_overlay_spec(repository_root: Path) -> OverlayArtifactSpec:
    lock_path = (
        repository_root / "tests" / "spark" / "artifacts-dsv2-overlay.lock.json"
    )
    try:
        with lock_path.open("r", encoding="utf-8") as stream:
            lock = json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        raise OverlayClasspathError(
            stage="lock-read",
            shape=type(error).__name__,
            fingerprint=_safe_fingerprint("overlay-lock-read", type(error).__name__),
            fix_hint="restore-the-reviewed-overlay-artifact-lock",
        ) from None
    expected_keys = {
        "schemaVersion",
        "artifactVariant",
        "connectorVersion",
        "sourceCommit",
        "buildContract",
        "builderSources",
        "baseArtifact",
        "overlayArtifact",
        "testRuntime",
        "writePolicy",
        "executionClasspathPolicy",
    }
    if set(lock) != expected_keys or (
        lock.get("schemaVersion") != "1"
        or lock.get("artifactVariant") != DSV2_OVERLAY_VARIANT
        or lock.get("connectorVersion") != CONNECTOR_VERSION
        or lock.get("sourceCommit")
        != "719817782a214b8ca72be520870013a3e0253d92"
        or lock.get("executionClasspathPolicy")
        != "exactly-one-base-connector-plus-one-class-overlay"
        or lock.get("writePolicy")
        != "direct-exact-pre-existing-table-append-only"
    ):
        raise OverlayClasspathError(
            stage="lock-binding",
            shape="version-or-policy-drift",
            fingerprint=_safe_fingerprint("overlay-lock-binding"),
            fix_hint="review-overlay-version-and-classpath-policy",
        )

    raw = load_artifact_specs(repository_root)[DSV2_RAW_VARIANT]
    base = lock.get("baseArtifact")
    overlay = lock.get("overlayArtifact")
    runtime = lock.get("testRuntime")
    if not isinstance(base, dict) or not isinstance(overlay, dict) or not isinstance(runtime, dict):
        raise OverlayClasspathError(
            stage="lock-shape",
            shape="artifact-or-runtime-not-object",
            fingerprint=_safe_fingerprint("overlay-lock-shape"),
            fix_hint="restore-the-reviewed-overlay-artifact-lock",
        )
    if (
        set(base) != {"kind", "output", "size", "sha256"}
        or base.get("kind") != "maven-jar"
        or base.get("output") != raw.output
        or base.get("size") != raw.size
        or base.get("sha256") != raw.sha256
        or set(overlay)
        != {
            "kind",
            "output",
            "size",
            "sha256",
            "classEntry",
            "classSize",
            "classSha256",
        }
        or overlay.get("kind") != "one-class-overlay-jar"
        or runtime
        != {
            "sparkVersion": "3.5.8",
            "scalaBinaryVersion": "2.12",
            "scalaVersion": "2.12.18",
            "javaVersion": "17",
        }
    ):
        raise OverlayClasspathError(
            stage="lock-binding",
            shape="base-overlay-runtime-drift",
            fingerprint=_safe_fingerprint("overlay-base-runtime"),
            fix_hint="review-overlay-against-the-exact-raw-runtime",
        )
    _validate_repository_bindings(repository_root, lock)
    return OverlayArtifactSpec(
        variant=DSV2_OVERLAY_VARIANT,
        output=str(overlay["output"]),
        size=int(overlay["size"]),
        sha256=str(overlay["sha256"]),
        class_entry=str(overlay["classEntry"]),
        class_size=int(overlay["classSize"]),
        class_sha256=str(overlay["classSha256"]),
    )


def _validate_repository_bindings(repository_root: Path, lock: dict[str, object]) -> None:
    build_contract = lock.get("buildContract")
    builder_sources = lock.get("builderSources")
    if not isinstance(build_contract, dict) or not isinstance(builder_sources, list):
        raise OverlayClasspathError(
            stage="repository-binding",
            shape="invalid-binding-container",
            fingerprint=_safe_fingerprint("overlay-repository-binding-container"),
            fix_hint="restore-reviewed-builder-source-bindings",
        )
    bindings = [build_contract, *builder_sources]
    for binding in bindings:
        if not isinstance(binding, dict) or set(binding) != {"ref", "sha256"}:
            raise OverlayClasspathError(
                stage="repository-binding",
                shape="invalid-binding",
                fingerprint=_safe_fingerprint("overlay-repository-binding"),
                fix_hint="restore-reviewed-builder-source-bindings",
            )
        relative = Path(str(binding["ref"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise OverlayClasspathError(
                stage="repository-binding",
                shape="path-escape",
                fingerprint=_safe_fingerprint("overlay-repository-path"),
                fix_hint="use-repository-relative-builder-paths",
            )
        actual, _ = _overlay_digest(repository_root / relative)
        if actual != binding["sha256"]:
            raise OverlayClasspathError(
                stage="repository-binding",
                shape="builder-source-drift",
                fingerprint="sha256:" + actual,
                fix_hint="rebuild-and-review-overlay-source-changes-together",
            )


def _overlay_digest(path: Path) -> tuple[str, int]:
    try:
        return _digest(path)
    except ArtifactClasspathError as error:
        raise OverlayClasspathError(
            stage="overlay-read",
            shape=error.shape,
            fingerprint=error.fingerprint,
            fix_hint="restore-the-reviewed-overlay-artifact-or-source",
        ) from None


def enforce_overlay_classpath(
    paths: list[Path], *, repository_root: Path
) -> SelectedOverlay:
    """Return the sole locked one-class overlay or fail before Spark starts."""

    spec = load_overlay_spec(repository_root)
    if len(paths) != 1:
        digests: list[str] = []
        for path in paths:
            digest, _ = _overlay_digest(path)
            digests.append(digest)
        connector_hashes = {
            item.sha256 for item in load_artifact_specs(repository_root).values()
        }
        has_overlay = spec.sha256 in digests
        has_connector = any(digest in connector_hashes for digest in digests)
        shape = (
            f"mixed-base-and-overlay:{len(paths)}"
            if has_overlay and has_connector
            else f"overlay-count:{len(paths)}"
        )
        raise OverlayClasspathError(
            stage="overlay-count",
            shape=shape,
            fingerprint=_safe_fingerprint("overlay-count", *sorted(digests)),
            fix_hint="select-exactly-one-reviewed-overlay-jar",
        )
    path = paths[0]
    actual_hash, actual_size = _overlay_digest(path)
    if actual_hash != spec.sha256 or actual_size != spec.size:
        raise OverlayClasspathError(
            stage="overlay-hash",
            shape=f"overlay-jar:size:{actual_size}",
            fingerprint="sha256:" + actual_hash,
            fix_hint="rebuild-the-reviewed-one-class-overlay",
        )
    try:
        with ZipFile(path) as archive:
            entries = archive.namelist()
            class_bytes = archive.read(spec.class_entry) if entries == [spec.class_entry] else b""
    except (BadZipFile, KeyError, OSError):
        raise OverlayClasspathError(
            stage="overlay-shape",
            shape="invalid-jar",
            fingerprint="sha256:" + actual_hash,
            fix_hint="rebuild-the-reviewed-one-class-overlay",
        ) from None
    class_hash = hashlib.sha256(class_bytes).hexdigest()
    if (
        entries != [spec.class_entry]
        or len(class_bytes) != spec.class_size
        or class_hash != spec.class_sha256
    ):
        raise OverlayClasspathError(
            stage="overlay-shape",
            shape=f"entries:{len(entries)},class-bytes:{len(class_bytes)}",
            fingerprint="sha256:" + class_hash,
            fix_hint="keep-exactly-one-reviewed-class-entry",
        )
    return SelectedOverlay(path=path, spec=spec)


def enforce_overlay_pair(
    *,
    base_paths: list[Path],
    overlay_paths: list[Path],
    repository_root: Path,
) -> OverlayClasspathPair:
    """Select one raw base plus one overlay and return overlay-first JAR order.

    Both classes must share Spark's child-first user classloader: loading the
    overlay alone from a parent classloader cannot resolve connector classes
    that exist only in its child. The live JVM still verifies each CodeSource.
    """

    base = enforce_connector_classpath(
        base_paths,
        expected_variant=DSV2_RAW_VARIANT,
        repository_root=repository_root,
    )
    overlay = enforce_overlay_classpath(
        overlay_paths,
        repository_root=repository_root,
    )
    if base.path == overlay.path:
        raise OverlayClasspathError(
            stage="pair-identity",
            shape="base-overlay-path-alias",
            fingerprint=_safe_fingerprint("overlay-pair-path-alias"),
            fix_hint="select-distinct-reviewed-base-and-overlay-jars",
        )
    return OverlayClasspathPair(
        base=base,
        overlay=overlay,
        runtime_classpath=(overlay.path, base.path),
    )


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
