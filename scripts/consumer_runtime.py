"""Typed primitives shared by normalized consumer contract runners."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request


CASE_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}")
DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
LANES = frozenset({"required", "preview", "nightly"})
ARTIFACT_ROLES = frozenset({"execution", "tool-provenance"})
ARTIFACT_USAGES = frozenset(
    {
        "python-wheel",
        "cloud-sdk-release-provenance",
        "spark-connector-dsv1-jar",
        "spark-connector-dsv2-jar",
        "spark-python-bridge",
        "spark-runtime",
    }
)
ADAPTER_CONTRACTS = {
    "python-pytest-v1": ("python", "python-pytest", "pytest"),
    "bq-cli-v1": ("bq", "bq-cli", "bq"),
    "spark-pyspark-pytest-v1": ("spark", "spark-pyspark", "pytest"),
    "spark-scala-shell-v1": ("spark", "spark-scala", "pytest"),
}
ADAPTER_REQUIRED_VERSIONS = {
    "python-pytest-v1": ("python", "client"),
    "bq-cli-v1": ("cloudSdk", "bq"),
    "spark-pyspark-pytest-v1": (
        "spark",
        "connector",
        "scala",
        "scalaBinary",
        "java",
        "python",
    ),
    "spark-scala-shell-v1": (
        "spark",
        "connector",
        "scala",
        "scalaBinary",
        "java",
        "python",
    ),
}
ADAPTER_REQUIRED_ARTIFACT_USAGES = {
    "python-pytest-v1": ("python-wheel",),
    "bq-cli-v1": ("cloud-sdk-release-provenance",),
    "spark-pyspark-pytest-v1": (
        "spark-connector-dsv1-jar",
        "spark-connector-dsv2-jar",
        "spark-python-bridge",
        "spark-runtime",
    ),
    "spark-scala-shell-v1": (
        "spark-connector-dsv1-jar",
        "spark-python-bridge",
        "spark-runtime",
    ),
}
ADAPTER_BOOTSTRAP = {
    "python-pytest-v1": {},
    "bq-cli-v1": {},
    "spark-pyspark-pytest-v1": {},
    "spark-scala-shell-v1": {},
}
ADAPTER_SETUP_OPERATIONS = {
    "python-pytest-v1": (
        "bqemu.health.ready",
        "bqemu.projects.create",
        "bqemu.projects.delete",
    ),
    "bq-cli-v1": ("bqemu.health.ready", "bqemu.projects.create"),
    "spark-pyspark-pytest-v1": (
        "bqemu.health.ready",
        "bqemu.projects.create",
        "bigquery.datasets.insert",
    ),
    "spark-scala-shell-v1": (
        "bqemu.health.ready",
        "bqemu.projects.create",
        "bigquery.datasets.insert",
    ),
}


class ConsumerRuntimeError(RuntimeError):
    """A stable, payload-free consumer runtime failure."""


@dataclass(frozen=True)
class ArtifactSpec:
    artifact_id: str
    role: str
    usage: str
    uri: str
    sha256: str


@dataclass(frozen=True)
class NormalizedConsumerCase:
    case_id: str
    display_name: str
    family: str
    lane: str
    runtime_profile_id: str
    runtime_kind: str
    versions: Mapping[str, str]
    runner_adapter_id: str
    selector_prefix: str
    required_artifact_usages: tuple[str, ...]
    bootstrap: Mapping[str, str]
    setup_operation_ids: tuple[str, ...]
    compatibility_profile_id: str
    scenario_set_id: str
    scenarios: tuple[Mapping[str, Any], ...]
    artifacts: tuple[ArtifactSpec, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "versions", MappingProxyType(dict(self.versions)))
        object.__setattr__(self, "bootstrap", MappingProxyType(dict(self.bootstrap)))
        object.__setattr__(
            self,
            "scenarios",
            tuple(_freeze_json_value(scenario) for scenario in self.scenarios),
        )


@dataclass(frozen=True)
class NormalizedConsumerManifest:
    cases: tuple[NormalizedConsumerCase, ...]


def load_normalized_manifest(path: Path) -> NormalizedConsumerManifest:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (OSError, json.JSONDecodeError, ValueError) as error:
        raise ConsumerRuntimeError("normalized consumer manifest is unreadable") from error
    root = _exact_object(payload, {"schemaVersion", "cases"}, "manifest")
    if root["schemaVersion"] != "1" or not isinstance(root["cases"], list):
        raise ConsumerRuntimeError("normalized consumer manifest has an unsupported schema")

    cases = tuple(_decode_case(value) for value in root["cases"])
    case_ids = [case.case_id for case in cases]
    if len(case_ids) != len(set(case_ids)):
        raise ConsumerRuntimeError("normalized consumer manifest has duplicate case IDs")
    return NormalizedConsumerManifest(cases=cases)


def load_normalized_case(path: Path, case_id: str) -> NormalizedConsumerCase:
    matches = [
        case for case in load_normalized_manifest(path).cases if case.case_id == case_id
    ]
    if len(matches) != 1:
        raise ConsumerRuntimeError("normalized consumer case was not found exactly once")
    return matches[0]


def select_normalized_cases(
    path: Path,
    *,
    family: str | None = None,
    lane: str | None = None,
) -> tuple[NormalizedConsumerCase, ...]:
    cases = load_normalized_manifest(path).cases
    return tuple(
        case
        for case in cases
        if (family is None or case.family == family)
        and (lane is None or case.lane == lane)
    )


def require_artifact(case: NormalizedConsumerCase, usage: str) -> ArtifactSpec:
    if usage not in ARTIFACT_USAGES:
        raise ConsumerRuntimeError("consumer artifact usage is unknown")
    matches = [artifact for artifact in case.artifacts if artifact.usage == usage]
    if len(matches) != 1:
        raise ConsumerRuntimeError(
            "consumer case must provide exactly one artifact for the requested usage"
        )
    return matches[0]


def file_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1 << 20):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def materialize_artifact(
    repository_root: Path,
    artifact: ArtifactSpec,
    *,
    configured_path: str = "",
    timeout_seconds: float = 180,
    max_bytes: int = 512 << 20,
) -> Path:
    if artifact.role != "execution":
        raise ConsumerRuntimeError("only execution artifacts may be materialized")
    if configured_path:
        target = Path(configured_path).expanduser().absolute()
        try:
            matches = target.is_file() and file_digest(target)[0] == artifact.sha256
        except OSError as error:
            raise ConsumerRuntimeError("configured consumer artifact is unreadable") from error
        if not matches:
            raise ConsumerRuntimeError("configured consumer artifact digest does not match")
        return target.resolve()

    parsed = urllib.parse.urlparse(artifact.uri)
    filename = Path(parsed.path).name
    if parsed.scheme != "https" or not parsed.netloc or not filename:
        raise ConsumerRuntimeError("execution artifact must use an absolute HTTPS URI")
    target = (
        repository_root
        / ".artifacts"
        / "consumer-downloads"
        / artifact.sha256
        / filename
    )
    try:
        if target.is_file() and file_digest(target)[0] == artifact.sha256:
            return target.resolve()
    except OSError as error:
        raise ConsumerRuntimeError("cached consumer artifact is unreadable") from error

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        request = urllib.request.Request(
            artifact.uri,
            headers={"User-Agent": "bqemu-consumer-contract/1"},
        )
        with tempfile.NamedTemporaryFile(
            prefix=target.name + ".",
            dir=target.parent,
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            digest = hashlib.sha256()
            size = 0
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                while chunk := response.read(1 << 20):
                    size += len(chunk)
                    if size > max_bytes:
                        raise ConsumerRuntimeError("consumer artifact exceeds the size limit")
                    digest.update(chunk)
                    stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        if digest.hexdigest() != artifact.sha256:
            raise ConsumerRuntimeError("downloaded consumer artifact digest does not match")
        temporary.replace(target)
        temporary = None
    except ConsumerRuntimeError:
        raise
    except (OSError, urllib.error.URLError) as error:
        raise ConsumerRuntimeError("consumer artifact is unavailable") from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return target.resolve()


CheckedCommand = Callable[[Sequence[str], str], object]
CapturedCommand = Callable[[Sequence[str], str], bytes | str]


def install_python_artifact(
    python_executable: Path,
    artifact_path: Path,
    operation: str,
    run_checked: CheckedCommand,
    *,
    uv_executable: str = "uv",
) -> None:
    run_checked(
        [
            uv_executable,
            "pip",
            "install",
            "--python",
            str(python_executable),
            "--force-reinstall",
            "--no-deps",
            str(artifact_path),
        ],
        operation,
    )


def check_python_dependencies(
    python_executable: Path,
    operation: str,
    run_checked: CheckedCommand,
    *,
    uv_executable: str = "uv",
) -> None:
    run_checked(
        [uv_executable, "pip", "check", "--python", str(python_executable)],
        operation,
    )


def verify_python_minor(
    python_executable: Path,
    expected: str,
    operation: str,
    run_capture: CapturedCommand,
) -> None:
    output = run_capture(
        [
            str(python_executable),
            "-c",
            "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
        ],
        operation,
    )
    if isinstance(output, bytes):
        try:
            actual = output.decode("ascii").strip()
        except UnicodeDecodeError as error:
            raise ConsumerRuntimeError("Python runtime identity output is invalid") from error
    else:
        actual = output.strip()
    if actual != expected:
        raise ConsumerRuntimeError("Python runtime identity does not match the consumer case")


def _decode_case(value: Any) -> NormalizedConsumerCase:
    case = _exact_object(
        value,
        {
            "id",
            "displayName",
            "family",
            "lane",
            "runtimeProfile",
            "runnerAdapter",
            "compatibilityProfile",
            "scenarioSet",
            "artifacts",
        },
        "case",
    )
    case_id = _nonempty_string(case["id"], "case.id")
    if CASE_ID_PATTERN.fullmatch(case_id) is None:
        raise ConsumerRuntimeError("normalized consumer case ID is unsafe")
    display_name = _nonempty_string(case["displayName"], "case.displayName")
    family = _nonempty_string(case["family"], "case.family")
    lane = _nonempty_string(case["lane"], "case.lane")
    if lane not in LANES:
        raise ConsumerRuntimeError("normalized consumer case lane is unknown")

    runtime = _exact_object(
        case["runtimeProfile"], {"id", "family", "kind", "versions"}, "runtimeProfile"
    )
    runtime_id = _nonempty_string(runtime["id"], "runtimeProfile.id")
    runtime_family = _nonempty_string(runtime["family"], "runtimeProfile.family")
    runtime_kind = _nonempty_string(runtime["kind"], "runtimeProfile.kind")
    versions = _string_map(runtime["versions"], "runtimeProfile.versions")

    adapter = _exact_object(
        case["runnerAdapter"],
        {
            "id",
            "family",
            "runtimeKind",
            "selectorPrefix",
            "requiredVersions",
            "requiredArtifactUsages",
            "bootstrap",
            "setupOperationIds",
        },
        "runnerAdapter",
    )
    adapter_id = _nonempty_string(adapter["id"], "runnerAdapter.id")
    if adapter_id not in ADAPTER_CONTRACTS:
        raise ConsumerRuntimeError("normalized consumer runner adapter is unknown")
    adapter_family = _nonempty_string(adapter["family"], "runnerAdapter.family")
    adapter_kind = _nonempty_string(adapter["runtimeKind"], "runnerAdapter.runtimeKind")
    selector_prefix = _nonempty_string(
        adapter["selectorPrefix"], "runnerAdapter.selectorPrefix"
    )
    expected_family, expected_kind, expected_prefix = ADAPTER_CONTRACTS[adapter_id]
    if (
        family != runtime_family
        or family != adapter_family
        or family != expected_family
        or runtime_kind != adapter_kind
        or runtime_kind != expected_kind
        or selector_prefix != expected_prefix
    ):
        raise ConsumerRuntimeError("normalized consumer runtime and adapter do not match")
    required_versions = _string_list(
        adapter["requiredVersions"], "runnerAdapter.requiredVersions"
    )
    required_usages = _string_list(
        adapter["requiredArtifactUsages"], "runnerAdapter.requiredArtifactUsages"
    )
    if len(required_versions) != len(set(required_versions)) or len(required_usages) != len(
        set(required_usages)
    ):
        raise ConsumerRuntimeError("normalized consumer adapter requirements are duplicated")
    if any(usage not in ARTIFACT_USAGES for usage in required_usages):
        raise ConsumerRuntimeError("normalized consumer artifact usage is unknown")
    if (
        tuple(required_versions) != ADAPTER_REQUIRED_VERSIONS[adapter_id]
        or tuple(required_usages) != ADAPTER_REQUIRED_ARTIFACT_USAGES[adapter_id]
    ):
        raise ConsumerRuntimeError("normalized consumer adapter requirements drifted")
    if set(versions) != set(required_versions):
        raise ConsumerRuntimeError("normalized consumer runtime versions drifted")
    if family == "spark" and not versions["scala"].startswith(
        versions["scalaBinary"] + "."
    ):
        raise ConsumerRuntimeError("normalized Scala runtime and binary versions drifted")
    bootstrap = _string_map(adapter["bootstrap"], "runnerAdapter.bootstrap", allow_empty=True)
    setup_operation_ids = _string_list(
        adapter["setupOperationIds"], "runnerAdapter.setupOperationIds", allow_empty=True
    )
    if (
        bootstrap != ADAPTER_BOOTSTRAP[adapter_id]
        or tuple(setup_operation_ids) != ADAPTER_SETUP_OPERATIONS[adapter_id]
    ):
        raise ConsumerRuntimeError("normalized consumer adapter setup drifted")

    compatibility = _exact_object(
        case["compatibilityProfile"],
        {"id", "scenarioIds", "sourceProvenance"},
        "compatibilityProfile",
    )
    compatibility_id = _nonempty_string(
        compatibility["id"], "compatibilityProfile.id"
    )
    compatibility_scenarios = _string_list(
        compatibility["scenarioIds"], "compatibilityProfile.scenarioIds"
    )
    sources = compatibility["sourceProvenance"]
    if not isinstance(sources, list):
        raise ConsumerRuntimeError("normalized source provenance must be a list")
    for source in sources:
        source_value = _exact_object(
            source, {"name", "version", "uri"}, "sourceProvenance"
        )
        _nonempty_string(source_value["name"], "sourceProvenance.name")
        _nonempty_string(source_value["version"], "sourceProvenance.version")
        source_uri = _nonempty_string(source_value["uri"], "sourceProvenance.uri")
        if urllib.parse.urlparse(source_uri).scheme != "https":
            raise ConsumerRuntimeError("normalized source provenance must use HTTPS")

    scenario_set = _exact_object(case["scenarioSet"], {"id", "scenarios"}, "scenarioSet")
    scenario_set_id = _nonempty_string(scenario_set["id"], "scenarioSet.id")
    raw_scenarios = scenario_set["scenarios"]
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        raise ConsumerRuntimeError("normalized scenario set is empty")
    scenarios = tuple(
        _decode_scenario(scenario, selector_prefix) for scenario in raw_scenarios
    )
    scenario_ids = [scenario["id"] for scenario in scenarios]
    if len(scenario_ids) != len(set(scenario_ids)):
        raise ConsumerRuntimeError("normalized scenario set has duplicate scenario IDs")
    if not set(scenario_ids).issubset(set(compatibility_scenarios)):
        raise ConsumerRuntimeError("normalized scenario set is outside its compatibility profile")
    operation_owners: dict[str, str] = {}
    for scenario in scenarios:
        for operation_id in scenario["operationIds"]:
            if operation_id in operation_owners:
                raise ConsumerRuntimeError(
                    "normalized scenario set assigns one operation to multiple scenarios"
                )
            operation_owners[operation_id] = scenario["id"]

    raw_artifacts = case["artifacts"]
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise ConsumerRuntimeError("normalized consumer case has no artifacts")
    artifacts = tuple(_decode_artifact(artifact) for artifact in raw_artifacts)
    artifact_ids = [artifact.artifact_id for artifact in artifacts]
    if len(artifact_ids) != len(set(artifact_ids)):
        raise ConsumerRuntimeError("normalized consumer case has duplicate artifact IDs")
    if any(artifact.usage not in required_usages for artifact in artifacts):
        raise ConsumerRuntimeError(
            "normalized consumer case has an artifact outside its adapter contract"
        )
    for usage in required_usages:
        if sum(artifact.usage == usage for artifact in artifacts) != 1:
            raise ConsumerRuntimeError(
                "normalized consumer case does not satisfy required artifact cardinality"
            )

    return NormalizedConsumerCase(
        case_id=case_id,
        display_name=display_name,
        family=family,
        lane=lane,
        runtime_profile_id=runtime_id,
        runtime_kind=runtime_kind,
        versions=versions,
        runner_adapter_id=adapter_id,
        selector_prefix=selector_prefix,
        required_artifact_usages=tuple(required_usages),
        bootstrap=bootstrap,
        setup_operation_ids=tuple(setup_operation_ids),
        compatibility_profile_id=compatibility_id,
        scenario_set_id=scenario_set_id,
        scenarios=scenarios,
        artifacts=artifacts,
    )


def _decode_scenario(value: Any, selector_prefix: str) -> dict[str, Any]:
    scenario = _exact_object(
        value,
        {"id", "operationIds", "selectors", "operationExpectations"},
        "scenario",
    )
    scenario_id = _nonempty_string(scenario["id"], "scenario.id")
    operation_ids = _string_list(scenario["operationIds"], "scenario.operationIds")
    selectors = _string_list(scenario["selectors"], "scenario.selectors")
    if len(operation_ids) != len(set(operation_ids)) or len(selectors) != len(set(selectors)):
        raise ConsumerRuntimeError("normalized scenario contains duplicate values")
    if any(not selector.startswith(selector_prefix + ":") for selector in selectors):
        raise ConsumerRuntimeError("normalized scenario selector does not match its adapter")
    raw_expectations = scenario["operationExpectations"]
    if not isinstance(raw_expectations, list):
        raise ConsumerRuntimeError("normalized operation expectations must be a list")
    expectations: list[dict[str, Any]] = []
    for raw_expectation in raw_expectations:
        expectation = _exact_object(
            raw_expectation, {"operationId", "min", "max", "after"}, "operationExpectation"
        )
        operation_id = _nonempty_string(
            expectation["operationId"], "operationExpectation.operationId"
        )
        minimum = expectation["min"]
        maximum = expectation["max"]
        after = _string_list(
            expectation["after"], "operationExpectation.after", allow_empty=True
        )
        if (
            operation_id not in operation_ids
            or not isinstance(minimum, int)
            or isinstance(minimum, bool)
            or not isinstance(maximum, int)
            or isinstance(maximum, bool)
            or minimum < 0
            or maximum < 0
            or (maximum != 0 and maximum < minimum)
            or operation_id in after
            or any(dependency not in operation_ids for dependency in after)
        ):
            raise ConsumerRuntimeError("normalized operation expectation is invalid")
        expectations.append(
            {"operationId": operation_id, "min": minimum, "max": maximum, "after": after}
        )
    if [expectation["operationId"] for expectation in expectations] != operation_ids:
        raise ConsumerRuntimeError("normalized operation expectations are not fully expanded")
    _reject_dependency_cycles(scenario_id, expectations)
    return {
        "id": scenario_id,
        "operationIds": operation_ids,
        "selectors": selectors,
        "operationExpectations": expectations,
    }


def _decode_artifact(value: Any) -> ArtifactSpec:
    artifact = _exact_object(
        value, {"id", "role", "usage", "uri", "sha256"}, "artifact"
    )
    artifact_id = _nonempty_string(artifact["id"], "artifact.id")
    role = _nonempty_string(artifact["role"], "artifact.role")
    usage = _nonempty_string(artifact["usage"], "artifact.usage")
    uri = _nonempty_string(artifact["uri"], "artifact.uri")
    sha256 = _nonempty_string(artifact["sha256"], "artifact.sha256")
    if role not in ARTIFACT_ROLES:
        raise ConsumerRuntimeError("normalized consumer artifact role is unknown")
    if usage not in ARTIFACT_USAGES:
        raise ConsumerRuntimeError("normalized consumer artifact usage is unknown")
    if (usage == "cloud-sdk-release-provenance") != (role == "tool-provenance"):
        raise ConsumerRuntimeError("normalized consumer artifact role and usage do not match")
    if DIGEST_PATTERN.fullmatch(sha256) is None:
        raise ConsumerRuntimeError("normalized consumer artifact digest is invalid")
    parsed = urllib.parse.urlparse(uri)
    if role == "execution" and (parsed.scheme != "https" or not parsed.netloc):
        raise ConsumerRuntimeError("normalized execution artifact must use HTTPS")
    if role == "tool-provenance" and (
        parsed.scheme != "oci"
        or not parsed.netloc
        or not uri.endswith("@sha256:" + sha256)
    ):
        raise ConsumerRuntimeError(
            "normalized tool provenance must use a digest-pinned OCI URI"
        )
    return ArtifactSpec(artifact_id, role, usage, uri, sha256)


def _reject_dependency_cycles(
    scenario_id: str, expectations: list[dict[str, Any]]
) -> None:
    dependencies = {
        expectation["operationId"]: expectation["after"] for expectation in expectations
    }
    states: dict[str, int] = {}

    def visit(operation_id: str) -> None:
        if states.get(operation_id) == 1:
            raise ConsumerRuntimeError(
                "normalized scenario operation dependencies contain a cycle"
            )
        if states.get(operation_id) == 2:
            return
        states[operation_id] = 1
        for dependency in dependencies[operation_id]:
            visit(dependency)
        states[operation_id] = 2

    for operation_id in dependencies:
        visit(operation_id)


def _exact_object(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ConsumerRuntimeError(f"normalized {label} has unknown or missing fields")
    return value


def _reject_duplicate_json_keys(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in pairs:
        if key in decoded:
            raise ValueError("duplicate JSON key")
        decoded[key] = value
    return decoded


def _nonempty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConsumerRuntimeError(f"normalized {label} must be a non-empty string")
    return value


def _string_map(value: Any, label: str, *, allow_empty: bool = False) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or (not allow_empty and not value)
        or any(not isinstance(key, str) or not key for key in value)
        or any(not isinstance(item, str) or not item for item in value.values())
    ):
        raise ConsumerRuntimeError(f"normalized {label} must contain non-empty strings")
    return dict(value)


def _string_list(value: Any, label: str, *, allow_empty: bool = False) -> list[str]:
    if (
        not isinstance(value, list)
        or (not allow_empty and not value)
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise ConsumerRuntimeError(f"normalized {label} must contain non-empty strings")
    return list(value)


def _freeze_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_json_value(item) for key, item in value.items()}
        )
    if isinstance(value, list) or isinstance(value, tuple):
        return tuple(_freeze_json_value(item) for item in value)
    return value
