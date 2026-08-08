from __future__ import annotations

import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts.consumer_runtime import (
    ArtifactSpec,
    ConsumerRuntimeError,
    check_python_dependencies,
    install_python_artifact,
    load_normalized_execution,
    load_normalized_manifest,
    materialize_artifact,
    verify_python_minor,
)


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "tests" / "integration" / "contract" / "consumers.normalized.json"


class ConsumerRuntimeTest(unittest.TestCase):
    def test_current_manifest_decodes_to_four_typed_cases(self) -> None:
        manifest = load_normalized_manifest(MANIFEST)

        self.assertEqual(len(manifest.cases), 4)
        self.assertEqual(
            {
                execution.runner_adapter_id
                for case in manifest.cases
                for execution in case.executions
            },
            {
                "python-pytest-v1",
                "bq-cli-v1",
                "spark-pyspark-pytest-v1",
                "spark-scala-shell-v1",
                "python-indirect-load-v1",
                "bq-indirect-load-v1",
                "spark-pyspark-indirect-load-v1",
                "spark-scala-indirect-load-v1",
            },
        )
        case = manifest.cases[0]
        with self.assertRaises(TypeError):
            case.versions["mutated"] = "true"  # type: ignore[index]
        with self.assertRaises(TypeError):
            case.executions[0].bootstrap["mutated"] = "true"  # type: ignore[index]
        with self.assertRaises(TypeError):
            case.executions[0].scenarios[0]["id"] = "mutated"  # type: ignore[index]

        selected_case, execution = load_normalized_execution(
            MANIFEST, case.case_id, "public"
        )
        self.assertEqual(selected_case.case_id, case.case_id)
        self.assertEqual(execution.execution_id, "public")

    def test_decode_rejects_unknown_schema_adapter_usage_digest_and_field(self) -> None:
        source = json.loads(MANIFEST.read_text(encoding="utf-8"))
        mutations = {
            "schema": lambda payload: payload.update(schemaVersion="3"),
            "adapter": lambda payload: payload["cases"][0]["executions"][0][
                "runnerAdapter"
            ].update(id="unknown-adapter"),
            "usage": lambda payload: payload["cases"][0]["artifacts"][0].update(
                usage="guessed-artifact"
            ),
            "digest": lambda payload: payload["cases"][0]["artifacts"][0].update(
                sha256="bad"
            ),
            "uri": lambda payload: next(
                case for case in payload["cases"] if case["family"] == "python"
            )["artifacts"][0].update(uri="file:///tmp/client.whl"),
            "field": lambda payload: payload["cases"][0].update(unknown=True),
            "runtime version": lambda payload: payload["cases"][0][
                "runtimeProfile"
            ]["versions"].update(unexpected="1"),
            "role": lambda payload: payload["cases"][0]["artifacts"][0].update(
                role="execution"
            ),
            "adapter requirements": lambda payload: payload["cases"][0][
                "executions"
            ][0]["runnerAdapter"].update(requiredArtifactUsages=["spark-runtime"]),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                payload = copy.deepcopy(source)
                mutate(payload)
                path = Path(directory) / "consumers.normalized.json"
                path.write_text(json.dumps(payload), encoding="utf-8")
                with self.assertRaises(ConsumerRuntimeError):
                    load_normalized_manifest(path)

    def test_decode_rejects_known_artifact_outside_adapter_contract(self) -> None:
        payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
        python_case = next(case for case in payload["cases"] if case["family"] == "python")
        spark_case = next(case for case in payload["cases"] if case["family"] == "spark")
        extra = copy.deepcopy(
            next(
                artifact
                for artifact in spark_case["artifacts"]
                if artifact["usage"] == "spark-runtime"
            )
        )
        python_case["artifacts"].append(extra)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "consumers.normalized.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(ConsumerRuntimeError):
                load_normalized_manifest(path)

    def test_decode_rejects_duplicate_json_keys(self) -> None:
        source = MANIFEST.read_text(encoding="utf-8")
        duplicate = source.replace(
            '{\n  "schemaVersion": "2",',
            '{\n  "schemaVersion": "2",\n  "schemaVersion": "2",',
            1,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "consumers.normalized.json"
            path.write_text(duplicate, encoding="utf-8")
            with self.assertRaises(ConsumerRuntimeError):
                load_normalized_manifest(path)

    def test_decode_rejects_scala_binary_runtime_mismatch(self) -> None:
        payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
        spark_case = next(case for case in payload["cases"] if case["family"] == "spark")
        spark_case["runtimeProfile"]["versions"]["scalaBinary"] = "2.13"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "consumers.normalized.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(ConsumerRuntimeError):
                load_normalized_manifest(path)

    def test_decode_rejects_duplicate_required_artifact_usage(self) -> None:
        payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
        case = next(case for case in payload["cases"] if case["family"] == "spark")
        duplicate = copy.deepcopy(case["artifacts"][0])
        duplicate["id"] += "-duplicate"
        case["artifacts"].append(duplicate)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "consumers.normalized.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(ConsumerRuntimeError):
                load_normalized_manifest(path)

    def test_materializer_uses_declared_sha_and_rejects_drift(self) -> None:
        contents = b"case-declared-artifact"
        artifact = ArtifactSpec(
            artifact_id="artifact",
            role="execution",
            usage="python-wheel",
            uri="https://example.invalid/client.whl",
            sha256=hashlib.sha256(contents).hexdigest(),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with mock.patch(
                "scripts.consumer_runtime.urllib.request.urlopen",
                return_value=io.BytesIO(contents),
            ):
                path = materialize_artifact(root, artifact)
            self.assertEqual(path.read_bytes(), contents)

            drifted = ArtifactSpec(
                artifact_id="drifted",
                role="execution",
                usage="python-wheel",
                uri="https://example.invalid/drifted.whl",
                sha256="a" * 64,
            )
            with (
                mock.patch(
                    "scripts.consumer_runtime.urllib.request.urlopen",
                    return_value=io.BytesIO(contents),
                ),
                self.assertRaises(ConsumerRuntimeError),
            ):
                materialize_artifact(root, drifted)

    def test_exact_install_dependency_check_and_identity_are_shared_primitives(self) -> None:
        calls: list[tuple[list[str], str]] = []

        def checked(command, operation):
            calls.append((list(command), operation))

        install_python_artifact(
            Path("/runtime/python"),
            Path("/artifacts/client.whl"),
            "install-client",
            checked,
            uv_executable="/tools/uv",
        )
        check_python_dependencies(
            Path("/runtime/python"),
            "check-client",
            checked,
            uv_executable="/tools/uv",
        )
        verify_python_minor(
            Path("/runtime/python"),
            "3.13",
            "verify-python",
            lambda _command, _operation: b"3.13\n",
        )

        self.assertEqual(calls[0][0][0], "/tools/uv")
        self.assertIn("--force-reinstall", calls[0][0])
        self.assertIn("--no-deps", calls[0][0])
        self.assertEqual(calls[1][0][1:3], ["pip", "check"])
        with self.assertRaises(ConsumerRuntimeError):
            verify_python_minor(
                Path("/runtime/python"),
                "3.13",
                "verify-python",
                lambda _command, _operation: b"3.12\n",
            )


if __name__ == "__main__":
    unittest.main()
