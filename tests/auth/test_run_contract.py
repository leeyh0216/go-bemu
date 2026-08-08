from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from zipfile import ZipFile

import run_contract as auth
from pyspark_connector import connector_version


class AuthConsumerManifestTests(unittest.TestCase):
    def test_connector_version_reads_exact_jar_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "connector.jar"
            with ZipFile(path, "w") as archive:
                archive.writestr(
                    "spark-bigquery-connector.properties",
                    "scala.binary.version=2.12\nconnector.version=99.1.7\n",
                )
            self.assertEqual(connector_version(path), "99.1.7")

            with ZipFile(path, "w") as archive:
                archive.writestr(
                    "spark-bigquery-connector.properties",
                    "connector.version=one\nconnector.version=two\n",
                )
            with self.assertRaises(RuntimeError):
                connector_version(path)

    def test_required_manifest_cases_cover_typed_auth_adapters(self) -> None:
        cases = auth.load_auth_cases("all")

        self.assertEqual(len(cases), 4)
        self.assertEqual(
            {case.consumer for case in cases},
            {"python", "bq", "pyspark", "scala-spark"},
        )
        self.assertEqual(len({case.case_id for case in cases}), len(cases))
        for case in cases:
            if case.consumer in {"pyspark", "scala-spark"}:
                artifact = auth.require_case_artifact(case, "spark-runtime")
                self.assertEqual(artifact.role, "execution")

    def test_exact_case_uses_versions_from_normalized_json(self) -> None:
        source = json.loads(auth.CONSUMER_MANIFEST.read_text(encoding="utf-8"))
        base = next(
            case
            for case in source["cases"]
            if case["runnerAdapter"]["id"] == "spark-pyspark-pytest-v1"
        )
        patched = json.loads(json.dumps(base))
        patched["id"] = "connector-patch"
        patched["runtimeProfile"]["versions"]["connector"] = "next-patch"

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "consumers.normalized.json"
            manifest.write_text(
                json.dumps({"schemaVersion": "1", "cases": [patched]}),
                encoding="utf-8",
            )
            selected = auth.load_auth_cases("connector-patch", manifest)

        self.assertEqual(selected[0].consumer, "pyspark")
        self.assertEqual(selected[0].versions["connector"], "next-patch")

    def test_unknown_or_non_required_case_is_rejected(self) -> None:
        with self.assertRaises(auth.ContractError):
            auth.load_auth_cases("missing")

    def test_runtime_artifact_is_force_installed_and_dependency_checked(self) -> None:
        case = next(
            case
            for case in auth.load_auth_cases("all")
            if case.consumer == "scala-spark"
        )
        with (
            mock.patch.object(
                auth,
                "materialize_case_artifact",
                return_value=Path("/tmp/runtime.tar.gz"),
            ),
            mock.patch.object(auth, "run_process", return_value=b"") as run,
        ):
            auth.install_case_python_artifact(
                case,
                Path("/tmp/python"),
                "spark-runtime",
                "install-spark-runtime-artifact",
            )

        self.assertEqual(run.call_count, 2)
        self.assertIn("--force-reinstall", run.call_args_list[0].args[0])
        self.assertIn("--no-deps", run.call_args_list[0].args[0])
        self.assertIn("check", run.call_args_list[1].args[0])

    def test_spark_runtime_preparation_installs_bridge_then_runtime(self) -> None:
        case = next(
            case
            for case in auth.load_auth_cases("all")
            if case.consumer == "pyspark"
        )
        with (
            mock.patch.object(auth, "verify_python_runtime") as verify,
            mock.patch.object(auth, "install_case_python_artifact") as install,
        ):
            auth.prepare_case_python_runtime(case, Path("/tmp/python"))

        verify.assert_called_once_with(Path("/tmp/python"), case.versions["python"])
        self.assertEqual(install.call_count, 2)
        self.assertEqual(install.call_args_list[0].args[2], "spark-python-bridge")
        self.assertFalse(install.call_args_list[0].kwargs["check_dependencies"])
        self.assertEqual(install.call_args_list[1].args[2], "spark-runtime")

    def test_bq_runtime_verifies_cloud_sdk_and_component_versions(self) -> None:
        case = next(
            case for case in auth.load_auth_cases("all") if case.consumer == "bq"
        )
        cloud_versions = {
            "Google Cloud SDK": case.versions["cloudSdk"],
            "bq": case.versions["bq"],
        }
        with mock.patch.object(
            auth,
            "run_process",
            side_effect=[
                f"This is BigQuery CLI {case.versions['bq']}\n".encode(),
                json.dumps(cloud_versions).encode(),
            ],
        ):
            auth.verify_bq_runtime(case, "bq", "gcloud")

        cloud_versions["Google Cloud SDK"] = "drifted"
        with (
            mock.patch.object(
                auth,
                "run_process",
                side_effect=[
                    f"This is BigQuery CLI {case.versions['bq']}\n".encode(),
                    json.dumps(cloud_versions).encode(),
                ],
            ),
            self.assertRaises(auth.ContractError),
        ):
            auth.verify_bq_runtime(case, "bq", "gcloud")


if __name__ == "__main__":
    unittest.main()
