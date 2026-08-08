from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests" / "spark"))

from artifact_variants import (  # noqa: E402
    ArtifactClasspathError,
    artifact_spec_from_json,
)


class SparkArtifactVariantsTest(unittest.TestCase):
    def test_normalized_connector_spec_decodes_without_version_inference(self) -> None:
        payload = {
            "variant": "dsv2-spark-runtime-raw",
            "output": "connector.jar",
            "size": 42,
            "sha256": "a" * 64,
            "provider": "example.Provider",
            "connectorVersion": "99.1.7",
        }

        spec = artifact_spec_from_json(json.dumps(payload))

        self.assertEqual(spec.connector_version, "99.1.7")
        self.assertEqual(spec.variant, "dsv2-spark-runtime-raw")

    def test_normalized_connector_spec_rejects_shape_drift(self) -> None:
        valid = {
            "variant": "dsv2-spark-runtime-raw",
            "output": "connector.jar",
            "size": 42,
            "sha256": "a" * 64,
            "provider": "example.Provider",
            "connectorVersion": "99.1.7",
        }
        mutations = {
            "unknown": lambda payload: payload.update(extra="value"),
            "digest": lambda payload: payload.update(sha256="bad"),
            "size": lambda payload: payload.update(size=True),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                payload = dict(valid)
                mutate(payload)
                with self.assertRaises(ArtifactClasspathError):
                    artifact_spec_from_json(json.dumps(payload))

        duplicate = json.dumps(valid)[:-1] + ',"size":43}'
        with self.assertRaises(ArtifactClasspathError):
            artifact_spec_from_json(duplicate)


if __name__ == "__main__":
    unittest.main()
