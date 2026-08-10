from __future__ import annotations

import copy
import sys
import unittest


ROOT = __import__("pathlib").Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests" / "integration" / "framework"))

from fetch_flink_artifacts import load_lock  # noqa: E402


class FlinkArtifactFetchTest(unittest.TestCase):
    def test_reviewed_lock_loads(self) -> None:
        lock = load_lock(ROOT / "tests" / "integration" / "flink" / "artifacts.lock.json")
        self.assertEqual(lock["connectorVersion"], "1.2.0")
        self.assertEqual(
            lock["runtime"]["image"],
            "flink@sha256:d50dd931a53add0125d35e6cc47d13c15fa6bbb65050b975b95d4d89c2a82581",
        )

    def test_unreviewed_lock_fails_before_network(self) -> None:
        lock = load_lock(ROOT / "tests" / "integration" / "flink" / "artifacts.lock.json")
        broken = copy.deepcopy(lock)
        broken["artifact"]["url"] = "https://example.test/connector.jar"
        import json, tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lock.json"
            path.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_lock(path)


if __name__ == "__main__":
    unittest.main()
