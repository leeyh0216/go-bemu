import json
import tempfile
import unittest
from pathlib import Path

import runtime


class RuntimeEventsTest(unittest.TestCase):
    def test_side_effect_event_contract_maps_load_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "emulator.log"
            path.write_text(
                "\n".join(
                    json.dumps(value)
                    for value in (
                        {
                            "time": "2026-08-09T20:00:00Z",
                            "event": "side_effect.before",
                            "operation": "load_parquet",
                            "tx_state": "before",
                        },
                        {
                            "time": "2026-08-09T20:00:01Z",
                            "event": "side_effect.error",
                            "operation": "load_parquet",
                            "tx_state": "rolled_back",
                        },
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            emulator = runtime.EmulatorRuntime.__new__(runtime.EmulatorRuntime)
            emulator.log_path = path
            events = emulator.runtime_events()

        self.assertEqual(
            [event.evidence() for event in events],
            [
                {
                    "timeUnixNanos": 1_786_305_600_000_000_000,
                    "actor": "bqemu",
                    "protocol": "internal",
                    "phase": "before",
                    "operation": "internal.load_parquet",
                    "status": "before",
                },
                {
                    "timeUnixNanos": 1_786_305_601_000_000_000,
                    "actor": "bqemu",
                    "protocol": "internal",
                    "phase": "after",
                    "operation": "internal.load_parquet",
                    "status": "rolled_back",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
