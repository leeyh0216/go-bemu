#!/usr/bin/env python3
"""Official google-cloud-bigquery 3.43.0 credential contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.auth import load_credentials_from_file
from google.cloud import bigquery
from google.oauth2.credentials import Credentials as AccessTokenCredentials


EXPECTED_VERSION = "3.43.0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--fixture-dir", type=Path, required=True)
    return parser.parse_args()


def assert_dataset(
    endpoint: str,
    project: str,
    dataset: str,
    credentials: object,
) -> None:
    client = bigquery.Client(
        project=project,
        credentials=credentials,
        client_options={"api_endpoint": endpoint},
    )
    try:
        observed = [item.dataset_id for item in client.list_datasets(max_results=10)]
    finally:
        client.close()
    if dataset not in observed:
        raise RuntimeError("dataset list did not contain the contract dataset")


def main() -> int:
    arguments = parse_args()
    if bigquery.__version__ != EXPECTED_VERSION:
        raise RuntimeError(
            f"google-cloud-bigquery version={bigquery.__version__}, want {EXPECTED_VERSION}"
        )

    completed: list[str] = []
    for filename in ("service-account.json", "authorized-user.json", "wif.json"):
        credentials, _ = load_credentials_from_file(
            str(arguments.fixture_dir / filename)
        )
        assert_dataset(
            arguments.endpoint,
            arguments.project,
            arguments.dataset,
            credentials,
        )
        completed.append(filename)

    token = (arguments.fixture_dir / "access-token.txt").read_text(
        encoding="utf-8"
    ).strip()
    if not token:
        raise RuntimeError("access-token.txt is empty")
    assert_dataset(
        arguments.endpoint,
        arguments.project,
        arguments.dataset,
        AccessTokenCredentials(token=token),
    )
    completed.append("access-token.txt")
    print(
        json.dumps(
            {
                "client": "google-cloud-bigquery",
                "version": EXPECTED_VERSION,
                "profiles": completed,
                "status": "passed",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
