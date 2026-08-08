#!/usr/bin/env python3
"""PySpark 3.5.8 and Spark BigQuery connector 0.44.2 auth contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession


EXPECTED_SPARK_VERSION = "3.5.8"
EXPECTED_CONNECTOR_VERSION = "0.44.2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connector-jar", type=Path, required=True)
    parser.add_argument("--http-endpoint", required=True)
    parser.add_argument("--grpc-endpoint", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--fixture-dir", type=Path, required=True)
    return parser.parse_args()


def base_reader(spark: SparkSession, arguments: argparse.Namespace):
    reader = spark.read.format("bigquery")
    options = {
        "parentProject": arguments.project,
        "billingProject": arguments.project,
        "project": arguments.project,
        "bigQueryHttpEndpoint": arguments.http_endpoint,
        "bigQueryStorageGrpcEndpoint": arguments.grpc_endpoint,
        "createReadSessionTimeoutInSeconds": "30",
        "httpConnectTimeout": "30000",
        "httpReadTimeout": "30000",
        "httpMaxRetry": "0",
    }
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader


def main() -> int:
    arguments = parse_args()
    if pyspark.__version__ != EXPECTED_SPARK_VERSION:
        raise RuntimeError(
            f"PySpark version={pyspark.__version__}, want {EXPECTED_SPARK_VERSION}"
        )
    if EXPECTED_CONNECTOR_VERSION not in arguments.connector_jar.name:
        raise RuntimeError("connector JAR filename does not contain the locked version")

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("bqemu-auth-contract-pyspark")
        .config("spark.jars", str(arguments.connector_jar))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        if spark.version != EXPECTED_SPARK_VERSION:
            raise RuntimeError(
                f"Spark runtime={spark.version}, want {EXPECTED_SPARK_VERSION}"
            )
        completed: list[str] = []
        for filename in (
            "service-account.json",
            "authorized-user.json",
            "wif.json",
        ):
            count = (
                base_reader(spark, arguments)
                .option("credentialsFile", str(arguments.fixture_dir / filename))
                .load(arguments.table)
                .count()
            )
            if count != 1:
                raise RuntimeError(f"{filename} read row count={count}, want 1")
            completed.append(filename)

        token = (arguments.fixture_dir / "access-token.txt").read_text(
            encoding="utf-8"
        ).strip()
        count = (
            base_reader(spark, arguments)
            .option("gcpAccessToken", token)
            .load(arguments.table)
            .count()
        )
        if count != 1:
            raise RuntimeError(f"access-token.txt read row count={count}, want 1")
        completed.append("access-token.txt")
    finally:
        spark.stop()

    print(
        json.dumps(
            {
                "client": "pyspark",
                "spark_version": EXPECTED_SPARK_VERSION,
                "connector_version": EXPECTED_CONNECTOR_VERSION,
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
