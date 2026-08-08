#!/usr/bin/env python3
"""PySpark and Spark BigQuery connector credential contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from zipfile import BadZipFile, ZipFile


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connector-jar", type=Path, required=True)
    parser.add_argument("--http-endpoint", required=True)
    parser.add_argument("--grpc-endpoint", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--fixture-dir", type=Path, required=True)
    parser.add_argument("--expected-spark-version", required=True)
    parser.add_argument("--expected-connector-version", required=True)
    parser.add_argument("--expected-scala-version", required=True)
    parser.add_argument("--expected-scala-binary-version", required=True)
    parser.add_argument("--expected-java-version", required=True)
    return parser.parse_args()


def connector_version(path: Path) -> str:
    try:
        with ZipFile(path) as archive:
            lines = archive.read("spark-bigquery-connector.properties").decode(
                "utf-8"
            ).splitlines()
    except (OSError, BadZipFile, KeyError, UnicodeDecodeError):
        raise RuntimeError("connector JAR identity metadata is invalid") from None
    versions = [
        value.strip()
        for line in lines
        if "=" in line
        for key, value in (line.split("=", 1),)
        if key.strip() == "connector.version"
    ]
    if len(versions) != 1 or not versions[0]:
        raise RuntimeError("connector JAR version identity is ambiguous")
    return versions[0]


def base_reader(spark, arguments: argparse.Namespace):
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
    import pyspark
    from pyspark.sql import SparkSession

    arguments = parse_args()
    if pyspark.__version__ != arguments.expected_spark_version:
        raise RuntimeError(
            f"PySpark version={pyspark.__version__}, want {arguments.expected_spark_version}"
        )
    actual_connector_version = connector_version(arguments.connector_jar)
    if actual_connector_version != arguments.expected_connector_version:
        raise RuntimeError(
            "connector JAR version="
            f"{actual_connector_version}, want {arguments.expected_connector_version}"
        )
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
        if spark.version != arguments.expected_spark_version:
            raise RuntimeError(
                f"Spark runtime={spark.version}, want {arguments.expected_spark_version}"
            )
        scala_version = (
            spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
        )
        if (
            scala_version != arguments.expected_scala_version
            or not scala_version.startswith(arguments.expected_scala_binary_version + ".")
        ):
            raise RuntimeError(
                "Scala runtime="
                f"{scala_version}, want {arguments.expected_scala_version} "
                f"({arguments.expected_scala_binary_version}.x)"
            )
        java_version = spark.sparkContext._jvm.java.lang.System.getProperty(
            "java.specification.version"
        )
        if java_version != arguments.expected_java_version:
            raise RuntimeError(
                f"Java runtime={java_version}, want {arguments.expected_java_version}"
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
                "spark_version": arguments.expected_spark_version,
                "connector_version": actual_connector_version,
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
