#!/usr/bin/env python3
"""Case-declared PySpark indirect Parquet writer for the load contract."""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import date

from pyspark.sql import SparkSession


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connector", required=True)
    parser.add_argument("--hadoop-gcs", required=True)
    parser.add_argument("--http-endpoint", required=True)
    parser.add_argument("--gcs-endpoint", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--destination", required=True)
    arguments = parser.parse_args()
    try:
        versions = json.loads(os.environ["BQEMU_RUNTIME_VERSIONS_JSON"])
    except (KeyError, json.JSONDecodeError) as error:
        raise RuntimeError("missing normalized runtime versions") from error

    python = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = python
    os.environ["PYSPARK_DRIVER_PYTHON"] = python
    spark = None
    stage = "bootstrap"
    try:
        spark = (
            SparkSession.builder.master("local[4]")
            .appName("bqemu-indirect-load-pyspark")
            .config("spark.jars", f"{arguments.connector},{arguments.hadoop_gcs}")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.pyspark.python", python)
            .config("spark.pyspark.driver.python", python)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("spark.hadoop.fs.gs.auth.service.account.enable", "false")
            .config("spark.hadoop.fs.gs.auth.null.enable", "true")
            .config("spark.hadoop.fs.gs.storage.root.url", arguments.gcs_endpoint)
            .config("spark.hadoop.fs.gs.storage.service.path", "storage/v1/")
            .config("spark.hadoop.fs.gs.copy.with.rewrite.enable", "false")
            .config("spark.hadoop.fs.gs.http.max.retry", "0")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        stage = "identity"
        if spark.version != versions["spark"]:
            raise RuntimeError("unexpected Spark version")
        scala_version = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
        if not str(scala_version).startswith(versions["scalaBinary"] + "."):
            raise RuntimeError("unexpected Scala binary version")
        frame = spark.createDataFrame(
            [
                (index, f"row-{index}", index % 2 == 0, date(2026, 1, 1))
                for index in range(8)
            ],
            ["id", "name", "active", "partition_date"],
        ).repartition(4)
        stage = "write"
        writer = frame.write.format("bigquery")
        options = {
            "parentProject": arguments.project,
            "billingProject": arguments.project,
            "project": arguments.project,
            "bigQueryHttpEndpoint": arguments.http_endpoint,
            "gcpAccessToken": "bqemu-load-contract-token",
            "temporaryGcsBucket": arguments.bucket,
            "writeMethod": "indirect",
            "intermediateFormat": "parquet",
            "spark.sql.sources.partitionOverwriteMode": "DYNAMIC",
            "httpConnectTimeout": "30000",
            "httpReadTimeout": "30000",
            "httpMaxRetry": "0",
        }
        for key, value in options.items():
            writer = writer.option(key, value)
        writer.mode("overwrite").save(arguments.destination)
        print(
            json.dumps(
                {
                    "connector": versions["connector"],
                    "entrypoint": "pyspark",
                    "mode": "dynamic-overwrite",
                    "partitions": 4,
                    "rows": 8,
                    "spark": spark.version,
                    "status": "passed",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    except Exception as error:  # noqa: BLE001 - report the complete child failure
        java_failures = []
        java_error = getattr(error, "java_exception", None)
        for _ in range(8):
            if java_error is None:
                break
            try:
                stack = java_error.getStackTrace()
                next_error = java_error.getCause()
                java_failures.append(
                    {
                        "error": str(java_error),
                        "stack": [str(frame) for frame in stack],
                    }
                )
            except Exception as bridge_error:  # noqa: BLE001 - diagnostics stay best-effort
                java_failures.append({"bridgeError": repr(bridge_error)})
                break
            java_error = next_error
        print(
            json.dumps(
                {
                    "entrypoint": "pyspark",
                    "error": repr(error),
                    "javaFailures": java_failures,
                    "stage": stage,
                    "status": "failed",
                    "traceback": traceback.format_exc(),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
