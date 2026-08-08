#!/usr/bin/env python3
"""Case-declared PySpark indirect Parquet writer for the load contract."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

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
            [(index, f"row-{index}", index % 2 == 0) for index in range(8)],
            ["id", "name", "active"],
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
            "httpConnectTimeout": "30000",
            "httpReadTimeout": "30000",
            "httpMaxRetry": "0",
        }
        for key, value in options.items():
            writer = writer.option(key, value)
        writer.mode("append").save(arguments.destination)
        print(
            json.dumps(
                {
                    "connector": versions["connector"],
                    "entrypoint": "pyspark",
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
    except Exception as error:  # noqa: BLE001 - subprocess boundary sanitizes diagnostics
        classes = [type(error).__name__]
        frames = []
        java_error = getattr(error, "java_exception", None)
        for _ in range(8):
            if java_error is None:
                break
            try:
                name = str(java_error.getClass().getName())
                stack = java_error.getStackTrace()
                next_error = java_error.getCause()
            except Exception:  # noqa: BLE001 - never expose a secondary bridge failure
                break
            if re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_.$]*", name):
                classes.append(name)
            try:
                if stack:
                    frame = f"{stack[0].getClassName()}.{stack[0].getMethodName()}"
                    if re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_.$]*", frame):
                        frames.append(frame)
            except Exception:  # noqa: BLE001 - diagnostics stay best-effort
                pass
            java_error = next_error
        print(
            json.dumps(
                {
                    "entrypoint": "pyspark",
                    "failureClasses": classes,
                    "failureFrames": frames,
                    "stage": stage,
                    "status": "failed",
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
