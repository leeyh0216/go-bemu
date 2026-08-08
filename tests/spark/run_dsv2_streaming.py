#!/usr/bin/env python3
"""Run one released DSv2 streaming epoch in an isolated Spark JVM.

The subprocess receives a temporary configuration file and writes only a
resource-free progress summary. Its connector classpath is validated before
Spark starts, then the live JVM provider and runtime versions are checked
before the public-edge write.

Official Spark and connector contracts:
https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-dsv2/spark-3.5-bigquery-lib/src/main/java/com/google/cloud/spark/bigquery/v2/Spark35BigQueryTableProvider.java
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-dsv2/spark-3.1-bigquery-lib/src/main/java/com/google/cloud/spark/bigquery/v2/BigQueryWriteBuilder.java#L34-L40
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any

from artifact_variants import (
    DSV2_PROVIDER,
    DSV2_RAW_VARIANT,
    SERVICE_ENTRY,
    enforce_connector_classpath,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
STATIC_ACCESS_TOKEN = "bqemu-spark-e2e-static-token"
DIRECT_WRITER_CONTEXT = (
    "com.google.cloud.spark.bigquery.write.context."
    "BigQueryDirectDataSourceWriterContext"
)


def _positive_seconds(value: object, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be a positive number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{field} must be a positive number")
    return parsed


def _safe_event(*, stage: str, shape: str, status: str, fix_hint: str) -> None:
    fingerprint = hashlib.sha256(
        f"dsv2-raw-streaming\0{stage}\0{shape}\0{status}".encode("utf-8")
    ).hexdigest()
    print(
        " ".join(
            (
                "version=0.44.2",
                "operation=dsv2-raw-streaming",
                f"stage={stage}",
                f"shape={shape}",
                f"fingerprint=sha256:{fingerprint}",
                f"status={status}",
                f"fix_hint={fix_hint}",
            )
        ),
        flush=True,
    )


def _load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        config = json.load(stream)
    required = {
        "connectorClasspath",
        "httpEndpoint",
        "grpcEndpoint",
        "projectId",
        "datasetId",
        "tableId",
        "inputDirectory",
        "checkpointDirectory",
        "truststore",
        "truststorePassword",
        "jvmLog",
        "resultPath",
        "testTimeoutSeconds",
        "rpcTimeoutSeconds",
    }
    if set(config) != required:
        raise ValueError("runner configuration shape drift")
    if not isinstance(config["connectorClasspath"], list):
        raise ValueError("connectorClasspath must be a list")
    return config


def _write_log_config(config: dict[str, Any]) -> Path:
    target = Path(str(config["jvmLog"])).with_suffix(".log4j2.properties")
    target.write_text(
        "\n".join(
            (
                "status = error",
                "name = BQEMUDSv2Contract",
                "appender.file.type = File",
                "appender.file.name = ContractFile",
                f"appender.file.fileName = {config['jvmLog']}",
                "appender.file.layout.type = PatternLayout",
                "appender.file.layout.pattern = %p %c %m%n",
                "rootLogger.level = warn",
                "rootLogger.appenderRef.file.ref = ContractFile",
                "logger.connector.name = com.google.cloud.spark.bigquery",
                "logger.connector.level = warn",
                "logger.connector.additivity = false",
                "logger.connector.appenderRef.file.ref = ContractFile",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    return target


def _connector_service_shape(java: Any, entry: str) -> tuple[int, int]:
    resources = (
        java.java.lang.Thread.currentThread()
        .getContextClassLoader()
        .getResources(entry)
    )
    dsv2_count = 0
    other_connector_count = 0
    while resources.hasMoreElements():
        resource = resources.nextElement()
        reader = java.java.io.BufferedReader(
            java.java.io.InputStreamReader(resource.openStream())
        )
        try:
            while True:
                line = reader.readLine()
                if line is None:
                    break
                provider = line.strip()
                if provider == DSV2_PROVIDER:
                    dsv2_count += 1
                elif provider.startswith("com.google.cloud.spark.bigquery"):
                    other_connector_count += 1
        finally:
            reader.close()
    return dsv2_count, other_connector_count


def _code_source_path(java_class: Any) -> Path:
    location = (
        java_class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()
    )
    return Path(str(location)).resolve()


def _run(config: dict[str, Any]) -> None:
    timeout = _positive_seconds(config["testTimeoutSeconds"], "testTimeoutSeconds")
    rpc_timeout = _positive_seconds(config["rpcTimeoutSeconds"], "rpcTimeoutSeconds")
    selected = enforce_connector_classpath(
        [Path(str(path)).resolve() for path in config["connectorClasspath"]],
        expected_variant=DSV2_RAW_VARIANT,
        repository_root=REPOSITORY_ROOT,
    )

    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    log_config = _write_log_config(config)
    trust_options = (
        f"-Djavax.net.ssl.trustStore={config['truststore']} "
        f"-Djavax.net.ssl.trustStorePassword={config['truststorePassword']} "
        "-Djavax.net.ssl.trustStoreType=PKCS12 "
        f"-Dlog4j.configurationFile={log_config.as_uri()}"
    )
    python_executable = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("bqemu-dsv2-raw-streaming-contract")
        .config("spark.jars", str(selected.path))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", trust_options)
        .config("spark.executor.extraJavaOptions", trust_options)
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        java = spark._jvm
        provider = java.org.apache.spark.sql.execution.datasources.DataSource.lookupDataSource(
            "bigquery", spark._jsparkSession.sessionState().conf()
        )
        runtime_scala = java.scala.util.Properties.versionNumberString()
        service_count, other_connector_count = _connector_service_shape(
            java, SERVICE_ENTRY
        )
        context_class = java.java.lang.Class.forName(
            DIRECT_WRITER_CONTEXT,
            False,
            java.java.lang.Thread.currentThread().getContextClassLoader(),
        )
        listed_jar_count = int(spark.sparkContext._jsc.sc().listJars().size())
        provider_source_matches = _code_source_path(provider) == selected.path
        context_source_matches = _code_source_path(context_class) == selected.path
        if (
            spark.version != "3.5.8"
            or runtime_scala != "2.12.18"
            or provider.getName() != DSV2_PROVIDER
            or service_count != 1
            or other_connector_count != 0
            or listed_jar_count != 1
            or not provider_source_matches
            or not context_source_matches
        ):
            raise RuntimeError("Spark runtime/provider identity drift")

        schema = StructType(
            (
                StructField("id", LongType(), nullable=False),
                StructField("active", BooleanType(), nullable=False),
                StructField("score", DoubleType(), nullable=False),
                StructField("payload", StringType(), nullable=True),
            )
        )
        frame = (
            spark.readStream.schema(schema)
            .option("maxFilesPerTrigger", "1")
            .json(str(config["inputDirectory"]))
            .repartition(1)
        )
        destination = ".".join(
            (str(config["projectId"]), str(config["datasetId"]), str(config["tableId"]))
        )
        options = {
            "table": destination,
            "parentProject": str(config["projectId"]),
            "billingProject": str(config["projectId"]),
            "project": str(config["projectId"]),
            "bigQueryHttpEndpoint": str(config["httpEndpoint"]),
            "bigQueryStorageGrpcEndpoint": str(config["grpcEndpoint"]),
            "gcpAccessToken": STATIC_ACCESS_TOKEN,
            "createReadSessionTimeoutInSeconds": str(math.ceil(rpc_timeout)),
            "httpConnectTimeout": str(math.ceil(rpc_timeout * 1000)),
            "httpReadTimeout": str(math.ceil(rpc_timeout * 1000)),
            "httpMaxRetry": "0",
            "writeMethod": "direct",
            "writeAtLeastOnce": "false",
            "checkpointLocation": str(config["checkpointDirectory"]),
        }
        writer = frame.writeStream.format("bigquery").outputMode("append")
        for key, value in options.items():
            writer = writer.option(key, value)
        query = writer.trigger(availableNow=True).start()
        if not query.awaitTermination(timeout):
            query.stop()
            query.awaitTermination(min(timeout, 10.0))
            raise TimeoutError("streaming query exceeded its configured timeout")
        if query.exception() is not None:
            raise RuntimeError("streaming query failed")
        progress = query.recentProgress
        batches = [
            {
                "batchId": int(item["batchId"]),
                "inputRows": int(item["numInputRows"]),
            }
            for item in progress
        ]
        result = {
            "schemaVersion": "1",
            "variant": DSV2_RAW_VARIANT,
            "sparkVersion": "3.5.8",
            "scalaVersion": "2.12.18",
            "provider": "Spark35BigQueryTableProvider",
            "serviceProviderCount": service_count,
            "listedJarCount": listed_jar_count,
            "providerCodeSourceMatches": provider_source_matches,
            "writerContextCodeSourceMatches": context_source_matches,
            "batches": batches,
        }
        Path(str(config["resultPath"])).write_text(
            json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        _safe_event(
            stage="query-complete",
            shape=f"batches:{len(batches)},input-rows:{sum(item['inputRows'] for item in batches)}",
            status="passed",
            fix_hint="none",
        )
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        _run(_load_config(arguments.config))
    except Exception as error:
        shape = type(error).__name__
        _safe_event(
            stage="runner",
            shape=shape,
            status="failed",
            fix_hint="inspect-redacted-ci-diagnostics",
        )
        raise RuntimeError(f"DSv2 streaming runner failed shape={shape}") from None
    return 0


if __name__ == "__main__":
    sys.exit(main())
