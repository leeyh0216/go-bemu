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
import re
import sys
from typing import Any

from artifact_variants import (
    DSV2_OVERLAY_VARIANT,
    DSV2_PROVIDER,
    DSV2_RAW_VARIANT,
    SERVICE_ENTRY,
    enforce_connector_classpath,
    enforce_overlay_pair,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
STATIC_ACCESS_TOKEN = "bqemu-spark-e2e-static-token"
DIRECT_WRITER_CONTEXT = (
    "com.google.cloud.spark.bigquery.write.context."
    "BigQueryDirectDataSourceWriterContext"
)


class RunnerStageFailure(RuntimeError):
    def __init__(self, stage: str, cause_shape: str):
        self.stage = stage
        self.cause_shape = cause_shape
        super().__init__(stage)


def _at_stage(stage: str, operation: Any) -> Any:
    try:
        return operation()
    except Exception as error:
        raise RunnerStageFailure(stage, _exception_type_shape(error)) from None


def _exception_type_shape(error: Exception) -> str:
    """Retain exception classes only; never inspect or log messages."""

    classes = [type(error).__name__]
    current = getattr(error, "_origin", None)
    for _ in range(8):
        if current is None:
            break
        try:
            name = str(current.getClass().getName()).rsplit(".", 1)[-1]
            if not re.fullmatch(r"[A-Za-z0-9_$]+", name):
                name = "UnknownJavaException"
            classes.append(name)
            current = current.getCause()
        except Exception:
            break
    return ":".join(classes)


def _positive_seconds(value: object, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be a positive number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{field} must be a positive number")
    return parsed


def _safe_event(
    *, operation: str, stage: str, shape: str, status: str, fix_hint: str
) -> None:
    fingerprint = hashlib.sha256(
        f"{operation}\0{stage}\0{shape}\0{status}".encode("utf-8")
    ).hexdigest()
    print(
        " ".join(
            (
                "version=0.44.2",
                f"operation={operation}",
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
    allowed_shapes = (required, required | {"overlayClasspath"})
    if set(config) not in allowed_shapes:
        raise ValueError("runner configuration shape drift")
    if not isinstance(config["connectorClasspath"], list):
        raise ValueError("connectorClasspath must be a list")
    if "overlayClasspath" in config and not isinstance(
        config["overlayClasspath"], list
    ):
        raise ValueError("overlayClasspath must be a list")
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
    pair = None
    variant = DSV2_RAW_VARIANT
    operation = "dsv2-raw-streaming"
    if "overlayClasspath" in config:
        pair = enforce_overlay_pair(
            base_paths=[
                Path(str(path)).resolve() for path in config["connectorClasspath"]
            ],
            overlay_paths=[
                Path(str(path)).resolve() for path in config["overlayClasspath"]
            ],
            repository_root=REPOSITORY_ROOT,
        )
        variant = DSV2_OVERLAY_VARIANT
        operation = "dsv2-overlay-streaming"

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
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("bqemu-dsv2-streaming-contract")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", trust_options)
        .config("spark.executor.extraJavaOptions", trust_options)
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if pair is not None:
        # Put the exact pair in one parent classloader. Duplicating the base in
        # Spark's driver/executor child loaders makes commit-message classes
        # non-identical and fails the DSv2 driver cast.
        pair_classpath = os.pathsep.join(
            str(path) for path in pair.runtime_classpath
        )
        builder = builder.config("spark.driver.extraClassPath", pair_classpath).config(
            "spark.executor.extraClassPath", pair_classpath
        )
        expected_listed_jar_count = 0
    else:
        builder = builder.config("spark.jars", str(selected.path))
        expected_listed_jar_count = 1
    spark = _at_stage("spark-start", builder.getOrCreate)
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        java = spark._jvm
        provider = _at_stage(
            "provider-lookup",
            lambda: java.org.apache.spark.sql.execution.datasources.DataSource.lookupDataSource(
                "bigquery", spark._jsparkSession.sessionState().conf()
            ),
        )
        runtime_scala = java.scala.util.Properties.versionNumberString()
        service_count, other_connector_count = _connector_service_shape(
            java, SERVICE_ENTRY
        )
        context_class = _at_stage(
            "writer-context-load",
            lambda: java.java.lang.Class.forName(
                DIRECT_WRITER_CONTEXT,
                False,
                java.java.lang.Thread.currentThread().getContextClassLoader(),
            ),
        )
        listed_jar_count = int(spark.sparkContext._jsc.sc().listJars().size())
        provider_source_matches = _code_source_path(provider) == selected.path
        expected_context_source = pair.overlay.path if pair is not None else selected.path
        context_source_matches = _code_source_path(context_class) == expected_context_source
        hook_names = {
            str(method.getName())
            for method in context_class.getDeclaredMethods()
            if str(method.getName())
            in {"onDataStreamingWriterCommit", "onDataStreamingWriterAbort"}
            and len(method.getParameterTypes()) == 2
            and str(method.getParameterTypes()[0].getName()) == "long"
            and str(method.getParameterTypes()[1].getName())
            == "[Lcom.google.cloud.spark.bigquery.write.context.WriterCommitMessageContext;"
            and str(method.getReturnType().getName()) == "void"
        }
        expected_hook_count = 2 if pair is not None else 0
        if pair is not None:
            runtime_classpath_matches = (
                spark.sparkContext.getConf().get("spark.driver.extraClassPath", "")
                == pair_classpath
                and spark.sparkContext.getConf().get(
                    "spark.executor.extraClassPath", ""
                )
                == pair_classpath
            )
        else:
            runtime_classpath_matches = True
        if (
            spark.version != "3.5.8"
            or runtime_scala != "2.12.18"
            or provider.getName() != DSV2_PROVIDER
            or service_count != 1
            or other_connector_count != 0
            or listed_jar_count != expected_listed_jar_count
            or not provider_source_matches
            or not context_source_matches
            or len(hook_names) != expected_hook_count
            or not runtime_classpath_matches
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
        query = _at_stage(
            "query-start", lambda: writer.trigger(availableNow=True).start()
        )
        terminated = _at_stage(
            "query-await", lambda: query.awaitTermination(timeout)
        )
        if not terminated:
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
            "variant": variant,
            "sparkVersion": "3.5.8",
            "scalaVersion": "2.12.18",
            "provider": "Spark35BigQueryTableProvider",
            "serviceProviderCount": service_count,
            "listedJarCount": listed_jar_count,
            "providerCodeSourceMatches": provider_source_matches,
            "writerContextCodeSourceMatches": context_source_matches,
            "streamingHookCount": len(hook_names),
            "runtimeClasspathOrderMatches": runtime_classpath_matches,
            "batches": batches,
        }
        Path(str(config["resultPath"])).write_text(
            json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        _safe_event(
            operation=operation,
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
        config = _load_config(arguments.config)
        operation = (
            "dsv2-overlay-streaming"
            if "overlayClasspath" in config
            else "dsv2-raw-streaming"
        )
        _run(config)
    except Exception as error:
        shape = (
            f"{error.stage}:{error.cause_shape}"
            if isinstance(error, RunnerStageFailure)
            else type(error).__name__
        )
        _safe_event(
            operation=locals().get("operation", "dsv2-streaming"),
            stage="runner",
            shape=shape,
            status="failed",
            fix_hint="inspect-redacted-ci-diagnostics",
        )
        raise RuntimeError(f"DSv2 streaming runner failed shape={shape}") from None
    return 0


if __name__ == "__main__":
    sys.exit(main())
