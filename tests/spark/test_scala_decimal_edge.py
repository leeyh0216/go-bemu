"""Standalone Scala Spark 3.5.8 decimal contract over the public edge."""

from __future__ import annotations

from decimal import Decimal
import hashlib
import math
import os
from pathlib import Path
import re
import subprocess
import uuid

import pytest

from conftest import (
    REPOSITORY_ROOT,
    STATIC_ACCESS_TOKEN,
    TRUSTSTORE_PASSWORD,
    PublicEdge,
    create_table,
    list_table_data,
    record_capability,
)


SCALA_SOURCE = REPOSITORY_ROOT / "tests" / "spark" / "scala" / "DecimalPublicEdge.scala"
SAFE_STAGE_PATTERN = re.compile(
    r"^BQEMU_SCALA_DECIMAL_STAGE=([a-z-]+)(?: failure=([A-Za-z0-9_.$]+))?$"
)


def _spark_shell() -> Path:
    import pyspark

    executable = Path(pyspark.__file__).resolve().parent / "bin" / "spark-shell"
    if not executable.is_file():
        raise RuntimeError("pinned PySpark distribution does not contain spark-shell")
    return executable


def _safe_scala_stage(output: str) -> str:
    stages = []
    for line in output.splitlines():
        match = SAFE_STAGE_PATTERN.fullmatch(line.strip())
        if match is not None:
            stages.append(match.group(1) + (":" + match.group(2) if match.group(2) else ""))
    return stages[-1] if stages else "missing"


@pytest.mark.operation("bigquery.tables.insert")
@pytest.mark.operation("grpc.bigquery-read.create-read-session")
@pytest.mark.operation("grpc.bigquery-read.read-rows")
@pytest.mark.operation("grpc.bigquery-write.create-write-stream")
@pytest.mark.operation("grpc.bigquery-write.append-rows")
@pytest.mark.operation("grpc.bigquery-write.finalize-write-stream")
@pytest.mark.operation("grpc.bigquery-write.batch-commit-write-streams")
@pytest.mark.operation("bigquery.tabledata.list")
@pytest.mark.capability("SBQ-WRITE-DIRECT-DECIMAL-V1")
def test_scala_decimal_read_and_direct_write_through_public_edge(
    connector_jar: Path,
    public_edge: PublicEdge,
    test_timeout: float,
) -> None:
    source_id = f"scala_decimal_source_{uuid.uuid4().hex[:8]}"
    destination_id = f"scala_decimal_write_{uuid.uuid4().hex[:8]}"
    create_table(
        public_edge,
        test_timeout,
        source_id,
        [
            {"name": "numeric_default", "type": "NUMERIC"},
            {"name": "bignumeric_default", "type": "BIGNUMERIC"},
            {
                "name": "numeric_explicit",
                "type": "NUMERIC",
                "precision": "20",
                "scale": "4",
            },
            {
                "name": "bignumeric_explicit",
                "type": "BIGNUMERIC",
                "precision": "10",
                "scale": "2",
            },
            {
                "name": "details",
                "type": "RECORD",
                "fields": [{"name": "amount", "type": "BIGNUMERIC"}],
            },
            {
                "name": "amounts",
                "type": "NUMERIC",
                "mode": "REPEATED",
                "precision": "12",
                "scale": "3",
            },
        ],
    )
    create_table(
        public_edge,
        test_timeout,
        destination_id,
        [
            {
                "name": "numeric_value",
                "type": "NUMERIC",
                "precision": "20",
                "scale": "4",
            },
            {
                "name": "bignumeric_value",
                "type": "BIGNUMERIC",
                "precision": "38",
                "scale": "18",
            },
        ],
    )
    source = f"{public_edge.project_id}.{public_edge.dataset_id}.{source_id}"
    destination = (
        f"{public_edge.project_id}.{public_edge.dataset_id}.{destination_id}"
    )

    rpc_seconds = max(1, math.ceil(float(os.getenv("BQEMU_SPARK_RPC_TIMEOUT_SECONDS", "30"))))
    trust_options = (
        f"-Djavax.net.ssl.trustStore={public_edge.truststore} "
        f"-Djavax.net.ssl.trustStorePassword={TRUSTSTORE_PASSWORD} "
        "-Djavax.net.ssl.trustStoreType=PKCS12"
    )
    environment = os.environ.copy()
    environment.update(
        {
            "JAVA_TOOL_OPTIONS": " ".join(
                value
                for value in (environment.get("JAVA_TOOL_OPTIONS"), trust_options)
                if value
            ),
            "SPARK_LOCAL_IP": "127.0.0.1",
            "BQEMU_SCALA_PROJECT": public_edge.project_id,
            "BQEMU_SCALA_SOURCE": source,
            "BQEMU_SCALA_DESTINATION": destination,
            "BQEMU_SCALA_HTTP_ENDPOINT": public_edge.http_endpoint,
            "BQEMU_SCALA_GRPC_ENDPOINT": public_edge.grpc_endpoint,
            "BQEMU_SCALA_ACCESS_TOKEN": STATIC_ACCESS_TOKEN,
            "BQEMU_SCALA_RPC_TIMEOUT_SECONDS": str(rpc_seconds),
            "BQEMU_SCALA_HTTP_TIMEOUT_MILLIS": str(rpc_seconds * 1000),
        }
    )
    completed = subprocess.run(
        [
            str(_spark_shell()),
            "--master",
            "local[2]",
            "--jars",
            str(connector_jar),
            "--conf",
            "spark.driver.host=127.0.0.1",
            "--conf",
            "spark.driver.bindAddress=127.0.0.1",
            "--conf",
            "spark.ui.enabled=false",
            "--conf",
            "spark.sql.session.timeZone=UTC",
            "--conf",
            f"spark.driver.extraJavaOptions={trust_options}",
            "--conf",
            f"spark.executor.extraJavaOptions={trust_options}",
            "-i",
            str(SCALA_SOURCE),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        input="",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=test_timeout,
        check=False,
    )
    stage = _safe_scala_stage(completed.stdout)
    if completed.returncode != 0 or stage != "complete":
        fingerprint = hashlib.sha256(completed.stdout.encode("utf-8")).hexdigest()
        pytest.fail(
            "Scala decimal public-edge process failed "
            f"shape=exit:{completed.returncode},stage:{stage},bytes:{len(completed.stdout)} "
            f"fingerprint=sha256:{fingerprint}"
        )

    result = list_table_data(public_edge, test_timeout, destination_id)
    values = [Decimal(str(cell["v"])) for cell in result["rows"][0]["f"]]
    if values != [
        Decimal("12.3400"),
        Decimal("12345678901234567890.123456789012345678"),
    ]:
        pytest.fail("Scala direct decimal value mismatch shape=single-row-two-decimals")
    record_capability(
        "SBQ-WRITE-DIRECT-DECIMAL-V1",
        "scala-2.12 spark-3.5.8 connector-0.44.2 arrow-read+proto-write",
    )
