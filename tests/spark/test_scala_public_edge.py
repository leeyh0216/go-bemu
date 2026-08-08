"""Scala Spark entrypoint for the released connector public edge."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest

from conftest import (
    PublicEdge,
    TRUSTSTORE_PASSWORD,
    connector_options,
    create_table,
    query,
)


def _scala_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


@pytest.mark.capability("SBQ-READ-ARROW-TABLE-V1")
def test_scala_spark_reads_through_connector(
    connector_jar: Path,
    public_edge: PublicEdge,
    test_timeout: float,
    tmp_path: Path,
) -> None:
    versions = json.loads(os.environ["BQEMU_RUNTIME_VERSIONS_JSON"])
    table_id = "scala_read_rows"
    create_table(
        public_edge,
        test_timeout,
        table_id,
        [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "label", "type": "STRING", "mode": "NULLABLE"},
        ],
    )
    query(
        public_edge,
        test_timeout,
        f"INSERT INTO `{public_edge.project_id}.{public_edge.dataset_id}.{table_id}` VALUES (1, 'one'), (2, 'two')",
    )

    options = connector_options(public_edge)
    option_entries = ",\n".join(
        f"  {_scala_string(key)} -> {_scala_string(value)}"
        for key, value in sorted(options.items())
    )
    script = tmp_path / "scala-public-edge.scala"
    script.write_text(
        f"""import scala.util.Properties

require(spark.version == {_scala_string(versions['spark'])}, s"Spark version drift: ${{spark.version}}")
require(Properties.versionNumberString == {_scala_string(versions['scala'])}, s"Scala version drift: ${{Properties.versionNumberString}}")
val connectorOptions = Map(
{option_entries}
)
val rows = spark.read
  .format("bigquery")
  .options(connectorOptions)
  .option("readDataFormat", "ARROW")
  .option("maxParallelism", "2")
  .load("{public_edge.project_id}.{public_edge.dataset_id}.{table_id}")
  .orderBy("id")
  .collect()
require(rows.length == 2, s"row count mismatch: ${{rows.length}}")
require(rows(0).getLong(0) == 1L && rows(0).getString(1) == "one")
require(rows(1).getLong(0) == 2L && rows(1).getString(1) == "two")
println("BQEMU_SCALA_CONTRACT_OK")
System.exit(0)
""",
        encoding="utf-8",
    )

    from pyspark.find_spark_home import _find_spark_home

    spark_shell = Path(_find_spark_home()) / "bin" / "spark-shell"
    trust_options = (
        f"-Djavax.net.ssl.trustStore={public_edge.truststore} "
        f"-Djavax.net.ssl.trustStorePassword={TRUSTSTORE_PASSWORD} "
        "-Djavax.net.ssl.trustStoreType=PKCS12"
    )
    environment = os.environ.copy()
    environment["SPARK_LOCAL_IP"] = "127.0.0.1"
    environment["JAVA_TOOL_OPTIONS"] = " ".join(
        value for value in (environment.get("JAVA_TOOL_OPTIONS"), trust_options) if value
    )
    result = subprocess.run(
        [
            str(spark_shell),
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
            "-i",
            str(script),
        ],
        cwd=tmp_path,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=test_timeout,
        check=False,
    )
    diagnostics = public_edge.truststore.parent / "scala-shell.log"
    diagnostics.write_text(result.stdout[-20000:], encoding="utf-8")
    assert result.returncode == 0, "Scala Spark process failed; inspect scala-shell.log"
    assert "BQEMU_SCALA_CONTRACT_OK" in result.stdout
