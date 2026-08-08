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
    observe_query_read_flow,
    public_edge_log_position,
    query,
    record_capability,
)


def _scala_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


@pytest.mark.capability("SBQ-READ-ARROW-FILTER-V1")
@pytest.mark.operation("grpc.bigquery-read.create-read-session")
@pytest.mark.operation("grpc.bigquery-read.read-rows")
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
    log_position = public_edge_log_position(public_edge)
    script = tmp_path / "scala-public-edge.scala"
    script.write_text(
        f"""import scala.util.Properties
import org.apache.spark.sql.functions._

require(spark.version == {_scala_string(versions['spark'])}, s"Spark version drift: ${{spark.version}}")
require(Properties.versionNumberString == {_scala_string(versions['scala'])}, s"Scala version drift: ${{Properties.versionNumberString}}")
require(Properties.versionNumberString.startsWith({_scala_string(versions['scalaBinary'] + '.')}), s"Scala binary version drift: ${{Properties.versionNumberString}}")
require(System.getProperty("java.specification.version") == {_scala_string(versions['java'])}, s"Java version drift: ${{System.getProperty("java.specification.version")}}")
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
val filteredRows = spark.read
  .format("bigquery")
  .options(connectorOptions)
  .option("readDataFormat", "ARROW")
  .option("maxParallelism", "2")
  .load("{public_edge.project_id}.{public_edge.dataset_id}.{table_id}")
  .where(
    col("id").isin(1L, 2L)
      .and(col("label").startsWith("o").or(col("label").endsWith("wo")))
      .and(col("label").eqNullSafe("one").or(col("id") === 2L))
      .and(not(col("label").contains("zzz")))
  )
  .select("id", "label")
  .orderBy("id")
  .collect()
require(filteredRows.length == 2, s"filtered row count mismatch: ${{filteredRows.length}}")
require(filteredRows(0).getLong(0) == 1L && filteredRows(0).getString(1) == "one")
require(filteredRows(1).getLong(0) == 2L && filteredRows(1).getString(1) == "two")
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
    observation = observe_query_read_flow(public_edge, since=log_position)
    matching_shapes = [
        shape
        for shape in observation["read_session_shapes"]
        if shape.get("format") == "ARROW"
        and shape.get("selected_field_count") == 2
        and isinstance(shape.get("row_restriction_bytes"), int)
        and shape["row_restriction_bytes"] > 0
    ]
    assert len(matching_shapes) == 1, "Scala filter was not pushed to Storage Read"
    record_capability(
        "SBQ-READ-ARROW-FILTER-V1",
        "scala arrow in+like+null-safe+not-filter:verified selected-fields:2 rows:2",
    )
