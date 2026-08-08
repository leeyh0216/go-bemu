var stage = "bootstrap"

try {
  stage = "version"
  val expectedSpark = sys.env("BQEMU_LOAD_EXPECTED_SPARK")
  val expectedScalaBinary = sys.env("BQEMU_LOAD_EXPECTED_SCALA_BINARY")
  require(spark.version == expectedSpark)
  require(scala.util.Properties.versionNumberString.startsWith(expectedScalaBinary + "."))

  val project = sys.env("BQEMU_LOAD_SPARK_PROJECT")
  val destination = sys.env("BQEMU_LOAD_SPARK_DESTINATION")
  val endpoint = sys.env("BQEMU_LOAD_SPARK_HTTP_ENDPOINT")
  val bucket = sys.env("BQEMU_LOAD_SPARK_BUCKET")

  import spark.implicits._
  stage = "write"
  val frame = (0L until 8L)
    .map(index => (index, s"row-$index", index % 2L == 0L, java.sql.Date.valueOf("2026-01-01")))
    .toDF("id", "name", "active", "partition_date")
    .repartition(4)
  var writer = frame.write.format("bigquery")
  Map(
    "parentProject" -> project,
    "billingProject" -> project,
    "project" -> project,
    "bigQueryHttpEndpoint" -> endpoint,
    "gcpAccessToken" -> "bqemu-load-contract-token",
    "temporaryGcsBucket" -> bucket,
    "writeMethod" -> "indirect",
    "intermediateFormat" -> "parquet",
    "spark.sql.sources.partitionOverwriteMode" -> "DYNAMIC",
    "httpConnectTimeout" -> "30000",
    "httpReadTimeout" -> "30000",
    "httpMaxRetry" -> "0"
  ).foreach { case (key, value) => writer = writer.option(key, value) }
  writer.mode("overwrite").save(destination)
  println("BQEMU_LOAD_SCALA_STAGE=complete")
  spark.stop()
  System.exit(0)
} catch {
  case failure: Throwable =>
    System.err.println(s"BQEMU_LOAD_SCALA_STAGE=$stage failure=$failure")
    failure.printStackTrace(System.err)
    try spark.stop() catch { case _: Throwable => () }
    System.exit(1)
}
