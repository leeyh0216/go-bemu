import java.math.BigDecimal
import java.util.Arrays

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DecimalType, StructField, StructType}

var stage = "bootstrap"

try {
  stage = "version"
  val expectedSparkVersion = sys.env("BQEMU_SCALA_EXPECTED_SPARK_VERSION")
  val expectedScalaVersion = sys.env("BQEMU_SCALA_EXPECTED_SCALA_VERSION")
  val expectedScalaBinaryVersion = sys.env("BQEMU_SCALA_EXPECTED_SCALA_BINARY_VERSION")
  val expectedJavaVersion = sys.env("BQEMU_SCALA_EXPECTED_JAVA_VERSION")
  require(spark.version == expectedSparkVersion, s"unexpected Spark version ${spark.version}")
  require(
    scala.util.Properties.versionNumberString == expectedScalaVersion,
    s"unexpected Scala version ${scala.util.Properties.versionNumberString}"
  )
  require(
    scala.util.Properties.versionNumberString.startsWith(expectedScalaBinaryVersion + "."),
    s"unexpected Scala binary version ${scala.util.Properties.versionNumberString}"
  )
  require(
    System.getProperty("java.specification.version") == expectedJavaVersion,
    s"unexpected Java version ${System.getProperty("java.specification.version")}"
  )

  val project = sys.env("BQEMU_SCALA_PROJECT")
  val source = sys.env("BQEMU_SCALA_SOURCE")
  val destination = sys.env("BQEMU_SCALA_DESTINATION")
  val connectorOptions = Map(
    "parentProject" -> project,
    "billingProject" -> project,
    "project" -> project,
    "bigQueryHttpEndpoint" -> sys.env("BQEMU_SCALA_HTTP_ENDPOINT"),
    "bigQueryStorageGrpcEndpoint" -> sys.env("BQEMU_SCALA_GRPC_ENDPOINT"),
    "gcpAccessToken" -> sys.env("BQEMU_SCALA_ACCESS_TOKEN"),
    "bigNumericDefaultPrecision" -> "38",
    "bigNumericDefaultScale" -> "18",
    "createReadSessionTimeoutInSeconds" -> sys.env("BQEMU_SCALA_RPC_TIMEOUT_SECONDS"),
    "httpConnectTimeout" -> sys.env("BQEMU_SCALA_HTTP_TIMEOUT_MILLIS"),
    "httpReadTimeout" -> sys.env("BQEMU_SCALA_HTTP_TIMEOUT_MILLIS"),
    "httpMaxRetry" -> "0"
  )

  stage = "arrow-read"
  var reader = spark.read.format("bigquery")
  connectorOptions.foreach { case (key, value) => reader = reader.option(key, value) }
  val input = reader
    .option("readDataFormat", "ARROW")
    .option("maxParallelism", "1")
    .option("preferredMinParallelism", "1")
    .load(source)

  val fields = input.schema.fields.map(field => field.name -> field.dataType).toMap
  require(fields("numeric_default") == DecimalType(38, 9))
  require(fields("bignumeric_default") == DecimalType(38, 18))
  require(fields("numeric_explicit") == DecimalType(20, 4))
  require(fields("bignumeric_explicit") == DecimalType(10, 2))
  require(
    fields("details").asInstanceOf[StructType]("amount").dataType == DecimalType(38, 18)
  )
  require(
    fields("amounts").asInstanceOf[ArrayType].elementType == DecimalType(12, 3)
  )
  require(input.collect().isEmpty)

  stage = "direct-write"
  val outputSchema = StructType(Seq(
    StructField("numeric_value", DecimalType(20, 4), nullable = true),
    StructField("bignumeric_value", DecimalType(38, 18), nullable = true)
  ))
  val outputRows = Arrays.asList(
    Row(
      new BigDecimal("12.3400"),
      new BigDecimal("12345678901234567890.123456789012345678")
    )
  )
  val output = spark.createDataFrame(outputRows, outputSchema).repartition(1)
  var writer = output.write.format("bigquery")
  connectorOptions.foreach { case (key, value) => writer = writer.option(key, value) }
  writer
    .option("writeMethod", "direct")
    .option("writeAtLeastOnce", "false")
    .mode("append")
    .save(destination)

  stage = "complete"
  println("BQEMU_SCALA_DECIMAL_STAGE=complete")
  spark.stop()
  System.exit(0)
} catch {
  case failure: Throwable =>
    System.err.println(
      s"BQEMU_SCALA_DECIMAL_STAGE=$stage failure=${failure.getClass.getName}"
    )
    try spark.stop() catch { case _: Throwable => () }
    System.exit(1)
}
