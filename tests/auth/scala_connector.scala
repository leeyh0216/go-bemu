import scala.io.Source
import java.util.Properties

val expectedSparkVersion = sys.env("BQEMU_AUTH_EXPECTED_SPARK_VERSION")
val expectedConnectorVersion = sys.env("BQEMU_AUTH_EXPECTED_CONNECTOR_VERSION")
val expectedScalaVersion = sys.env("BQEMU_AUTH_EXPECTED_SCALA_VERSION")
val expectedScalaBinaryVersion = sys.env("BQEMU_AUTH_EXPECTED_SCALA_BINARY_VERSION")
val expectedJavaVersion = sys.env("BQEMU_AUTH_EXPECTED_JAVA_VERSION")
val project = sys.env("BQEMU_AUTH_PROJECT")
val table = sys.env("BQEMU_AUTH_TABLE")
val fixtureDirectory = sys.env("BQEMU_AUTH_FIXTURE_DIR")
val httpEndpoint = sys.env("BQEMU_AUTH_HTTP_ENDPOINT")
val grpcEndpoint = sys.env("BQEMU_AUTH_GRPC_ENDPOINT")

require(
  spark.version == expectedSparkVersion,
  "Spark runtime=" + spark.version + ", want " + expectedSparkVersion
)
require(
  scala.util.Properties.versionNumberString == expectedScalaVersion,
  "Scala runtime=" + scala.util.Properties.versionNumberString + ", want " + expectedScalaVersion
)
require(
  scala.util.Properties.versionNumberString.startsWith(expectedScalaBinaryVersion + "."),
  "Scala binary runtime=" + scala.util.Properties.versionNumberString + ", want " + expectedScalaBinaryVersion + ".x"
)
require(
  System.getProperty("java.specification.version") == expectedJavaVersion,
  "Java runtime=" + System.getProperty("java.specification.version") + ", want " + expectedJavaVersion
)
val connectorResources = Thread.currentThread
  .getContextClassLoader
  .getResources("spark-bigquery-connector.properties")
val connectorVersions = scala.collection.mutable.ArrayBuffer.empty[String]
while (connectorResources.hasMoreElements) {
  val stream = connectorResources.nextElement.openStream()
  try {
    val properties = new Properties()
    properties.load(stream)
    connectorVersions += Option(properties.getProperty("connector.version")).getOrElse("")
  } finally stream.close()
}
require(
  connectorVersions.toSeq == Seq(expectedConnectorVersion),
  "connector JAR version identity does not match the normalized case"
)

def baseReader() = {
  spark.read
    .format("bigquery")
    .option("parentProject", project)
    .option("billingProject", project)
    .option("project", project)
    .option("bigQueryHttpEndpoint", httpEndpoint)
    .option("bigQueryStorageGrpcEndpoint", grpcEndpoint)
    .option("createReadSessionTimeoutInSeconds", "30")
    .option("httpConnectTimeout", "30000")
    .option("httpReadTimeout", "30000")
    .option("httpMaxRetry", "0")
}

val credentialProfiles = Seq(
  "service-account.json",
  "authorized-user.json",
  "wif.json"
)
credentialProfiles.foreach { filename =>
  val count = baseReader()
    .option("credentialsFile", fixtureDirectory + "/" + filename)
    .load(table)
    .count()
  require(count == 1, filename + " read row count=" + count + ", want 1")
}

val tokenSource = Source.fromFile(fixtureDirectory + "/access-token.txt")
val accessToken = try tokenSource.mkString.trim finally tokenSource.close()
require(accessToken.nonEmpty, "access-token.txt is empty")
val tokenCount = baseReader()
  .option("gcpAccessToken", accessToken)
  .load(table)
  .count()
require(tokenCount == 1, "access-token.txt read row count=" + tokenCount + ", want 1")

println(
  s"""{"client":"scala-spark","connector_version":"$expectedConnectorVersion","profiles":4,"spark_version":"$expectedSparkVersion","status":"passed"}"""
)
spark.stop()
System.exit(0)
