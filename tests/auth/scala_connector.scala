import scala.io.Source

val expectedSparkVersion = "3.5.8"
val expectedConnectorVersion = "0.44.2"
val project = sys.env("BQEMU_AUTH_PROJECT")
val table = sys.env("BQEMU_AUTH_TABLE")
val fixtureDirectory = sys.env("BQEMU_AUTH_FIXTURE_DIR")
val httpEndpoint = sys.env("BQEMU_AUTH_HTTP_ENDPOINT")
val grpcEndpoint = sys.env("BQEMU_AUTH_GRPC_ENDPOINT")

require(
  spark.version == expectedSparkVersion,
  "Spark runtime=" + spark.version + ", want " + expectedSparkVersion
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
  """{"client":"scala-spark","connector_version":"0.44.2","profiles":4,"spark_version":"3.5.8","status":"passed"}"""
)
spark.stop()
System.exit(0)
