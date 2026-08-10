// Executes the released 1.2.0 connector against BQEMU's real TLS gRPC service.
// It is compiled inside the checksum-pinned Flink runtime image by run_contract.py.
package flinkcontract;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.sink.serializer.CdcChangeTypeProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public final class FlinkCdcMain {
  private static final String SCHEMA = "{" +
      "\"type\":\"record\",\"name\":\"CdcRow\",\"fields\":[" +
      "{\"name\":\"id\",\"type\":\"long\"}," +
      "{\"name\":\"value\",\"type\":\"string\"}," +
      "{\"name\":\"sequence\",\"type\":\"long\"}" +
      "]}";

  private FlinkCdcMain() {}

  private static GenericRecord row(Schema schema, long id, String value, long sequence) {
    GenericRecord row = new GenericData.Record(schema);
    row.put("id", id);
    row.put("value", value);
    row.put("sequence", sequence);
    return row;
  }

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 3) {
      throw new IllegalArgumentException("expected project dataset table");
    }
    Schema schema = new Schema.Parser().parse(SCHEMA);
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(2);

    CredentialsOptions credentials = CredentialsOptions.builder()
        .setAccessToken("bqemu-flink-contract-token")
        .build();
    BigQueryConnectOptions connection = BigQueryConnectOptions.builder()
        .setProjectId(arguments[0])
        .setDataset(arguments[1])
        .setTable(arguments[2])
        .setCredentialsOptions(credentials)
        .build();
    BigQuerySinkConfig<GenericRecord> config = BigQuerySinkConfig.<GenericRecord>newBuilder()
        .connectOptions(connection)
        .schemaProvider(new BigQuerySchemaProviderImpl(schema))
        .serializer(new AvroToProtoSerializer())
        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .streamExecutionEnvironment(environment)
        .enableCdc(true)
        .cdcSequenceField("sequence")
        .cdcChangeTypeProvider(CdcChangeTypeProvider.upsertOnly())
        .build();

    environment.fromCollection(List.of(
        row(schema, 1, "old", 16),
        row(schema, 1, "new", 17)))
        .sinkTo(BigQuerySink.get(config));
    environment.execute("bqemu-flink-cdc-contract");
    System.out.println("BQEMU_FLINK_CDC_OK");
  }
}
