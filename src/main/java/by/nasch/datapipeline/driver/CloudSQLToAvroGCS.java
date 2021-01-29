package by.nasch.datapipeline.driver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;

public class CloudSQLToAvroGCS {

    private static final String SCHEMA = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"topLevelRecord\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"id\",\n" +
            "    \"type\" : [ \"long\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"date_time\",\n" +
            "    \"type\" : [ \"string\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"site_name\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"posa_continent\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"user_location_country\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"user_location_region\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"user_location_city\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"orig_destination_distance\",\n" +
            "    \"type\" : [ \"double\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"user_id\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"is_mobile\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"is_package\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"channel\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"srch_ci\",\n" +
            "    \"type\" : [ \"string\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"srch_co\",\n" +
            "    \"type\" : [ \"string\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"srch_adults_cnt\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"srch_children_cnt\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"srch_rm_cnt\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"srch_destination_id\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"srch_destination_type_id\",\n" +
            "    \"type\" : [ \"int\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"hotel_id\",\n" +
            "    \"type\" : [ \"long\", \"null\" ]\n" +
            "  } ]\n" +
            "}";

    public static void main(String[] args) {
        CloudSQLToAvroGCSOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(CloudSQLToAvroGCSOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        Schema schema = getAvroSchema();
        pipeline.apply("Read from Cloud SQL DB", readCloudSQL(options))
                .apply("Convert JSON to Avro generic record", ParDo.of(new JsonToGenericRecordFn(schema)))
                .setCoder(AvroGenericCoder.of(schema))
                .apply("Write to GCS",
                        AvroIO.<GenericRecord>writeGenericRecords(schema)
                                .to(options.getOutputFile())
                                .withoutSharding()
                                .withSuffix(".avro"));
        pipeline.run().waitUntilFinish();
    }

    public interface CloudSQLToAvroGCSOptions extends PipelineOptions {
        @Validation.Required
        @Description("The driver class name to use, e.g. \"com.mysql.jdbc.Driver\"")
        ValueProvider<String> getDriverClassName();
        void setDriverClassName(ValueProvider<String> value);

        @Validation.Required
        @Description("DB URL")
        ValueProvider<String> getUrl();
        void setUrl(ValueProvider<String> value);

        @Validation.Required
        @Description("DB username")
        ValueProvider<String> getUsername();
        void setUsername(ValueProvider<String> value);

        @Validation.Required
        @Description("DB password")
        ValueProvider<String> getPassword();
        void setPassword(ValueProvider<String> value);

        @Validation.Required
        @Description("Target avro file name")
        ValueProvider<String> getOutputFile();
        void setOutputFile(ValueProvider<String> value);
    }

    private static JdbcIO.Read<String> readCloudSQL(CloudSQLToAvroGCSOptions options) {
        return JdbcIO.<String>read()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration
                                .create(options.getDriverClassName(), options.getUrl())
                                .withUsername(options.getUsername())
                                .withPassword(options.getPassword()))
                .withQuery("select id, date_time, site_name, posa_continent, user_location_country," +
                        " user_location_region, user_location_city, orig_destination_distance, user_id, is_mobile," +
                        " is_package, channel, srch_ci, srch_co, srch_adults_cnt, srch_children_cnt, srch_rm_cnt," +
                        " srch_destination_id, srch_destination_type_id, hotel_id from avrotable")
                .withCoder(StringUtf8Coder.of())
                .withRowMapper(new ResultSetToJson());
    }

    private static class ResultSetToJson implements JdbcIO.RowMapper<String> {
        @Override
        public String mapRow(ResultSet resultSet) throws Exception {
            int size = resultSet.getMetaData().getColumnCount();
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode record = mapper.createObjectNode();
            for (int i = 1; i <= size; i++) {
                String name = resultSet.getMetaData().getColumnName(i);
                Object value = resultSet.getObject(i);
                record.putPOJO(name, value);
            }
            return mapper.writeValueAsString(record);
        }
    }

    public static Schema getAvroSchema() {
        return new Schema.Parser().parse(SCHEMA);
    }

    private static class JsonToGenericRecordFn extends DoFn<String, GenericRecord> {

        private Schema schema;
        private final String schemaJson;

        public JsonToGenericRecordFn(Schema schema) {
            this.schemaJson = schema.toString();
        }

        @Setup
        public void setup() {
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            GenericRecord record =
                    new JsonAvroConverter()
                            .convertToGenericDataRecord(json.getBytes(StandardCharsets.UTF_8), schema);
            c.output(record);
        }
    }
}
