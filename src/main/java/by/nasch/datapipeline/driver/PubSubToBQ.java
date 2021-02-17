package by.nasch.datapipeline.driver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PubSubToBQ {

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

    public interface PubSubToBQOptions extends PipelineOptions {
        @Validation.Required
        String getSubscriptionPath();
        void setSubscriptionPath(String subscriptionPath);

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
        @Description("Output table specification in the form [project_id]:[dataset_id].[table_id] to write to")
        ValueProvider<String> getOutputTable();
        void setOutputTable(ValueProvider<String> value);
    }

    private static class JsonToIdFn extends DoFn<String, List<Long>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            Gson gson = new Gson();
            Map<String,Object> result = gson.fromJson(json, Map.class);
            c.output(new ArrayList<Long>(){{
                add(((Double) result.get("id")).longValue());
            }});
        }
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

    public static Schema getAvroSchema() {
        return new Schema.Parser().parse(SCHEMA);
    }

    private static class CollectIdsFn implements SerializableFunction<Iterable<List<Long>>, List<Long>> {

        @Override
        public List<Long> apply(Iterable<List<Long>> input) {
            List<Long> ids = new ArrayList<>();
            for (List<Long> record : input) {
                ids.add(record.get(0));
            }
            return ids;
        }
    }

    private static class GenericRecordToRowFn extends DoFn<GenericRecord, Row> {

        private org.apache.beam.sdk.schemas.Schema schema;

        public GenericRecordToRowFn(Schema schema) {
            this.schema = AvroUtils.toBeamSchema(schema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Row row = AvroUtils.toBeamRowStrict(c.element(), schema);
            c.output(row);
        }
    }

    private static class RowToTableRow extends DoFn<Row, TableRow> {

        private org.apache.beam.sdk.schemas.Schema schema;

        public RowToTableRow(Schema schema) {
            this.schema = AvroUtils.toBeamSchema(schema);
        }

        public static TableRow convertJsonToTableRow(String json) {
            TableRow row;
            try (InputStream inputStream =
                         new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
                row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize json to table row: " + json, e);
            }
            return row;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            RowJson.RowJsonSerializer rowJsonSerializer = RowJson.RowJsonSerializer.forSchema(schema);
            String json = RowJsonUtils.rowToJson(RowJsonUtils.newObjectMapperWith(rowJsonSerializer), c.element());
            TableRow row = convertJsonToTableRow(json);
            c.output(row);
        }
    }

    public static void main(String[] args) {
        PubSubToBQOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(PubSubToBQOptions.class);
        Pipeline p = Pipeline.create(options);

        Schema schema = getAvroSchema();
        PCollection<List<Long>> input = p.apply("Read JSON from PubSub",
                PubsubIO.readStrings().fromSubscription(options.getSubscriptionPath()))
                .apply("Convert JSON to Avro generic record",
                        ParDo.of(new JsonToIdFn()))
                .apply("Split to windows",
                        Window.<List<Long>>into(FixedWindows.of(Duration.standardMinutes(1)))
                                .triggering(AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(30)))
                                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                                .discardingFiredPanes()
                                .withAllowedLateness(Duration.standardSeconds(15)));

        PCollection<String> enrichedData = input
                .apply("Collect window ids",
                        Combine.globally(new CollectIdsFn()).withoutDefaults())
                .apply("Read additional info",
                        JdbcIO.<List<Long>, String>readAll()
                                .withDataSourceConfiguration(
                                        JdbcIO.DataSourceConfiguration
                                                .create(options.getDriverClassName(), options.getUrl())
                                                .withUsername(options.getUsername())
                                                .withPassword(options.getPassword()))
                                .withQuery("SELECT * FROM avrotable WHERE id = ANY (?)")
                                .withCoder(StringUtf8Coder.of())
                                .withParameterSetter((JdbcIO.PreparedStatementSetter<List<Long>>)
                                        (element, preparedStatement) -> {
                                    Array ids =
                                            preparedStatement.getConnection().createArrayOf(
                                                    "BIGINT", element.toArray(new Long[0]));
                                    preparedStatement.setArray(1, ids);
                                }).withRowMapper((JdbcIO.RowMapper<String>) resultSet -> {
                            int size = resultSet.getMetaData().getColumnCount();
                            ObjectMapper mapper = new ObjectMapper();
                            ObjectNode record = mapper.createObjectNode();
                            for (int i = 1; i <= size; i++) {
                                String name = resultSet.getMetaData().getColumnName(i);
                                Object value = resultSet.getObject(i);
                                record.putPOJO(name, value);
                            }
                            return mapper.writeValueAsString(record);
                        }));

        enrichedData.apply("Convert JSON to Avro generic record", ParDo.of(new JsonToGenericRecordFn(schema)))
                .setCoder(AvroGenericCoder.of(schema))
                .apply("Convert GenericRecord to Row", ParDo.of(new GenericRecordToRowFn(schema)))
                .setCoder(RowCoder.of(AvroUtils.toBeamSchema(schema)))
                .setRowSchema(AvroUtils.toBeamSchema(schema))
                .apply("Convert Row to TableRow", ParDo.of(new RowToTableRow(schema)))
                .apply("Write data to BigQuery", CounterAndWriteUnited.of(
                        BigQueryIO.writeTableRows()
                                .withSchema(BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(schema)))
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .to(options.getOutputTable())));

        p.run().waitUntilFinish();
    }
}
