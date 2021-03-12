package by.nasch.datapipeline.driver;

import by.nasch.datapipeline.schema.BQPubSubTableSchema;
import by.nasch.datapipeline.combine.CollectIds;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

import java.sql.Array;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PubSubToBQ {

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

    private static class JsonToIdFn extends DoFn<String, KV<Long, String>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            Gson gson = new Gson();
            Map<String,Object> result = gson.fromJson(json, Map.class);
            c.output(KV.of(((Double) result.get("id")).longValue(), json));
        }
    }

    private static class JsonToTableRowConvertFn extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Gson gson = new Gson();
            TableRow tableRow = gson.fromJson(c.element(), TableRow.class);
            c.output(tableRow);
        }
    }

    public static void main(String[] args) {
        PubSubToBQOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(PubSubToBQOptions.class);
        Pipeline p = Pipeline.create(options);
        PCollection<KV<Long, String>> input = p.apply("Read JSON from PubSub",
                PubsubIO.readStrings().fromSubscription(options.getSubscriptionPath()))
                .apply("Split to windows", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("Extract ID from json and make pair",
                        ParDo.of(new JsonToIdFn()));

        PCollection<KV<Long, String>> enrichedData = input
                .apply("Collect window ids", Combine.globally(new CollectIds()).withoutDefaults())
                .apply("Read additional info",
                        JdbcIO.<List<Long>, KV<Long, String>>readAll()
                                .withDataSourceConfiguration(
                                        JdbcIO.DataSourceConfiguration
                                                .create(options.getDriverClassName(), options.getUrl())
                                                .withUsername(options.getUsername())
                                                .withPassword(options.getPassword()))
                                .withQuery("SELECT id, user_location_country, user_location_region, user_location_city" +
                                        " FROM avrotable WHERE id = ANY (?)")
                                .withCoder(KvCoder.of(VarLongCoder.of(), StringUtf8Coder.of()))
                                .withParameterSetter((JdbcIO.PreparedStatementSetter<List<Long>>)
                                        (element, preparedStatement) -> {
                                    Array ids =
                                            preparedStatement.getConnection().createArrayOf(
                                                    "BIGINT", element.toArray(new Long[0]));
                                    preparedStatement.setArray(1, ids);
                                }).withRowMapper((JdbcIO.RowMapper<KV<Long, String>>) resultSet -> {
                            int size = resultSet.getMetaData().getColumnCount();
                            ObjectMapper mapper = new ObjectMapper();
                            ObjectNode record = mapper.createObjectNode();
                            for (int i = 2; i <= size; i++) {
                                String name = resultSet.getMetaData().getColumnName(i);
                                Object value = resultSet.getObject(i);
                                record.putPOJO(name, value);
                            }
                            Long id = resultSet.getLong("id");
                            return KV.of(id, mapper.writeValueAsString(record));
                        }));

        GroupByKey.applicableTo(enrichedData);
        TupleTag<String> INPUT_TAG = new TupleTag<>();
        TupleTag<String> ENRICHED_TAG = new TupleTag<>();

        PCollection<String> result = KeyedPCollectionTuple
                .of(INPUT_TAG, input)
                .and(ENRICHED_TAG, enrichedData)
                .apply("Merge pcollections", CoGroupByKey.create())
                .apply("Compose Final Object",
                        ParDo.of(new DoFn<KV<Long, CoGbkResult>, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                String originalJson = c.element().getValue().getOnly(INPUT_TAG);
                                String additionalJson = c.element().getValue().getOnly(ENRICHED_TAG);

                                ObjectMapper mapper = new ObjectMapper();
                                ObjectNode record = mapper.createObjectNode();
                                try {
                                    JsonNode inputRecord = mapper.readTree(originalJson);
                                    JsonNode additionalRecord = mapper.readTree(additionalJson);
                                    for (Iterator<Map.Entry<String, JsonNode>> it = inputRecord.fields();
                                         it.hasNext(); ) {
                                        Map.Entry<String, JsonNode> next = it.next();
                                        record.putPOJO(next.getKey(), next.getValue());
                                    }
                                    for (Iterator<Map.Entry<String, JsonNode>> it = additionalRecord.fields();
                                         it.hasNext(); ) {
                                        Map.Entry<String, JsonNode> next = it.next();
                                        record.putPOJO(next.getKey(), next.getValue());
                                    }
                                } catch (JsonProcessingException e) {
                                    e.printStackTrace();
                                }
                                String json = null;
                                try {
                                    json = mapper.writeValueAsString(record);
                                } catch (JsonProcessingException e) {
                                    e.printStackTrace();
                                }
                                c.output(json);
                            }
                        }));

        result.apply("Convert JSON to TableRow", ParDo.of(new JsonToTableRowConvertFn()))
                .apply("Write data to BigQuery",
                        BigQueryIO.writeTableRows()
                                .withSchema(BQPubSubTableSchema.createSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .to(options.getOutputTable()));

        p.run().waitUntilFinish();
    }
}
