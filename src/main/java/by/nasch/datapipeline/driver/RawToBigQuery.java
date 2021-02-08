package by.nasch.datapipeline.driver;

import by.nasch.datapipeline.fn.CounterMetricFn;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class RawToBigQuery {

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

    private static final Logger LOGGER = LoggerFactory.getLogger(RawToBigQuery.class);

    public static void main(String[] args) {
        RawToBigQueryOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(RawToBigQueryOptions.class);
        options.setPlannerName("org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner");

        Pipeline p = Pipeline.create(options);
        Schema schema = new Schema.Parser().parse(SCHEMA);
        p.apply("Read .avro data from GCP Bucket", readAvro(options, schema))
                .apply("Convert GenericRecord to Row", ParDo.of(new GenericRecordToRowFn(schema)))
                .setCoder(RowCoder.of(AvroUtils.toBeamSchema(schema)))
                .setRowSchema(AvroUtils.toBeamSchema(schema))
                .apply("Perform SQL Transform",
                        SqlTransform.query("SELECT * from PCOLLECTION limit 5"))
                .apply("Convert Row to TableRow", ParDo.of(new RowToTableRow(schema)))
                .apply("Write data to BigQuery", CounterAndWriteUnited.of(
                        BigQueryIO.writeTableRows()
                                .withSchema(BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(schema)))
                                .withCreateDisposition(options.getCreateDisposition())
                                .withWriteDisposition(options.getWriteDisposition())
                                .to(options.getOutputTable())));
        p.run().waitUntilFinish();
        LOGGER.debug("Raw To BigQuery job finished.");
    }

    public interface RawToBigQueryOptions extends BeamSqlPipelineOptions {
        @Validation.Required
        @Description("The GCS location of the avro files")
        ValueProvider<String> getInputFilePattern();
        void setInputFilePattern(ValueProvider<String> value);

        @Validation.Required
        @Description("Output table specification in the form [project_id]:[dataset_id].[table_id] to write to")
        ValueProvider<String> getOutputTable();
        void setOutputTable(ValueProvider<String> value);

        @Validation.Required
        @Description("Specifies what to do with existing data in the table, in case the table already exists")
        @Default.Enum(value = "WRITE_TRUNCATE")
        BigQueryIO.Write.WriteDisposition getWriteDisposition();
        void setWriteDisposition(BigQueryIO.Write.WriteDisposition value);

        @Validation.Required
        @Description("Specifies what to do when the table does not exist")
        @Default.Enum(value = "CREATE_IF_NEEDED")
        BigQueryIO.Write.CreateDisposition getCreateDisposition();
        void setCreateDisposition(BigQueryIO.Write.CreateDisposition value);
    }

    private static AvroIO.Read<GenericRecord> readAvro(RawToBigQueryOptions options, Schema schema) {
        return AvroIO
                .readGenericRecords(schema)
                .from(options.getInputFilePattern());
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
}

class CounterAndWriteUnited<T, OutputT extends POutput> extends PTransform<PCollection<T>, OutputT> {

    private static final String NAMESPACE = "by.nasch.datapipeline";
    private static final String METRIC_COUNTER = "count_bq_writes";
    private final PTransform<PCollection<T>, OutputT> wrappedTransform;
    private final Counter counter = Metrics.counter(NAMESPACE, METRIC_COUNTER);

    public CounterAndWriteUnited(PTransform<PCollection<T>, OutputT> wrappedTransform) {
        this.wrappedTransform = wrappedTransform;
    }

    @Override
    public OutputT expand(PCollection<T> input) {
        PCollection<T> sinkInput =
                input.apply("Count sink input", ParDo.of(new CounterMetricFn<>(counter)));
        return wrappedTransform.expand(sinkInput);
    }

    public static <T, OutputT extends POutput> CounterAndWriteUnited<T, OutputT>
    of(PTransform<PCollection<T>, OutputT> sinkTransform) {
        return new CounterAndWriteUnited<>(sinkTransform);
    }
}
