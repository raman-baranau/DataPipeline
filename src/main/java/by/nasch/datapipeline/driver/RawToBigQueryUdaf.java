package by.nasch.datapipeline.driver;

import by.nasch.datapipeline.udaf.CountDistinct;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

public class RawToBigQueryUdaf {

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

    public interface RawToBigQueryUdafOptions extends BeamSqlPipelineOptions {
        @Validation.Required
        @Description("The GCS location of the avro files")
        ValueProvider<String> getInputFilePattern();
        void setInputFilePattern(ValueProvider<String> value);

        @Validation.Required
        @Description("Output table specification in the form [project_id]:[dataset_id].[table_id] to write to")
        ValueProvider<String> getOutputTable();
        void setOutputTable(ValueProvider<String> value);
    }

    private static AvroIO.Parse<Row> readAvro(RawToBigQueryUdafOptions options,
                                              org.apache.beam.sdk.schemas.Schema schema) {
        return AvroIO
                .parseGenericRecords(new RawToBigQueryUdaf.GenericRecordToRowFn())
                .withCoder(RowCoder.of(schema))
                .from(options.getInputFilePattern());
    }

    private static class GenericRecordToRowFn implements SerializableFunction<GenericRecord, Row> {

        @Override
        public Row apply(GenericRecord input) {
            return AvroUtils.toBeamRowStrict(input, AvroUtils.toBeamSchema(input.getSchema()));
        }
    }

    private static class RowToTableRow extends DoFn<Row, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow();
            row.set("unique_count", c.element().getInt32(0));
            c.output(row);
        }
    }

    public static void main(String[] args) {
        RawToBigQueryUdafOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(RawToBigQueryUdafOptions.class);
        options.setPlannerName("org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner");

        Pipeline p = Pipeline.create(options);
        p.apply("Read .avro data from GCP Bucket",
                readAvro(options, AvroUtils.toBeamSchema(new Schema.Parser().parse(SCHEMA))))
                .apply("Perform SQL Transform",
                        SqlTransform.query("SELECT count_distinct(hotel_id) from PCOLLECTION")
                                .registerUdaf("count_distinct", new CountDistinct()))
                .apply("Convert Row to TableRow", ParDo.of(new RowToTableRow()))
                .apply("Write data to BigQuery",
                        BigQueryIO.writeTableRows()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .to(options.getOutputTable()));
        p.run().waitUntilFinish();
    }
}
