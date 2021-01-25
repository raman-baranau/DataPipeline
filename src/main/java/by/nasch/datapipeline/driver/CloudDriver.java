package by.nasch.datapipeline.driver;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Predicate;

/**
 * Pipeline template for ingestion data from AVRO files to BigQuery
 * <p>
 * This Maven command example creates and stages a template at the Cloud Storage location specified with --templateLocation:
 * mvn compile exec:java \
 * -Dexec.mainClass=by.nasch.datapipeline.driver.CloudDriver \
 * "-Dexec.args=--runner=DataflowRunner \
 * --project={PROJECT_NAME} \
 * --stagingLocation=gs://{BUCKET_NAME}/{staging} \
 * --writeDisposition=WRITE_TRUNCATE \
 * --templateLocation=gs://{BUCKET_NAME}/templates/{template}"
 */
public class CloudDriver {

    public static void main(String[] args) {

        CloudDriverOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(CloudDriverOptions.class);

        Pipeline p = Pipeline.create(options);
        p.apply("Read .avro data from GCP Bucket", readAvro(options))
                .apply("Convert JSON to TableRow", ParDo.of(new JsonToTableRowFn()))
                .apply("Write data to BigQuery",
                        BigQueryIO.writeTableRows()
                                .withCreateDisposition(options.getCreateDisposition())
                                .withWriteDisposition(options.getWriteDisposition())
                                .to(options.getOutputTable()));
        p.run().waitUntilFinish();
    }

    public interface CloudDriverOptions extends DataflowPipelineOptions {
        @Validation.Required
        @Description("The GCS location of the avro files you'd like to process")
        ValueProvider<String> getInputFilePattern();
        void setInputFilePattern(ValueProvider<String> value);

        @Validation.Required
        @Description("Output table specification in the form [project_id]:[dataset_id].[table_id] to write to")
        ValueProvider<String> getOutputTable();
        void setOutputTable(ValueProvider<String> value);

        @Validation.Required
        @Default.Enum(value = "WRITE_APPEND")
        @Description("Specifies what to do with existing data in the table, in case the table already exists")
        BigQueryIO.Write.WriteDisposition getWriteDisposition();
        void setWriteDisposition(BigQueryIO.Write.WriteDisposition value);

        @Validation.Required
        @Default.Enum(value = "CREATE_NEVER")
        @Description("Specifies what to do when the table does not exist")
        BigQueryIO.Write.CreateDisposition getCreateDisposition();
        void setCreateDisposition(BigQueryIO.Write.CreateDisposition value);
    }

    private static AvroIO.Parse<String> readAvro(CloudDriverOptions options) {
        return AvroIO
                .parseGenericRecords(new CloudDriver.GenericRecordToJsonWithDatesConversionFn())
                .withCoder(StringUtf8Coder.of())
                .from(options.getInputFilePattern());
    }

    private static class GenericRecordToJsonWithDatesConversionFn
            implements SerializableFunction<GenericRecord, String> {
        private final SimpleDateFormat betaDateFormatter;
        private final ObjectMapper mapper;

        public GenericRecordToJsonWithDatesConversionFn() {
            this.mapper = new ObjectMapper();
            this.mapper.getFactory().configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
            this.betaDateFormatter = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss Z");
            this.betaDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        @Override
        public String apply(GenericRecord input) {
            String json = new String(new JsonAvroConverter().convertToJson(input));
            try {
                return convertDates(input.getSchema(), json);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private String convertDates(Schema schema, String json) throws IOException {
            Predicate<Schema.Field> withSqlTypeProp =
                    field -> field.getProp("sqlType") != null;

            Predicate<Schema.Field> sqlTypeIsDate =
                    field -> Types.DATE == Integer.parseInt(field.getProp("sqlType"));

            Predicate<Schema.Field> sqlTypeIsTimestamp =
                    field -> Types.TIMESTAMP == Integer.parseInt(field.getProp("sqlType"));

            ObjectNode jsonObj = (ObjectNode) mapper.readTree(json);
            schema.getFields().stream()
                    .filter(withSqlTypeProp.and(sqlTypeIsDate.or(sqlTypeIsTimestamp)))
                    .map(Schema.Field::name)
                    .forEach(dateColumnName -> {
                        JsonNode dateNode = jsonObj.get(dateColumnName);
                        if (!dateNode.isNull()) {
                            Date date = new Date(dateNode.asLong());
                            jsonObj.put(dateColumnName, betaDateFormatter.format(date));
                        }
                    });
            return mapper.writeValueAsString(jsonObj);
        }
    }

    private static class JsonToTableRowFn extends DoFn<String, TableRow> {

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
            TableRow tableRow = convertJsonToTableRow(c.element());
            c.output(tableRow);
        }
    }
}
