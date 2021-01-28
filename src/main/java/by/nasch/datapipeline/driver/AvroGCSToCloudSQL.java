package by.nasch.datapipeline.driver;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Predicate;


public class AvroGCSToCloudSQL {

    public static void main(String[] args) {

        AvroGCSToCloudSQLOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(AvroGCSToCloudSQLOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read .avro data from GCP Bucket", readAvro(options))
                .apply("Convert JSON to TableRow", ParDo.of(new AvroGCSToCloudSQL.JsonToTableRowFn()))
                .apply("Write data to Postgre CloudSQL",
                        JdbcIO.<TableRow>write()
                                .withDataSourceConfiguration(
                                        JdbcIO.DataSourceConfiguration
                                                .create(options.getDriverClassName(), options.getUrl())
                                                .withUsername(options.getUsername())
                                                .withPassword(options.getPassword()))
                                .withStatement("insert into avrotable " +
                                        "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                                .withPreparedStatementSetter(new JsonStatementSetter())
                );
        pipeline.run().waitUntilFinish();
    }

    public interface AvroGCSToCloudSQLOptions extends PipelineOptions {
        @Validation.Required
        @Description("The GCS location of the avro files")
        ValueProvider<String> getInputFilePattern();
        void setInputFilePattern(ValueProvider<String> value);

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
    }

    private static AvroIO.Parse<String> readAvro(AvroGCSToCloudSQLOptions options) {
        return AvroIO
                .parseGenericRecords(new GenericRecordToJsonWithDatesConversionFn())
                .withCoder(StringUtf8Coder.of())
                .from(options.getInputFilePattern());
    }

    private static class JsonStatementSetter implements JdbcIO.PreparedStatementSetter<TableRow> {

        @Override
        public void setParameters(TableRow r, PreparedStatement preparedStatement) throws Exception {
            int count = 1;
            for (String key : r.keySet()) {
                preparedStatement.setObject(count++, r.get(key));
            }
        }
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
