package by.nasch.datapipeline.driver;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.io.IOException;

public class SimpleDriver {

    private static final String BASE_DIR =
            "C:\\Users\\Raman_Baranau\\IdeaProjects" +
            "\\DataPipeline\\src\\main" +
            "\\resources\\";

    public static void main(String[] args) {
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader =
                null;
        try {
            dataFileReader = new DataFileReader<>(
                    new File(BASE_DIR + "test-dataset.avro"), datumReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Schema schema = dataFileReader.getSchema();

        PCollection<GenericRecord> lines = p.apply(
                "ReadDataset",
                AvroIO.readGenericRecords(schema)
                        .from(BASE_DIR + "test-dataset.avro"));
        lines.apply("WriteDataset",
                AvroIO.writeGenericRecords(schema)
                        .withoutSharding()
                        .withSuffix(".avro")
                        .to("gs://beam-beam/dataset"));
        p.run().waitUntilFinish();
    }
}
