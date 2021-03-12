package by.nasch.datapipeline.driver;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class SimpleTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    private static class SimpleFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String a = c.element();
            c.output(a + "%");
        }
    }

    @Test
    public void test() {
        PCollection<String> input = pipeline.apply(Create.of(Arrays.asList("one", "two", "three", "four", "five")));
        PCollection<String> output = input.apply(ParDo.of(new SimpleFn()));
        PAssert.that(output)
                .containsInAnyOrder("one%", "two%", "three%", "four%", "five%");
        pipeline.run();
    }
}
