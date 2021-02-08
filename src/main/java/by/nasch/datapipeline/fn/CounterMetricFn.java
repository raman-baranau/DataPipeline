package by.nasch.datapipeline.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;

public class CounterMetricFn<T> extends DoFn<T, T> {

    private final Counter counter;

    public CounterMetricFn(Counter counter) {
        this.counter = counter;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        counter.inc();
        context.output(context.element());//null
    }
}
