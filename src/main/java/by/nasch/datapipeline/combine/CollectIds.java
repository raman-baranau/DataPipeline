package by.nasch.datapipeline.combine;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.List;

public class CollectIds extends Combine.CombineFn<KV<Long, String>, List<Long>, List<Long>> {
    @Override
    public List<Long> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Long> addInput(List<Long> mutableAccumulator, KV<Long, String> input) {
        mutableAccumulator.add(input.getKey());
        return mutableAccumulator;
    }

    @Override
    public List<Long> mergeAccumulators(Iterable<List<Long>> accumulators) {
        List<Long> list = new ArrayList<>();
        for (List<Long> accumulator : accumulators) {
            list.addAll(accumulator);
        }
        return list;
    }

    @Override
    public List<Long> extractOutput(List<Long> accumulator) {
        return accumulator;
    }
}
