package by.nasch.datapipeline.udaf;

import java.util.HashSet;
import java.util.Set;

import static org.apache.beam.sdk.transforms.Combine.CombineFn;

public class CountDistinct extends CombineFn<Object, Set<Object>, Integer> {
    @Override
    public Set<Object> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<Object> addInput(Set<Object> mutableAccumulator, Object input) {
        mutableAccumulator.add(input);
        return mutableAccumulator;
    }

    @Override
    public Set<Object> mergeAccumulators(Iterable<Set<Object>> accumulators) {
        Set<Object> set = new HashSet<>();
        for (Set<Object> accumulator : accumulators) {
            set.addAll(accumulator);
        }
        return set;
    }

    @Override
    public Integer extractOutput(Set<Object> accumulator) {
        return accumulator.size();
    }
}
