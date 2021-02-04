package by.nasch.datapipeline.udaf;

import java.util.HashSet;
import java.util.Set;

import static org.apache.beam.sdk.transforms.Combine.CombineFn;

public class CountDistinct extends CombineFn<Long, Set<Long>, Integer> {
    @Override
    public Set<Long> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<Long> addInput(Set<Long> mutableAccumulator, Long input) {
        mutableAccumulator.add(input);
        return mutableAccumulator;
    }

    @Override
    public Set<Long> mergeAccumulators(Iterable<Set<Long>> accumulators) {
        Set<Long> set = new HashSet<>();
        for (Set<Long> accumulator : accumulators) {
            set.addAll(accumulator);
        }
        return set;
    }

    @Override
    public Integer extractOutput(Set<Long> accumulator) {
        return accumulator.size();
    }
}
