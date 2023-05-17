package io.trino.operator.aggregation;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;

public class DoubleSumStateFactory implements
        AccumulatorStateFactory<DoubleSumAggregation.DoubleSumAccumulatorState>,
        NewAccumulatorStateFactory<DoubleSumAggregation.DoubleSumAccumulatorState> {
    @Override
    public DoubleSumAggregation.DoubleSumAccumulatorState createSingleState() {
        return new DoubleSumAggregation.DoubleSumState();
    }

    @Override
    public DoubleSumAggregation.DoubleSumAccumulatorState createGroupedState() {
        return new DoubleSumAggregation.DoubleSumState();
    }

    public ObjectBigArray<DoubleSumAggregation.DoubleSumAccumulatorState> createNewGroupedState() {
        return new ObjectBigArray<>(DoubleSumAggregation.DoubleSumState::new);
    }
}
