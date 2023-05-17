package io.trino.operator.aggregation;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;

public class DecimalAverageStateFactory
        implements AccumulatorStateFactory<DecimalAverageAggregation.DecimalAverageAccumulatorState>,
        NewAccumulatorStateFactory<DecimalAverageAggregation.DecimalAverageAccumulatorState> {
    @Override
    public DecimalAverageAggregation.DecimalAverageAccumulatorState createSingleState() {
        return new DecimalAverageAggregation.DecimalAverageState();
    }

    @Override
    public DecimalAverageAggregation.DecimalAverageAccumulatorState createGroupedState() {
        return new DecimalAverageAggregation.DecimalAverageState();
    }

    public ObjectBigArray<DecimalAverageAggregation.DecimalAverageAccumulatorState> createNewGroupedState() {
        return new ObjectBigArray<>(DecimalAverageAggregation.DecimalAverageState::new);
    }
}
