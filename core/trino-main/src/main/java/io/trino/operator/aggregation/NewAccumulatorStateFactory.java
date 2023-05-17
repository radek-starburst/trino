package io.trino.operator.aggregation;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorState;

public interface NewAccumulatorStateFactory<T extends AccumulatorState>
{
    default ObjectBigArray<T> createNewGroupedState() {
        throw new UnsupportedOperationException("createNewGroupedState is not supported");
    }
}

