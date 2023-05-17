package io.trino.operator.aggregation;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;

public class DecimalAverageStateSerializer implements AccumulatorStateSerializer<DecimalAverageAggregation.DecimalAverageAccumulatorState> {

    @Override
    public Type getSerializedType() {
        return BigintType.BIGINT;
    }

    @Override
    public void serialize(DecimalAverageAggregation.DecimalAverageAccumulatorState state, BlockBuilder out) {
        out.writeLong(state.getCounter());
        out.writeLong(state.getDecimal()[0]);
        out.writeLong(state.getDecimal()[1]);
        if (state.isOverflowNull()) {
            out.appendNull();
        } else {
            out.writeLong(state.getOverflow());
        }
    }

    @Override
    public void deserialize(Block block, int index, DecimalAverageAggregation.DecimalAverageAccumulatorState state) {
        state.setCounter(block.getLong(index * 4, 0));
        state.setDecimals(new long[] {block.getLong(index * 4 + 1, 0), block.getLong(index * 4 + 2, 0)});
        if(block.isNull(index * 4 + 3)) {
            state.setOverflow(block.getLong(index * 4 + 3, 0));
            state.setOverflowIsNull(true);
        } else {
            state.setOverflow(0);               // mozna uniknac ifow.
            state.setOverflowIsNull(false);
        }
    }

    @Override
    public int getPositionsPerState() {
        return 4;
    }
}
