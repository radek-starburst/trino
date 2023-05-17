/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.aggregation;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SpecificAggregationState;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.Int128Math.addWithOverflow;
import static io.trino.spi.type.Int128Math.divideRoundUp;
import static java.math.RoundingMode.HALF_UP;

@AggregationFunction("avg")
@Description("Calculates the average value")
public final class DecimalAverageAggregation
{
    private static final BigInteger TWO = new BigInteger("2");
    private static final BigInteger OVERFLOW_MULTIPLIER = TWO.pow(128);

    private DecimalAverageAggregation() {}

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputShortDecimal(
            @AggregationState DecimalAverageAccumulatorState state,
            @SqlType("decimal(p,s)") long rightLow)
    {
        state.setCounter(state.getCounter() + 1);

        long[] decimal = state.getDecimal();
        long rightHigh = rightLow >> 63;

        long overflow = addWithOverflow(
                decimal[0],
                decimal[1],
                rightHigh,
                rightLow,
                decimal,
                0);

        if (overflow != 0) {
            state.setOverflowIsNull(false);
            state.setOverflow(overflow + state.getOverflow());
        }
    }

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputLongDecimal(
            @AggregationState DecimalAverageAccumulatorState state,
            @BlockPosition @SqlType(value = "decimal(p, s)", nativeContainerType = Int128.class) Block block,
            @BlockIndex int position)
    {
        state.setCounter(state.getCounter() + 1);

        long[] decimal = state.getDecimal();

        long rightHigh = block.getLong(position, 0);
        long rightLow = block.getLong(position, SIZE_OF_LONG);

        long overflow = addWithOverflow(
                decimal[0],
                decimal[1],
                rightHigh,
                rightLow,
                decimal,
                0);

        if (overflow != 0) {
            state.setOverflowIsNull(false);
            state.setOverflow(overflow + state.getOverflow());
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState DecimalAverageAccumulatorState state,
            @AggregationState DecimalAverageAccumulatorState otherState)
    {
        state.setCounter(state.getCounter() + otherState.getCounter());

        long[] decimal = state.getDecimal();
        long[] otherDecimal = state.getDecimal();

        long overflow = 0;
        if (state.getCounter() > 0) {
            overflow = addWithOverflow(
                    decimal[0],
                    decimal[1],
                    otherDecimal[0],
                    otherDecimal[1],
                    decimal,
                    0);
        }
        else {
            decimal[0] = otherDecimal[0];
            decimal[1] = otherDecimal[1];
        }

        boolean isOverflowStateNull = otherState.isOverflowNull();
        int isOtherOverflowNull = isOverflowStateNull ? 1 : 0;
        if (overflow != 0 || isOtherOverflowNull == 0) {
            state.setOverflow(state.getOverflow() + overflow + otherState.getOverflow() * isOtherOverflowNull);
            state.setOverflowIsNull(false);
        }
    }

    @OutputFunction("decimal(p,s)")
    public static void outputShortDecimal(
            @TypeParameter("decimal(p,s)") Type type,
            @AggregationState DecimalAverageAccumulatorState state,
            BlockBuilder out)
    {
        DecimalType decimalType = (DecimalType) type;
        if (state.getCounter() == 0) {
            out.appendNull();
            return;
        }
        Int128 average = average(state.getDecimal(), state.getCounter(), state.isOverflowNull() ? state.getOverflow() : 0, decimalType);
        if (decimalType.isShort()) {
            writeShortDecimal(out, average.toLongExact());
        }
        else {
            type.writeObject(out, average);
        }
    }

    @AccumulatorStateMetadata(stateFactoryClass = DecimalAverageStateFactory.class, stateSerializerClass = DecimalAverageStateSerializer.class)
    public interface DecimalAverageAccumulatorState extends AccumulatorState {
        long[] getDecimal();
        long getCounter();
        long getOverflow();

        boolean isOverflowNull();

        void setDecimals(long[] decimals);

        void setCounter(long counter);

        void setOverflow(long overflow);

        void setOverflowIsNull(boolean isNull);
    }

    public static class DecimalAverageState implements DecimalAverageAccumulatorState {
        private long[] decimals;
        long counter;
        long overflow;
        boolean isOverflowNull;

        public DecimalAverageState() {
            this.decimals = new long[2];
            this.isOverflowNull = true;
        }

        public DecimalAverageState(long[] decimals, long counter, long overflow, boolean isOverflowNull) {
            this.decimals = decimals;
            this.isOverflowNull = isOverflowNull;
            this.counter = counter;
            this.overflow = overflow;
        }

        public long[] getDecimal() {
            return decimals;
        }
        public long getCounter() {
            return counter;
        }
        public long getOverflow() {
            return overflow;
        }

        @Override
        public boolean isOverflowNull() {
            return isOverflowNull;
        }

        public void setDecimals(long[] decimals) {
            this.decimals = decimals;
        }

        public void setCounter(long counter) {
            this.counter = counter;
        }

        public void setOverflow(long overflow) {
            this.overflow = overflow;
        }

        @Override
        public void setOverflowIsNull(boolean isNull) {
            this.isOverflowNull = isNull;
        }

        @Override
        public long getEstimatedSize() {
            return 0;
        }

        @Override
        public AccumulatorState copy() {
            return new DecimalAverageState(this.decimals, this.counter, this.overflow, this.isOverflowNull);
        }
    }

    @VisibleForTesting
    public static Int128 average(long[] decimals, long counter, long overflow, DecimalType type)
    {
        if (overflow != 0) {
            BigDecimal sum = new BigDecimal(Int128.valueOf(decimals[0], decimals[1]).toBigInteger(), type.getScale());
            sum = sum.add(new BigDecimal(OVERFLOW_MULTIPLIER.multiply(BigInteger.valueOf(overflow))));

            BigDecimal count = BigDecimal.valueOf(counter);
            return Decimals.encodeScaledValue(sum.divide(count, type.getScale(), HALF_UP), type.getScale());
        }

        Int128 result = divideRoundUp(decimals[0], decimals[1], 0, 0, counter, 0);
        if (overflows(result)) {
            throw new ArithmeticException("Decimal overflow");
        }
        return result;
    }
}
