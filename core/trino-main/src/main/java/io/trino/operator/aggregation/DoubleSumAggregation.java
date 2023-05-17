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

import io.trino.operator.aggregation.state.LongAndDoubleState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.RemoveInputFunction;
import io.trino.spi.function.SpecificAggregationState;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction("sum")
public final class DoubleSumAggregation
{
    private DoubleSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState DoubleSumState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setCounter(state.getCounter() + 1);
        state.setSum(state.getSum() + value);
    }

    @RemoveInputFunction
    public static void removeInput(@AggregationState DoubleSumState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setCounter(state.getCounter() - 1);
        state.setSum(state.getSum() - value);
    }

    @CombineFunction
    public static void combine(@AggregationState DoubleSumState state, @AggregationState DoubleSumState otherState)
    {
        state.setCounter(state.getCounter() + otherState.getCounter());
        state.setSum(state.getSum() + otherState.getSum());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState DoubleSumState state, BlockBuilder out)
    {
        if (state.getCounter() == 0) {
            out.appendNull();
        }
        else {
            DoubleType.DOUBLE.writeDouble(out, state.getSum());
        }
    }

    @AccumulatorStateMetadata(stateFactoryClass = DoubleSumStateFactory.class, stateSerializerClass = DoubleSumStateSerializer.class)
    public interface DoubleSumAccumulatorState extends AccumulatorState {
        double getSum();
        long getCounter();

        void setCounter(long counter);

        void setSum(double sum);
    }

    public static class DoubleSumState implements DoubleSumAccumulatorState {

        public DoubleSumState(double sum, long counter) {
            this.counter = counter;
            this.sum = sum;
        }

        public DoubleSumState() {

        }

        private double sum;
        private long counter;

        public double getSum() {
            return sum;
        }
        public long getCounter() {
            return counter;
        }

        public void setCounter(long counter) {
            this.counter = counter;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        @Override
        public long getEstimatedSize() {
            return 0;
        }

        @Override
        public AccumulatorState copy() {
            return new DoubleSumState(sum, counter);
        }
    }
}
