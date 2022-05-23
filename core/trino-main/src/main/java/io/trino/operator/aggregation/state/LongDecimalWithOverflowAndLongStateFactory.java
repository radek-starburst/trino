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
package io.trino.operator.aggregation.state;

import io.trino.array.DoubleBigArray;
import io.trino.array.LongBigArray;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

public class LongDecimalWithOverflowAndLongStateFactory
        implements AccumulatorStateFactory<LongDecimalWithOverflowAndLongState>
{
    @Override
    public LongDecimalWithOverflowAndLongState createSingleState()
    {
        return new SingleLongDecimalWithOverflowAndLongState();
    }

    @Override
    public LongDecimalWithOverflowAndLongState createGroupedState()
    {
        return new GroupedLongDecimalWithOverflowAndLongState();
    }

    public static class GroupedLongDecimalWithOverflowAndLongState
            extends LongDecimalWithOverflowStateFactory.GroupedLongDecimalWithOverflowState
            implements LongDecimalWithOverflowAndLongState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedLongDecimalWithOverflowAndLongState.class).instanceSize();
        private final DoubleBigArray doubles = new DoubleBigArray();

        @Override
        public void ensureCapacity(long size)
        {
            doubles.ensureCapacity(size);
            super.ensureCapacity(size);
        }

        @Override
        public double getDouble()
        {
            return doubles.get(getGroupId());
        }

        @Override
        public void setDouble(double value)
        {
            doubles.set(getGroupId(), value);
        }

        @Override
        public void addDouble(double value)
        {
            doubles.add(getGroupId(), value);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + isNotNull.sizeOf() + unscaledDecimals.sizeOf() + (overflows == null ? 0 : overflows.sizeOf());
        }
    }

    public static class SingleLongDecimalWithOverflowAndLongState
            extends LongDecimalWithOverflowStateFactory.SingleLongDecimalWithOverflowState
            implements LongDecimalWithOverflowAndLongState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleLongDecimalWithOverflowAndLongState.class).instanceSize();

        protected double doubleValue;

        public SingleLongDecimalWithOverflowAndLongState() {}

        // for copying
        private SingleLongDecimalWithOverflowAndLongState(double longValue)
        {
            this.doubleValue = longValue;
        }

        @Override
        public double getDouble()
        {
            return doubleValue;
        }

        @Override
        public void setDouble(double longValue)
        {
            this.doubleValue = longValue;
        }

        @Override
        public void addDouble(double value)
        {
            doubleValue += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + SIZE;
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleLongDecimalWithOverflowAndLongState(doubleValue);
        }
    }
}
