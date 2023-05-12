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

import io.trino.array.BooleanBigArray;
import io.trino.array.LongBigArray;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupId;
import org.openjdk.jol.info.ClassLayout;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.System.arraycopy;

public class NullableInt128StateFactory
        implements AccumulatorStateFactory<NullableInt128State>
{
    @Override
    public NullableInt128State createSingleState()
    {
        return new SingleNullableInt128State();
    }

    @Override
    public NullableInt128State createGroupedState()
    {
        return new GroupedNullableInt128State();
    }

    public static class GroupedNullableInt128State implements NullableInt128State
    {
        private static final int INSTANCE_SIZE = (int) ClassLayout.parseClass(GroupedNullableInt128State.class).instanceSize();
        /**
         * Stores 128-bit decimals as pairs of longs
         */
        protected final LongBigArray unscaledDecimals = new LongBigArray();

        protected final BooleanBigArray isNotNull = new BooleanBigArray();

        @Override
        public void ensureCapacity(long size)
        {
            unscaledDecimals.ensureCapacity(size * 2);
            isNotNull.ensureCapacity(size);
        }

        @Override
        public long[] getArray(long groupId)
        {
            return unscaledDecimals.getSegment(groupId * 2);
        }

        @Override
        public int getArrayOffset(long groupId)
        {
            return unscaledDecimals.getOffset(groupId * 2);
        }

        @Override
        public boolean isNotNull(long groupId) {
            return isNotNull.get(groupId);
        }

        @Override
        public void setIsNotNull(long groupId, boolean isNotNull) {
            this.isNotNull.set(groupId, isNotNull);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + unscaledDecimals.sizeOf() + isNotNull.sizeOf();
        }
    }

    public static class SingleNullableInt128State
            implements NullableInt128State
    {
        private static final int INSTANCE_SIZE = (int) ClassLayout.parseClass(SingleNullableInt128State.class).instanceSize();
        protected static final int SIZE = (int) sizeOf(new long[2]);

        protected final long[] unscaledDecimal = new long[2];
        protected boolean isNotNull = false;

        public SingleNullableInt128State() {}

        // for copying
        private SingleNullableInt128State(long[] unscaledDecimal, boolean isNotNull)
        {
            arraycopy(unscaledDecimal, 0, this.unscaledDecimal, 0, 2);
            this.isNotNull = isNotNull;
        }

        @Override
        public long[] getArray(@GroupId long groupId)
        {
            return unscaledDecimal;
        }

        @Override
        public int getArrayOffset(@GroupId long groupId)
        {
            return 0;
        }

        @Override
        public boolean isNotNull(long groupId) {
            return isNotNull;
        }

        @Override
        public void setIsNotNull(long groupId, boolean isNotNull) {
            this.isNotNull = isNotNull;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + SIZE;
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleNullableInt128State(unscaledDecimal, this.isNotNull);
        }
    }
}
