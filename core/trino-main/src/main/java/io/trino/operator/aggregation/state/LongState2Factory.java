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

import io.trino.array.LongBigArray;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.System.arraycopy;

public class LongState2Factory
        implements AccumulatorStateFactory<LongState2>
{
    @Override
    public LongState2 createSingleState()
    {
        return new SingleLongState2();
    }

    @Override
    public LongState2 createGroupedState()
    {
        return new GroupedLongState2();
    }

    public static class GroupedLongState2
            extends AbstractGroupedAccumulatorState
            implements LongState2
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedLongState2.class).instanceSize();
        protected final LongBigArray values = new LongBigArray();

        @Override
        public void ensureCapacity(long size)
        {
            values.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + values.sizeOf();
        }

        @Override
        public long getValue()
        {
            return values.get(getGroupId());
        }

        @Override
        public void setValue(long value)
        {
            values.set(getGroupId(), value);
        }

        @Override
        public void setValue(int groupId, long value)
        {
            values.set(groupId, value);
        }

        @Override
        public long getValue(int groupId)
        {
            return values.get(groupId);
        }
    }

    public static class SingleLongState2
            implements LongState2
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleLongState2.class).instanceSize();
        protected static final int SIZE = (int) 8;

        protected long value = 0;

        public SingleLongState2() {}

        private SingleLongState2(long value)
        {
            this.value = value;
        }

        @Override
        public long getValue()
        {
            return value;
        }

        @Override
        public void setValue(long value)
        {
            this.value = value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + SIZE;
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleLongState2(this.value);
        }
    }
}
