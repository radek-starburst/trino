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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.GroupId;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class NullableInt128StateSerializer
        implements AccumulatorStateSerializer<NullableInt128State>
{
    @Override
    public Type getSerializedType()
    {
        return DecimalType.createDecimalType(Decimals.MAX_SHORT_PRECISION + 1);
    }

    @Override
    public void serialize(@GroupId long groupId, NullableInt128State state, BlockBuilder out)
    {
        if (state.isNotNull(groupId)) {
            long[] decimal = state.getArray(groupId);
            int offset = state.getArrayOffset(groupId);
            out.writeLong(decimal[offset]).writeLong(decimal[offset + 1]).closeEntry();
        } else {
            out.appendNull();
        }
    }

    @Override
    public void deserialize(@GroupId long groupId, Block block, int index, NullableInt128State state)
    {
        long[] decimal = state.getArray(groupId);
        int offset = state.getArrayOffset(groupId);

        if (!block.isNull(index)) {
            decimal[offset] = block.getLong(index, 0);
            decimal[offset + 1] = block.getLong(index, SIZE_OF_LONG);
            state.setIsNotNull(groupId, true);
        } else {
            state.setIsNotNull(groupId, false);
            decimal[offset] = 0;
            decimal[offset + 1] = 0;
        }
    }
}
