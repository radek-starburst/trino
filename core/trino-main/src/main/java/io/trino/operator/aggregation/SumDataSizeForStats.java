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

import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction(value = SumDataSizeForStats.NAME, hidden = true)
public final class SumDataSizeForStats
{
    public static final String NAME = "$internal$sum_data_size_for_stats";

    private SumDataSizeForStats() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState NullableLongState state, @BlockPosition @SqlType("T") Block block, @BlockIndex int index, @GroupId long groupId)
    {
        update(state, block.getEstimatedDataSizeForStats(index), groupId);
    }

    @CombineFunction
    public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState otherState, @GroupId long groupId)
    {
        update(state, otherState.getValue(groupId), groupId);
    }

    private static void update(NullableLongState state, long size, long groupId)
    {
        if (state.isNull(groupId)) {
            state.setNull(groupId, false);
            state.setValue(groupId, size);
        }
        else {
            state.setValue(groupId, state.getValue(groupId) + size);
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState NullableLongState state, BlockBuilder out, @GroupId long groupId)
    {
        NullableLongState.write(groupId, BigintType.BIGINT, state, out);
    }
}
