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

import io.trino.operator.aggregation.state.LongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;

@AggregationFunction("count_if")
public final class CountIfAggregation
{
    private CountIfAggregation() {}

    @InputFunction
    public static void input(@AggregationState LongState state, @SqlType(StandardTypes.BOOLEAN) boolean value, @GroupId long groupId)
    {
        if (value) {
            state.setValue(groupId, state.getValue(groupId) + 1);
        }
    }

    @RemoveInputFunction
    public static void removeInput(@AggregationState LongState state, @SqlType(StandardTypes.BOOLEAN) boolean value, @GroupId long groupId)
    {
        if (value) {
            state.setValue(groupId, state.getValue(groupId) - 1);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState LongState state, @AggregationState LongState otherState, @GroupId long groupId)
    {
        state.setValue(groupId, state.getValue(groupId) + otherState.getValue(groupId));
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState LongState state, BlockBuilder out, @GroupId long groupId)
    {
        BIGINT.writeLong(out, state.getValue(groupId));
    }
}
