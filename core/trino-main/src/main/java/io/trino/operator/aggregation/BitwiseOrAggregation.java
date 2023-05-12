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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction("bitwise_or_agg")
public final class BitwiseOrAggregation
{
    private BitwiseOrAggregation() {}

    @InputFunction
    public static void bitOr(@AggregationState NullableLongState state, @SqlType(StandardTypes.BIGINT) long value, @GroupId long groupId)
    {
        if (state.isNull(groupId)) {
            state.setValue(groupId, value);
        }
        else {
            state.setValue(groupId, state.getValue(groupId) | value);
        }

        state.setNull(groupId, false);
    }

    @CombineFunction
    public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState otherState, @GroupId long groupId)
    {
        if (state.isNull(groupId)) {
            state.set(groupId, otherState);
        }
        else if (!otherState.isNull(groupId)) {
            state.setValue(groupId, state.getValue(groupId) | otherState.getValue(groupId));
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState NullableLongState state, BlockBuilder out, @GroupId long groupId)
    {
        NullableLongState.write(groupId, BigintType.BIGINT, state, out);
    }
}
