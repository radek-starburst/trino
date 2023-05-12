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
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.GroupId;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static java.lang.Math.round;

@AggregationFunction("avg")
public final class IntervalDayToSecondAverageAggregation
{
    private IntervalDayToSecondAverageAggregation() {}

    @InputFunction
    public static void input(LongAndDoubleState state, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long value, @GroupId long groupId)
    {
        state.setLong(groupId, state.getLong(groupId) + 1);
        state.setDouble(groupId, state.getDouble(groupId) + value);
    }

    @CombineFunction
    public static void combine(LongAndDoubleState state, LongAndDoubleState otherState, @GroupId long groupId)
    {
        state.setLong(groupId, state.getLong(groupId) + otherState.getLong(groupId));
        state.setDouble(groupId,state.getDouble(groupId) + otherState.getDouble(groupId));
    }

    @OutputFunction(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static void output(LongAndDoubleState state, BlockBuilder out, @GroupId long groupId)
    {
        long count = state.getLong(groupId);
        if (count == 0) {
            out.appendNull();
        }
        else {
            double value = state.getDouble(groupId);
            INTERVAL_DAY_TIME.writeLong(out, round(value / count));
        }
    }
}
