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
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.GroupId;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("geometric_mean")
public final class RealGeometricMeanAggregations
{
    private RealGeometricMeanAggregations() {}

    @InputFunction
    public static void input(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.REAL) long value, @GroupId long groupId)
    {
        state.setLong(groupId, state.getLong(groupId) + 1);
        state.setDouble(groupId, state.getDouble(groupId) + Math.log(intBitsToFloat((int) value)));
    }

    @CombineFunction
    public static void combine(@AggregationState LongAndDoubleState state, @AggregationState LongAndDoubleState otherState, @GroupId long groupId)
    {
        state.setLong(groupId, state.getLong(groupId) + otherState.getLong(groupId));
        state.setDouble(groupId, state.getDouble(groupId) + otherState.getDouble(groupId));
    }

    @OutputFunction(StandardTypes.REAL)
    public static void output(@AggregationState LongAndDoubleState state, BlockBuilder out, @GroupId long groupId)
    {
        long count = state.getLong(groupId);
        if (count == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToRawIntBits((float) Math.exp(state.getDouble(groupId) / count)));
        }
    }
}
