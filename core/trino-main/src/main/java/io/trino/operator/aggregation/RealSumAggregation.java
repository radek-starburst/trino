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

import io.trino.operator.aggregation.state.NullableDoubleState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("sum")
public final class RealSumAggregation
{
    private RealSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState NullableDoubleState state, @SqlType(StandardTypes.REAL) long value, @GroupId long groupId)
    {
        state.setNull(groupId, false);
        state.setValue(groupId, state.getValue(groupId) + intBitsToFloat((int) value));
    }

    @CombineFunction
    public static void combine(@AggregationState NullableDoubleState state, @AggregationState NullableDoubleState otherState, @GroupId long groupId)
    {
        if (state.isNull(groupId)) {
            if (otherState.isNull(groupId)) {
                return;
            }
            state.set(groupId, otherState);
            return;
        }

        if (!otherState.isNull(groupId)) {
            state.setValue(groupId, state.getValue(groupId) + otherState.getValue(groupId));
        }
    }

    @OutputFunction(StandardTypes.REAL)
    public static void output(@AggregationState NullableDoubleState state, BlockBuilder out, @GroupId long groupId)
    {
        if (state.isNull(groupId)) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToRawIntBits((float) state.getValue(groupId)));
        }
    }
}
