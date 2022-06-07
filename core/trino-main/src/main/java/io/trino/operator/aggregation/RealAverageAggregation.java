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

import io.trino.operator.aggregation.state.DoubleState;
import io.trino.operator.aggregation.state.LongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.RemoveInputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.GroupId;

import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("avg")
@Description("Returns the average value of the argument")
public final class RealAverageAggregation
{
    private RealAverageAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState LongState count,
            @AggregationState DoubleState sum,
            @SqlType("REAL") long value,
            @GroupId long groupId)
    {
        count.setValue(groupId, count.getValue(groupId) + 1);
        sum.setValue(groupId, sum.getValue(groupId) + intBitsToFloat((int) value));
    }

    @RemoveInputFunction
    public static void removeInput(
            @AggregationState LongState count,
            @AggregationState DoubleState sum,
            @SqlType("REAL") long value,
            @GroupId long groupId)
    {
        count.setValue(groupId, count.getValue(groupId) - 1);
        sum.setValue(groupId, sum.getValue(groupId) - intBitsToFloat((int) value));
    }

    @CombineFunction
    public static void combine(
            @AggregationState LongState count,
            @AggregationState DoubleState sum,
            @AggregationState LongState otherCount,
            @AggregationState DoubleState otherSum,
            @GroupId long groupId)
    {
        count.setValue(groupId, count.getValue(groupId) + otherCount.getValue(groupId));
        sum.setValue(groupId, sum.getValue(groupId) + otherSum.getValue(groupId));
    }

    @OutputFunction("REAL")
    public static void output(
            @AggregationState LongState count,
            @AggregationState DoubleState sum,
            BlockBuilder out,
            @GroupId long groupId)
    {
        if (count.getValue(groupId) == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToIntBits((float) (sum.getValue(groupId) / count.getValue(groupId))));
        }
    }
}
