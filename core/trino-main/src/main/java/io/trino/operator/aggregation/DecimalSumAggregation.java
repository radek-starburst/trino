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
import io.trino.operator.aggregation.state.*;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.GroupId;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.type.Int128Math.addWithOverflow;

@AggregationFunction("sum")
@Description("Calculates the sum over the input values")
public final class DecimalSumAggregation
{
    private DecimalSumAggregation() {}

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputShortDecimal(
            @AggregationState Int128State decimalState,
            @AggregationState LongState overflowState,
            @AggregationState BooleanState isEmptyState,
            @SqlType("decimal(p,s)") long rightLow,
            @GroupId long groupId)
    {
        isEmptyState.setValue(groupId, true);

        long[] decimal = decimalState.getArray(groupId);
        int offset = decimalState.getArrayOffset(groupId);

        long rightHigh = rightLow >> 63;

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                rightHigh,
                rightLow,
                decimal,
                offset);

        if (overflow != 0) {
            overflowState.setValue(groupId, overflow + overflowState.getValue(groupId));
        }
    }

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputLongDecimal(
            @AggregationState Int128State decimalState,
            @AggregationState LongState overflowState,
            @AggregationState BooleanState isEmptyState,
            @BlockPosition @SqlType(value = "decimal(p,s)", nativeContainerType = Int128.class) Block block,
            @BlockIndex int position,
            @GroupId long groupId) {
        isEmptyState.setValue(groupId, true);

        long[] decimal = decimalState.getArray(groupId);
        int offset = decimalState.getArrayOffset(groupId);

        long rightHigh = block.getLong(position, 0);
        long rightLow = block.getLong(position, SIZE_OF_LONG);

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                rightHigh,
                rightLow,
                decimal,
                offset);

        if (overflow != 0) {
            overflowState.setValue(groupId, overflow + overflowState.getValue(groupId));
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState Int128State decimalState,
            @AggregationState LongState overflowState,
            @AggregationState BooleanState isEmptyState,
            @AggregationState Int128State otherDecimalState,
            @AggregationState LongState otherOverflowState,
            @AggregationState BooleanState otherIsEmptyState,
            @GroupId long groupId)
    {
        if (!otherIsEmptyState.getValue(groupId)) {
            return;
        }

        long[] decimal = decimalState.getArray(groupId);
        int decimalOffset = decimalState.getArrayOffset(groupId);
        long[] otherDecimal = otherDecimalState.getArray(groupId);
        int otherDecimalOffset = otherDecimalState.getArrayOffset(groupId);

        long overflow = addWithOverflow(
                decimal[decimalOffset],
                decimal[decimalOffset + 1],
                otherDecimal[otherDecimalOffset],
                otherDecimal[otherDecimalOffset + 1],
                decimal,
                decimalOffset);

        isEmptyState.setValue(groupId, true);

        if(overflow != 0 || otherOverflowState.getValue(groupId) != 0) {
            overflowState.setValue(groupId, overflowState.getValue(groupId) + overflow + otherOverflowState.getValue(groupId));
        }
    }

    @OutputFunction("decimal(38,s)")
    public static void outputLongDecimal(
            @AggregationState Int128State decimalState,
            @AggregationState LongState overflowState,
            @AggregationState BooleanState isEmptyState,
            BlockBuilder out,
            @GroupId long groupId)
    {
        if (isEmptyState.getValue(groupId)) {
            if (overflowState.getValue(groupId) != 0) {
                throw new ArithmeticException("Decimal overflow");
            }

            long[] decimal = decimalState.getArray(groupId);
            int offset = decimalState.getArrayOffset(groupId);

            long rawHigh = decimal[offset];
            long rawLow = decimal[offset + 1];

            Decimals.throwIfOverflows(rawHigh, rawLow);
            out.writeLong(rawHigh);
            out.writeLong(rawLow);
            out.closeEntry();
        }
        else {
            out.appendNull();
        }
    }
}
