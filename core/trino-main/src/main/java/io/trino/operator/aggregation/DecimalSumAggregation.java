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
import io.trino.spi.function.IsStateNullFunction;
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
            @AggregationState NullableInt128State decimalState,
            @AggregationState NullableLongState overflowState,
            @SqlType("decimal(p,s)") long rightLow,
            @GroupId long groupId)
    {
        decimalState.setIsNotNull(groupId, true);

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
            overflowState.setNull(groupId, false);
            overflowState.setValue(groupId, overflow + overflowState.getValue(groupId));
        }
    }

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputLongDecimal(
            @AggregationState NullableInt128State decimalState,
            @AggregationState NullableLongState overflowState,
            @BlockPosition @SqlType(value = "decimal(p,s)", nativeContainerType = Int128.class) Block block,
            @BlockIndex int position,
            @GroupId long groupId) {
        decimalState.setIsNotNull(groupId, true);

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
            overflowState.setNull(groupId, false);
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState NullableInt128State decimalState,
            @AggregationState NullableLongState  overflowState,
            @AggregationState NullableInt128State otherDecimalState,
            @AggregationState NullableLongState otherOverflowState,
            @GroupId long groupId) {
        long[] decimal = decimalState.getArray(groupId);
        int decimalOffset = decimalState.getArrayOffset(groupId);
        long[] otherDecimal = otherDecimalState.getArray(groupId);
        int otherDecimalOffset = otherDecimalState.getArrayOffset(groupId);

        if (decimalState.isNotNull(groupId)) {
            long overflow = addWithOverflow(
                    decimal[decimalOffset],
                    decimal[decimalOffset + 1],
                    otherDecimal[otherDecimalOffset],
                    otherDecimal[otherDecimalOffset + 1],
                    decimal,
                    decimalOffset);
            if (overflow != 0) {
                overflowState.setValue(groupId, Math.addExact(overflow, otherOverflowState.getValue(groupId)));
                overflowState.setNull(groupId, false);
            }
        } else {
            decimalState.setIsNotNull(groupId, true);
            decimal[decimalOffset] = otherDecimal[otherDecimalOffset];
            decimal[decimalOffset + 1] = otherDecimal[otherDecimalOffset + 1];
            otherOverflowState.setValue(groupId, otherOverflowState.getValue(groupId));
            otherOverflowState.setNull(groupId, otherOverflowState.isNull(groupId));
        }
    }

    @IsStateNullFunction
    public static boolean isStateNull(
            @AggregationState NullableInt128State decimalState,
            @AggregationState NullableLongState overflowState,
            @GroupId long groupId
    ) {
        return !decimalState.isNotNull(groupId);
    }


    @OutputFunction("decimal(38,s)")
    public static void outputLongDecimal(
            @AggregationState NullableInt128State decimalState,
            @AggregationState NullableLongState overflowState,
            BlockBuilder out,
            @GroupId long groupId)
    {
        if (decimalState.isNotNull(groupId)) {
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
