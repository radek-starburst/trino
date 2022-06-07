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

import com.google.common.annotations.VisibleForTesting;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.Slices.wrappedLongArray;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

@AggregationFunction("checksum")
@Description("Checksum of the given values")
public final class ChecksumAggregationFunction
{
    @VisibleForTesting
    public static final long PRIME64 = 0x9E3779B185EBCA87L;

    private ChecksumAggregationFunction() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(
                    operator = OperatorType.XX_HASH_64,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
                    MethodHandle xxHash64Operator,
            @AggregationState NullableLongState state,
            @NullablePosition @BlockPosition @SqlType("T") Block block,
            @BlockIndex int position,
            @GroupId long groupId)
            throws Throwable
    {
        state.setNull(groupId, false);
        if (block.isNull(position)) {
            state.setValue(groupId, state.getValue(groupId) + PRIME64);
        }
        else {
            long valueHash = (long) xxHash64Operator.invokeExact(block, position);
            state.setValue(groupId, state.getValue(groupId) + valueHash * PRIME64);
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState NullableLongState state,
            @AggregationState NullableLongState otherState,
            @GroupId long groupId)
    {
        state.setNull(groupId, state.isNull(groupId) && otherState.isNull(groupId));
        state.setValue(groupId, state.getValue(groupId) + otherState.getValue(groupId));
    }

    @OutputFunction("VARBINARY")
    public static void output(
            @AggregationState NullableLongState state,
            BlockBuilder out,
            @GroupId long groupId)
    {
        if (state.isNull(groupId)) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, wrappedLongArray(state.getValue(groupId)));
        }
    }
}
