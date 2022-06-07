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
package io.trino.operator.aggregation.state;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.function.GroupId;
import io.trino.spi.type.Type;

@AccumulatorStateMetadata(stateSerializerClass = NullableDoubleStateSerializer.class)
public interface NullableDoubleState
        extends AccumulatorState
{
    double getValue(@GroupId long groupId);

    void setValue(@GroupId long groupId, double value);

    @InitialBooleanValue(true)
    boolean isNull(@GroupId long groupId);

    void setNull(@GroupId long groupId, boolean value);

    default void set(@GroupId long groupId, NullableDoubleState state)
    {
        setValue(groupId, state.getValue(groupId));
        setNull(groupId, state.isNull(groupId));
    }

    static void write(Type type, NullableDoubleState state, BlockBuilder out, @GroupId long groupId)
    {
        if (state.isNull(groupId)) {
            out.appendNull();
        }
        else {
            type.writeDouble(out, state.getValue(groupId));
        }
    }
}
