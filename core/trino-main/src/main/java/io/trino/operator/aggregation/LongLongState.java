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

import io.trino.operator.aggregation.state.InitialBooleanValue;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.GroupId;

public interface LongLongState
        extends AccumulatorState
{
    long getFirst(@GroupId long groupId);

    void setFirst(@GroupId long groupId, long first);

    @InitialBooleanValue(true)
    boolean isFirstNull(@GroupId long groupId);

    void setFirstNull(@GroupId long groupId, boolean firstNull);

    long getSecond(@GroupId long groupId);

    void setSecond(@GroupId long groupId, long second);

    @InitialBooleanValue(true)
    boolean isSecondNull(@GroupId long groupId);

    void setSecondNull(@GroupId long groupId, boolean secondNull);
}
