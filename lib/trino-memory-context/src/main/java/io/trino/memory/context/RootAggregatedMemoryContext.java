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
package io.trino.memory.context;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RootAggregatedMemoryContext
        extends AbstractAggregatedMemoryContext
{
    private final MemoryReservationHandler reservationHandler;
    private final long guaranteedMemory;

    RootAggregatedMemoryContext(MemoryReservationHandler reservationHandler, long guaranteedMemory)
    {
        this.reservationHandler = requireNonNull(reservationHandler, "reservationHandler is null");
        this.guaranteedMemory = guaranteedMemory;
    }

    @Override
    synchronized ListenableFuture<Void> updateBytes(String allocationTag, long delta)
    {
        checkState(!isClosed(), "RootAggregatedMemoryContext is already closed");
        ListenableFuture<Void> future = reservationHandler.reserveMemory(allocationTag, delta);
        addBytes(delta);
//        System.out.println(String.format("RootAggregatedMemoryContext::updateBytes %s=%d", allocationTag, delta));
        // make sure we never block queries below guaranteedMemory
        if (getBytes() < guaranteedMemory) {
            future = NOT_BLOCKED;
        }
        return future;
    }

    public static ConcurrentHashMap<String, Long> c = new ConcurrentHashMap<>();

    @Override
    synchronized boolean tryUpdateBytes(String allocationTag, long delta)
    {
        checkState(!isClosed(), "RootAggregatedMemoryContext is already closed");
        if (reservationHandler.tryReserveMemory(allocationTag, delta)) {
            if(delta > 0) {
                c.putIfAbsent(allocationTag, delta);
                c.computeIfPresent(allocationTag, (k, v) -> v + delta);
//                System.out.printf("RootAggregatedMemoryContext::tryUpdateBytes %s=%d%n", allocationTag, delta);
            }
            addBytes(delta);
            return true;
        }
        return false;
    }

    @Override
    void closeContext()
    {
        reservationHandler.reserveMemory(FORCE_FREE_TAG, -getBytes());
    }
}
