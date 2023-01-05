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
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.execution.TaskId;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.airlift.stats.TDigest;
import io.trino.spi.Mergeable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DirectExchangeClientStatus
        implements Mergeable<DirectExchangeClientStatus>, OperatorInfo
{
    private final long bufferedBytes;
    private final long maxBufferedBytes;
    private final long averageBytesPerRequest;
    private final long successfulRequestsCount;
    private final int bufferedPages;
    private final int spilledPages;
    private final long spilledBytes;
    private final boolean noMoreLocations;
    private final List<PageBufferClientStatus> pageBufferClientStatuses;
    private final TDigestHistogram bufferUtilization;
    private final TDigestHistogram clientWaitingTimeInMillis;

    private final Map<TaskId, Long> zeros;
    private final TDigestHistogram residualError;

    private final long zero;

    @JsonCreator
    public DirectExchangeClientStatus(
            @JsonProperty("bufferedBytes") long bufferedBytes,
            @JsonProperty("maxBufferedBytes") long maxBufferedBytes,
            @JsonProperty("averageBytesPerRequest") long averageBytesPerRequest,
            @JsonProperty("successfulRequestsCount") long successFullRequestsCount,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("spilledPages") int spilledPages,
            @JsonProperty("spilledBytes") long spilledBytes,
            @JsonProperty("noMoreLocations") boolean noMoreLocations,
            @JsonProperty("pageBufferClientStatuses") List<PageBufferClientStatus> pageBufferClientStatuses,
            @JsonProperty("clientWaitingTimeInMillis") TDigestHistogram clientWaitingTimeInMillis,
            @JsonProperty("bufferUtilization") TDigestHistogram bufferUtilization,
            @JsonProperty("residualError") TDigestHistogram residualError,
            @JsonProperty("zero") Long zero,
            @JsonProperty("zeros") Map<TaskId, Long> zeros)
    {
        this.bufferedBytes = bufferedBytes;
        this.maxBufferedBytes = maxBufferedBytes;
        this.averageBytesPerRequest = averageBytesPerRequest;
        this.successfulRequestsCount = successFullRequestsCount;
        this.bufferedPages = bufferedPages;
        this.spilledPages = spilledPages;
        this.spilledBytes = spilledBytes;
        this.noMoreLocations = noMoreLocations;
        this.pageBufferClientStatuses = ImmutableList.copyOf(requireNonNull(pageBufferClientStatuses, "pageBufferClientStatuses is null"));
        this.clientWaitingTimeInMillis = requireNonNull(clientWaitingTimeInMillis, "clientWaitingTimeInMillis is null");
        this.bufferUtilization = requireNonNull(bufferUtilization, "bufferUtilization is null");
        this.zeros = zeros;
        this.zero = zero;
        this.residualError = residualError;
    }

    @JsonProperty
    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    @JsonProperty
    public long getMaxBufferedBytes()
    {
        return maxBufferedBytes;
    }

    @JsonProperty
    public long getAverageBytesPerRequest()
    {
        return averageBytesPerRequest;
    }

    @JsonProperty
    public long getSuccessfulRequestsCount()
    {
        return successfulRequestsCount;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @JsonProperty
    public int getSpilledPages()
    {
        return spilledPages;
    }

    @JsonProperty
    public synchronized TDigestHistogram getBufferUtilization()
    {
        return new TDigestHistogram(bufferUtilization.getDigest());
    }

    @JsonProperty
    public long getSpilledBytes()
    {
        return spilledBytes;
    }

    @JsonProperty
    public boolean isNoMoreLocations()
    {
        return noMoreLocations;
    }

    @JsonProperty
    public List<PageBufferClientStatus> getPageBufferClientStatuses()
    {
        return pageBufferClientStatuses;
    }

    @JsonProperty
    public synchronized TDigestHistogram getClientWaitingTimeInMillis()
    {
        return new TDigestHistogram(TDigest.copyOf(clientWaitingTimeInMillis.getDigest()));
    }

    @JsonProperty
    public Map<TaskId, Long> getZeros()
    {
        return zeros;
    }

    @JsonProperty
    public Long getZero()
    {
        return zero;
    }

    @JsonProperty
    public synchronized TDigestHistogram getResidualError()
    {
        return new TDigestHistogram(TDigest.copyOf(residualError.getDigest()));
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bufferBytes", bufferedBytes)
                .add("maxBufferedBytes", maxBufferedBytes)
                .add("averageBytesPerRequest", averageBytesPerRequest)
                .add("successfulRequestsCount", successfulRequestsCount)
                .add("bufferedPages", bufferedPages)
                .add("spilledPages", spilledPages)
                .add("spilledBytes", spilledBytes)
                .add("noMoreLocations", noMoreLocations)
                .add("pageBufferClientStatuses", pageBufferClientStatuses)
                .add("clientWaitingTimeInMillis", clientWaitingTimeInMillis)
                .add("bufferUtilization", bufferUtilization)
                .toString();
    }

    @Override
    public DirectExchangeClientStatus mergeWith(DirectExchangeClientStatus other)
    {
        Map<TaskId, Long> merged = new HashMap<>(zeros);
        other.getZeros().forEach((key, value) -> merged.merge(key, value, Long::sum));

        return new DirectExchangeClientStatus(
                (bufferedBytes + other.bufferedBytes) / 2, // this is correct as long as all clients have the same buffer size (capacity)
                Math.max(maxBufferedBytes, other.maxBufferedBytes),
                mergeAvgs(averageBytesPerRequest, successfulRequestsCount, other.averageBytesPerRequest, other.successfulRequestsCount),
                successfulRequestsCount + other.successfulRequestsCount,
                bufferedPages + other.bufferedPages,
                spilledPages + other.spilledPages,
                spilledBytes + other.spilledBytes,
                noMoreLocations && other.noMoreLocations, // if at least one has some locations, mergee has some too
                ImmutableList.of(), // pageBufferClientStatuses may be long, so we don't want to combine the lists
                getClientWaitingTimeInMillis().mergeWith(other.getClientWaitingTimeInMillis()),
                getBufferUtilization().mergeWith(other.getBufferUtilization()),
                getResidualError().mergeWith(other.getResidualError()),
                zero + other.zero,
                merged
            );
    }

    private static long mergeAvgs(long value1, long count1, long value2, long count2)
    {
        if (count1 == 0) {
            return value2;
        }
        if (count2 == 0) {
            return value1;
        }
        // AVG_n+m = AVG_n * n / (n + m) + AVG_m * m / (n + m)
        return (value1 * count1 / (count1 + count2)) + (value2 * count2 / (count1 + count2));
    }
}
