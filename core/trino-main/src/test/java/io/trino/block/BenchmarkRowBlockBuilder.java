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
package io.trino.block;

import com.google.common.collect.ImmutableList;
import io.trino.operator.project.BenchmarkDictionaryBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.testng.annotations.Test;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 15)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkRowBlockBuilder
{
    @Benchmark
    public SingleRowBlockWriter benchmarkBeginBlockEntryAlloc(BenchmarkData data)
    {
        return data.rowBlockBuilder.beginBlockEntryAlloc();
    }

    @Benchmark
    public SingleRowBlockWriter benchmarkBeginBlockEntryNoAlloc(BenchmarkData data)
    {
        return data.rowBlockBuilder.beginBlockEntry();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private RowBlockBuilder rowBlockBuilder;

        @Setup(Level.Invocation)
        public void setup()
        {
            rowBlockBuilder = new RowBlockBuilder(ImmutableList.of(VARCHAR), null, 1);
        }
    }

    @Test
    public void testBeginBlockEntryNoAlloc()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        benchmarkBeginBlockEntryNoAlloc(data);
    }

    @Test
    public void testBeginBlockEntryAlloc()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        benchmarkBeginBlockEntryAlloc(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkRowBlockBuilder().benchmarkBeginBlockEntryAlloc(data);
        data.setup();
        new BenchmarkRowBlockBuilder().benchmarkBeginBlockEntryNoAlloc(data);
        benchmark(BenchmarkRowBlockBuilder.class).run();
    }
}
