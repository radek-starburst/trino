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

import com.google.common.collect.ImmutableList;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.GroupByIdBlock;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.sql.tree.QualifiedName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.*;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

// TODO: Benchmark na liczbie blokow zeby wykazac mayHaveNull
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 6)
@Measurement(iterations = 12, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDecimalAggregation
{
    private static final int ELEMENT_COUNT = 1_000_000;

    @Benchmark
    @OperationsPerInvocation(ELEMENT_COUNT)
    public GroupedAggregator benchmarkAddInput(BenchmarkData data)
    {
        GroupedAggregator aggregator = data.getPartialAggregatorFactory().createGroupedAggregator();
        aggregator.processPage(data.getGroupIds(), data.getValues());
        return aggregator;
    }

    @Benchmark
    @OperationsPerInvocation(ELEMENT_COUNT)
    public Block benchmarkEvaluateIntermediate(BenchmarkData data)
    {
        BlockBuilder builder = data.aggregator.getType().createBlockBuilder(null, data.getGroupCount());
        for (int groupId = 0; groupId < data.getGroupCount(); groupId++) {
            data.aggregator.evaluate(groupId, builder);
        }
        return builder.build();
    }

    @Benchmark
    public Block benchmarkAddIntermediate(BenchmarkData data)
    {
        GroupedAggregator aggregator = data.getFinalAggregatorFactory().createGroupedAggregator();
        // Add the intermediate input multiple times to invoke the combine behavior
        aggregator.processPage(data.getGroupIds(), data.getIntermediateValues());
        aggregator.processPage(data.getGroupIds(), data.getIntermediateValues());
        BlockBuilder builder = aggregator.getType().createBlockBuilder(null, data.getGroupCount());
        for (int groupId = 0; groupId < data.getGroupCount(); groupId++) {
            aggregator.evaluate(groupId, builder);
        }
        return builder.build();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"LONG"})
        private String type = "SHORT";

        @Param({"avg", "sum"})
        private String function = "avg";

        @Param({"10000"})
        private int groupCount = 10;

        private AggregatorFactory partialAggregatorFactory;
        private AggregatorFactory finalAggregatorFactory;
        private GroupByIdBlock groupIds;
        private Page values;
        private Page intermediateValues;
        private GroupedAggregator aggregator;
        private GroupedAggregator aggregatorFilledWithIntermediateValues;

        @Setup
        public void setup()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution();

            switch (type) {
                case "SHORT": {
                    DecimalType type = createDecimalType(14, 3);
                    values = createValues(functionResolution, type, type::writeLong);
                    break;
                }
                case "LONG": {
                    DecimalType type = createDecimalType(30, 10);
                    values = createValues(functionResolution, type, (builder, value) -> type.writeObject(builder, Int128.valueOf(value)));
                    break;
                }
            }

            BlockBuilder ids = BIGINT.createBlockBuilder(null, ELEMENT_COUNT);
            for (int i = 0; i < ELEMENT_COUNT; i++) {
                BIGINT.writeLong(ids, ThreadLocalRandom.current().nextLong(groupCount));
            }
            groupIds = new GroupByIdBlock(groupCount, ids.build());
            intermediateValues = new Page(createIntermediateValues(partialAggregatorFactory.createGroupedAggregator(), groupIds, values));
            aggregator = getPartialAggregatorFactory().createGroupedAggregator();
            aggregator.processPage(getGroupIds(), getValues());
            aggregatorFilledWithIntermediateValues = getFinalAggregatorFactory().createGroupedAggregator();
            aggregatorFilledWithIntermediateValues.processPage(getGroupIds(), getIntermediateValues());
            aggregatorFilledWithIntermediateValues.processPage(getGroupIds(), getIntermediateValues());
        }

        private Block createIntermediateValues(GroupedAggregator aggregator, GroupByIdBlock groupIds, Page inputPage)
        {
            aggregator.processPage(groupIds, inputPage);
            BlockBuilder builder = aggregator.getType().createBlockBuilder(null, toIntExact(groupIds.getGroupCount()));
            for (int groupId = 0; groupId < groupIds.getGroupCount(); groupId++) {
                aggregator.evaluate(groupId, builder);
            }
            return builder.build();
        }

        private Page createValues(TestingFunctionResolution functionResolution, DecimalType type, ValueWriter writer)
        {
            TestingAggregationFunction implementation = functionResolution.getAggregateFunction(QualifiedName.of(function), fromTypes(type));
            partialAggregatorFactory = implementation.createAggregatorFactory(PARTIAL, ImmutableList.of(0), OptionalInt.empty());
            finalAggregatorFactory = implementation.createAggregatorFactory(FINAL, ImmutableList.of(0), OptionalInt.empty());

            BlockBuilder builder = type.createBlockBuilder(null, ELEMENT_COUNT);
            for (int i = 0; i < ELEMENT_COUNT; i++) {
                writer.write(builder, i);
            }
            return new Page(builder.build());
        }

        public AggregatorFactory getPartialAggregatorFactory()
        {
            return partialAggregatorFactory;
        }

        public AggregatorFactory getFinalAggregatorFactory()
        {
            return finalAggregatorFactory;
        }

        public Page getValues()
        {
            return values;
        }

        public GroupByIdBlock getGroupIds()
        {
            return groupIds;
        }

        public int getGroupCount()
        {
            return groupCount;
        }

        public Page getIntermediateValues()
        {
            return intermediateValues;
        }

        interface ValueWriter
        {
            void write(BlockBuilder valuesBuilder, int value);
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();

        assertEquals(data.groupIds.getPositionCount(), data.getValues().getPositionCount());

        new BenchmarkDecimalAggregation().benchmarkAddInput(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // ensure the benchmarks are valid before running
        new BenchmarkDecimalAggregation().verify();

        String system = System.getProperty("os.name");

        ProcessBuilder pb = new ProcessBuilder("git", "rev-parse", "--abbrev-ref", "HEAD");
        String outputDir = String.format(
                "jmh/%s_%s", IOUtils.toString(pb.start().getInputStream(), StandardCharsets.UTF_8).replace("/", "_").replaceAll("\\s+",""), LocalDateTime.now());
        new File(outputDir).mkdirs();

        Function<ChainedOptionsBuilder, ChainedOptionsBuilder> baseOptionsBuilderConsumer = (options) ->
                options
                        .output(Path.of(outputDir, "stdout.log").toString())
                        .jvmArgsAppend(
                                "-Xmx32g");
//                                "-XX:+UnlockDiagnosticVMOptions",
//                                "-XX:+PrintAssembly",
//                                "-XX:+LogCompilation",
//                                "-XX:+TraceClassLoading");
        Function<ChainedOptionsBuilder, ChainedOptionsBuilder> profilers = system.equals("Linux")
                ? (options) ->
                options
                        .addProfiler(LinuxPerfProfiler.class)
                        .addProfiler(LinuxPerfNormProfiler.class)
                        .addProfiler(LinuxPerfAsmProfiler.class, String.format("hotThreshold=0.05;tooBigThreshold=3000;saveLog=true;saveLogTo=%s", outputDir))
                :  (options) ->
                options
                        .addProfiler(AsyncProfiler.class, String.format("dir=%s;output=flamegraph;event=cpu", outputDir))
                        .addProfiler(DTraceAsmProfiler.class, String.format("hotThreshold=0.05;tooBigThreshold=3000;saveLog=true;saveLogTo=%s", outputDir));

        Benchmarks.benchmark(BenchmarkDecimalAggregation.class)
                .includeMethod("benchmarkAddIntermediate")
//                .withOptions(optionsBuilder ->
//                        profilers.apply(baseOptionsBuilderConsumer.apply(optionsBuilder))
//                                .build())
                .run();
    }
}
