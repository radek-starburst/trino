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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;
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
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.UpdateMemory.NOOP;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * This class attempts to emulate aggregations done while running real-life queries.
 * Some of the numbers here has been inspired by tpch benchmarks, however,
 * there is no guarantee that results correlate with the benchmark itself.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGroupByHashOnSimulatedData
{
    private static final int DEFAULT_POSITIONS = 10_000_000;
    private static final int EXPECTED_SIZE = 10_000;
    private static final int DEFAULT_PAGE_SIZE = 8192;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(TYPE_OPERATORS);

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final List<GroupByIdBlock> results = new ArrayList<>();

    private final JoinCompiler joinCompiler = new JoinCompiler(TYPE_OPERATORS);

    @Benchmark
    @OperationsPerInvocation(DEFAULT_POSITIONS)
    public Object groupBy(BenchmarkTpch data)
    {
        GroupByHash groupByHash =
                GroupByHash.createGroupByHash(data.getTypes(), data.getChannels(), Optional.empty(), EXPECTED_SIZE, false, joinCompiler, TYPE_OPERATOR_FACTORY, NOOP);
        addInputPages(groupByHash, data.getPages());

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Test
    public void runGroupByTpch()
    {
        BenchmarkGroupByHashOnSimulatedData benchmark = new BenchmarkGroupByHashOnSimulatedData();
        for (AggregationDefinition query : AggregationDefinition.values()) {
            BenchmarkTpch data = new BenchmarkTpch(query, 10_000);
            data.setup();
            benchmark.groupBy(data);
        }
    }

    private void addInputPages(GroupByHash groupByHash, List<Page> pages)
    {
        results.clear();
        for (Page page : pages) {
            Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
            boolean finished;
            do {
                finished = work.process();
                results.add(work.getResult()); // Pretend the results are used
            }
            while (!finished);
        }
    }

    public interface BlockWriter
    {
        void write(BlockBuilder blockBuilder, int positionCount, long randomSeed);
    }

    public enum ColumnType
    {
        BIGINT(BigintType.BIGINT, (blockBuilder, positionCount, seed) -> {
            Random r = new Random(seed);
            for (int i = 0; i < positionCount; i++) {
                blockBuilder.writeLong((r.nextLong() >>> 1)); // Only positives
            }
        }),
        INT(IntegerType.INTEGER, (blockBuilder, positionCount, seed) -> {
            Random r = new Random(seed);
            for (int i = 0; i < positionCount; i++) {
                blockBuilder.writeInt(r.nextInt());
            }
        }),
        DOUBLE(DoubleType.DOUBLE, (blockBuilder, positionCount, seed) -> {
            Random r = new Random(seed);
            for (int i = 0; i < positionCount; i++) {
                blockBuilder.writeLong((r.nextLong() >>> 1)); // Only positives
            }
        }),
        VARCHAR_25(VarcharType.VARCHAR, (blockBuilder, positionCount, seed) -> {
            writeVarchar(blockBuilder, positionCount, seed, 25);
        }),
        VARCHAR_117(VarcharType.VARCHAR, (blockBuilder, positionCount, seed) -> {
            writeVarchar(blockBuilder, positionCount, seed, 117);
        }),
        CHAR_1(CharType.createCharType(1), (blockBuilder, positionCount, seed) -> {
            Random r = new Random(seed);
            for (int i = 0; i < positionCount; i++) {
                byte value = (byte) r.nextInt();
                while (value == ' ') {
                    value = (byte) r.nextInt();
                }
                CharType.createCharType(1).writeSlice(blockBuilder, Slices.wrappedBuffer(value));
            }
        }),
        /**/;

        private static void writeVarchar(BlockBuilder blockBuilder, int positionCount, long seed, int maxLength)
        {
            Random r = new Random(seed);

            for (int i = 0; i < positionCount; i++) {
                int length = 1 + r.nextInt(maxLength - 1);
                byte[] bytes = new byte[length];
                r.nextBytes(bytes);
                VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.wrappedBuffer(bytes));
            }
        }

        final Type type;
        final BlockWriter blockWriter;

        ColumnType(Type type, BlockWriter blockWriter)
        {
            this.type = requireNonNull(type, "type is null");
            this.blockWriter = requireNonNull(blockWriter, "blockWriter is null");
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkTpch
    {
        @Param
        private AggregationDefinition query;

        private final int positions;
        private List<Page> pages;
        private List<Type> types;
        private int[] channels;

        public BenchmarkTpch()
        {
            this.positions = DEFAULT_POSITIONS;
        }

        public BenchmarkTpch(AggregationDefinition query, int positions)
        {
            this.query = requireNonNull(query, "query is null");
            this.positions = positions;
        }

        @Setup
        public void setup()
        {
            types = query.channels.stream().map(channel -> channel.columnType.type).collect(toUnmodifiableList());
            channels = IntStream.range(0, query.channels.size()).toArray();
            pages = createPages(query);
        }

        private List<Page> createPages(AggregationDefinition definition)
        {
            List<Page> result = new ArrayList<>();
            int channelCount = definition.channels.size();
            int pageSize = definition.pageSize;
            int pageCount = positions / pageSize;

            Block[][] blocks = new Block[channelCount][];
            for (int i = 0; i < definition.channels.size(); i++) {
                ChannelDefinition channel = definition.channels.get(i);
                blocks[i] = channel.createBlocks(pageCount, pageSize, i);
            }

            for (int i = 0; i < pageCount; i++) {
                int finalI = i;
                Block[] pageBlocks = IntStream.range(0, channelCount).mapToObj(channel -> blocks[channel][finalI]).toArray(Block[]::new);
                result.add(new Page(pageBlocks));
            }

            return result;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public int[] getChannels()
        {
            return channels;
        }
    }

    public enum AggregationDefinition
    {
        BIGINT_2_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 2)),
        BIGINT_10_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 10)),
        BIGINT_1K_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 1000)),
        BIGINT_10K_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 10_000)),
        BIGINT_100K_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 100_000)),
        BIGINT_1M_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 1_000_000)),
        BIGINT_10M_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 10_000_000)),
        BIGINT_2_GROUPS_1_SMALL_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 2, 50, 1)),
        BIGINT_2_GROUPS_1_BIG_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 2, 10000, 1)),
        BIGINT_2_GROUPS_MULTIPLE_SMALL_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 2, 50, 10)),
        BIGINT_2_GROUPS_MULTIPLE_BIG_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 2, 10000, 10)),
        BIGINT_10K_GROUPS_1_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 10000, 20000, 1)),
        BIGINT_10K_GROUPS_MULTIPLE_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 10000, 20000, 20)),
        DOUBLE_10_GROUPS(new ChannelDefinition(ColumnType.DOUBLE, 10)),
        TWO_TINY_VARCHAR_DICTIONARIES(
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10)),
        FIVE_TINY_VARCHAR_DICTIONARIES(
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10)),
        TWO_SMALL_VARCHAR_DICTIONARIES(
                new ChannelDefinition(ColumnType.CHAR_1, 30, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 30, 10)),
        TWO_SMALL_VARCHAR_DICTIONARIES_WITH_SMALL_PAGE_SIZE( // low cardinality optimisation will not kick in here
                1000,
                new ChannelDefinition(ColumnType.CHAR_1, 30, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 30, 10)),
        VARCHAR_2_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 2)),
        VARCHAR_10_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 10)),
        VARCHAR_1K_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 1000)),
        VARCHAR_10K_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 10_000)),
        VARCHAR_100K_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 100_000)),
        VARCHAR_1M_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 1_000_000)),
        VARCHAR_10M_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 10_000_000)),
        VARCHAR_2_GROUPS_1_SMALL_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 2, 50, 1)),
        VARCHAR_2_GROUPS_1_BIG_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 2, 10000, 1)),
        VARCHAR_2_GROUPS_MULTIPLE_SMALL_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 2, 50, 10)),
        VARCHAR_2_GROUPS_MULTIPLE_BIG_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 2, 10000, 10)),
        VARCHAR_10K_GROUPS_1_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 10000, 20000, 1)),
        VARCHAR_10K_GROUPS_MULTIPLE_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 10000, 20000, 20)),
        TINY_CHAR_10_GROUPS(new ChannelDefinition(ColumnType.CHAR_1, 10)),
        BIG_VARCHAR_10_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_117, 10)),
        BIG_VARCHAR_1M_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_117, 1_000_000)),
        DOUBLE_BIGINT_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 10),
                new ChannelDefinition(ColumnType.BIGINT, 10)),
        BIGINT_AND_TWO_INTS_5K(
                new ChannelDefinition(ColumnType.BIGINT, 500),
                new ChannelDefinition(ColumnType.INT, 10),
                new ChannelDefinition(ColumnType.INT, 10)),
        FIVE_MIXED_SHORT_COLUMNS_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.INT, 1),
                new ChannelDefinition(ColumnType.DOUBLE, 2)),
        FIVE_MIXED_SHORT_COLUMNS_100K_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 20),
                new ChannelDefinition(ColumnType.INT, 10),
                new ChannelDefinition(ColumnType.DOUBLE, 20)),
        FIVE_MIXED_LONG_COLUMNS_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 1),
                new ChannelDefinition(ColumnType.VARCHAR_117, 2)),
        FIVE_MIXED_LONG_COLUMNS_100K_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 20),
                new ChannelDefinition(ColumnType.VARCHAR_25, 10),
                new ChannelDefinition(ColumnType.VARCHAR_117, 20)),
        TEN_MIXED_SHORT_COLUMNS_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 1),
                new ChannelDefinition(ColumnType.INT, 2),
                new ChannelDefinition(ColumnType.BIGINT, 1),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 1),
                new ChannelDefinition(ColumnType.BIGINT, 2),
                new ChannelDefinition(ColumnType.INT, 1),
                new ChannelDefinition(ColumnType.VARCHAR_25, 5),
                new ChannelDefinition(ColumnType.INT, 1),
                new ChannelDefinition(ColumnType.DOUBLE, 1)),
        TEN_MIXED_SHORT_COLUMNS_100K_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.INT, 2),
                new ChannelDefinition(ColumnType.BIGINT, 2),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 5),
                new ChannelDefinition(ColumnType.BIGINT, 2),
                new ChannelDefinition(ColumnType.INT, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 5),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 2)),
        TEN_MIXED_LONG_COLUMNS_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 1),
                new ChannelDefinition(ColumnType.VARCHAR_117, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 1),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 1),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 1),
                new ChannelDefinition(ColumnType.VARCHAR_25, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 1),
                new ChannelDefinition(ColumnType.DOUBLE, 1)),
        TEN_MIXED_LONG_COLUMNS_100K_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 2)),
        /**/;

        private final int pageSize;
        private final List<ChannelDefinition> channels;

        AggregationDefinition(ChannelDefinition... channels)
        {
            this(DEFAULT_PAGE_SIZE, channels);
        }

        AggregationDefinition(int pageSize, ChannelDefinition... channels)
        {
            this.pageSize = pageSize;
            this.channels = Arrays.stream(requireNonNull(channels, "channels is null")).collect(toImmutableList());
        }
    }

    public static class ChannelDefinition
    {
        private final ColumnType columnType;
        private final int distinctValuesCount;
        private final int dictionaryPositions;
        private final int distinctDictionaries;

        public ChannelDefinition(ColumnType columnType, int distinctValuesCount)
        {
            this(columnType, distinctValuesCount, -1, -1);
        }

        public ChannelDefinition(ColumnType columnType, int distinctValuesCount, int distinctDictionaries)
        {
            this(columnType, distinctValuesCount, distinctValuesCount, distinctDictionaries);
        }

        public ChannelDefinition(ColumnType columnType, int distinctValuesCount, int dictionaryPositions, int distinctDictionaries)
        {
            this.columnType = requireNonNull(columnType, "columnType is null");
            this.distinctValuesCount = distinctValuesCount;
            this.dictionaryPositions = dictionaryPositions;
            this.distinctDictionaries = distinctDictionaries;
        }

        public Block[] createBlocks(int blockCount, int positionsPerBlock, int channel)
        {
            Block[] blocks = new Block[blockCount];
            if (dictionaryPositions == -1) { // No dictionaries
                BlockBuilder allValues = columnType.type.createBlockBuilder(null, distinctValuesCount);
                columnType.blockWriter.write(allValues, distinctValuesCount, channel);
                Random r = new Random(channel);
                for (int i = 0; i < blockCount; i++) {
                    BlockBuilder block = columnType.type.createBlockBuilder(null, positionsPerBlock);
                    for (int j = 0; j < positionsPerBlock; j++) {
                        int position = r.nextInt(distinctValuesCount);
                        columnType.type.appendTo(allValues, position, block);
                    }
                    blocks[i] = block.build();
                }
            }
            else {
                Random r = new Random(1);

                int totalValueCount = Math.max(distinctValuesCount, dictionaryPositions);
                BlockBuilder allValues = columnType.type.createBlockBuilder(null, totalValueCount);
                columnType.blockWriter.write(allValues, totalValueCount, channel);
                Set<Integer> usedValues = nOutOfM(r, distinctValuesCount, totalValueCount);

                Block[] dictionaries = new Block[distinctDictionaries];
                Set<Integer>[] possibleIndexesSet = new Set[distinctDictionaries];
                for (int i = 0; i < distinctDictionaries; i++) {
                    BlockBuilder dictionaryBuilder = columnType.type.createBlockBuilder(null, dictionaryPositions);
                    possibleIndexesSet[i] = new HashSet<>();
                    Set<Integer> distinctValues = nOutOfM(r, dictionaryPositions, totalValueCount);
                    List<Integer> positions = new ArrayList<>(distinctValues);
                    Collections.shuffle(positions);

                    for (int j = 0; j < dictionaryPositions; j++) {
                        int position = positions.get(j);
                        if (usedValues.contains(position)) {
                            possibleIndexesSet[i].add(j);
                        }
                        columnType.type.appendTo(allValues, position, dictionaryBuilder);
                    }
                    dictionaries[i] = dictionaryBuilder.build();
                }
                List<Integer>[] possibleIndexes = Arrays.stream(possibleIndexesSet).map(x -> x.stream().collect(toList())).toArray(List[]::new);

                for (int i = 0; i < blockCount; i++) {
                    int[] indexes = new int[positionsPerBlock];
                    int dictionaryId = r.nextInt(distinctDictionaries);
                    Block dictionary = dictionaries[dictionaryId];
                    int possibleValuesCount = possibleIndexes[dictionaryId].size();
                    for (int j = 0; j < positionsPerBlock; j++) {
                        indexes[j] = possibleIndexes[dictionaryId].get(r.nextInt(possibleValuesCount));
                    }

                    blocks[i] = new DictionaryBlock(dictionary, indexes);
                }
            }
            return blocks;
        }

        private Set<Integer> nOutOfM(Random r, int n, int m)
        {
            Set<Integer> usedValues = new HashSet<>();
            while (usedValues.size() < n) {
                int left = n - usedValues.size();
                for (int i = 0; i < left; i++) {
                    usedValues.add(r.nextInt(m));
                }
            }
            return usedValues;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkGroupByHashOnSimulatedData.class)
                .withOptions(optionsBuilder -> optionsBuilder
                        .jvmArgs("-Xmx4g"))
                .run();
    }
}
