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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.metadata.TableHandle;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.RecordingMetastoreConfig;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.plugin.hive.metastore.recording.HiveMetastoreRecording;
import io.trino.plugin.hive.metastore.recording.RecordingHiveMetastore;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.*;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.DataProvider;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static io.trino.Session.SessionBuilder;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.plugin.hive.metastore.recording.TestRecordingHiveMetastore.createJsonCodec;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.isDirectory;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;

public abstract class AbstractHiveCostBasedPlanTest
        extends BasePlanTest
{
    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        String catalog = "local";
        SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(getSchema())
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name());
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(sessionBuilder.build())
                .withNodeCountForStats(8)
                .build();
        queryRunner.createCatalog(
                catalog,
                createConnectorFactory(),
                ImmutableMap.of());
        return queryRunner;
    }

    protected ConnectorFactory createConnectorFactory()
    {
        RecordingMetastoreConfig recordingConfig = new RecordingMetastoreConfig()
                .setRecordingPath(getRecordingPath())
                .setReplay(true);
        try {
            // The RecordingHiveMetastore loads the metadata files generated through HiveMetadataRecorder
            // which essentially helps to generate the optimal query plans for validation purposes. These files
            // contains all the metadata including statistics.
            RecordingHiveMetastore metastore = new RecordingHiveMetastore(
                    new UnimplementedHiveMetastore(),
                    new HiveMetastoreRecording(recordingConfig, createJsonCodec()));
            return new TestingHiveConnectorFactory(metastore);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getSchema()
    {
        String fileName = Paths.get(getRecordingPath()).getFileName().toString();
        return fileName.split("\\.")[0];
    }

    private String getRecordingPath()
    {
        URL resource = getClass().getResource(getMetadataDir());
        if (resource == null) {
            throw new RuntimeException("Hive metadata directory doesn't exist: " + getMetadataDir());
        }

        File[] files = new File(resource.getPath()).listFiles();
        if (files == null) {
            throw new RuntimeException("Hive metadata recording file doesn't exist in directory: " + getMetadataDir());
        }

        return Arrays.stream(files)
                .filter(f -> !f.isDirectory())
                .collect(onlyElement())
                .getPath();
    }

    protected abstract String getMetadataDir();

    protected abstract Stream<String> getQueryResourcePaths();

    @DataProvider
    public Object[][] getQueriesDataProvider()
    {
        return getQueryResourcePaths()
                .collect(toDataProvider());
    }

    private String getQueryPlanResourcePath(String queryResourcePath)
    {
        String subDir = isPartitioned() ? "partitioned" : "unpartitioned";
        java.nio.file.Path tempPath = Paths.get(queryResourcePath.replaceAll("\\.sql$", ".joins.csv"));
        return Paths.get(tempPath.getParent().toString(), subDir, tempPath.getFileName().toString()).toString();
    }

    protected abstract boolean isPartitioned();

    protected void generate()
    {
        initPlanTest();
        try {
            getQueryResourcePaths()
                    .parallel()
                    .forEach(queryResourcePath -> {
                        try {
                            Path queryPlanWritePath = Paths.get(
                                    getSourcePath().toString(),
                                    "src/test/resources",
                                    getQueryPlanResourcePath(queryResourcePath));
                            createParentDirs(queryPlanWritePath.toFile());
                            write(generateQueryPlan(readQuery(queryResourcePath)).getBytes(UTF_8), queryPlanWritePath.toFile());
                            System.out.println("Generated expected plan for query: " + queryResourcePath);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
        finally {
            destroyPlanTest();
        }
    }

    public static String readQuery(String resource)
    {
        return read(resource).replaceAll("\\s+;\\s+$", "")
                .replace("${database}.${schema}.", "")
                .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"")
                .replace("${scale}", "1");
    }

    private static String read(String resource)
    {
        try {
            return Resources.toString(getResource(AbstractHiveCostBasedPlanTest.class, resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String resolveJoiningType(Assignment left, Assignment right) {
        if(left.columnHandle.isPresent()) {
            return left.columnHandle.get().getType().toString();
        }
        if(right.columnHandle.isPresent()) {
            return right.columnHandle.get().getType().toString();
        }
        return "unknown";
    }

    private String resolveColumnName(Optional<HiveColumnHandle> columnHandle, Optional<TableHandle> tableHandle) {
        if(columnHandle.isEmpty()) {
            return "";
        }
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle.get().getConnectorHandle();
        return String.format("%s.%s", hiveTableHandle.getTableName(), columnHandle.get().getName());
    }

    private String generateQueryPlan(String query)
    {
        Plan plan = plan(query, OPTIMIZED_AND_VALIDATED, false);

        JoinInsightVisitor joinInsightVisitor = new JoinInsightVisitor();
        plan.getRoot().accept(joinInsightVisitor, null);
        StringBuilder result = new StringBuilder();
        result.append(String.format("Type,DistributionType,JoiningColumnType,ColumnLeft,ColumnRight%n"));
        for(JoinInfo joinInfo : joinInsightVisitor.joins) {
            String joinType = !joinInfo.isCross ? joinInfo.type.toString() : "cross";
            result.append(String.format("%s,%s,,,%n", joinType, joinInfo.distributionType.get()));
            for(JoinNode.EquiJoinClause criterium : joinInfo.criteria) {
                Assignment left = lookupForColumnBySymbol(criterium.getLeft(), joinInfo.leftAssignments);
                Assignment right = lookupForColumnBySymbol(criterium.getRight(), joinInfo.rightAssignments);
                result.append(String.format(",,%s,%s,%s%n",
                        resolveJoiningType(left, right),
                        resolveColumnName(left.columnHandle, left.tableHandle),
                        resolveColumnName(right.columnHandle, right.tableHandle))
                );
            }
        }
        return result.toString();
    }

    private Assignment lookupForColumnBySymbol(Symbol symbol, Map<String, Assignment> assignmentMap) {
        Assignment assignment = assignmentMap.get(symbol.getName());
        //checkArgument(assignment != null, String.format("Not found column for symbol %s", symbol));
        if(assignment == null) {
            System.out.println("Not found" + symbol.getName());
            return new Assignment("unknown", Optional.empty(), Optional.empty());
        }
        return assignment;
    }

    protected Path getSourcePath()
    {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        verify(isDirectory(workingDir), "Working directory is not a directory");
        String topDirectoryName = workingDir.getFileName().toString();
        switch (topDirectoryName) {
            case "trino-benchto-benchmarks":
                return workingDir;
            case "trino":
                return workingDir.resolve("testing/trino-benchto-benchmarks");
            default:
                throw new IllegalStateException("This class must be executed from trino-benchto-benchmarks or Trino source directory");
        }
    }

    private static class Assignment {
        private final String name;
        private final Optional<HiveColumnHandle> columnHandle;

        private final Optional<TableHandle> tableHandle;

        private Assignment(String name, Optional<HiveColumnHandle> columnHandle, Optional<TableHandle> tableHandle) {
            this.name = name;
            this.columnHandle = columnHandle;
            this.tableHandle = tableHandle;
        }

        public String toString() {
            if(columnHandle.isPresent()) {
                return String.format("%s [%s,%s]", name, ((HiveColumnHandle) columnHandle.get()).getColumnType(), this.tableHandle.get());
            } else {
                return String.format("%s", name);
            }
        }
    }

    private static class JoinInfo {
        private final List<JoinNode.EquiJoinClause> criteria;
        private final Map<String, Assignment> leftAssignments;
        private final Map<String, Assignment> rightAssignments;
        private final JoinNode.Type type;
        private final Optional<JoinNode.DistributionType> distributionType;

        private final boolean isCross;

        public JoinInfo(List<JoinNode.EquiJoinClause> criteria, Map<String, Assignment> assignmentsLHS, Map<String, Assignment> assignmentsRHS, Optional<JoinNode.DistributionType> distributionType, JoinNode.Type type, boolean isCross) {
            this.criteria = criteria;
            this.leftAssignments = assignmentsLHS;
            this.rightAssignments = assignmentsRHS;
            this.distributionType = distributionType;
            this.type = type;
            this.isCross = isCross;
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(String.format("JOIN"));
            return "";
        }
    }

    public static class JoinInsightVisitor extends PlanVisitor<Void, Void> {

        final List<JoinInfo> joins;
        public JoinInsightVisitor() {
            this.joins = new ArrayList<> ();
        }

        @Override
        public Void visitPlan(PlanNode node, Void context) {
            node.getSources().forEach(n -> n.accept(this, context));
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context) {
            List<JoinNode.EquiJoinClause> criteria = node.getCriteria();

            Map<String, Assignment> leftAssignments = new HashMap<>();
            Map<String, Assignment> rightAssignments = new HashMap<>();

            PlanNodeSearcher.searchFrom(node.getLeft())
                    .where(n -> n instanceof ProjectNode)
                    .findAll()
                    .stream()
                    .flatMap(it -> ((ProjectNode) it).getAssignments().getMap().entrySet().stream())
                    .map(it -> new Assignment(it.getKey().getName(), Optional.empty(), Optional.empty()))
                    .forEach(it -> leftAssignments.put(it.name, it));

            PlanNodeSearcher.searchFrom(node.getRight())
                    .where(n -> n instanceof ProjectNode)
                    .findAll()
                    .stream()
                    .flatMap(it -> ((ProjectNode) it).getAssignments().getMap().entrySet().stream())
                    .map(it -> new Assignment(it.getKey().getName(), Optional.empty(), Optional.empty()))
                    .forEach(it -> rightAssignments.put(it.name, it));

            PlanNodeSearcher.searchFrom(node.getLeft())
                    .where(n -> n instanceof TableScanNode)
                    .findAll()
                    .stream()
                    .flatMap(it -> ((TableScanNode) it).getAssignments().entrySet().stream().map(e -> new Assignment(e.getKey().getName(), Optional.of((HiveColumnHandle) e.getValue()), Optional.of(((TableScanNode) it).getTable()))))
                    .forEach(it -> leftAssignments.put(it.name, it));

            PlanNodeSearcher.searchFrom(node.getRight())
                    .where(n -> n instanceof TableScanNode)
                    .findAll()
                    .stream()
                    .flatMap(it -> ((TableScanNode) it).getAssignments().entrySet().stream().map(e -> new Assignment(e.getKey().getName(), Optional.of((HiveColumnHandle) e.getValue()), Optional.of(((TableScanNode) it).getTable()))))
                    .forEach(it -> rightAssignments.put(it.name, it));

            JoinInfo joinInfo = new JoinInfo(criteria, leftAssignments, rightAssignments, node.getDistributionType(), node.getType(), node.isCrossJoin());
            joins.add(joinInfo);

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);
            return null;
        }
    }
}
