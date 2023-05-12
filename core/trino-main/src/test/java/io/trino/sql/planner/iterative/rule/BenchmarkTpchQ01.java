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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class BenchmarkTpchQ01
{
    @Benchmark
    public MaterializedResult benchmarkQ01(BenchmarkInfo benchmarkInfo)
    {
        return benchmarkInfo.getQueryRunner().execute(
                """
                    SELECT
                      l.returnflag,
                      l.linestatus,
                      sum(l.quantity)                                       AS sum_qty,
                      sum(l.extendedprice)                                  AS sum_base_price,
                      sum(l.extendedprice * (1 - l.discount))               AS sum_disc_price,
                      sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,
                      avg(l.quantity)                                       AS avg_qty,
                      avg(l.extendedprice)                                  AS avg_price,
                      avg(l.discount)                                       AS avg_disc,
                      count(*)                                              AS count_order
                    FROM
                      "lineitem" AS l
                    WHERE
                      l.shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
                    GROUP BY
                      l.returnflag,
                      l.linestatus
                    ORDER BY
                      l.returnflag,
                      l.linestatus
                    """);
    }

    @State(Thread)
    public static class BenchmarkInfo
    {
        private LocalQueryRunner queryRunner;

        @Setup
        public void setup()
        {
            Session session = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build();
            queryRunner = LocalQueryRunner.create(session);
            queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        }

        public QueryRunner getQueryRunner()
        {
            return queryRunner;
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkTpchQ01.class).run();
    }
}
