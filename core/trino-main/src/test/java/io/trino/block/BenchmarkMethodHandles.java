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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.profile.DTraceAsmProfiler;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMethodHandles
{

    private static final MethodHandle mhCF;

    private static final MethodHandle composedF;
    private static MethodHandle composed;
    private static MethodHandle mhC;

    private static final MutableCallSite callSite = new MutableCallSite(
            MethodType.methodType(int.class, BenchmarkMethodHandles.class, int.class, int.class));
    private static final MethodHandle invoker = callSite.dynamicInvoker();

    static {
        try {
            mhCF = MethodHandles.lookup().findVirtual(BenchmarkMethodHandles.class, "compute",
                    MethodType.methodType(int.class, int.class));
            mhC = MethodHandles.lookup().findVirtual(BenchmarkMethodHandles.class, "compute",
                    MethodType.methodType(int.class, int.class));
            composedF = MethodHandles.dropArguments(mhC, 2, int.class);
            composed = MethodHandles.dropArguments(mhC, 2, int.class);
            callSite.setTarget(composed);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);

        }
    }

    @Benchmark
    public int benchmarkDirectCall() throws Throwable {
        return compute(ThreadLocalRandom.current().nextInt());
    }

    @Benchmark
    public int benchmarkStaticFinal() throws Throwable {
        return (int) mhCF.invokeExact(this, ThreadLocalRandom.current().nextInt());
    }

    @Benchmark
    public int benchmarkStaticNonFinal() throws Throwable {
        return (int) mhC.invokeExact(this, ThreadLocalRandom.current().nextInt());
    }

    @Benchmark
    public int benchmarkStaticFinalComposed() throws Throwable {
        return (int) composedF.invokeExact(this, ThreadLocalRandom.current().nextInt(), 0);
    }

    @Benchmark
    public int benchmarkStaticComposed() throws Throwable {
        return (int) composed.invokeExact(this, ThreadLocalRandom.current().nextInt(), 0);
    }

    @Benchmark
    public int benchmarkStaticComposedHackedViaStaticFinalCallSite() throws Throwable {
        return (int) invoker.invokeExact(this, ThreadLocalRandom.current().nextInt(), 0);
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public int compute(int i) {
        return i * 2;
    }

    public static void main(String[] args)
            throws Exception
    {
        String outputDir = String.format("jmh/%s", LocalDateTime.now());
        new File(outputDir).mkdirs();

        Function<ChainedOptionsBuilder, ChainedOptionsBuilder> baseOptionsBuilderConsumer = (options) ->
                options
                        .output(Path.of(outputDir, "stdout.log").toString())
                        .jvmArgsAppend(
                                "-Xmx32g",
                                "-XX:+UnlockDiagnosticVMOptions",
                                "-XX:+PrintAssembly",
                                "-Xlog:class+load=info",
                                "-XX:+LogCompilation");
        Function<ChainedOptionsBuilder, ChainedOptionsBuilder> profilers = (options) ->
                options
                        .addProfiler(AsyncProfiler.class, String.format("dir=%s;output=flamegraph;event=cpu", outputDir))
                        .addProfiler(DTraceAsmProfiler.class, String.format("hotThreshold=0.05;tooBigThreshold=3000;saveLog=true;saveLogTo=%s", outputDir));

        benchmark(BenchmarkMethodHandles.class)
//                .includeMethod("benchmarkStaticComposedHackedViaStaticFinalCallSite")
//                .includeMethod("benchmarkStaticComposed")
//                .includeMethod("benchmarkStaticFinalComposed")
//                .withOptions(optionsBuilder ->
//                        profilers.apply(baseOptionsBuilderConsumer.apply(optionsBuilder))
//                                .build())
                .run();
    }
}
