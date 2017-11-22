package com.facebook.presto.operator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 5000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 5000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMapType
{
    private static final int MAP_COUNT = 100;
    private static final int INTERNAL_ITERATIONS = 1_000_000;
    private static final String KEY_PREFIX = "Key";
    private static final String VALUE_PREFIX = "Value";

    @Benchmark
    @OperationsPerInvocation(INTERNAL_ITERATIONS)
    public Map<Object, Object> getCurrentMap()
            throws Throwable
    {
        Map<Object, Object> map = new HashMap<>();
        for (int j = 0; j < MAP_COUNT; j += 2) {
            map.put(KEY_PREFIX + j, VALUE_PREFIX + (j + 1));
        }
        return Collections.unmodifiableMap(map);
    }

    @Benchmark
    @OperationsPerInvocation(INTERNAL_ITERATIONS)
    public Map<Object, Object> getImmutableMap()
            throws Throwable
    {
        ImmutableMap.Builder<Object, Object> builder = new ImmutableMap.Builder<>();
        for (int j = 0; j < MAP_COUNT; j += 2) {
            builder.put(KEY_PREFIX + j, VALUE_PREFIX + (j + 1));
        }
        return builder.build();
    }


    public static void main(String[] args)
            throws Throwable
    {
        // For testing: Just making sure functions are working
        new BenchmarkMapType().getCurrentMap();
        new BenchmarkMapType().getImmutableMap();

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.BULK)
                .include(".*" + BenchmarkMapType.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
