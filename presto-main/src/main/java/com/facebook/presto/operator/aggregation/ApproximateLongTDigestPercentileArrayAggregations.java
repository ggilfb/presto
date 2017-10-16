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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.state.TDigestAndPercentileArrayState;
import com.facebook.presto.operator.aggregation.stats.TDigest;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.aggregation.stats.TDigest.createMergingDigest;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Failures.checkCondition;

@AggregationFunction("approx_tdigest_percentile")
public final class ApproximateLongTDigestPercentileArrayAggregations
{
    private ApproximateLongTDigestPercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        initializePercentilesArray(state, percentilesArrayBlock);
        initializeDigest(state);

        TDigest digest = state.getDigest();
        state.addMemoryUsage(0);
        digest.add(value);
        state.addMemoryUsage(0);
    }

    @InputFunction
    public static void weightedInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        initializePercentilesArray(state, percentilesArrayBlock);
        initializeDigest(state);

        TDigest digest = state.getDigest();
        state.addMemoryUsage(0);
        // TODO: Rather hacky, cast it to an int for now, let's see if this blows up.
        digest.add(value, (int)weight);
        state.addMemoryUsage(0);
    }

    @CombineFunction
    public static void combine(@AggregationState TDigestAndPercentileArrayState state, TDigestAndPercentileArrayState otherState)
    {
        /*QuantileDigest otherDigest = otherState.getDigest();
        QuantileDigest digest = state.getDigest();

        if (digest == null) {
            state.setDigest(otherDigest);
            state.addMemoryUsage(otherDigest.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
            digest.merge(otherDigest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.setPercentiles(otherState.getPercentiles());*/
        // TODO
    }

    @OutputFunction("array(bigint)")
    public static void output(@AggregationState TDigestAndPercentileArrayState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();

        for (int i = 0; i < percentiles.size(); i++) {
            Double percentile = percentiles.get(i);
            // if these were longs, this should be long...
            BIGINT.writeLong(blockBuilder, (long)digest.quantile(percentile));
        }

        out.closeEntry();
    }

    private static void initializePercentilesArray(@AggregationState TDigestAndPercentileArrayState state, Block percentilesArrayBlock)
    {
        if (state.getPercentiles() == null) {
            ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();

            for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
                checkCondition(!percentilesArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "Percentile cannot be null");
                double percentile = DOUBLE.getDouble(percentilesArrayBlock, i);
                checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
                percentilesListBuilder.add(percentile);
            }

            state.setPercentiles(percentilesListBuilder.build());
        }
    }

    private static void initializeDigest(@AggregationState TDigestAndPercentileArrayState state)
    {
        TDigest digest = state.getDigest();
        if (digest == null) {
            digest = createMergingDigest(10);
            state.setDigest(digest);
            state.addMemoryUsage(0);
        }
    }
}
