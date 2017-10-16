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

import java.util.List;

import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.sortableLongToDouble;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("approx_tdigest_percentile")
public final class ApproximateDoubleTDigestPercentileArrayAggregations
{
    private ApproximateDoubleTDigestPercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongTDigestPercentileArrayAggregations.input(state, doubleToSortableLong(value), percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongTDigestPercentileArrayAggregations.weightedInput(state, doubleToSortableLong(value), weight, percentilesArrayBlock);
    }

    @CombineFunction
    public static void combine(@AggregationState TDigestAndPercentileArrayState state, TDigestAndPercentileArrayState otherState)
    {
        ApproximateLongTDigestPercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction("array(double)")
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
            DOUBLE.writeDouble(blockBuilder, sortableLongToDouble((long)digest.quantile(percentile)));
        }

        out.closeEntry();
    }
}