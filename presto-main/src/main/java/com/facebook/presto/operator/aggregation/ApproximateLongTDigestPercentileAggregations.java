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

import com.facebook.presto.operator.aggregation.stats.TDigest;
import com.facebook.presto.operator.aggregation.state.TDigestAndPercentileState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.operator.aggregation.stats.TDigest.createMergingDigest;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;

@AggregationFunction("approx_tdigest_percentile")
public final class ApproximateLongTDigestPercentileAggregations
{
    private ApproximateLongTDigestPercentileAggregations() {}

    @InputFunction
    public static void input(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        TDigest digest = state.getDigest();
        if (digest == null) {
            digest = createMergingDigest(10);
            state.setDigest(digest);
            state.addMemoryUsage(0);
        }

        state.addMemoryUsage(0);
        digest.add(value);
        state.addMemoryUsage(0);

        // use last percentile
        state.setPercentile(percentile);
    }

    @InputFunction
    public static void weightedInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        // TODO: Do weighted inputs later
        /*checkWeight(weight);

        QuantileDigest digest = state.getDigest();

        if (digest == null) {
            digest = new QuantileDigest(0.01);
            state.setDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());

        // use last percentile
        state.setPercentile(percentile);*/
    }

    @InputFunction
    public static void weightedInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        // TODO: Do weighted inputs later
        /*
        checkWeight(weight);

        QuantileDigest digest = state.getDigest();

        if (digest == null) {
            if (accuracy > 0 && accuracy < 1) {
                digest = new QuantileDigest(accuracy);
            }
            else {
                throw new IllegalArgumentException("Percentile accuracy must be strictly between 0 and 1");
            }
            state.setDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());

        // use last percentile
        state.setPercentile(percentile);*/
    }

    @CombineFunction
    public static void combine(@AggregationState TDigestAndPercentileState state, TDigestAndPercentileState otherState)
    {
        TDigest otherDigest = otherState.getDigest();
        TDigest digest = state.getDigest();
        if (digest == null) {
            state.setDigest(otherDigest);
            // TODO: Fix memory estimation for digest
            state.addMemoryUsage(0);
        }
        else {
            state.addMemoryUsage(0);
            digest.add(ImmutableList.of(otherDigest));
            state.addMemoryUsage(0);
        }

        state.setPercentile(otherState.getPercentile());
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState TDigestAndPercentileState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        double percentile = state.getPercentile();

        if ( digest == null || digest.size() == 0) {
            out.appendNull();
            return;
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            BIGINT.writeLong(out, (long)digest.quantile(percentile));
        }
    }

    private static void checkWeight(long weight)
    {
        checkCondition(weight > 0, INVALID_FUNCTION_ARGUMENT, "percentile weight must be > 0");
    }
}
