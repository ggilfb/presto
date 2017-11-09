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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.stats.TDigest;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TDigestAndPercentileArrayStateFactory
        implements AccumulatorStateFactory<TDigestAndPercentileArrayState>
{
    @Override
    public TDigestAndPercentileArrayState createSingleState()
    {
        return new TSingleDigestAndPercentileArrayState();
    }

    @Override
    public Class<? extends TDigestAndPercentileArrayState> getSingleStateClass()
    {
        return TSingleDigestAndPercentileArrayState.class;
    }

    @Override
    public TDigestAndPercentileArrayState createGroupedState()
    {
        return new TGroupedDigestAndPercentileArrayState();
    }

    @Override
    public Class<? extends TDigestAndPercentileArrayState> getGroupedStateClass()
    {
        return TGroupedDigestAndPercentileArrayState.class;
    }

    public static class TGroupedDigestAndPercentileArrayState
            extends AbstractGroupedAccumulatorState
            implements TDigestAndPercentileArrayState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(TGroupedDigestAndPercentileArrayState.class).instanceSize();
        private final ObjectBigArray<TDigest> digests = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> percentilesArray = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentilesArray.ensureCapacity(size);
        }

        @Override
        public TDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(TDigest digest)
        {
            digests.set(getGroupId(), requireNonNull(digest, "digest is null"));
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentilesArray.get(getGroupId());
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            percentilesArray.set(getGroupId(), requireNonNull(percentiles, "percentiles is null"));
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentilesArray.sizeOf();
        }
    }

    public static class TSingleDigestAndPercentileArrayState
            implements TDigestAndPercentileArrayState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(TSingleDigestAndPercentileArrayState.class).instanceSize();
        private TDigest digest;
        private List<Double> percentiles;

        @Override
        public TDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(TDigest digest)
        {
            this.digest = requireNonNull(digest, "digest is null");
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            this.percentiles = requireNonNull(percentiles, "percentiles is null");
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            // TBD: To fix, could we get a proper memory estimation?
            /*if (digest != null) {
                estimatedSize += 0; // digest.estimatedInMemorySizeInBytes();
            }*/
            if (percentiles != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize;
        }
    }
}
