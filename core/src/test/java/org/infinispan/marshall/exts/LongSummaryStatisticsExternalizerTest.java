package org.infinispan.marshall.exts;

import java.util.LongSummaryStatistics;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.testng.annotations.Test;
import org.testng.Assert;

@Test(groups = "unit", testName = "marshall.LongSummaryStatisticsExternalizerTest")
public class LongSummaryStatisticsExternalizerTest extends AbstractExternalizerTest<LongSummaryStatistics> {

    public void test() throws Exception {
        LongSummaryStatistics stats = new LongSummaryStatistics();
        stats.accept(1);
        stats.accept(-Long.MAX_VALUE);

        LongSummaryStatistics deserialized = deserialize(stats);
        assertStatsAreEqual(stats, deserialized);
    }

    @Override
    AbstractExternalizer<LongSummaryStatistics> createExternalizer() {
        return new LongSummaryStatisticsExternalizer();
    }

    private void assertStatsAreEqual(LongSummaryStatistics original, LongSummaryStatistics deserialized) {
        Assert.assertEquals(original.getCount(), deserialized.getCount());
        Assert.assertEquals(original.getMin(), deserialized.getMin());
        Assert.assertEquals(original.getMax(), deserialized.getMax());
        Assert.assertEquals(original.getSum(), deserialized.getSum());
        Assert.assertEquals(original.getAverage(), deserialized.getAverage());
    }
}
