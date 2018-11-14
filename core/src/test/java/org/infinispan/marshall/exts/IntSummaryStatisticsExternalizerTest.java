package org.infinispan.marshall.exts;

import java.util.IntSummaryStatistics;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.testng.annotations.Test;
import org.testng.Assert;

@Test(groups = "unit", testName = "marshall.IntSummaryStatisticsExternalizerTest")
public class IntSummaryStatisticsExternalizerTest extends AbstractExternalizerTest<IntSummaryStatistics> {

    public void test() throws Exception {
        IntSummaryStatistics stats = new IntSummaryStatistics();
        stats.accept(1);
        stats.accept(-Integer.MAX_VALUE);

        IntSummaryStatistics deserialized = deserialize(stats);
        assertStatsAreEqual(stats, deserialized);
    }

    @Override
    AbstractExternalizer<IntSummaryStatistics> createExternalizer() {
        return new IntSummaryStatisticsExternalizer();
    }

    private void assertStatsAreEqual(IntSummaryStatistics original, IntSummaryStatistics deserialized) {
        Assert.assertEquals(original.getCount(), deserialized.getCount());
        Assert.assertEquals(original.getMin(), deserialized.getMin());
        Assert.assertEquals(original.getMax(), deserialized.getMax());
        Assert.assertEquals(original.getSum(), deserialized.getSum());
        Assert.assertEquals(original.getAverage(), deserialized.getAverage());
    }
}
