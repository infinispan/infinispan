package org.infinispan.marshall.exts;

import java.io.IOException;
import java.util.DoubleSummaryStatistics;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "marshall.DoubleSummaryStatisticsExternalizerTest")
public class DoubleSummaryStatisticsExternalizerTest extends AbstractExternalizerTest<DoubleSummaryStatistics> {

    public void testFinite() throws Exception {
        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        stats.accept(10.0/3);
        stats.accept(-0.1);

        DoubleSummaryStatistics deserialized = deserialize(stats);
        assertStatsAreEqual(stats, deserialized);
    }

    public void testPositiveNegativeInfinites() throws Exception {
        // In JDK 10+, we expect the externalizer to pass inner state values through constructor, instead of via
        // reflection. The state of this statistics instance however doesn't pass constructor validations, so
        // deserialization of this instance fails in JDK 10+.

        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        stats.accept(Double.POSITIVE_INFINITY);
        stats.accept(Double.NEGATIVE_INFINITY);

        try {
            DoubleSummaryStatistics deserialized = deserialize(stats);
            assertStatsAreEqual(stats, deserialized);
        } catch (IOException e) {
            if (SecurityActions.getConstructor(DoubleSummaryStatistics.class,
                    long.class, double.class, double.class, double.class) != null) {
                // JDK 10+, ignore
            } else {
                throw e;
            }
        }
    }

    public void testNaN() throws Exception {
        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        stats.accept(-1);
        stats.accept(Double.NaN);

        DoubleSummaryStatistics deserialized = deserialize(stats);
        assertStatsAreEqual(stats, deserialized);
    }

    public void testInfinity() throws Exception {
        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        stats.accept(Double.POSITIVE_INFINITY);
        stats.accept(-1);

        DoubleSummaryStatistics deserialized = deserialize(stats);
        assertStatsAreEqual(stats, deserialized);
    }

    @Override
    AbstractExternalizer<DoubleSummaryStatistics> createExternalizer() {
        return new DoubleSummaryStatisticsExternalizer();
    }

    private void assertStatsAreEqual(DoubleSummaryStatistics original, DoubleSummaryStatistics deserialized) {
        Assert.assertEquals(original.getCount(), deserialized.getCount());
        Assert.assertEquals(original.getMin(), deserialized.getMin());
        Assert.assertEquals(original.getMax(), deserialized.getMax());
        Assert.assertEquals(original.getSum(), deserialized.getSum());
        Assert.assertEquals(original.getAverage(), deserialized.getAverage());
    }
}
