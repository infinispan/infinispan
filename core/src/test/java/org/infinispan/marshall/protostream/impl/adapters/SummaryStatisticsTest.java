package org.infinispan.marshall.protostream.impl.adapters;

import static org.testng.AssertJUnit.assertEquals;


import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.LongSummaryStatistics;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.SummaryStatisticsTest")
public class SummaryStatisticsTest extends AbstractAdapterTest {

   public void testDoubleFiniteStats() throws Exception {
      DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
      stats.accept(10.0/3);
      stats.accept(-0.1);

      DoubleSummaryStatistics deserialized = deserialize(stats);
      assertStatsAreEqual(stats, deserialized);
   }

   public void testDoubleNaN() throws Exception {
      DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
      stats.accept(-1);
      stats.accept(Double.NaN);

      DoubleSummaryStatistics deserialized = deserialize(stats);
      assertStatsAreEqual(stats, deserialized);
   }

   public void testDoubleInfinity() throws Exception {
      DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
      stats.accept(Double.POSITIVE_INFINITY);
      stats.accept(-1);

      DoubleSummaryStatistics deserialized = deserialize(stats);
      assertStatsAreEqual(stats, deserialized);
   }

   public void testIntStatsAreMarshallable() throws Exception {
      IntSummaryStatistics original = new IntSummaryStatistics();
      original.accept(1);
      original.accept(-Integer.MAX_VALUE);

      IntSummaryStatistics deserialized = deserialize(original);
      assertEquals(original.getCount(), deserialized.getCount());
      assertEquals(original.getMin(), deserialized.getMin());
      assertEquals(original.getMax(), deserialized.getMax());
      assertEquals(original.getSum(), deserialized.getSum());
      assertEquals(original.getAverage(), deserialized.getAverage());
   }

   public void testLongStatsAreMarshallable() throws Exception {
      LongSummaryStatistics original = new LongSummaryStatistics();
      original.accept(1);
      original.accept(-Long.MAX_VALUE);

      LongSummaryStatistics deserialized = deserialize(original);
      assertEquals(original.getCount(), deserialized.getCount());
      assertEquals(original.getMin(), deserialized.getMin());
      assertEquals(original.getMax(), deserialized.getMax());
      assertEquals(original.getSum(), deserialized.getSum());
      assertEquals(original.getAverage(), deserialized.getAverage());
   }

   private void assertStatsAreEqual(DoubleSummaryStatistics original, DoubleSummaryStatistics deserialized) {
      assertEquals(original.getCount(), deserialized.getCount());
      assertEquals(original.getMin(), deserialized.getMin());
      assertEquals(original.getMax(), deserialized.getMax());
      assertEquals(original.getSum(), deserialized.getSum());
      assertEquals(original.getAverage(), deserialized.getAverage());
   }
}
