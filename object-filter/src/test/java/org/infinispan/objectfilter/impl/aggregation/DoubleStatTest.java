package org.infinispan.objectfilter.impl.aggregation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public class DoubleStatTest {

   private static final double DELTA = 0.0000001d;

   @Test
   public void testEmptySum() throws Exception {
      DoubleStat sum = new DoubleStat();
      assertNull(sum.getSum());
   }

   @Test
   public void testEmptyAvg() throws Exception {
      DoubleStat avg = new DoubleStat();
      assertNull(avg.getAvg());
   }

   @Test
   public void testSum() throws Exception {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(20);
      assertEquals(30.0d, sum.getSum(), DELTA);
   }

   @Test
   public void testAvg() throws Exception {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(20);
      assertEquals(15.0d, avg.getAvg(), DELTA);
   }

   @Test
   public void testSumWithNaN() throws Exception {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(Double.NaN);
      assertEquals(Double.NaN, sum.getSum(), DELTA);
   }

   @Test
   public void testAvgWithNaN() throws Exception {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(Double.NaN);
      assertEquals(Double.NaN, avg.getAvg(), DELTA);
   }

   @Test
   public void testSumWithPlusInf() throws Exception {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(Double.POSITIVE_INFINITY);
      assertEquals(Double.POSITIVE_INFINITY, sum.getSum(), DELTA);
   }

   @Test
   public void testAvgWithPlusInf() throws Exception {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(Double.POSITIVE_INFINITY);
      assertEquals(Double.POSITIVE_INFINITY, avg.getAvg(), DELTA);
   }

   @Test
   public void testSumWithMinusInf() throws Exception {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(Double.NEGATIVE_INFINITY);
      assertEquals(Double.NEGATIVE_INFINITY, sum.getSum(), DELTA);
   }

   @Test
   public void testAvgWithMinusInf() throws Exception {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(Double.NEGATIVE_INFINITY);
      assertEquals(Double.NEGATIVE_INFINITY, avg.getAvg(), DELTA);
   }

   @Test
   public void testSumWithMinusInfAndPlusInf() throws Exception {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(Double.NEGATIVE_INFINITY);
      sum.update(Double.POSITIVE_INFINITY);
      assertEquals(Double.NaN, sum.getSum(), DELTA);
   }

   @Test
   public void testAvgWithMinusInfAndPlusInf() throws Exception {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(Double.NEGATIVE_INFINITY);
      avg.update(Double.POSITIVE_INFINITY);
      assertEquals(Double.NaN, avg.getAvg(), DELTA);
   }
}
