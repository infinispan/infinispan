package org.infinispan.objectfilter.impl.aggregation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public class DoubleSumTest {

   private static final double DELTA = 0.0000001d;

   @Test
   public void testEmptySum() throws Exception {
      DoubleAvg sum = new DoubleAvg();
      assertNull(sum.getSum());
   }

   @Test
   public void testSum() throws Exception {
      DoubleSum sum = new DoubleSum();
      sum.update(10);
      sum.update(20);
      assertEquals(30.0d, sum.getSum(), DELTA);
   }

   @Test
   public void testSumWithNaN() throws Exception {
      DoubleSum sum = new DoubleSum();
      sum.update(10);
      sum.update(Double.NaN);
      assertEquals(Double.NaN, sum.getSum(), DELTA);
   }

   @Test
   public void testSumWithPlusInf() throws Exception {
      DoubleSum sum = new DoubleSum();
      sum.update(10);
      sum.update(Double.POSITIVE_INFINITY);
      assertEquals(Double.POSITIVE_INFINITY, sum.getSum(), DELTA);
   }

   @Test
   public void testSumWithMinusInf() throws Exception {
      DoubleSum sum = new DoubleSum();
      sum.update(10);
      sum.update(Double.NEGATIVE_INFINITY);
      assertEquals(Double.NEGATIVE_INFINITY, sum.getSum(), DELTA);
   }

   @Test
   public void testSumWithMinusInfAndPlusInf() throws Exception {
      DoubleSum sum = new DoubleSum();
      sum.update(10);
      sum.update(Double.NEGATIVE_INFINITY);
      sum.update(Double.POSITIVE_INFINITY);
      assertEquals(Double.NaN, sum.getSum(), DELTA);
   }
}
