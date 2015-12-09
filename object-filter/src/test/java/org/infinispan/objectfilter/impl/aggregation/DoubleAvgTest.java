package org.infinispan.objectfilter.impl.aggregation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public class DoubleAvgTest {

   private static final double DELTA = 0.0000001d;

   @Test
   public void testEmptyAvg() throws Exception {
      DoubleAvg avg = new DoubleAvg();
      assertNull(avg.getAvg());
   }

   @Test
   public void testAvg() throws Exception {
      DoubleAvg avg = new DoubleAvg();
      avg.update(10);
      avg.update(20);
      assertEquals(15.0d, avg.getAvg(), DELTA);
   }

   @Test
   public void testAvgWithNaN() throws Exception {
      DoubleAvg avg = new DoubleAvg();
      avg.update(10);
      avg.update(Double.NaN);
      assertEquals(Double.NaN, avg.getAvg(), DELTA);
   }

   @Test
   public void testAvgWithPlusInf() throws Exception {
      DoubleAvg avg = new DoubleAvg();
      avg.update(10);
      avg.update(Double.POSITIVE_INFINITY);
      assertEquals(Double.POSITIVE_INFINITY, avg.getAvg(), DELTA);
   }

   @Test
   public void testAvgWithMinusInf() throws Exception {
      DoubleAvg avg = new DoubleAvg();
      avg.update(10);
      avg.update(Double.NEGATIVE_INFINITY);
      assertEquals(Double.NEGATIVE_INFINITY, avg.getAvg(), DELTA);
   }

   @Test
   public void testAvgWithMinusInfAndPlusInf() throws Exception {
      DoubleAvg avg = new DoubleAvg();
      avg.update(10);
      avg.update(Double.NEGATIVE_INFINITY);
      avg.update(Double.POSITIVE_INFINITY);
      assertEquals(Double.NaN, avg.getAvg(), DELTA);
   }
}
