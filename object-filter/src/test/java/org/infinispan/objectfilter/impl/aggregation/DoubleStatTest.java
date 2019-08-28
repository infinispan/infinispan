package org.infinispan.objectfilter.impl.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public class DoubleStatTest {

   private static final double DELTA = 0.0000001d;

   @Test
   public void testEmptySum() {
      DoubleStat sum = new DoubleStat();
      assertNull(sum.getSum());
   }

   @Test
   public void testEmptyAvg() {
      DoubleStat avg = new DoubleStat();
      assertNull(avg.getAvg());
   }

   @Test
   public void testSum() {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(20);
      Double computedSum = sum.getSum();
      assertNotNull(computedSum);
      assertEquals(30.0d, computedSum, DELTA);
   }

   @Test
   public void testAvg() {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(20);
      Double computedAvg = avg.getAvg();
      assertNotNull(computedAvg);
      assertEquals(15.0d, computedAvg, DELTA);
   }

   @Test
   public void testSumWithNaN() {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(Double.NaN);
      Double computedSum = sum.getSum();
      assertNotNull(computedSum);
      assertEquals(Double.NaN, computedSum, DELTA);
   }

   @Test
   public void testAvgWithNaN() {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(Double.NaN);
      Double computedAvg = avg.getAvg();
      assertNotNull(computedAvg);
      assertEquals(Double.NaN, computedAvg, DELTA);
   }

   @Test
   public void testSumWithPlusInf() {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(Double.POSITIVE_INFINITY);
      Double computedSum = sum.getSum();
      assertNotNull(computedSum);
      assertEquals(Double.POSITIVE_INFINITY, computedSum, DELTA);
   }

   @Test
   public void testAvgWithPlusInf() {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(Double.POSITIVE_INFINITY);
      Double computedAvg = avg.getAvg();
      assertNotNull(computedAvg);
      assertEquals(Double.POSITIVE_INFINITY, computedAvg, DELTA);
   }

   @Test
   public void testSumWithMinusInf() {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(Double.NEGATIVE_INFINITY);
      Double computedSum = sum.getSum();
      assertNotNull(computedSum);
      assertEquals(Double.NEGATIVE_INFINITY, computedSum, DELTA);
   }

   @Test
   public void testAvgWithMinusInf() {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(Double.NEGATIVE_INFINITY);
      Double computedAvg = avg.getAvg();
      assertNotNull(computedAvg);
      assertEquals(Double.NEGATIVE_INFINITY, computedAvg, DELTA);
   }

   @Test
   public void testSumWithMinusInfAndPlusInf() {
      DoubleStat sum = new DoubleStat();
      sum.update(10);
      sum.update(Double.NEGATIVE_INFINITY);
      sum.update(Double.POSITIVE_INFINITY);
      Double computedSum = sum.getSum();
      assertNotNull(computedSum);
      assertEquals(Double.NaN, computedSum, DELTA);
   }

   @Test
   public void testAvgWithMinusInfAndPlusInf() {
      DoubleStat avg = new DoubleStat();
      avg.update(10);
      avg.update(Double.NEGATIVE_INFINITY);
      avg.update(Double.POSITIVE_INFINITY);
      Double computedAvg = avg.getAvg();
      assertNotNull(computedAvg);
      assertEquals(Double.NaN, computedAvg, DELTA);
   }
}
