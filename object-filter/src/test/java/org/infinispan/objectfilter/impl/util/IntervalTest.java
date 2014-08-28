package org.infinispan.objectfilter.impl.util;

import org.testng.annotations.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class IntervalTest {

   @Test
   public void testContains() {
      assertTrue(new Interval(Interval.getMinusInf(), false, 1000, false).contains(20));
      assertFalse(new Interval(Interval.getMinusInf(), false, 1000, false).contains(1000));
      assertFalse(new Interval(Interval.getMinusInf(), false, 1000, false).contains(1001));

      assertTrue(new Interval(1000, false, Interval.getPlusInf(), false).contains(2000));
      assertFalse(new Interval(1000, false, Interval.getPlusInf(), false).contains(1000));
      assertFalse(new Interval(1000, false, Interval.getPlusInf(), false).contains(999));
   }
}