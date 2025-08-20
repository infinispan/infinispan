package org.infinispan.query.objectfilter.impl.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "query.objectfilter.impl.util.IntervalTest", groups = "functional")
public class IntervalTest {

   @Test
   public void testContains() {
      assertTrue(new Interval(Interval.<Integer>getMinusInf(), false, 1000, false).contains(20));
      assertFalse(new Interval(Interval.<Integer>getMinusInf(), false, 1000, false).contains(1000));
      assertFalse(new Interval(Interval.<Integer>getMinusInf(), false, 1000, false).contains(1001));

      assertTrue(new Interval(1000, false, Interval.<Integer>getPlusInf(), false).contains(2000));
      assertFalse(new Interval(1000, false, Interval.<Integer>getPlusInf(), false).contains(1000));
      assertFalse(new Interval(1000, false, Interval.<Integer>getPlusInf(), false).contains(999));
   }
}
