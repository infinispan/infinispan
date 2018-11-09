package org.infinispan.commons.stat;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * An unit test for {@link DefaultSimpleStat}
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public class DefaultSimpleStatTest {

   @Test
   public void testDefaultValues() {
      SimpleStat stat = new DefaultSimpleStat();
      assertEquals(-10, stat.getMin(-10));
      assertEquals(-11, stat.getAverage(-11));
      assertEquals(-12, stat.getMax(-12));
      assertEquals(0, stat.count());
      assertTrue(stat.isEmpty());
   }

   @Test
   public void testValues() {
      SimpleStat stat = new DefaultSimpleStat();
      stat.record(-1);
      stat.record(0);
      stat.record(1);
      assertEquals(-1, stat.getMin(-10));
      assertEquals(0, stat.getAverage(-11));
      assertEquals(1, stat.getMax(-12));
      assertEquals(3, stat.count());
      assertFalse(stat.isEmpty());
   }

}
