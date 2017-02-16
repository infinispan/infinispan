package org.infinispan.objectfilter.impl.util;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class IntervalTreeTest {

   @Test
   public void testSingleIntervalStab() {
      Interval<Integer> interval = new Interval<>(3, true, 100, false);
      IntervalTree<Integer, String> tree = new IntervalTree<>();
      tree.add(interval);

      List<IntervalTree.Node<Integer, String>> result = tree.stab(3);
      assertEquals(1, result.size());
      assertEquals(interval, result.get(0).interval);

      result = tree.stab(100);
      assertEquals(0, result.size());

      result = tree.stab(4);
      assertEquals(1, result.size());
      assertEquals(interval, result.get(0).interval);

      result = tree.stab(99);
      assertEquals(1, result.size());
      assertEquals(interval, result.get(0).interval);

      result = tree.stab(2);
      assertEquals(0, result.size());

      result = tree.stab(101);
      assertEquals(0, result.size());
   }
}
