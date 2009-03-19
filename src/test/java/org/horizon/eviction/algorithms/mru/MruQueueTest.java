package org.horizon.eviction.algorithms.mru;

import org.horizon.eviction.algorithms.BaseQueueTest;
import org.testng.annotations.Test;

import java.util.Iterator;

@Test(groups = "unit", testName = "eviction.algorithms.mru.MruQueueTest")
public class MruQueueTest extends BaseQueueTest {
   protected MRUQueue getNewEvictionQueue() {
      return new MRUQueue();
   }

   public void testOrder() throws Exception {
      MRUQueue queue = getNewEvictionQueue();
      fillQueue(queue, 500);

      for (int i = 0; i < 500; i++) {
         if ((i < 100) || (i >= 300 && i < 400)) {
            // visit the entries from 0-99 and the entries from 300 - 399
            queue.visit(i);
         }
      }

      int[] expectedOrder = new int[500];

      // most recently used first
      int index = 0;
      // this is 399 - 300, then 99 - 0
      for (int i = 399; i > 299; i--) expectedOrder[index++] = i;
      for (int i = 99; i > -1; i--) expectedOrder[index++] = i;

      // then 499 - 400, then 299 - 100
      for (int i = 499; i > 399; i--) expectedOrder[index++] = i;
      for (int i = 299; i > 99; i--) expectedOrder[index++] = i;

      int i = 0;
      for (Iterator<Object> it = queue.iterator(); it.hasNext();) {
         assert it.next().equals(expectedOrder[i++]);
         it.remove();
      }

      assert queue.isEmpty();
   }

   public void testReordering() throws Exception {
      MRUQueue queue = getNewEvictionQueue();
      fillQueue(queue, 100);

      for (int i = 0; i < 100; i++) {
//          this should move all the even numbered entries to the bottom of the lruQueue.
//          maxAgeQueue should be unaffected.
         if (i % 2 == 0) queue.visit(i);
      }

      assert 100 == queue.size();

      int count = 0;
      for (Iterator<Object> it = queue.iterator(); it.hasNext();) {
         int entryIndex = (Integer) it.next();

         if (count < 50) {
            // the top 50 should be all evens in the mruQueue
            assert entryIndex % 2 == 0;
         } else {
            // the bottom fifty should all be odd #'s (and 0)
            assert entryIndex % 2 != 0;
         }
         it.remove();
         count++;
      }
      assert queue.isEmpty();
   }

   @Override
   protected int[] expectedKeysForSizingTest() {
      int[] ints = new int[1000];
      int index = 0;
      for (int i = 999; i > -1; i--) ints[index++] = i;
      return ints;
   }
}
