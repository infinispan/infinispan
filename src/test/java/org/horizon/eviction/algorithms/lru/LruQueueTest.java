package org.horizon.eviction.algorithms.lru;

import org.horizon.eviction.EvictionQueue;
import org.horizon.eviction.algorithms.BaseQueueTest;
import org.testng.annotations.Test;

import java.util.Iterator;

@Test(groups = "unit", testName = "eviction.algorithms.lru.LruQueueTest")
public class LruQueueTest extends BaseQueueTest {
   protected EvictionQueue getNewEvictionQueue() {
      return new LRUQueue();
   }

   public void testOrder() throws Exception {
      LRUQueue queue = new LRUQueue();
      fillQueue(queue, 500);

      for (int i = 0; i < 500; i++) {
         if ((i < 100) || (i >= 300 && i < 400)) {
            // visit the entries from 0-99 and the entries from 300 - 399
            queue.visit(i);
         }
      }

      int[] expectedOrder = new int[500];

      // least recently udes first
      // this is 100 - 299, then 400 - 499
      int index = 0;
      for (int i = 100; i < 300; i++) expectedOrder[index++] = i;
      for (int i = 400; i < 500; i++) expectedOrder[index++] = i;

      // then 0 - 99, and then 300 - 399
      for (int i = 0; i < 100; i++) expectedOrder[index++] = i;
      for (int i = 300; i < 400; i++) expectedOrder[index++] = i;

      int i = 0;
      for (Iterator<Object> it = queue.iterator(); it.hasNext();) {
         assert it.next().equals(expectedOrder[i++]);
         it.remove();
      }

      assert queue.isEmpty();
   }

   public void testReordering() throws Exception {
      LRUQueue queue = new LRUQueue();
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
            // the top 50 should be all odds in the lruQueue/
            assert entryIndex % 2 != 0;
         } else {
            // the bottom fifty should all be even #'s (and 0)
            assert entryIndex % 2 == 0;
         }
         it.remove();
         count++;
      }
      assert queue.isEmpty();
   }
}
