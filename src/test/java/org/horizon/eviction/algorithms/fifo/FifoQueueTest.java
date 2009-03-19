package org.horizon.eviction.algorithms.fifo;

import org.horizon.eviction.EvictionQueue;
import org.horizon.eviction.algorithms.BaseQueueTest;
import org.testng.annotations.Test;

import java.util.Iterator;

@Test(groups = "unit", testName = "eviction.algorithms.fifo.FifoQueueTest")
public class FifoQueueTest extends BaseQueueTest {

   public void testOrder() throws Exception {
      FIFOQueue queue = new FIFOQueue();

      fillQueue(queue, 5000);

      assert getFirstEntry(queue).equals(0);

      // now make sure the ordering is correct.
      int k = 0;
      for (Iterator<Object> i = queue.iterator(); i.hasNext();) {
         Object key = i.next();
         assert key.equals(k);
         i.remove();
         k++;
         if (k == 2500) break;
      }

      assert getFirstEntry(queue).equals(2500);

      assert 2500 == queue.size();
      assert !queue.isEmpty();

      k = 2500;

      for (Iterator<Object> i = queue.iterator(); i.hasNext();) {
         Object key = i.next();
         assert key.equals(k);
         i.remove();
         k++;
      }

      assert 0 == queue.size();
      assert queue.isEmpty();
   }

   protected EvictionQueue getNewEvictionQueue() {
      return new FIFOQueue();
   }
}
