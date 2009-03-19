package org.horizon.eviction.algorithms.lfu;

import org.horizon.eviction.EvictionQueue;
import org.horizon.eviction.algorithms.BaseQueueTest;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

@Test(groups = "unit", testName = "eviction.algorithms.lfu.LfuQueueTest")
public class LfuQueueTest extends BaseQueueTest {

   @Override
   protected void testContents(EvictionQueue q) {
      // we should expect the keys in any order here, since they all have an equal visit count
      Set<Integer> allKeys = intSet(1000);

      for (Object key : q) assert allKeys.remove(key);

      assert allKeys.isEmpty();
   }

   private Set<Integer> intSet(int count) {
      Set<Integer> s = new HashSet<Integer>();
      for (int i = 0; i < count; i++) s.add(i);
      return s;
   }

   private int numVisits(LFUQueue queue, Object key) {
      return queue.visitLog.get(key);
   }

   public void testOrder() throws Exception {
      LFUQueue queue = (LFUQueue) getNewEvictionQueue();
      fillQueue(queue, 500);
      assert 500 == queue.size();

      Set<Integer> allKeys = intSet(500);
      for (Object key : queue) assert allKeys.remove(key) : "Unexpected key " + key + " encountered!";
      assert allKeys.isEmpty() : "Not all keys were encountered.  Remaining keys " + allKeys;
      for (int i = 1; i < 500; i += 2) queue.visit(i); // visit all the even keys

      for (Object key : queue) System.out.println("Key: " + key);

      assert (Integer) getFirstEntry(queue) % 2 == 1 : "First key should be odd.  Was " + getFirstEntry(queue);

      System.out.println(queue.visitLog);

      // now check the sort order.
      for (Object key : queue) {
         Integer k = (Integer) key;
         assert numVisits(queue, key) == 1 + (k % 2) : "Expecting " + (1 + (k % 2)) + " visits on key " + key + " but it was " + numVisits(queue, key);
      }

      int k = 0;
      for (Iterator<Object> it = queue.iterator(); it.hasNext() && it.next() != null;) {
         if (k == 250) break;
         it.remove();
         k++;
      }

      assert 250 == queue.size();
      assert !queue.contains(275);

      for (int i = 0; i < 500; i += 2) {
         queue.visit(i);
         assert 2 == numVisits(queue, i) : "Expected visit count to be 2 but it was " + numVisits(queue, i) + " on key " + i;
         if (i > 250) queue.visit(i); // visit again!
      }

      assert 250 == queue.size();

      for (Object key : queue) {
         k = (Integer) key;
         if (k <= 250)
            assert 2 == numVisits(queue, key) : "Expected visit count to be 2 but it was " + numVisits(queue, key) + " on key " + key;
         else
            assert 3 == numVisits(queue, key) : "Expected visit count to be 3 but it was " + numVisits(queue, key) + " on key " + key;
      }
   }

   public void testPrune() throws Exception {
      LFUQueue queue = (LFUQueue) getNewEvictionQueue();
      fillQueue(queue, 500);

      assert 500 == queue.size();

      int i = 0;
      for (Iterator it = queue.iterator(); it.hasNext() && it.next() != null;) {
         if (i % 2 == 0) it.remove();
         i++;
      }
      assert 250 == queue.size();
   }

   protected EvictionQueue getNewEvictionQueue() {
      return new LFUQueue();
   }
}
