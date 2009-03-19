package org.horizon.eviction.algorithms;

import org.horizon.eviction.EvictionQueue;
import org.testng.annotations.Test;

@Test(groups = "unit")
public abstract class BaseQueueTest {
   protected Object getFirstEntry(EvictionQueue q) {
      return q.iterator().next();
   }

   protected void fillQueue(EvictionQueue q, int numEntries) {
      for (int i = 0; i < numEntries; i++) q.add(i);
   }

   protected abstract EvictionQueue getNewEvictionQueue();

   public void testSizingAndContents() {
      EvictionQueue q = getNewEvictionQueue();

      assert q.isEmpty();
      assert q.size() == 0;

      fillQueue(q, 1000);

      assert !q.isEmpty();
      assert 1000 == q.size();

      assert q.contains(1);
      assert q.contains(999);
      assert !q.contains(1000);

      testContents(q);

      for (int i = 0; i < 1000; i++) {
         assert q.size() == 1000 - i;
         q.remove(i);
      }

      assert q.size() == 0 : "Was expecting size=0 but it was " + q.size();
      assert q.isEmpty();
   }

   protected int[] expectedKeysForSizingTest() {
      int[] ints = new int[1000];
      for (int i = 0; i < 1000; i++) ints[i] = i;
      return ints;
   }

   protected void testContents(EvictionQueue q) {
      int i = 0;
      int[] expected = expectedKeysForSizingTest();
      for (Object key : q) {
         assert key.equals(expected[i]);
         if (i < 1000) i++;
      }
   }
}
