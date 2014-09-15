package org.infinispan.util.concurrent;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.AssertJUnit.fail;

/**
* @author Dan Berindei
* @since 7.0
*/
@Test(groups = "stress", testName = "util.concurrent.BoundedConcurrentHashMapStressTest")
public class BoundedConcurrentHashMapStressTest extends AbstractInfinispanTest {
   private void testRemovePerformance(BoundedConcurrentHashMap.Eviction eviction) {
      final int COUNT = 200000;
      Map<Integer, Integer> bchm = new BoundedConcurrentHashMap<Integer, Integer>(
            COUNT, 1, eviction, AnyEquivalence.INT, AnyEquivalence.INT);

      // fill the cache
      for (int i = 0; i < COUNT; i++)
         bchm.put(i, i);

      // force a single cache hit (so that accessQueue has a head item)
      bchm.get(0);

      // remove items
      long start = System.currentTimeMillis();
      for (int i = 1; i < COUNT; i++)
      {
         bchm.get(i);
         bchm.remove(i);

         // original version needs ~5 min for 200k entries (2h for 1M)
         // fixed version needs < 200ms for 200k (500ms for 1M)
         if (System.currentTimeMillis() - start > 5000)
            fail(eviction.name() + ": removing " + COUNT + " entries takes more than 5 seconds!");
      }
   }

   public void testLRURemovePerformance() {
      testRemovePerformance(BoundedConcurrentHashMap.Eviction.LRU);
   }

   public void testLIRSRemovePerformance() {
      testRemovePerformance(BoundedConcurrentHashMap.Eviction.LIRS);
   }


}
