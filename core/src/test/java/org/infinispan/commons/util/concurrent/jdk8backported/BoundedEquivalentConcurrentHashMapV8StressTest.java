package org.infinispan.commons.util.concurrent.jdk8backported;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.AssertJUnit.fail;

/**
* @author William Burns
* @since 7.1
*/
@Test(groups = "stress", testName = "util.concurrent.BoundedEquivalentConcurrentHashMapV8StressTest")
public class BoundedEquivalentConcurrentHashMapV8StressTest extends AbstractInfinispanTest {
   private void testRemovePerformance(BoundedEquivalentConcurrentHashMapV8.Eviction eviction) {
      final int COUNT = 10000000;
      Map<Integer, Integer> bchm = new BoundedEquivalentConcurrentHashMapV8<Integer, Integer>(
            COUNT, COUNT >> 1, eviction, BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener(), 
            AnyEquivalence.INT, AnyEquivalence.INT);
      long startIncludePut = System.currentTimeMillis();
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

         // Only check every 100
         if (i % 100 == 0) {
            // original version needs ~5 min for 200k entries (2h for 1M)
            // fixed version needs < 200ms for 200k (500ms for 1M)
            if (System.currentTimeMillis() - start > 50000)
               fail(eviction.name() + ": removing " + COUNT + " entries takes more than 50 seconds!");
         }
      }
      System.out.println("BCHMV8 Stress Test " + eviction + " took " + 
            (System.currentTimeMillis() - start) + " milliseconds");
      System.out.println("BCHMV8 Entire Stress Test " + eviction + " took " + 
            (System.currentTimeMillis() - startIncludePut) + " milliseconds");
   }

   public void testLRURemovePerformance() {
      testRemovePerformance(BoundedEquivalentConcurrentHashMapV8.Eviction.LRU);
   }

   public void testLIRSRemovePerformance() {
      testRemovePerformance(BoundedEquivalentConcurrentHashMapV8.Eviction.LIRS);
   }
}
