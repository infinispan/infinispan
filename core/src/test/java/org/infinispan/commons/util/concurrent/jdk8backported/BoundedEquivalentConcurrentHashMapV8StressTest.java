package org.infinispan.commons.util.concurrent.jdk8backported;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Eviction;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.fail;
import static org.testng.AssertJUnit.assertNotNull;

/**
* @author William Burns
* @since 7.1
*/
@Test(groups = "stress", testName = "util.concurrent.BoundedEquivalentConcurrentHashMapV8StressTest")
public class BoundedEquivalentConcurrentHashMapV8StressTest extends AbstractInfinispanTest {
   protected static final int COUNT = 10_000_000;
   
   protected void testRemovePerformance(int count, Map<Integer, Integer> bchm,
         String evictionName) {
      long startIncludePut = System.currentTimeMillis();

      int j = 0;
      long firstHalfNano = System.nanoTime();
      // fill the cache
      for (; j < count / 2; j++)
         bchm.put(j, j);
      firstHalfNano = System.nanoTime() - firstHalfNano;

      long secondHalfNano = System.nanoTime();
      for (; j < count; j++)
         bchm.put(j, j);
      secondHalfNano = System.nanoTime() - secondHalfNano;
      
      // force a single cache hit (so that accessQueue has a head item)
      bchm.get(0);

      // remove items
      long start = System.currentTimeMillis();
      long getNano = 0;
      long removeNano = 0;
      for (int i = 1; i < count; i++)
      {
         long getBegin = System.nanoTime();
         assertNotNull(bchm.get(i));
         getNano += System.nanoTime() - getBegin;
         long removeBegin = System.nanoTime();
         bchm.remove(i);
         removeNano += System.nanoTime() - removeBegin;

         // Only check every 100
         if (i % 100 == 0) {
            if (System.currentTimeMillis() - start > TimeUnit.SECONDS.toMillis(50))
               fail(evictionName + ": removing " + count + " entries takes more than 50 seconds!");
         }
      }
      System.out.println("BCHM Stress Test " + evictionName + " took " + 
            (System.currentTimeMillis() - start) + " milliseconds");
      System.out.println("BCHM Entire Stress Test " + evictionName + " took " + 
            (System.currentTimeMillis() - startIncludePut) + " milliseconds");
      
      System.out.println("First half of puts took " + firstHalfNano + " nanoseconds");
      System.out.println("Second half of puts took " + secondHalfNano + " nanoseconds");
      System.out.println("Gets took: " + getNano + " nanoseconds");
      System.out.println("Removes took: " + removeNano + " nanoseconds");
   }

   public void testNoEvictionRemovePerformance() {
      Eviction lru = Eviction.NONE;
      testRemovePerformance(COUNT, new BoundedEquivalentConcurrentHashMapV8<Integer, Integer>(COUNT, COUNT >> 1, lru,
            BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener(), AnyEquivalence.INT, AnyEquivalence.INT),
            lru.toString());
   }

   @Test(priority = 5)
   public void testLRURemovePerformance() {
      Eviction lru = Eviction.LRU;
      testRemovePerformance(COUNT, new BoundedEquivalentConcurrentHashMapV8<Integer, Integer>(COUNT, COUNT >> 1, lru,
            BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener(), AnyEquivalence.INT, AnyEquivalence.INT),
            lru.toString());
   }

   @Test(priority = 10)
   public void testLIRSRemovePerformance() {
      Eviction lirs = Eviction.LIRS;
      testRemovePerformance(COUNT, new BoundedEquivalentConcurrentHashMapV8<Integer, Integer>(COUNT, COUNT >> 1, lirs,
            BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener(), AnyEquivalence.INT, AnyEquivalence.INT),
            lirs.toString());
   }
}
