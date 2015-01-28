package org.infinispan.util.concurrent;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8StressTest;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.Eviction;
import org.testng.annotations.Test;

/**
* @author Dan Berindei
* @since 7.0
*/
@Test(groups = "stress", testName = "util.concurrent.BoundedConcurrentHashMapStressTest")
public class BoundedConcurrentHashMapStressTest extends BoundedEquivalentConcurrentHashMapV8StressTest {
   public void testNoEvictionRemovePerformance() {
      Eviction eviction = Eviction.NONE;
      testRemovePerformance(COUNT, new BoundedConcurrentHashMap<Integer, Integer>(
            COUNT, 1, eviction, AnyEquivalence.INT, AnyEquivalence.INT), eviction.toString());
   }
   
   @Test(priority=5)
   public void testLRURemovePerformance() {
      Eviction eviction = Eviction.LRU;
      testRemovePerformance(COUNT, new BoundedConcurrentHashMap<Integer, Integer>(
            COUNT, 1, eviction, AnyEquivalence.INT, AnyEquivalence.INT), eviction.toString());
   }

   @Test(priority=10)
   public void testLIRSRemovePerformance() {
      Eviction eviction = Eviction.LIRS;
      testRemovePerformance(COUNT, new BoundedConcurrentHashMap<Integer, Integer>(
            COUNT, 1, eviction, AnyEquivalence.INT, AnyEquivalence.INT), eviction.toString());
   }


}
