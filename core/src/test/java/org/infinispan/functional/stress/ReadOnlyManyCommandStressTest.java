package org.infinispan.functional.stress;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commands.GetAllCommandStressTest;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.Traversable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "ReadOnlyManyCommandStressTest", timeOut = 15*60*1000)
@InCacheMode(CacheMode.SCATTERED_SYNC)
public class ReadOnlyManyCommandStressTest extends GetAllCommandStressTest {

   @Override
   protected void workerLogic(Cache<Integer, Integer> cache, Set<Integer> threadKeys, int iteration) {
      FunctionalMap.ReadOnlyMap<Integer, Integer> ro = ReadOnlyMapImpl.create(FunctionalMapImpl.create(cache.getAdvancedCache()));
      Traversable<Pair> results = ro.evalMany(threadKeys, view -> new Pair(view.key(), view.get() + 1));
      Counter counter = new Counter();
      results.forEach(p -> { counter.inc(); assertEquals(p.key + 1, p.value); });
      assertEquals(threadKeys.size(), counter.get());
   }

   private static class Pair implements Serializable {
      public final int key;
      public final int value;

      public Pair(int key, int value) {
         this.key = key;
         this.value = value;
      }
   }

   private static class Counter {
      private int counter;

      public void inc() {
         ++counter;
      }

      public int get() {
         return counter;
      }
   }
}
