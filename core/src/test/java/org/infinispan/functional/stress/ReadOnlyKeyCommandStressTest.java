package org.infinispan.functional.stress;

import org.infinispan.Cache;
import org.infinispan.commands.GetAllCommandStressTest;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "stress", testName = "functional.stress.ReadOnlyKeyCommandStressTest")
@InCacheMode(CacheMode.DIST_SYNC)
public class ReadOnlyKeyCommandStressTest extends GetAllCommandStressTest {
   @Override
   protected void workerLogic(Cache<Integer, Integer> cache, Set<Integer> threadKeys, int iteration) {
      FunctionalMap.ReadOnlyMap<Integer, Integer> ro = ReadOnlyMapImpl.create(FunctionalMapImpl.create(cache.getAdvancedCache()));
      List<CompletableFuture> futures = new ArrayList(threadKeys.size());
      for (Integer key : threadKeys) {
         futures.add(ro.eval(key, (view -> view.get() + 1)).thenAccept(value -> assertEquals(Integer.valueOf(key + 1), value)));
      }
      futures.stream().forEach(f -> f.join());
   }
}
