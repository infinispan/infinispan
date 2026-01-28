package org.infinispan.functional.stress;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commands.GetAllCommandStressTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "functional.stress.ReadOnlyKeyCommandStressTest")
@InCacheMode(CacheMode.DIST_SYNC)
public class ReadOnlyKeyCommandStressTest extends GetAllCommandStressTest {
   @Override
   protected void workerLogic(Cache<Integer, Integer> cache, Set<Integer> threadKeys, int iteration) {
      FunctionalMap.ReadOnlyMap<Integer, Integer> ro = FunctionalMap.create(cache.getAdvancedCache()).toReadOnlyMap();
      List<CompletableFuture<Void>> futures = new ArrayList<>(threadKeys.size());
      for (Integer key : threadKeys) {
         futures.add(ro.eval(key, (view -> view.get() + 1)).thenAccept(value -> assertEquals(Integer.valueOf(key + 1), value)));
      }
      futures.forEach(CompletableFuture::join);
   }
}
