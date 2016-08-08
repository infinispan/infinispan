package org.infinispan.functional.stress;

import org.infinispan.Cache;
import org.infinispan.commands.GetAllCommandStressTest;
import org.infinispan.commons.api.functional.EntryView;
import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.Traversable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.test.fwk.InCacheMode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.testng.AssertJUnit.assertEquals;

@InCacheMode(CacheMode.DIST_SYNC)
public class ReadOnlyKeyCommandStressTest extends GetAllCommandStressTest {
   @Override
   protected void workerLogic(Cache<Integer, Integer> cache, Set<Integer> threadKeys, int iteration) {
      FunctionalMap.ReadOnlyMap<Integer, Integer> ro = ReadOnlyMapImpl.create(FunctionalMapImpl.create(cache.getAdvancedCache()));
      Function<EntryView.ReadEntryView<Integer, Integer>, Integer> func =
         (Function<EntryView.ReadEntryView<Integer, Integer>, Integer> & Serializable) (view -> view.get() + 1);
      List<CompletableFuture> futures = new ArrayList(threadKeys.size());
      for (Integer key : threadKeys) {
         futures.add(ro.eval(key, func).thenAccept(value -> assertEquals(Integer.valueOf(key + 1), value)));
      }
      futures.stream().forEach(f -> f.join());
   }
}
