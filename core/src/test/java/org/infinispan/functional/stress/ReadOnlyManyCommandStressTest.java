package org.infinispan.functional.stress;

import org.infinispan.Cache;
import org.infinispan.commands.GetAllCommandStressTest;
import org.infinispan.commands.StressTest;
import org.infinispan.commons.api.functional.EntryView;
import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.Traversable;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "stress")
@InCacheMode(CacheMode.DIST_SYNC)
public class ReadOnlyManyCommandStressTest extends GetAllCommandStressTest {

   @Override
   protected void workerLogic(Cache<Integer, Integer> cache, Set<Integer> threadKeys, int iteration) {
      FunctionalMap.ReadOnlyMap<Integer, Integer> ro = ReadOnlyMapImpl.create(FunctionalMapImpl.create(cache.getAdvancedCache()));
      Function<EntryView.ReadEntryView<Integer, Integer>, Pair> func =
         (Function<EntryView.ReadEntryView<Integer, Integer>, Pair> & Serializable) (view -> new Pair(view.key(), view.get() + 1));
      Traversable<Pair> results = ro.evalMany(threadKeys, func);
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
