package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Stress test designed to test to verify that get many works properly under constant
 * topology changes
 *
 * @author wburns
 * @since 7.2
 */
@Test(groups = "stress", testName = "commands.GetAllCommandStressTest", timeOut = 15*60*1000)
@InCacheMode({ CacheMode.DIST_SYNC })
public class GetAllCommandStressTest extends StressTest {
   protected final String CACHE_NAME = getClass().getName();
   protected final static int CACHE_COUNT = 6;
   protected final static int THREAD_MULTIPLIER = 4;
   protected final static int CACHE_ENTRY_COUNT = 50000;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      builderUsed.clustering().stateTransfer().chunkSize(25000);
      // Uncomment this line to make it transactional
//      builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      // This is increased just for the put all command when doing full tracing
      builderUsed.clustering().remoteTimeout(30000);
      createClusteredCaches(CACHE_COUNT, CACHE_NAME, builderUsed);
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      // Amend first so we can increase the transport thread pool
      TestCacheManagerFactory.amendGlobalConfiguration(gcb, flags);
      // we need to increase the transport and remote thread pools to default values
      BlockingThreadPoolExecutorFactory executorFactory = new BlockingThreadPoolExecutorFactory(
            25, 25, 10000, 30000);
      gcb.transport().transportThreadPool().threadPoolFactory(executorFactory);

      gcb.transport().remoteCommandThreadPool().threadPoolFactory(executorFactory);

      EmbeddedCacheManager cm = TestCacheManagerFactory.newDefaultCacheManager(true, gcb,
            new ConfigurationBuilder(), false);
      cacheManagers.add(cm);
      return cm;
   }

   public void testStressNodesLeavingWhileMultipleIterators() throws Throwable {
      final Map<Integer, Integer> masterValues = new HashMap<Integer, Integer>();
      int threadWorkerCount = THREAD_MULTIPLIER * (CACHE_COUNT - 1);
      final Set<Integer>[] keys = new Set[threadWorkerCount];
      for (int i = 0; i < keys.length; ++i) {
         keys[i] = new HashSet<>();
      }
      // First populate our caches
      for (int i = 0; i < CACHE_ENTRY_COUNT; ++i) {
         masterValues.put(i, i);
         keys[i % threadWorkerCount].add(i);
      }

      cache(0, CACHE_NAME).putAll(masterValues);

      for (int i = 0; i < keys.length; ++i) {
         keys[i] = Collections.unmodifiableSet(keys[i]);
      }

      List<Future<Void>> futures = forkWorkerThreads(CACHE_NAME, THREAD_MULTIPLIER, CACHE_COUNT, keys, this::workerLogic);

      // Then spawn a thread that just constantly kills the last cache and recreates over and over again
      futures.add(forkRestartingThread());

      waitAndFinish(futures, 1, TimeUnit.MINUTES);
   }

   protected void workerLogic(Cache<Integer, Integer> cache, Set<Integer> threadKeys, int iteration) {
      Map<Integer, Integer> results = cache.getAdvancedCache().getAll(threadKeys);
      assertEquals("Missing: " + diff(threadKeys, results.keySet()), threadKeys.size(), results.size());
      for (Integer key : threadKeys) {
         assertEquals(key, results.get(key));
      }
   }

   private Set<Integer> diff(Set<Integer> superset, Set<Integer> subset) {
      Set<Integer> diff = new HashSet<>(superset);
      diff.removeAll(subset);
      return diff;
   }
}
