package org.infinispan.commands;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Stress test designed to test to verify that get many works properly under constant
 * topology changes
 *
 * @author wburns
 * @since 7.2
 */
@Test(groups = "stress", testName = "commands.PutMapCommandStressTest", timeOut = 15*60*1000)
public class PutMapCommandStressTest extends StressTest {
   protected final static int NUM_OWNERS = 3;
   protected final static int CACHE_COUNT = 6;
   protected final static int THREAD_MULTIPLIER = 1;
   protected final static int THREAD_WORKER_COUNT = (CACHE_COUNT - 1) * THREAD_MULTIPLIER;
   protected final static int CACHE_ENTRY_COUNT = 50000;

   protected boolean enableStore;

   @Override
   public Object[] factory() {
      return new Object[]{
            new PutMapCommandStressTest().enableStore(false).cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new PutMapCommandStressTest().enableStore(false).cacheMode(CacheMode.DIST_SYNC).transactional(true),

            new PutMapCommandStressTest().enableStore(true).cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new PutMapCommandStressTest().enableStore(true).cacheMode(CacheMode.DIST_SYNC).transactional(true),
      };
   }

   PutMapCommandStressTest enableStore(boolean enableStore) {
      this.enableStore = enableStore;
      return this;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      builderUsed.clustering().hash().numOwners(NUM_OWNERS);
      builderUsed.clustering().stateTransfer().chunkSize(25000);
      // This is increased just for the put all command when doing full tracing
      builderUsed.clustering().remoteTimeout(12000);
      if (transactional) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (enableStore) {
         builderUsed.persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .shared(true)
               .storeName(PutMapCommandStressTest.class.toString());
      }
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
            new ConfigurationBuilder());
      cacheManagers.add(cm);
      return cm;
   }

   public void testStressNodesLeavingWhileMultiplePutMap() throws Throwable {
      final Map<Integer, Integer> masterValues = new HashMap<Integer, Integer>();
      final Map<Integer, Integer>[] keys = new Map[THREAD_WORKER_COUNT];
      for (int i = 0; i < keys.length; ++i) {
         keys[i] = new HashMap<>();
      }
      // First populate our caches
      for (int i = 0; i < CACHE_ENTRY_COUNT; ++i) {
         masterValues.put(i, i);
         keys[i % THREAD_WORKER_COUNT].put(i, i);
      }

      cache(0, CACHE_NAME).putAll(masterValues);

      for (int i = 0; i < keys.length; ++i) {
         keys[i] = Collections.unmodifiableMap(keys[i]);
      }

      List<Future<Void>> futures = forkWorkerThreads(CACHE_NAME, THREAD_MULTIPLIER, CACHE_COUNT, keys, (cache, keysToUse, iteration) -> {
               // UNCOMMENT following to test insertions by themselves
//               for (Entry<Integer, Integer> entry : keysToUse.entrySet()) {
//                  cache.put(entry.getKey(), entry.getValue());
//               }
               cache.getAdvancedCache().putAll(keysToUse);
               // UNCOMMENT following to make sure puts are propagated properly
//               List<Cache<Integer, Integer>> caches = caches(CACHE_NAME);
//               for (int key : keysToUse.keySet()) {
//                  int hasValue = 0;
//                  for (Cache<Integer, Integer> cacheCheck : caches) {
//                     Integer value = cacheCheck.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).get(key);
//                     if (value != null && value.intValue() == key) {
//                        hasValue++;
//                     }
//                  }
//                  if (hasValue != NUM_OWNERS) {
//                     assertEquals("Key was " + key, NUM_OWNERS, hasValue);
//                  }
//               }
      });

      // TODO: need to figure out code to properly test having a node dying constantly
      // Then spawn a thread that just constantly kills the last cache and recreates over and over again
      futures.add(forkRestartingThread(CACHE_COUNT));
      waitAndFinish(futures, 1, TimeUnit.MINUTES);
   }
}
