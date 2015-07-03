package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Stress test designed to test to verify that get many works properly under constant
 * topology changes
 *
 * @author wburns
 * @since 7.2
 */
@Test(groups = "stress", testName = "commands.GetAllCommandStressTest")
public class GetAllCommandStressTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   protected final static int CACHE_COUNT = 6;
   protected final static int THREAD_MULTIPLIER = 4;
   protected final static int THREAD_WORKER_COUNT = (CACHE_COUNT - 1) * THREAD_MULTIPLIER;
   protected final static int CACHE_ENTRY_COUNT = 50000;
   protected ConfigurationBuilder builderUsed;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(CacheMode.DIST_SYNC);
      builderUsed.clustering().hash().numOwners(2);
      builderUsed.clustering().stateTransfer().chunkSize(25000);
      // Uncomment this line to make it transactional
//      builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      // This is increased just for the put all command when doing full tracing
      builderUsed.clustering().sync().replTimeout(12000);
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

   public void testStressNodesLeavingWhileMultipleIterators() throws InterruptedException, ExecutionException, TimeoutException {
      final Map<Integer, Integer> masterValues = new HashMap<Integer, Integer>();
      final Set<Integer>[] keys = new Set[THREAD_WORKER_COUNT];
      for (int i = 0; i < keys.length; ++i) {
         keys[i] = new HashSet<Integer>();
      }
      // First populate our caches
      for (int i = 0; i < CACHE_ENTRY_COUNT; ++i) {
         masterValues.put(i, i);
         keys[i % THREAD_WORKER_COUNT].add(i);
      }

      cache(0, CACHE_NAME).putAll(masterValues);

      for (int i = 0; i < keys.length; ++i) {
         keys[i] = Collections.unmodifiableSet(keys[i]);
      }

      final AtomicBoolean complete = new AtomicBoolean(false);
      final Exchanger<Throwable> exchanger = new Exchanger<Throwable>();
      // Now we spawn off CACHE_COUNT of threads.  All but one will constantly calling to iterator while another
      // will constantly be killing and adding new caches
      Future<Void>[] futures = new Future[THREAD_WORKER_COUNT + 1];
      for (int j = 0; j < THREAD_MULTIPLIER; ++j) {
      // We iterate over all but the last cache since we kill it constantly
      for (int i = 0; i < CACHE_COUNT - 1; ++i) {
         final int offset = j * (CACHE_COUNT -1) + i;
         final Cache<Integer, Integer> cache = cache(i, CACHE_NAME);
         futures[i + j * (CACHE_COUNT -1)] = fork(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               Set<Integer> keysToUse = keys[offset];
               try {
                  int iteration = 0;
                  
                  while (!complete.get()) {
                     log.tracef("Starting iteration %s", iteration);
                     Map<Integer, Integer> results = cache.getAdvancedCache().getAll(keysToUse);
                     assertEquals(keysToUse.size(), results.size());
                     for (Integer key : keysToUse) {
                        assertEquals(key, results.get(key));
                     }
                     iteration++;
                  }
                  System.out.println(Thread.currentThread() + " finished " + iteration + " iterations!");
                  return null;
               } catch (Throwable e) {
                  // Stop all the others as well
                  complete.set(true);
                  exchanger.exchange(e);
                  throw e;
               }
            }
         });
      }
      }

      // Then spawn a thread that just constantly kills the last cache and recreates over and over again
      futures[futures.length - 1] = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            TestResourceTracker.testThreadStarted(GetAllCommandStressTest.this);
            try {
               Cache<?, ?> cacheToKill = cache(CACHE_COUNT - 1);
               while (!complete.get()) {
                  Thread.sleep(1000);
                  if (cacheManagers.remove(cacheToKill.getCacheManager())) {
                     log.trace("Killing cache to force rehash");
                     cacheToKill.getCacheManager().stop();
                     List<Cache<Object, Object>> caches = caches(CACHE_NAME);
                     if (caches.size() > 0) {
                        TestingUtil.blockUntilViewsReceived(60000, false, caches);
                        TestingUtil.waitForRehashToComplete(caches);
                     }
                  } else {
                     throw new IllegalStateException("Cache Manager " + cacheToKill.getCacheManager() +
                                                           " wasn't found for some reason!");
                  }

                  log.trace("Adding new cache again to force rehash");
                  // We should only create one so just make it the next cache manager to kill
                  cacheToKill = createClusteredCaches(1, CACHE_NAME, builderUsed).get(0);
                  log.trace("Added new cache again to force rehash");
               }
               return null;
            } catch (Exception e) {
               // Stop all the others as well
               complete.set(true);
               exchanger.exchange(e);
               throw e;
            }
         }
      });

      try {
         // If this returns means we had an issue
         Throwable e = exchanger.exchange(null, 1, TimeUnit.MINUTES);
         fail("Found an throwable in at least 1 thread" + e);
      } catch (TimeoutException e) { }

      complete.set(true);

      // Make sure they all finish properly
      for (int i = 0; i < futures.length; ++i) {
         try {
            futures[i].get(20, TimeUnit.MINUTES);
         } catch (TimeoutException e) {
            System.err.println("Future " + i + " did not complete in time allotted.");
            throw e;
         }
      }
   }
}
