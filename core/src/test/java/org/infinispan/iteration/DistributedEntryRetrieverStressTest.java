package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import java.io.Serializable;
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

import static org.testng.Assert.fail;

/**
 * Stress test designed to test to verify that distributed entry retrieval works properly.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "stress", testName = "iteration.DistributedEntryRetrieverStressTest")
public class DistributedEntryRetrieverStressTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   protected final static int CACHE_COUNT = 5;
   protected final static int THREAD_MULTIPLIER = 5;
   protected final static int CACHE_ENTRY_COUNT = 250000;
   protected ConfigurationBuilder builderUsed;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(CacheMode.DIST_SYNC);
      builderUsed.clustering().hash().numOwners(3);
      builderUsed.clustering().stateTransfer().chunkSize(25000);
      // This is increased just for the put all command when doing full tracing
      builderUsed.clustering().sync().replTimeout(12000000);
      // This way if an iterator gets stuck we know earlier
//      builderUsed.clustering().stateTransfer().timeout(240, TimeUnit.SECONDS);
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

      EmbeddedCacheManager cm = TestCacheManagerFactory.newDefaultCacheManager(true, gcb, new ConfigurationBuilder(),                                                                               false);
      cacheManagers.add(cm);
      return cm;
   }

   @Test
   public void testStressNodesLeavingWhileMultipleIterators() throws InterruptedException, ExecutionException, TimeoutException {
      final Map<Integer, Integer> masterValues = new HashMap<Integer, Integer>();
      // First populate our caches
      for (int i = 0; i < CACHE_ENTRY_COUNT; ++i) {
         masterValues.put(i, i);
      }

      cache(0, CACHE_NAME).putAll(masterValues);

      final AtomicBoolean complete = new AtomicBoolean(false);
      final Exchanger<Exception> exchanger = new Exchanger<Exception>();
      // Now we spawn off CACHE_COUNT of threads.  All but one will constantly calling to iterator while another
      // will constantly be killing and adding new caches
      Future<Void>[] futures = new Future[(CACHE_COUNT - 1) * THREAD_MULTIPLIER + 1];
      for (int j = 0; j < THREAD_MULTIPLIER; ++j) {
      // We iterate over all but the last cache since we kill it constantly
      for (int i = 0; i < CACHE_COUNT - 1; ++i) {
         final Cache<Integer, Integer> cache = cache(i, CACHE_NAME);
         futures[i + j * (CACHE_COUNT -1)] = fork(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               try {
                  int iteration = 0;
                  while (!complete.get()) {
                     log.tracef("Starting iteration %s", iteration);
                     Map<Integer, Integer> seenValues = new HashMap<Integer, Integer>();
                     try (CloseableIterable<CacheEntry<Integer, Integer>> iterable = cache.getAdvancedCache().filterEntries(new AllEntriesFilter())) {
                        for (Map.Entry<Integer, Integer> entry : iterable) {
                           if (seenValues.containsKey(entry.getKey())) {
                              log.tracef("Seen values were: %s", seenValues);
                              throw new IllegalArgumentException(Thread.currentThread() + "-Found duplicate value: " + entry.getKey() + " on iteration " + iteration);
                           } else if (!masterValues.get(entry.getKey()).equals(entry.getValue())) {
                              log.tracef("Seen values were: %s", seenValues);
                              throw new IllegalArgumentException(Thread.currentThread() + "-Found incorrect value: " + entry.getKey() + " with value " + entry.getValue() + " on iteration " + iteration);
                           }
                           seenValues.put(entry.getKey(), entry.getValue());
                        }
                        if (seenValues.size() != masterValues.size()) {
                           Map<Integer, Set<Map.Entry<Integer, Integer>>> target = generateEntriesPerSegment(cache.getAdvancedCache().getDistributionManager().getConsistentHash(), masterValues.entrySet());
                           Map<Integer, Set<Map.Entry<Integer, Integer>>> actual = generateEntriesPerSegment(cache.getAdvancedCache().getDistributionManager().getConsistentHash(), seenValues.entrySet());
                           for (Map.Entry<Integer, Set<Map.Entry<Integer, Integer>>> entry : target.entrySet()) {
                              Set<Map.Entry<Integer, Integer>> entrySet = entry.getValue();
                              Set<Map.Entry<Integer, Integer>> actualEntries = actual.get(entry.getKey());
                              if (actualEntries != null) {
                                 entrySet.removeAll(actualEntries);
                              }
                              if (!entrySet.isEmpty()) {
                                 throw new IllegalArgumentException(Thread.currentThread() + "-Found incorrect amount of entries for segment " +
                                                                          entry.getKey() + " missing entries " + entrySet
                                                                          + " on iteration " + iteration);
                              }
                           }
                        }
                        iteration++;
                     }
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
      }
      }

      // Then spawn a thread that just constantly kills the last cache and recreates over and over again
      futures[futures.length - 1] = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            TestResourceTracker.backgroundTestStarted(DistributedEntryRetrieverStressTest.this);
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
         Exception e = exchanger.exchange(null, 5, TimeUnit.MINUTES);
         fail("Found an exception in at least 1 thread", e);
      } catch (TimeoutException e) {

      }

      complete.set(true);

      // Make sure they all finish properly
      for (int i = 0; i < futures.length; ++i) {
         try {
            futures[i].get(2, TimeUnit.MINUTES);
         } catch (TimeoutException e) {
            log.errorf("Future %s did not complete in time allotted.", i);
            throw e;
         }
      }
   }

   private <K, V> Map<Integer, Set<Map.Entry<K, V>>> generateEntriesPerSegment(ConsistentHash hash, Iterable<Map.Entry<K, V>> entries) {
      Map<Integer, Set<Map.Entry<K, V>>> returnMap = new HashMap<Integer, Set<Map.Entry<K, V>>>();

      for (Map.Entry<K, V> value : entries) {
         int segment = hash.getSegment(value.getKey());
         Set<Map.Entry<K, V>> set = returnMap.get(segment);
         if (set == null) {
            set = new HashSet<Map.Entry<K, V>>();
            returnMap.put(segment, set);
         }
         set.add(new ImmortalCacheEntry(value.getKey(), value.getValue()));
      }
      return returnMap;
   }

   protected static class AllEntriesFilter implements KeyValueFilter, Serializable {

      @Override
      public boolean accept(Object key, Object value, Metadata metadata) {
         return true;
      }
   }
}
