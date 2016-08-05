package org.infinispan.stream.stress;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.StressTest;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.stream.CacheCollectors;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Stress test designed to test to verify that distributed stream works properly when constant rehashes occur
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "stress", testName = "stream.stress.DistributedStreamRehashStressTest")
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC })
public class DistributedStreamRehashStressTest extends StressTest {
   protected final String CACHE_NAME = getClass().getName();
   protected final static int CACHE_COUNT = 5;
   protected final static int THREAD_MULTIPLIER = 5;
   protected final static long CACHE_ENTRY_COUNT = 250000;
   protected ConfigurationBuilder builderUsed;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      builderUsed.clustering().hash().numOwners(3);
      builderUsed.clustering().stateTransfer().chunkSize(25000);
      // This is increased just for the put all command when doing full tracing
      builderUsed.clustering().remoteTimeout(12000000);
      // This way if an iterator gets stuck we know earlier
      builderUsed.clustering().stateTransfer().timeout(240, TimeUnit.SECONDS);
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

      EmbeddedCacheManager cm = TestCacheManagerFactory.newDefaultCacheManager(true, gcb, new ConfigurationBuilder(),
              false);
      cacheManagers.add(cm);
      return cm;
   }

   public void testStressNodesLeavingWhileMultipleCollectors() throws Throwable {
      testStressNodesLeavingWhilePerformingCallable((cache, masterValues, iteration) -> {
         Map<Integer, Integer> results = cache.entrySet().stream().filter(
                 (Serializable & Predicate<Map.Entry<Integer, Integer>>)
                         e -> (e.getKey().intValue() & 1) == 1).collect(
                 CacheCollectors.serializableCollector(() -> Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
         assertEquals(CACHE_ENTRY_COUNT / 2, results.size());
         for (Map.Entry<Integer, Integer> entry : results.entrySet()) {
            assertEquals(entry.getKey(), entry.getValue());
            assertTrue((entry.getKey() & 1) == 1, "Mismatched value was " + entry.getKey());
         }
      });
   }

   public void testStressNodesLeavingWhileMultipleCount() throws Throwable {
      testStressNodesLeavingWhilePerformingCallable(((cache, masterValues, iteration) -> {
         long size;
         assertEquals(CACHE_ENTRY_COUNT, (size = cache.entrySet().stream().count()),
                 "We didn't get a matching size! Expected " + CACHE_ENTRY_COUNT + " but was " + size);
      }));
   }

   public void testStressNodesLeavingWhileMultipleIterators() throws Throwable {
      testStressNodesLeavingWhilePerformingCallable((cache, masterValues, iteration) -> {
         Map<Integer, Integer> seenValues = new HashMap<>();
         Iterator<Map.Entry<Integer, Integer>> iterator = cache.entrySet().stream()
                 .distributedBatchSize(50000)
                 .iterator();
         while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = iterator.next();
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
            findMismatchedSegments(cache.getAdvancedCache().getDistributionManager().getConsistentHash(),
                    masterValues, seenValues, iteration);
         }
      });
   }

   public void testStressNodesLeavingWhileMultipleIteratorsLocalSegments() throws Throwable {
      testStressNodesLeavingWhilePerformingCallable((cache, masterValues, iteration) -> {
         Map<Integer, Integer> seenValues = new HashMap<>();
         AdvancedCache<Integer, Integer> advancedCache = cache.getAdvancedCache();
         ConsistentHash ch = advancedCache.getDistributionManager().getConsistentHash();
         Set<Integer> targetSegments = ch.getSegmentsForOwner(advancedCache.getCacheManager().getAddress());
         masterValues = masterValues.entrySet().stream()
                 .filter(e -> targetSegments.contains(ch.getSegment(e.getKey())))
                 .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
         Iterator<Map.Entry<Integer, Integer>> iterator = cache.entrySet().stream()
                 .distributedBatchSize(50000)
                 .filterKeySegments(targetSegments)
                 .iterator();
         while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = iterator.next();
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
            findMismatchedSegments(ch, masterValues, seenValues, iteration);
         }
      });
   }

   private void findMismatchedSegments(ConsistentHash ch, Map<Integer, Integer> masterValues,
           Map<Integer, Integer> seenValues, int iteration) {
      Map<Integer, Set<Map.Entry<Integer, Integer>>> target = generateEntriesPerSegment(ch, masterValues.entrySet());
      Map<Integer, Set<Map.Entry<Integer, Integer>>> actual = generateEntriesPerSegment(ch, seenValues.entrySet());
      for (Map.Entry<Integer, Set<Map.Entry<Integer, Integer>>> entry : target.entrySet()) {
         Set<Map.Entry<Integer, Integer>> entrySet = entry.getValue();
         Set<Map.Entry<Integer, Integer>> actualEntries = actual.get(entry.getKey());
         if (actualEntries != null) {
            entrySet.removeAll(actualEntries);
         }
         if (!entrySet.isEmpty()) {
            throw new IllegalArgumentException(Thread.currentThread() + "-Found incorrect amount " +
                    (actualEntries != null ? actualEntries.size() : 0) + " of entries, expected " +
                    entrySet.size() + " for segment " + entry.getKey() + " missing entries " + entrySet
                    + " on iteration " + iteration);
         }
      }
   }

   void testStressNodesLeavingWhilePerformingCallable(final PerformOperation operation)
      throws Throwable {
      final Map<Integer, Integer> masterValues = new HashMap<Integer, Integer>();
      // First populate our caches
      for (int i = 0; i < CACHE_ENTRY_COUNT; ++i) {
         masterValues.put(i, i);
      }

      cache(0, CACHE_NAME).putAll(masterValues);

      System.out.println("Done with inserts!");


      List<Future<Void>> futures = forkWorkerThreads(CACHE_NAME, THREAD_MULTIPLIER, CACHE_COUNT, null,
         (cache, args, iteration) -> operation.perform(cache, masterValues, iteration));
      futures.add(forkRestartingThread());
      waitAndFinish(futures, 1, TimeUnit.MINUTES);
   }

   interface PerformOperation {
      void perform(Cache<Integer, Integer> cacheToUse, Map<Integer, Integer> masterValues, int iteration);
   }

   private <K, V> Map<Integer, Set<Map.Entry<K, V>>> generateEntriesPerSegment(ConsistentHash hash,
           Iterable<Map.Entry<K, V>> entries) {
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
}
