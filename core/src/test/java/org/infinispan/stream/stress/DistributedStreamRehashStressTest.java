package org.infinispan.stream.stress;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.StressTest;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.function.SerializablePredicate;
import org.testng.annotations.Test;

/**
 * Stress test designed to test to verify that distributed stream works properly when constant rehashes occur
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "stress", testName = "stream.stress.DistributedStreamRehashStressTest", timeOut = 15*60*1000)
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC })
public class DistributedStreamRehashStressTest extends StressTest {
   protected final String CACHE_NAME = "testCache";
   protected static final int CACHE_COUNT = 5;
   protected static final int THREAD_MULTIPLIER = 5;
   protected static final long CACHE_ENTRY_COUNT = 250000;

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

      EmbeddedCacheManager cm = TestCacheManagerFactory.newDefaultCacheManager(true, gcb, new ConfigurationBuilder());
      cacheManagers.add(cm);
      return cm;
   }

   public void testStressNodesLeavingWhileMultipleCollectors() throws Throwable {
      testStressNodesLeavingWhilePerformingCallable((cache, masterValues, iteration) -> {
         SerializablePredicate<Map.Entry<Integer, Integer>> predicate = e -> (e.getKey() & 1) == 1;
         // Remote invocation with data from cache
         Map<Integer, Integer> results = cache.entrySet().stream()
               .filter(predicate)
               .collect(() -> Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
         // Local invocation
         Map<Integer, Integer> filteredMasterValues = masterValues.entrySet().stream()
               .filter(predicate)
               .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
         KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
         findMismatchedSegments(keyPartitioner, filteredMasterValues, results, iteration);
         assertEquals(CACHE_ENTRY_COUNT / 2, results.size());
      });
   }

   public void testStressNodesLeavingWhileMultipleCount() throws Throwable {
      testStressNodesLeavingWhilePerformingCallable(((cache, masterValues, iteration) -> {
         long size;
         assertEquals(CACHE_ENTRY_COUNT, (size = cache.entrySet().stream().count()),
                 "We didn't get a matching size! Expected " + CACHE_ENTRY_COUNT + " but was " + size);
      }));
   }

   // TODO: this fails still for some reason - NEED to find out why!
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
            KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
            findMismatchedSegments(keyPartitioner, masterValues, seenValues, iteration);
         }
      });
   }

   public void testStressNodesLeavingWhileMultipleIteratorsLocalSegments() throws Throwable {
      testStressNodesLeavingWhilePerformingCallable((cache, masterValues, iteration) -> {
         Map<Integer, Integer> seenValues = new HashMap<>();
         KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
         AdvancedCache<Integer, Integer> advancedCache = cache.getAdvancedCache();
         LocalizedCacheTopology cacheTopology = advancedCache.getDistributionManager().getCacheTopology();
         Set<Integer> targetSegments = cacheTopology.getWriteConsistentHash().getSegmentsForOwner(cacheTopology.getLocalAddress());
         masterValues = masterValues.entrySet().stream()
                 .filter(e -> targetSegments.contains(keyPartitioner.getSegment(e.getKey())))
                 .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
         Iterator<Map.Entry<Integer, Integer>> iterator = cache.entrySet().stream()
                 .distributedBatchSize(50000)
                 .filterKeySegments(IntSets.from(targetSegments))
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
            findMismatchedSegments(keyPartitioner, masterValues, seenValues, iteration);
         }
      });
   }

   private void findMismatchedSegments(KeyPartitioner keyPartitioner, Map<Integer, Integer> masterValues,
                                       Map<Integer, Integer> seenValues, int iteration) {
      Map<Integer, Set<Map.Entry<Integer, Integer>>> target = generateEntriesPerSegment(keyPartitioner,
                                                                                        masterValues.entrySet());
      Map<Integer, Set<Map.Entry<Integer, Integer>>> actual = generateEntriesPerSegment(keyPartitioner, seenValues.entrySet());
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
      final Map<Integer, Integer> masterValues = new HashMap<>();
      // First populate our caches
      for (int i = 0; i < CACHE_ENTRY_COUNT; ++i) {
         masterValues.put(i, i);
      }

      cache(0, CACHE_NAME).putAll(masterValues);

      System.out.println("Done with inserts!");


      List<Future<Void>> futures = forkWorkerThreads(CACHE_NAME, THREAD_MULTIPLIER, CACHE_COUNT, new Object[THREAD_MULTIPLIER * CACHE_COUNT],
         (cache, args, iteration) -> operation.perform(cache, masterValues, iteration));
      futures.add(forkRestartingThread(CACHE_COUNT));
      waitAndFinish(futures, 1, TimeUnit.MINUTES);
   }

   interface PerformOperation {
      void perform(Cache<Integer, Integer> cacheToUse, Map<Integer, Integer> masterValues, int iteration);
   }

   private <K, V> Map<Integer, Set<Map.Entry<K, V>>> generateEntriesPerSegment(KeyPartitioner keyPartitioner,
                                                                               Iterable<Map.Entry<K, V>> entries) {
      Map<Integer, Set<Map.Entry<K, V>>> returnMap = new HashMap<>();

      for (Map.Entry<K, V> value : entries) {
         int segment = keyPartitioner.getSegment(value.getKey());
         Set<Map.Entry<K, V>> set = returnMap.computeIfAbsent(segment, k -> new HashSet<>());
         set.add(new ImmortalCacheEntry(value.getKey(), value.getValue()));
      }
      return returnMap;
   }
}
