package org.infinispan.conflict.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "conflict.impl.SegmentHashClearConflictManagerTest")
public class SegmentHashClearConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-clear-conflict-cache";
   private static final int NUMBER_OF_CACHE_ENTRIES = 50;

   public SegmentHashClearConflictManagerTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().whenSplit(partitionHandling).mergePolicy(null)
            .stateTransfer().fetchInMemoryState(true);
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   public void testClearResetsHashes() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);

      // Populate, then clear, then populate with different data
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> cache.put(i, "v" + i));
      cache.clear();
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> cache.put(i, "new-v" + i));

      // If clear didn't reset the tracker, the hashes would include stale entries
      assertEquals(0, getConflicts(0).count(),
            "No conflicts expected — clear should have reset hashes before new data was added");
   }

   public void testConflictsDetectedAfterClear() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);

      cache.clear();

      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      cache.put(key, "value");

      // Introduce a local conflict
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, "conflict");

      long conflictCount = getConflicts(0).count();
      assertTrue(conflictCount >= 1, "Expected at least one conflict after clear + local write");
   }

   public void testClearOnOneNodeCreatesConflict() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);

      // Put entries that will be owned by multiple nodes
      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      cache.put(key, "value");

      // Clear only on one node — the other still has the entry
      cache.withFlags(Flag.CACHE_MODE_LOCAL).clear();

      List<Map<Address, CacheEntry<Object, Object>>> conflicts = getConflicts(0).collect(Collectors.toList());
      assertTrue(conflicts.size() >= 1,
            "Expected conflicts when one node cleared but the other still has entries");
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }
}
