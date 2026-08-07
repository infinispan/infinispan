package org.infinispan.conflict.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

@Test(groups = "functional", testName = "conflict.impl.SegmentHashEvictionConflictManagerTest")
public class SegmentHashEvictionConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-eviction-conflict-cache";
   private static final int MAX_ENTRIES = 10;

   public SegmentHashEvictionConflictManagerTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().whenSplit(partitionHandling).mergePolicy(null)
            .stateTransfer().fetchInMemoryState(true);
      builder.memory().maxCount(MAX_ENTRIES);
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   public void testEvictionUpdatesHashTracker() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);

      // Insert more entries than maxCount to trigger eviction
      // Evicted entries should be removed from the hash tracker via EvictionManagerImpl
      for (int i = 0; i < MAX_ENTRIES * 3; i++) {
         cache.put(new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME)), "v" + i);
      }

      // If eviction didn't update the tracker, we'd see phantom hash mismatches
      assertEquals(0, getConflicts(0).count(),
            "No false conflicts after eviction — tracker should reflect only in-memory entries");
   }

   public void testConflictDetectionAfterEviction() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);

      // Fill cache to trigger evictions
      MagicKey conflictKey = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      cache.put(conflictKey, "original");

      for (int i = 0; i < MAX_ENTRIES * 2; i++) {
         cache.put(new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME)), "filler" + i);
      }

      // Put the conflict key back (may have been evicted) and introduce a local conflict
      cache.put(conflictKey, "value");
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(conflictKey, "conflict");

      List<Map<Address, CacheEntry<Object, Object>>> conflicts = getConflicts(0).collect(Collectors.toList());
      assertTrue(conflicts.size() >= 1, "Expected at least one conflict after eviction + local write");
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }
}
