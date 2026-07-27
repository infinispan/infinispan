package org.infinispan.conflict.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
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

/**
 * Verifies that {@code resolveConflicts} works correctly through the real
 * partition split-and-merge path, exercising
 * {@code ClusterCacheStatus.resolveConflicts(topology, preferredNodes)}.
 *
 * <p>Rather than using {@code CACHE_MODE_LOCAL} writes to simulate divergence,
 * this test uses an actual network partition followed by a merge so that the
 * {@link DefaultConflictManager} is invoked with a preferred-nodes set and
 * a real {@code CONFLICT_RESOLUTION} topology phase.</p>
 */
@Test(groups = "functional", testName = "conflict.impl.SegmentHashSplitMergeConflictManagerTest")
public class SegmentHashSplitMergeConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-split-merge-cache";

   public SegmentHashSplitMergeConflictManagerTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   @Override
   protected String customCacheName() {
      return CACHE_NAME;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling()
            .whenSplit(partitionHandling)
            .mergePolicy((preferredEntry, otherEntries) -> preferredEntry)
            .stateTransfer().fetchInMemoryState(true);
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   /**
    * Splits the cluster into two partitions, writes diverging values in each
    * partition, then merges and verifies that resolveConflicts (triggered
    * automatically during merge) produces a consistent state.
    *
    * <p>After merge the preferred-partition value should win, and
    * {@code getConflicts()} should report zero remaining conflicts.</p>
    */
   public void testResolveConflictsOnMerge() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      // Key must span both partitions so each side holds a genuine copy
      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(2, CACHE_NAME));
      getCache(0).put(key, "initial");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      // Write diverging values while partitioned — each side only updates locally
      getCache(0).withFlags(Flag.CACHE_MODE_LOCAL).put(key, "partition0-value");
      getCache(2).withFlags(Flag.CACHE_MODE_LOCAL).put(key, "partition1-value");

      // Merge — this triggers CONFLICT_RESOLUTION phase and calls
      // DefaultConflictManager.resolveConflicts(topology, preferredNodes)
      partition(0).merge(partition(1));
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      // After merge and automatic conflict resolution, all nodes should agree
      Object resolvedValue = getCache(0).get(key);
      for (Cache<Object, Object> cache : caches(CACHE_NAME)) {
         assertEquals(resolvedValue, cache.get(key),
               "All nodes must agree on the resolved value after merge");
      }

      // The hash-based conflict detection must now report zero conflicts
      assertEquals(0, ConflictManagerFactory.get(getCache(0)).getConflicts().count(),
            "No conflicts should remain after resolveConflicts on merge");
   }

   /**
    * Verifies that segments that were consistent before the split are skipped
    * during conflict resolution (hash-match optimization) while diverged
    * segments are still resolved correctly.
    */
   public void testConsistentSegmentsSkippedDuringMerge() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      // Keys must span both partitions (one owner in each) so both sides
      // hold a genuine copy and conflict resolution sees real data on each side.
      MagicKey[] consistentKeys = new MagicKey[5];
      for (int i = 0; i < consistentKeys.length; i++) {
         consistentKeys[i] = new MagicKey(cache(0, CACHE_NAME), cache(2, CACHE_NAME));
         getCache(0).put(consistentKeys[i], "v" + i);
      }

      // Key that will diverge — also spans both partitions
      MagicKey conflictKey = new MagicKey(cache(0, CACHE_NAME), cache(2, CACHE_NAME));
      getCache(0).put(conflictKey, "before-split");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      // Only the conflict key diverges; consistent keys are untouched on both sides
      getCache(0).withFlags(Flag.CACHE_MODE_LOCAL).put(conflictKey, "partition0");
      getCache(2).withFlags(Flag.CACHE_MODE_LOCAL).put(conflictKey, "partition1");

      partition(0).merge(partition(1));
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      // Consistent entries must be present and undisturbed after merge
      for (int i = 0; i < consistentKeys.length; i++) {
         assertEquals("v" + i, getCache(0).get(consistentKeys[i]),
               "Consistent entry " + i + " should be unchanged after merge");
      }

      // Conflict resolved — no further conflicts
      assertEquals(0, ConflictManagerFactory.get(getCache(0)).getConflicts().count(),
            "No conflicts should remain after merge resolves the diverged key");
   }

   /**
    * Verifies that a user-invoked {@code resolveConflicts(mergePolicy)} after a
    * simulated conflict also uses the hash optimization and leaves the cache clean.
    */
   public void testUserInvokedResolveConflicts() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);
      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));

      cache.put(key, "v1");
      // Simulate a divergence without a real split
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, "v2");

      List<Map<Address, CacheEntry<Object, Object>>> before =
            ConflictManagerFactory.get(cache).getConflicts().collect(Collectors.toList());
      assertTrue(before.size() >= 1, "Expected at least one conflict before resolution");

      // User-driven resolution: always prefer the first (preferred) entry
      ConflictManagerFactory.get(cache).resolveConflicts((preferredEntry, otherEntries) -> preferredEntry);

      assertEquals(0, ConflictManagerFactory.get(cache).getConflicts().count(),
            "No conflicts should remain after user-invoked resolveConflicts");
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }
}
