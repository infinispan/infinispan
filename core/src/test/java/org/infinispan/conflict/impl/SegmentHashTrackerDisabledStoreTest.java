package org.infinispan.conflict.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Verifies that the {@link SegmentHashTracker} is disabled when a non-shared
 * persistence store is configured, and that conflict detection still works
 * correctly by falling back to on-demand {@link SegmentHasher} computation.
 *
 * <p>The tracker is intentionally disabled for non-shared stores because
 * the tracker only reflects in-memory state; a non-shared store may hold
 * entries that are not in the data container, making the incremental hash
 * unreliable. When disabled, {@link DefaultConflictManager} computes bucket
 * hashes on demand by scanning the data container directly.</p>
 */
@Test(groups = "functional", testName = "conflict.impl.SegmentHashTrackerDisabledStoreTest")
public class SegmentHashTrackerDisabledStoreTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-tracker-disabled-store-cache";
   private static final int NUMBER_OF_CACHE_ENTRIES = 50;

   public SegmentHashTrackerDisabledStoreTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().whenSplit(partitionHandling).mergePolicy(null)
            .stateTransfer().fetchInMemoryState(true);
      // Non-shared store: tracker must be disabled
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(false);
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   /**
    * The tracker must report {@code isEnabled() == false} when a non-shared
    * store is present.
    */
   public void testTrackerIsDisabledWithNonSharedStore() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      for (Cache<Object, Object> cache : caches(CACHE_NAME)) {
         SegmentHashTracker tracker = TestingUtil.extractComponent(cache, SegmentHashTracker.class);
         assertFalse(tracker.isEnabled(),
               "SegmentHashTracker must be disabled when a non-shared store is configured");
      }
   }

   /**
    * When the tracker is disabled, conflict detection falls back to on-demand
    * SegmentHasher computation. Consistent data must still yield zero conflicts.
    */
   public void testNoConflictsWithTrackerDisabled() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      for (int i = 0; i < NUMBER_OF_CACHE_ENTRIES; i++) {
         getCache(0).put(i, "v" + i);
      }

      assertEquals(0, getConflicts(0).count(),
            "Expected no conflicts when data is consistent, even with tracker disabled");
   }

   /**
    * Conflicts must still be detected via on-demand hash computation when the
    * tracker is disabled.
    */
   public void testConflictsDetectedWithTrackerDisabled() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);
      for (int i = 0; i < NUMBER_OF_CACHE_ENTRIES; i++) {
         cache.put(i, "v" + i);
      }

      // Verify tracker really is disabled before testing conflict detection
      SegmentHashTracker tracker = TestingUtil.extractComponent(cache, SegmentHashTracker.class);
      assertFalse(tracker.isEnabled(), "Pre-condition: tracker must be disabled");

      int conflictsIntroduced = 0;
      LocalizedCacheTopology topology = cache.getDistributionManager().getCacheTopology();
      for (int i = 0; i < NUMBER_OF_CACHE_ENTRIES; i += 10) {
         Address primary = topology.getDistribution(i).primary();
         manager(primary).getCache(CACHE_NAME).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL).put(i, "CONFLICT");
         conflictsIntroduced++;
      }

      List<Map<Address, CacheEntry<Object, Object>>> conflicts =
            getConflicts(0).collect(Collectors.toList());
      assertEquals(conflictsIntroduced, conflicts.size(),
            "All conflicts should be detected via on-demand SegmentHasher when tracker is disabled");
   }

   /**
    * Conflict resolution (user-driven) must work correctly when the tracker is
    * disabled and the fallback computation path is active.
    */
   public void testConflictResolutionWithTrackerDisabled() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);
      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));

      cache.put(key, "v1");
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, "v2");

      assertTrue(getConflicts(0).count() >= 1, "Expected at least one conflict before resolution");

      ConflictManagerFactory.get(cache).resolveConflicts((preferredEntry, otherEntries) -> preferredEntry);

      assertEquals(0, getConflicts(0).count(),
            "No conflicts should remain after resolution with tracker disabled");
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }
}
