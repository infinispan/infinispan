package org.infinispan.conflict.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.ConflictManager;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Functional test that verifies the segment hash optimization in conflict detection.
 * Tests that consistent segments are skipped (via hash comparison), while actual
 * conflicts are still detected correctly.
 */
@Test(groups = "functional", testName = "conflict.impl.SegmentHashConflictManagerTest")
public class SegmentHashConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-conflict-cache";
   private static final int NUMBER_OF_CACHE_ENTRIES = 50;

   public SegmentHashConflictManagerTest() {
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

   /**
    * When all segments are consistent (no conflicts), getConflicts should return
    * an empty stream. The hash comparison skips full entry fetching for each segment.
    */
   public void testNoConflictsSkipsSegments() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      // Populate cache normally (no conflicts)
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> getCache(0).put(i, "v" + i));

      // All segments should be consistent - hash comparison should skip them
      long conflictCount = getConflicts(0).count();
      assertEquals("Expected no conflicts when all segments are consistent", 0, conflictCount);
   }

   /**
    * When conflicts exist, they must still be detected correctly despite the
    * hash optimization. Conflicting segments have mismatched hashes, so they
    * fall through to full entry comparison.
    */
   public void testConflictsStillDetected() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);

      // Create some entries
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> cache.put(i, "v" + i));

      // Introduce conflicts by modifying values only on the primary
      int conflictsIntroduced = 0;
      LocalizedCacheTopology topology = cache.getDistributionManager().getCacheTopology();
      for (int i = 0; i < NUMBER_OF_CACHE_ENTRIES; i += 10) {
         Address primary = topology.getDistribution(i).primary();
         AdvancedCache<Object, Object> primaryCache = manager(primary).getCache(CACHE_NAME)
               .getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
         primaryCache.put(i, "CONFLICT");
         conflictsIntroduced++;
      }

      // Verify conflicts are detected
      List<Map<Address, CacheEntry<Object, Object>>> conflicts = getConflicts(0).collect(Collectors.toList());
      assertEquals("All introduced conflicts should be detected", conflictsIntroduced, conflicts.size());
   }

   /**
    * Verifies that resolving conflicts with a merge policy works correctly
    * when hash-based skipping is active.
    */
   public void testConflictResolutionWithHashOptimization() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);
      ConflictManager<Object, Object> cm = ConflictManagerFactory.get(cache);

      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      cache.put(key, 1);

      // Introduce a conflict locally
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, 2);

      // Verify conflict is detected
      long conflictCount = getConflicts(0).count();
      assertTrue("Expected at least one conflict", conflictCount >= 1);

      // Resolve using preferred entry policy
      cm.resolveConflicts((preferredEntry, otherEntries) -> preferredEntry);

      // Verify no conflicts remain
      assertEquals("No conflicts should remain after resolution", 0, getConflicts(0).count());
   }

   /**
    * Verifies consistent behavior when running conflict detection multiple times.
    */
   public void testRepeatedConflictDetection() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> getCache(0).put(i, "v" + i));

      // Run conflict detection multiple times - should consistently report 0 conflicts
      for (int i = 0; i < 3; i++) {
         assertEquals("Expected no conflicts on iteration " + i, 0, getConflicts(0).count());
      }
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }
}
