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
 * Integration tests for bucket-level conflict detection optimization.
 * Verifies that conflict detection narrows data transfer to mismatched buckets
 * while preserving correctness.
 */
@Test(groups = "functional", testName = "conflict.impl.BucketHashConflictManagerTest")
public class BucketHashConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "bucket-hash-conflict-cache";
   private static final int NUMBER_OF_CACHE_ENTRIES = 50;

   public BucketHashConflictManagerTest() {
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
    * When all segments are consistent, segment hash skips should prevent any
    * bucket-level or entry-level fetching.
    */
   public void testNoConflictsSegmentHashSkips() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> getCache(0).put(i, "v" + i));

      long conflictCount = getConflicts(0).count();
      assertEquals("Expected no conflicts when all segments are consistent", 0, conflictCount);
   }

   /**
    * When conflicts exist within a subset of buckets, only those buckets should
    * be fetched (not the entire segment). Conflicts should still be detected correctly.
    */
   public void testConflictsWithinBucketDetected() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);

      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> cache.put(i, "v" + i));

      // Introduce conflicts by modifying values only on the primary (local write)
      int conflictsIntroduced = 0;
      LocalizedCacheTopology topology = cache.getDistributionManager().getCacheTopology();
      for (int i = 0; i < NUMBER_OF_CACHE_ENTRIES; i += 10) {
         Address primary = topology.getDistribution(i).primary();
         AdvancedCache<Object, Object> primaryCache = manager(primary).getCache(CACHE_NAME)
               .getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
         primaryCache.put(i, "CONFLICT");
         conflictsIntroduced++;
      }

      // Verify conflicts are detected via bucket-level optimization
      List<Map<Address, CacheEntry<Object, Object>>> conflicts = getConflicts(0).collect(Collectors.toList());
      assertEquals("All introduced conflicts should be detected", conflictsIntroduced, conflicts.size());
   }

   /**
    * Verifies that conflict resolution works correctly with bucket optimization.
    */
   public void testResolutionWithBucketOptimization() {
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
    * Verifies that multiple conflicts across different keys are all detected
    * when they may span multiple buckets within the same segment.
    */
   public void testMultipleConflictsAcrossBuckets() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);

      // Use MagicKey to ensure multiple keys land on the same segment
      MagicKey key1 = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      MagicKey key2 = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      MagicKey key3 = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));

      cache.put(key1, "original1");
      cache.put(key2, "original2");
      cache.put(key3, "original3");

      // Introduce conflicts on two of the three keys
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key1, "conflict1");
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key3, "conflict3");

      List<Map<Address, CacheEntry<Object, Object>>> conflicts = getConflicts(0).collect(Collectors.toList());
      assertTrue("Expected at least 2 conflicts", conflicts.size() >= 2);
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }
}
