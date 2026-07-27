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
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Verifies the small-segment threshold code path in
 * {@link DefaultConflictManager#findMismatchedBuckets}.
 *
 * <p>When a segment has a hash mismatch but contains at most
 * {@code SMALL_SEGMENT_THRESHOLD} (64) entries, the per-bucket narrowing step
 * is skipped and all entries in the segment are fetched directly. This test
 * uses a large segment count to keep per-segment entry counts small, and then
 * verifies that conflicts are still detected and resolved correctly in that
 * code path.</p>
 */
@Test(groups = "functional", testName = "conflict.impl.SegmentHashSmallSegmentConflictManagerTest")
public class SegmentHashSmallSegmentConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-small-segment-cache";
   // 256 segments with NUM_ENTRIES entries will keep ~1-2 entries per segment on average
   private static final int NUM_SEGMENTS = 256;
   // Well below the SMALL_SEGMENT_THRESHOLD of 64 per segment
   private static final int NUM_ENTRIES = 40;

   public SegmentHashSmallSegmentConflictManagerTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      // A non-null mergePolicy is required to enable resolveConflictsOnMerge(),
      // which wires SegmentHashInterceptor into the chain.
      builder.clustering().partitionHandling().whenSplit(partitionHandling)
            .mergePolicy((preferredEntry, otherEntries) -> preferredEntry)
            .stateTransfer().fetchInMemoryState(true);
      // Large segment count keeps entries-per-segment well below the threshold of 64
      builder.clustering().hash().numSegments(NUM_SEGMENTS);
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   /**
    * With few entries per segment the segment-level hash mismatch triggers the
    * small-segment code path (all-buckets fallback). Conflicts must still be
    * detected correctly.
    */
   public void testConflictsDetectedViaSmallSegmentPath() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);

      // Distribute entries across multiple segments
      for (int i = 0; i < NUM_ENTRIES; i++) {
         cache.put("key-" + i, "v" + i);
      }

      // Introduce one conflict per entry on the primary — the segment for each
      // key will have a hash mismatch with at most 1 entry (well below threshold)
      int conflictsIntroduced = 0;
      LocalizedCacheTopology topology = cache.getDistributionManager().getCacheTopology();
      for (int i = 0; i < NUM_ENTRIES; i += 5) {
         String key = "key-" + i;
         Address primary = topology.getDistribution(key).primary();
         manager(primary).getCache(CACHE_NAME).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL).put(key, "CONFLICT-" + i);
         conflictsIntroduced++;
      }

      List<Map<Address, CacheEntry<Object, Object>>> conflicts =
            getConflicts(0).collect(Collectors.toList());
      assertEquals(conflictsIntroduced, conflicts.size(),
            "All conflicts should be detected via the small-segment all-buckets fallback path");
   }

   /**
    * When all segments are consistent (no conflicts), the small-segment path
    * must still correctly report zero conflicts.
    */
   public void testNoConflictsWithSmallSegments() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      for (int i = 0; i < NUM_ENTRIES; i++) {
         getCache(0).put("key-" + i, "v" + i);
      }

      assertEquals(0, getConflicts(0).count(),
            "Expected no conflicts with small segments and consistent data");
   }

   /**
    * Uses a MagicKey to put exactly one entry in a known segment, then
    * introduces a local conflict. Verifies the small-segment path detects it.
    */
   public void testSingleEntrySegmentConflict() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);
      // MagicKey ensures the key lands on the primary of node 0
      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      cache.put(key, "original");

      // Introduce local conflict — segment now has 1 entry and a hash mismatch
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, "conflict");

      List<Map<Address, CacheEntry<Object, Object>>> conflicts =
            getConflicts(0).collect(Collectors.toList());
      assertTrue(conflicts.size() >= 1,
            "Single-entry segment conflict should be detected via the small-segment path");
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }
}
