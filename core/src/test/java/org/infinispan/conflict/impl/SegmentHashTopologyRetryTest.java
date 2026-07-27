package org.infinispan.conflict.impl;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.impl.SegmentHashInterceptor;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Verifies that when {@code StateTransferInterceptor} retries a write command
 * (because an {@code OutdatedTopologyException} caused the originator to
 * re-execute the same command), the {@link SegmentHashTracker} is not
 * double-counted.
 *
 * <h2>Retry correctness</h2>
 * On the first execution of a new-entry PUT, the MVCC entry has no
 * {@code oldValue} so the interceptor calls {@code recordInsert}. When the
 * command is retried, the MVCC entry is re-wrapped from the data container
 * which now holds the value written by the first execution. Therefore
 * {@code oldValue != null}, and the interceptor calls
 * {@code recordUpdate(old=v, new=v)}. Because the old hash equals the new
 * hash, the XOR delta is zero and the tracker is unchanged.
 *
 * <p>This test verifies that property directly: after a key is inserted and
 * then the same put is applied a second time (mimicking a retry), the tracker
 * must reflect exactly one entry — not two.</p>
 *
 * <h2>Update retry</h2>
 * When an update is retried, {@code oldValue} on the second execution will be
 * the value from the first execution (not the original value), so the delta
 * will again be zero. The test verifies that case too.
 */
@Test(groups = "functional", testName = "conflict.impl.SegmentHashTopologyRetryTest")
public class SegmentHashTopologyRetryTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-topology-retry-cache";

   public SegmentHashTopologyRetryTest() {
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
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   /**
    * Simulates a topology retry for a new-entry PUT: the same key+value is
    * put twice. The second put mimics the retry path where the MVCC entry
    * picks up {@code oldValue} from the data container.
    *
    * <p>The tracker must show entry count == 1 (not 2) after both puts.</p>
    */
   public void testNewEntryPutRetryDoesNotDoubleCountInTracker() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);
      // Ensure the SegmentHashInterceptor is present in the chain
      assertInterceptorPresent(cache);

      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));

      // First put — interceptor calls recordInsert (oldValue == null)
      cache.put(key, "value");

      // Retry: put same key with same value — interceptor calls
      // recordUpdate(old="value", new="value") → delta == 0 → tracker unchanged
      cache.put(key, "value");

      // The tracker must reflect exactly one entry for this key, not two
      assertEquals(0, ConflictManagerFactory.get(cache).getConflicts().count(),
            "Tracker must not double-count after a put-retry with the same key and value");

      // Explicitly verify the entry count in the tracker via bucket hashes
      int segment = cache.getDistributionManager().getCacheTopology().getSegment(key);
      SegmentHashTracker tracker = extractComponent(cache, SegmentHashTracker.class);
      int totalCount = tracker.getBucketHashes(segment).stream()
            .mapToInt(BucketHash::entryCount).sum();
      assertEquals(1, totalCount,
            "Tracker entry count must be 1 after put + retry (not 2)");
   }

   /**
    * Simulates a topology retry for a key update. On retry the MVCC entry
    * wraps the previously-written value as {@code oldValue}, so the delta is
    * computed against the correct baseline. The tracker must converge to the
    * state as if the update happened exactly once.
    */
   public void testUpdateRetryDoesNotCorruptTracker() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);
      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));

      // Establish initial value
      cache.put(key, "original");

      // First update — interceptor calls recordUpdate(old="original", new="updated")
      cache.put(key, "updated");

      // Retry of the same update — on retry, oldValue from container == "updated"
      // → recordUpdate(old="updated", new="updated") → delta == 0
      cache.put(key, "updated");

      // No conflicts should exist, and count is still 1
      assertEquals(0, ConflictManagerFactory.get(cache).getConflicts().count(),
            "No conflicts after update + retry");

      int segment = cache.getDistributionManager().getCacheTopology().getSegment(key);
      SegmentHashTracker tracker = extractComponent(cache, SegmentHashTracker.class);
      int totalCount = tracker.getBucketHashes(segment).stream()
            .mapToInt(BucketHash::entryCount).sum();
      assertEquals(1, totalCount,
            "Tracker entry count must remain 1 after update + retry");
   }

   /**
    * Simulates a retry of a removal: the same remove is applied twice.
    * On the second execution the entry is gone, so the interceptor sees a
    * removed entry with no value and takes no action (entry count stays 0).
    */
   public void testRemoveRetryDoesNotUnderCountInTracker() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);
      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));

      cache.put(key, "value");

      int segment = cache.getDistributionManager().getCacheTopology().getSegment(key);
      SegmentHashTracker tracker = extractComponent(cache, SegmentHashTracker.class);

      // First remove — recordRemove called
      cache.remove(key);

      // Retry: remove same key again — entry is already absent; the interceptor
      // wraps a null/NullCacheEntry and processEntry returns without touching the tracker
      cache.remove(key);

      int totalCount = tracker.getBucketHashes(segment).stream()
            .mapToInt(BucketHash::entryCount).sum();
      assertEquals(0, totalCount,
            "Tracker entry count must be 0 after remove + retry (not -1 or -2)");

      assertEquals(0, ConflictManagerFactory.get(cache).getConflicts().count(),
            "No conflicts after remove + retry");
   }

   /**
    * End-to-end: after a simulated retry sequence (insert, retry, update,
    * retry, remove, retry), the tracker must be consistent with the actual
    * data container and report zero conflicts.
    */
   public void testTrackerConsistentAfterMixedRetries() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      AdvancedCache<Object, Object> cache = getCache(0);

      for (int i = 0; i < 20; i++) {
         MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
         // insert + retry
         cache.put(key, "v" + i);
         cache.put(key, "v" + i);
         // update + retry
         cache.put(key, "updated-" + i);
         cache.put(key, "updated-" + i);
      }

      assertEquals(0, ConflictManagerFactory.get(cache).getConflicts().count(),
            "No conflicts after mixed insert/update with simulated retries");
   }

   private void assertInterceptorPresent(AdvancedCache<Object, Object> cache) {
      SegmentHashInterceptor interceptor = extractInterceptorChain(cache)
            .findInterceptorExtending(SegmentHashInterceptor.class);
      assertNotNull(interceptor,
            "SegmentHashInterceptor must be present in the interceptor chain");
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }
}
