package org.infinispan.stats;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies where accesses are counted in a distributed cache.
 * <p>
 * Each access is counted once, on the node that received the request. A node that executes a command forwarded by
 * another node does not count it.
 * </p>
 */
@Test(groups = "functional", testName = "stats.DistributedHotKeyTrackingTest")
public class DistributedHotKeyTrackingTest extends MultipleCacheManagersTest {

   private static final int K = 10;
   private static final int ACCESSES = 30;

   @Override
   protected void createCacheManagers() throws Throwable {
      // A single owner, so an access from the other node is always forwarded and never has a backup.
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      builder.statistics().enable().hotKeys().enabled(true).topK(K);
      createClusteredCaches(2, TestDataSCI.INSTANCE, builder);
   }

   @AfterMethod(alwaysRun = true)
   public void resetTrackers() {
      for (Cache<?, ?> cache : caches()) {
         cache.clear();
         cache.getAdvancedCache().getStats().reset();
      }
   }

   public void testForwardedReadCountedOnlyOnReceivingNode() {
      Object key = getKeyForCache(1);
      cache(1).put(key, "value");
      resetTrackers();

      for (int i = 0; i < ACCESSES; i++) {
         cache(0).get(key);
      }

      assertThat(tracker(0).totalReads()).isEqualTo(ACCESSES);
      assertThat(keys(tracker(0).getTopReads(K))).containsExactly(key);

      assertThat(tracker(1).totalReads()).isZero();
      assertThat(tracker(1).getTopReads(K)).isEmpty();
   }

   public void testForwardedWriteCountedOnlyOnReceivingNode() {
      Object key = getKeyForCache(1);

      for (int i = 0; i < ACCESSES; i++) {
         cache(0).put(key, "v" + i);
      }

      assertThat(tracker(0).totalWrites()).isEqualTo(ACCESSES);
      assertThat(keys(tracker(0).getTopWrites(K))).containsExactly(key);

      assertThat(tracker(1).totalWrites()).isZero();
      assertThat(tracker(1).getTopWrites(K)).isEmpty();
   }

   public void testOwnerCountsItsOwnRequests() {
      Object key = getKeyForCache(1);

      for (int i = 0; i < ACCESSES; i++) {
         cache(1).put(key, "v" + i);
         cache(1).get(key);
      }

      assertThat(tracker(1).totalWrites()).isEqualTo(ACCESSES);
      assertThat(tracker(1).totalReads()).isEqualTo(ACCESSES);

      assertThat(tracker(0).totalWrites()).isZero();
      assertThat(tracker(0).totalReads()).isZero();
   }

   /**
    * Returns the hot key tracker of a node's cache.
    *
    * @param index the node index
    * @return that node's tracker
    */
   private HotKeyTracker tracker(int index) {
      return TestingUtil.extractComponent(cache(index), HotKeyTracker.class);
   }

   /**
    * Extracts the keys of a top-k result, keeping its order.
    *
    * @param top the top-k result
    * @return the keys, most frequent first
    */
   private static List<Object> keys(List<HeavyKeeper.KeyFrequency<Object>> top) {
      return top.stream().map(HeavyKeeper.KeyFrequency::key).toList();
   }
}
