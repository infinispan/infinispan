package org.infinispan.client.hotrod.near;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.BloomFilter;
import org.infinispan.commons.util.MurmurHash3BloomFilter;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies that the bloom filter the client keeps for the server stays in sync with the near cache without having to
 * walk it, and that it is only resent when the bits the server has actually changed.
 */
@Test(groups = "functional", testName = "client.hotrod.near.NearCacheBloomFilterBitsTest")
public class NearCacheBloomFilterBitsTest extends SingleHotRodServerTest {
   // Small on purpose, both to force evictions and to keep the filter small enough for colliding keys to be close
   private static final int NEAR_CACHE_SIZE = 4;

   private AssertsNearCache<Integer, String> assertClient;

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(NEAR_CACHE_SIZE)
            .nearCacheUseBloomFilter(true);
      assertClient = AssertsNearCache.create(this.cache(), builder);
      return assertClient.manager;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() {
      super.clearContent();
      // Empties the near cache as well and leaves the server with a filter matching it
      assertClient.remote.clear();
      CompletionStages.join(((InternalRemoteCache<?, ?>) assertClient.remote).updateBloomFilter());
      assertClient.resetEvents();
   }

   @Override
   protected void teardown() {
      if (assertClient != null) {
         assertClient.stop();
         assertClient = null;
      }

      super.teardown();
   }

   public void testBitsFollowNearCacheContents() {
      for (int i = 0; i < NEAR_CACHE_SIZE * 3; ++i) {
         assertClient.remote.put(i, "v1");
         assertClient.remote.get(i);
         assertClient.resetEvents();
         assertBloomBitsMatchNearCache();
      }

      for (int i = 0; i < NEAR_CACHE_SIZE * 3; ++i) {
         assertClient.remote.remove(i);
         assertClient.resetEvents();
         assertBloomBitsMatchNearCache();
      }
   }

   public void testBitsAreEmptyAfterClear() {
      for (int i = 0; i < NEAR_CACHE_SIZE; ++i) {
         assertClient.remote.put(i, "v1");
         assertClient.remote.get(i);
      }
      assertClient.resetEvents();
      assertTrue(nearCacheService().calculateBloomBits().length > 0, "No bit was set for a populated near cache");

      nearCacheService().clear();

      assertEquals(0, nearCacheService().size());
      assertArrayEquals(new byte[0], nearCacheService().calculateBloomBits());
   }

   public void testFalsePositiveDoesNotRefreshServerFilter() {
      NearCacheService<Integer, String> nearCacheService = nearCacheService();
      int version = assertClient.bloomFilterVersion();

      // These are the removals a stale server filter asks for, none of them changes what the near cache holds
      for (int i = 0; i < 100; ++i) {
         assertFalse(nearCacheService.remove(i));
      }
      assertClient.resetEvents();

      assertEquals(version, assertClient.bloomFilterVersion(),
            "A false positive invalidation must not make the client resend the filter");
   }

   public void testRemovalClearingNoBitDoesNotRefreshServerFilter() {
      int key = 1;
      int sharedBitsKey = findKeyUsingOnlyBitsOf(key);

      cacheLocally(key);
      cacheLocally(sharedBitsKey);
      int version = assertClient.bloomFilterVersion();

      // Every bit of this key is also used by a key which stays in the near cache, so the server filter is already
      // the one the client wants it to have
      assertTrue(nearCacheService().remove(sharedBitsKey));
      assertClient.resetEvents();

      assertEquals(version, assertClient.bloomFilterVersion(),
            "A removal which cleared no bit must not make the client resend the filter");
      assertBloomBitsMatchNearCache();
   }

   public void testEvictionsRefreshServerFilter() {
      int version = assertClient.bloomFilterVersion();

      for (int i = 0; i < NEAR_CACHE_SIZE * 4; ++i) {
         cacheLocally(i);
      }

      assertTrue(assertClient.bloomFilterVersion() > version, "Evictions never refreshed the server filter");
      assertBloomBitsMatchNearCache();
   }

   private void cacheLocally(int key) {
      assertClient.remote.put(key, "v1");
      assertClient.remote.get(key);
      assertClient.resetEvents();
      // A get overlapping a bloom filter update does not cache, wait for any pending one to be done
      eventually(() -> !assertClient.hasPendingBloomUpdate());
   }

   private void assertBloomBitsMatchNearCache() {
      NearCacheService<Integer, String> nearCacheService = nearCacheService();
      // Makes sure any pending eviction is applied before the contents are read
      nearCacheService.size();

      BloomFilter<byte[]> expected = MurmurHash3BloomFilter.createFilter(nearCacheService.getBloomFilterBits());
      for (Map.Entry<Integer, MetadataValue<String>> entry : nearCacheService) {
         expected.addToFilter(assertClient.remote.keyToBytes(entry.getKey()));
      }

      assertArrayEquals(expected.getIntSet().toBitSet(), nearCacheService.calculateBloomBits(),
            "The bloom filter kept by the client does not match the near cache contents");
   }

   /**
    * Finds a key which only maps to bits the given key already uses, so that removing it clears nothing.
    */
   private int findKeyUsingOnlyBitsOf(int key) {
      BloomFilter<byte[]> filter = MurmurHash3BloomFilter.createFilter(nearCacheService().getBloomFilterBits());
      filter.addToFilter(assertClient.remote.keyToBytes(key));

      for (int candidate = key + 1; candidate < key + 100_000; ++candidate) {
         if (filter.possiblyPresent(assertClient.remote.keyToBytes(candidate))) {
            return candidate;
         }
      }
      throw new AssertionError("No key sharing all of its bits with " + key + " was found");
   }

   private NearCacheService<Integer, String> nearCacheService() {
      return assertClient.nearCacheService.get();
   }
}
