package org.infinispan.client.hotrod.near;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.EvictInvalidatedNearCacheTest")
public class EvictInvalidatedNearCacheTest extends SingleHotRodServerTest {

   private int entryCount;
   private boolean bloomFilter;

   AssertsNearCache<Integer, String> assertClient;

   EvictInvalidatedNearCacheTest entryCount(int entryCount) {
      this.entryCount = entryCount;
      return this;
   }

   EvictInvalidatedNearCacheTest bloomFilter(boolean bloomFilter) {
      this.bloomFilter = bloomFilter;
      return this;
   }
   @Factory
   public Object[] factory() {
      return new Object[]{
            new EvictInvalidatedNearCacheTest().entryCount(1).bloomFilter(false),
            new EvictInvalidatedNearCacheTest().entryCount(1).bloomFilter(true),
            new EvictInvalidatedNearCacheTest().entryCount(20).bloomFilter(false),
            new EvictInvalidatedNearCacheTest().entryCount(20).bloomFilter(true),
      };
   }

   @Override
   protected String parameters() {
      return "maxEntries=" + entryCount + ", bloomFilter=" + bloomFilter;
   }


   @Override
   protected void teardown() {
      if (assertClient != null) {
         assertClient.stop();
         assertClient = null;
      }

      super.teardown();
   }

   @AfterMethod(alwaysRun=true)
   @Override
   protected void clearContent() {
      super.clearContent();
      RemoteCache<?, ?> remoteCache = remoteCacheManager.getCache();
      remoteCache.clear(); // Clear the near cache too
      if (bloomFilter) {
         CompletionStages.join(((InternalRemoteCache) remoteCache).updateBloomFilter());
      }
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      assertClient = createClient();
      return assertClient.manager;
   }

   protected <K, V> AssertsNearCache<K, V> createClient() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.remoteCache("").nearCacheMode(getNearCacheMode()).nearCacheMaxEntries(entryCount).nearCacheUseBloomFilter(bloomFilter);
      return AssertsNearCache.create(cache(), builder);
   }

   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.INVALIDATED;
   }

   public void testEvictAfterReachingMax() {
      assertClient.expectNoNearEvents();
      for (int i = 0; i < entryCount; ++i) {
         assertClient.put(i, "v1").expectNearPreemptiveRemove(i);
         // We wait until all pending bloom updates are done so our events are ordered as we expect
         eventually(() -> !assertClient.hasPendingBloomUpdate());
         assertClient.get(i, "v1").expectNearGetMissWithValue(i, "v1");
      }

      int extraKey = entryCount + 1;
      assertClient.put(extraKey, "v1").expectNearPreemptiveRemove(extraKey);
      assertClient.get(extraKey, "v1").expectNearGetMissWithValue(extraKey, "v1");

      // Caffeine is not deterministic as to which one it evicts - so we just verify size
      assertEquals(entryCount, assertClient.nearCacheSize());
   }
}
