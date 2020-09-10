package org.infinispan.client.hotrod.near;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.EvictInvalidatedNearCacheTest")
public class EvictInvalidatedNearCacheTest extends SingleHotRodServerTest {

   AssertsNearCache<Integer, String> assertClient;

   @Override
   protected void teardown() {
      if (assertClient != null) {
         assertClient.stop();
         assertClient = null;
      }

      super.teardown();
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      assertClient = createClient();
      return assertClient.manager;
   }

   protected <K, V> AssertsNearCache<K, V> createClient() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.nearCache().mode(getNearCacheMode()).maxEntries(2);
      return AssertsNearCache.create(this.<byte[], Object>cache(), builder);
   }

   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.INVALIDATED;
   }

   public void testEvictAfterReachingMax() {
      assertClient.expectNoNearEvents();
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.put(2, "v1").expectNearPreemptiveRemove(2);
      assertClient.get(1, "v1").expectNearGetNull(1).expectNearPutIfAbsent(1, "v1");
      assertClient.get(2, "v1").expectNearGetNull(2).expectNearPutIfAbsent(2, "v1");
      assertClient.put(3, "v1").expectNearPreemptiveRemove(3);
      assertClient.get(3, "v1").expectNearGetNull(3).expectNearPutIfAbsent(3, "v1");

      // Caffeine is not deterministic as to which one it evicts - so we just verify size
      assertEquals(2, assertClient.nearCacheSize());
   }
}
