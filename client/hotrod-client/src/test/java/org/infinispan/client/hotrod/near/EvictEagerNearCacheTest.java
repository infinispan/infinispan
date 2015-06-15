package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

@Deprecated
@Test(groups = "functional", testName = "client.hotrod.near.EvictEagerNearCacheTest")
public class EvictEagerNearCacheTest extends SingleHotRodServerTest {

   AssertsNearCache<Integer, String> assertClient;

   protected RemoteCacheManager getRemoteCacheManager() {
      assertClient = createClient();
      return assertClient.manager;
   }

   protected <K, V> AssertsNearCache<K, V> createClient() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.nearCache().mode(getNearCacheMode()).maxEntries(2);
      return AssertsNearCache.create(this.<byte[], Object>cache(), builder);
   }

   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.EAGER;
   }

   public void testEvictAfterReachingMax() {
      assertClient.expectNoNearEvents();
      assertClient.put(1, "v1").expectNearPut(1, "v1");
      assertClient.put(2, "v1").expectNearPut(2, "v1");
      assertClient.get(1, "v1").expectNearGetValue(1, "v1");
      assertClient.get(2, "v1").expectNearGetValue(2, "v1");
      assertClient.put(3, "v1").expectNearPut(3, "v1");
      assertClient.get(3, "v1").expectNearGetValue(3, "v1");
      assertClient.get(1, "v1").expectNearGetNull(1).expectNearPutIfAbsent(1, "v1");
   }

}
