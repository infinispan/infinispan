package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodIgnoreCacheTest")
public class HotRodIgnoreCacheTest extends HotRodMultiNodeTest {

   public void testIgnoreCache() throws InterruptedException {
      HotRodClient client = clients().get(0);
      client.put("k1", "v1");
      assertStatus(client.get("k1"), OperationStatus.Success);

      servers().get(0).getCacheIgnore().ignoreCache(cacheName());
      eventually(() -> servers().get(1).getCacheIgnore().getIgnoredCaches().contains(cacheName()));
      assertStatus(client.get("k1"), OperationStatus.ServerError);

      servers().get(1).getCacheIgnore().unignoreCache(cacheName());
      eventually(() -> !servers().get(0).getCacheIgnore().getIgnoredCaches().contains(cacheName()));
      assertStatus(client.get("k1"), OperationStatus.Success);
   }

   @Override
   protected String cacheName() {
      return this.getClass().getSimpleName();
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }
}
