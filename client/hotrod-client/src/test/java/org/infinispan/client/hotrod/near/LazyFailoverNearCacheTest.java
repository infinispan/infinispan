package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.event.StickyServerLoadBalancingStrategy;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.findServerAndKill;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Deprecated
@Test(groups = "functional", testName = "client.hotrod.near.LazyFailoverNearCacheTest")
public class LazyFailoverNearCacheTest extends EagerFailoverNearCacheTest {

   @Override
   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.LAZY;
   }

   public void testNearCacheClearedUponFailover() {
      AssertsNearCache<Integer, String> stickyClient = createStickyAssertClient();
      try {
         stickyClient.put(1, "v1").expectNearRemove(1, headClient(), tailClient());
         stickyClient.get(1, "v1").expectNearGetNull(1).expectNearPutIfAbsent(1, "v1");
         stickyClient.put(2, "v1").expectNearRemove(2, headClient(), tailClient());
         stickyClient.get(2, "v1").expectNearGetNull(2).expectNearPutIfAbsent(2, "v1");
         stickyClient.put(3, "v1").expectNearRemove(3, headClient(), tailClient());
         stickyClient.get(3, "v1").expectNearGetNull(3).expectNearPutIfAbsent(3, "v1");
         findServerAndKill(stickyClient.manager, servers, cacheManagers);
         // Since each client is separate remote cache manager, you need to get
         // each client to do an operation to receive the near cache clear.
         // These gets should return non-null, but the get should be resolved remotely!
         stickyClient.get(1, "v1").expectNearClear().expectNearPutIfAbsent(1, "v1");
         stickyClient.expectNoNearEvents();
         headClient().get(2, "v1").expectNearClear().expectNearPutIfAbsent(2, "v1");
         headClient().expectNoNearEvents();
         tailClient().get(3, "v1").expectNearClear().expectNearPutIfAbsent(3, "v1");
         tailClient().expectNoNearEvents();
      } finally {
         stickyClient.stop();
      }
   }

}
