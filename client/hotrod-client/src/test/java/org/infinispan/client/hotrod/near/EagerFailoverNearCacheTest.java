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
@Test(groups = "functional", testName = "client.hotrod.near.EagerFailoverNearCacheTest")
public class EagerFailoverNearCacheTest extends MultiHotRodServersTest {

   List<AssertsNearCache<Integer, String>> assertClients = new ArrayList<>(2);

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      return hotRodCacheConfiguration(builder);
   }

   @Override
   protected RemoteCacheManager createClient(int i) {
      AssertsNearCache<Integer, String> asserts = createStickyAssertClient();
      assertClients.add(asserts);
      return asserts.manager;
   }

   protected <K, V> AssertsNearCache<K, V> createStickyAssertClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());
      clientBuilder.balancingStrategy(StickyServerLoadBalancingStrategy.class);
      clientBuilder.nearCache().mode(getNearCacheMode()).maxEntries(-1);
      return AssertsNearCache.create(this.<byte[], Object>cache(0), clientBuilder);
   }

   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.EAGER;
   }

   public void testNearCacheClearedUponFailover() {
      AssertsNearCache<Integer, String> stickyClient = createStickyAssertClient();
      try {
         stickyClient.put(1, "v1").expectNearPut(1, "v1", headClient(), tailClient());
         stickyClient.put(2, "v1").expectNearPut(2, "v1", headClient(), tailClient());
         stickyClient.put(3, "v1").expectNearPut(3, "v1", headClient(), tailClient());
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

   protected AssertsNearCache<Integer, String> tailClient() {
      return assertClients.get(1);
   }

   protected AssertsNearCache<Integer, String> headClient() {
      return assertClients.get(0);
   }

}
