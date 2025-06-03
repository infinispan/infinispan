package org.infinispan.client.hotrod.near;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.findServerAndKill;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.event.StickyServerLoadBalancingStrategy;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.InvalidatedFailoverNearCacheTest")
public class InvalidatedFailoverNearCacheTest extends MultiHotRodServersTest {

   List<AssertsNearCache<Integer, String>> assertClients = new ArrayList<>(2);

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      assertClients.forEach(AssertsNearCache::stop);
      assertClients.clear();

      super.destroy();
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
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host(server.getHost()).port(server.getPort());
      clientBuilder.balancingStrategy(StickyServerLoadBalancingStrategy.class);
      clientBuilder.remoteCache("").nearCacheMode(getNearCacheMode());
      return AssertsNearCache.create(this.cache(0), clientBuilder);
   }

   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.INVALIDATED;
   }

   public void testNearCacheClearedUponFailover() {
      AssertsNearCache<Integer, String> stickyClient = createStickyAssertClient();
      try {
         stickyClient.put(1, "v1").expectNearPreemptiveRemove(1);
         stickyClient.get(1, "v1").expectNearGetMissWithValue(1, "v1");
         stickyClient.put(2, "v1").expectNearPreemptiveRemove(2);
         stickyClient.get(2, "v1").expectNearGetMissWithValue(2, "v1");
         stickyClient.put(3, "v1").expectNearPreemptiveRemove(3);
         stickyClient.get(3, "v1").expectNearGetMissWithValue(3, "v1");
         boolean headClientClear = isClientListenerAttachedToSameServer(headClient(), stickyClient);
         boolean tailClientClear = isClientListenerAttachedToSameServer(tailClient(), stickyClient);
         killServerForClient(stickyClient);

         // The clear will be executed when the connection to the server is closed from the listener.
         stickyClient.expectNearClear();

         // Also make sure the near cache is fully connected so the events will work deterministically
         eventually(stickyClient::isNearCacheConnected);

         stickyClient.get(1, "v1")
               .expectNearGetMissWithValue(1, "v1");
         stickyClient.expectNoNearEvents();
         if (headClientClear) {
            headClient().expectNearClear();
         }
         eventually(() -> headClient().isNearCacheConnected());
         headClient().get(2, "v1").expectNearGetMissWithValue(2, "v1");
         headClient().expectNoNearEvents();
         if (tailClientClear) {
            tailClient().expectNearClear();
         }
         eventually(() -> tailClient().isNearCacheConnected());
         tailClient().get(3, "v1").expectNearGetMissWithValue(3, "v1");
         tailClient().expectNoNearEvents();
      } finally {
         stickyClient.stop();
      }
   }

   protected boolean isClientListenerAttachedToSameServer(AssertsNearCache<Integer, String> client1,
                                                          AssertsNearCache<Integer, String> client2) {
      return true;
   }

   protected void killServerForClient(AssertsNearCache<Integer, String> stickyClient) {
      findServerAndKill(stickyClient.manager, servers, cacheManagers);
   }

   protected AssertsNearCache<Integer, String> tailClient() {
      return assertClients.get(1);
   }

   protected AssertsNearCache<Integer, String> headClient() {
      return assertClients.get(0);
   }


}
