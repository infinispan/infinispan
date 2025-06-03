package org.infinispan.client.hotrod.near;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.ClusterInvalidatedNearCacheTest")
public class ClusterInvalidatedNearCacheTest extends MultiHotRodServersTest {

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
      builder.clustering().hash().numOwners(1);
      return hotRodCacheConfiguration(builder);
   }

   @Override
   protected RemoteCacheManager createClient(int i) {
      AssertsNearCache<Integer, String> asserts = createAssertClient();
      assertClients.add(asserts);
      return asserts.manager;
   }

   private <K, V> AssertsNearCache<K, V> createAssertClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.remoteCache("").nearCacheMode(getNearCacheMode());
      return AssertsNearCache.create(this.cache(0), clientBuilder);
   }

   public void testNearCacheUpdatesSeenByAllClients() {
      Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      expectNearCacheUpdates(headClient(), key0, tailClient());
      expectNearCacheUpdates(tailClient(), key1, headClient());
   }

   private AssertsNearCache<Integer, String> tailClient() {
      return assertClients.get(1);
   }

   private AssertsNearCache<Integer, String> headClient() {
      return assertClients.get(0);
   }

   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.INVALIDATED;
   }

   protected void expectNearCacheUpdates(AssertsNearCache<Integer, String> producer,
         Integer key, AssertsNearCache<Integer, String> consumer) {
      producer.get(key, null).expectNearGetMiss(key);
      producer.put(key, "v1").expectNearPreemptiveRemove(key);
      producer.get(key, "v1").expectNearGetMissWithValue(key, "v1");
      producer.put(key, "v2").expectNearRemove(key, consumer);
      producer.get(key, "v2").expectNearGetMissWithValue(key, "v2");
      producer.remove(key).expectNearRemove(key, consumer);
      producer.get(key, null).expectNearGetMiss(key);
   }

}
