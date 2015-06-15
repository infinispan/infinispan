package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Deprecated
@Test(groups = "functional", testName = "client.hotrod.near.ClusterEagerNearCacheTest")
public class ClusterEagerNearCacheTest extends MultiHotRodServersTest {

   List<AssertsNearCache<Integer, String>> assertClients = new ArrayList<>(2);

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
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
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.nearCache().mode(getNearCacheMode()).maxEntries(-1);
      return AssertsNearCache.create(this.<byte[], Object>cache(0), clientBuilder);
   }

   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.EAGER;
   }

   public void testNearCacheUpdatesSeenByAllClients() {
      Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      expectNearCacheUpdates(headClient(), key0, tailClient());
      expectNearCacheUpdates(tailClient(), key1, headClient());
   }

   protected void expectNearCacheUpdates(AssertsNearCache<Integer, String> producer,
         Integer key, AssertsNearCache<Integer, String> consumer) {
      producer.get(key, null).expectNearGetNull(key);
      producer.put(key, "v1").expectNearPut(key, "v1", consumer);
      producer.get(key, "v1").expectNearGetValue(key, "v1");
      producer.put(key, "v2").expectNearPut(key, "v2", consumer);
      producer.get(key, "v2").expectNearGetValue(key, "v2");
      producer.remove(key).expectNearRemove(key, consumer);
      producer.get(key, null).expectNearGetNull(key);
   }

   private AssertsNearCache<Integer, String> tailClient() {
      return assertClients.get(1);
   }

   private AssertsNearCache<Integer, String> headClient() {
      return assertClients.get(0);
   }

}
