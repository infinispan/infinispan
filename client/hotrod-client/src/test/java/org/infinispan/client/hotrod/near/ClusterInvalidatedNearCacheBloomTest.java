package org.infinispan.client.hotrod.near;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.ClusterInvalidatedNearCacheBloomTest")
public class ClusterInvalidatedNearCacheBloomTest extends MultiHotRodServersTest {
   private static final int CLUSTER_MEMBERS = 3;
   private static final int NEAR_CACHE_SIZE = 4;

   List<AssertsNearCache<Integer, String>> assertClients = new ArrayList<>(CLUSTER_MEMBERS);

   AssertsNearCache<Integer, String> client0;
   AssertsNearCache<Integer, String> client1;
   AssertsNearCache<Integer, String> client2;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(CLUSTER_MEMBERS, getCacheConfiguration());

      client0 = assertClients.get(0);
      client1 = assertClients.get(1);
      client2 = assertClients.get(2);
   }

   @BeforeMethod
   void beforeMethod() {
      assertClients.forEach(AssertsNearCache::expectNoNearEvents);
      assertClients.forEach(ac -> CompletionStages.join(ac.remote.updateBloomFilter()));
   }

   @AfterMethod
   void afterMethod() {
      caches().forEach(Cache::clear);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      for (AssertsNearCache<Integer, String> assertsNearCache : assertClients) {
         try {
            assertsNearCache.expectNoNearEvents(50, TimeUnit.MILLISECONDS);
         } catch (InterruptedException e) {
            throw new AssertionError(e);
         }
      }
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

      clientBuilder.connectionPool().maxActive(1);

      clientBuilder.nearCache().mode(NearCacheMode.INVALIDATED)
            .maxEntries(NEAR_CACHE_SIZE)
            .bloomFilter(true);
      return AssertsNearCache.create(cache(0), clientBuilder);
   }

   public void testInvalidationFromOtherClientModification() {
      int key = 0;

      client1.get(key, null).expectNearGetNull(key);
      client2.get(key, null).expectNearGetNull(key);

      String value = "v1";
      client1.put(key, value).expectNearPreemptiveRemove(key);
      client2.expectNoNearEvents();

      client2.get(key, value).expectNearGetNull(key).expectNearPutIfAbsent(key, value);
      client2.get(key, value).expectNearGetValue(key, value);

      // Client 1 should only get a preemptive remove as it never cached the value locally
      // However client 2 should get a remove as it had it cached
      client1.remove(key).expectNearPreemptiveRemove(key, client2);
   }

   public void testClientsBothCachedAndCanUpdate() {
      int key = 0;
      String value = "v1";

      client1.put(key, value).expectNearPreemptiveRemove(key);

      client1.get(key, value).expectNearGetNull(key).expectNearPutIfAbsent(key, value);
      client2.get(key, value).expectNearGetNull(key).expectNearPutIfAbsent(key, value);

      String value2 = "v2";
      client1.put(key, value2).expectNearRemove(key, client2);

      // Even though our near cache is emptied - the bloom filter hasn't yet been updated so we will still be hit
      client2.put(key, value).expectNearRemove(key, client1);

      // Force the clients to update the bloom filters on the servers so now we won't see the writes
      CompletionStages.join(client1.remote.updateBloomFilter());
      CompletionStages.join(client2.remote.updateBloomFilter());

      client2.put(key, value).expectNearPreemptiveRemove(key);

      client1.put(key, value).expectNearPreemptiveRemove(key);
   }
}
