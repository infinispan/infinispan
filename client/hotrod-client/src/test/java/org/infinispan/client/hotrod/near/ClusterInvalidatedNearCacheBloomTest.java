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
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.ClusterInvalidatedNearCacheBloomTest")
public class ClusterInvalidatedNearCacheBloomTest extends MultiHotRodServersTest {
   private static final int CLUSTER_MEMBERS = 3;
   // This has to be 16 due to how NearCache bloom filter is calculated by dividing by 16 to require
   // one update + 3 before it will send the updated bloom filter
   private static final int NEAR_CACHE_SIZE = 16;

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
            assertsNearCache.expectNoNearEvents(500, TimeUnit.MILLISECONDS);
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
      clientBuilder.remoteCache("").nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(NEAR_CACHE_SIZE)
            .nearCacheUseBloomFilter(true);
      return AssertsNearCache.create(cache(0), clientBuilder);
   }

   public void testInvalidationFromOtherClientModification() {
      int key = 0;

      int bloomFilterVersion = client2.bloomFilterVersion();

      client1.get(key, null).expectNearGetMiss(key);
      client2.get(key, null).expectNearGetMiss(key);

      String value = "v1";
      client1.put(key, value).expectNearPreemptiveRemove(key);

      // We wait until the pending bloom updates is complete to avoid out of turn updates
      eventuallyEquals(bloomFilterVersion + 2, () -> client2.bloomFilterVersion());

      client2.get(key, value).expectNearGetMissWithValue(key, value);
      client2.get(key, value).expectNearGetValue(key, value);

      // Both client1 and client2 have remote removes due to having to add a null get to the bloom filter
      // to guarantee consistency in ISPN-13612
      client1.remove(key).expectNearRemove(key, client2);
   }

   public void testClientsBothCachedAndCanUpdate() {
      int key = 0;
      String value = "v1";

      client1.put(key, value).expectNearPreemptiveRemove(key);

      client1.get(key, value).expectNearGetMissWithValue(key, value);
      client2.get(key, value).expectNearGetMissWithValue(key, value);

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
