package org.infinispan.client.hotrod.near;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
   // The bloom filter batch size is derived from this by dividing by 16, so with 16 entries a client buffers
   // 4 removals before it pushes them to the server on its own
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
      // Clearing the caches invalidates whatever the clients still hold, so drain those events and reset the
      // server side bloom filters to let every test start from a clean slate
      for (AssertsNearCache<Integer, String> client : assertClients) {
         client.clearNearCache();
         eventually("Expected all near cache events to be drained",
               () -> client.events.poll(50, TimeUnit.MILLISECONDS) == null);
      }
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

   public void testInvalidationFromOtherClientModification() throws InterruptedException {
      int key = 0;

      // A read that finds nothing is still registered with the server, which is what guarantees that a write
      // racing with the read is not lost, see ISPN-13612
      client1.get(key, null).expectNearGetMiss(key);
      client2.get(key, null).expectNearGetMiss(key);

      // Neither client cached anything, so both of them tell the server to drop the key from its filter again
      client1.flushBloomFilterRemovals();
      client2.flushBloomFilterRemovals();

      String value = "v1";
      client1.put(key, value).expectNearPreemptiveRemove(key);
      // No near cache holds the key any longer, so the write does not have to be replicated to any of them
      client1.expectNoNearEvents(50, TimeUnit.MILLISECONDS);
      client2.expectNoNearEvents(50, TimeUnit.MILLISECONDS);

      client2.get(key, value).expectNearGetMissWithValue(key, value);
      client2.get(key, value).expectNearGetValue(key, value);

      // Only client2 cached the value, so only it has to hear about the removal
      client1.remove(key).expectNearPreemptiveRemove(key, client2);
   }

   public void testClientsBothCachedAndCanUpdate() throws InterruptedException {
      int key = 0;
      String value = "v1";

      client0.put(key, value).expectNearPreemptiveRemove(key);

      client1.get(key, value).expectNearGetMissWithValue(key, value);
      client2.get(key, value).expectNearGetMissWithValue(key, value);

      // Both near caches hold the entry, so both of them are invalidated
      client0.put(key, "v2").expectNearPreemptiveRemove(key, client1, client2);

      // Removals are batched up on the client, push them so the servers know the entry is gone for good
      client1.flushBloomFilterRemovals();
      client2.flushBloomFilterRemovals();

      // Removing the individual keys is enough, no client had to recompute and resend its entire filter
      client0.put(key, value).expectNearPreemptiveRemove(key);
      client1.expectNoNearEvents(50, TimeUnit.MILLISECONDS);
      client2.expectNoNearEvents(50, TimeUnit.MILLISECONDS);
   }

   /**
    * The server counts how many times a key was read, so a removal that refers to an earlier read must not stop the
    * invalidation of the entry the near cache holds now. Getting this wrong would leave a stale value behind forever.
    */
   public void testStaleRemovalDoesNotDropLiveEntry() {
      int key = 0;

      client0.put(key, "v1").expectNearPreemptiveRemove(key);
      client1.get(key, "v1").expectNearGetMissWithValue(key, "v1");

      // client1 buffers the removal of its first read without sending it out yet
      client0.put(key, "v2").expectNearPreemptiveRemove(key, client1);

      // Reading the key again registers it with the server a second time
      client1.get(key, "v2").expectNearGetMissWithValue(key, "v2");

      // The buffered removal only undoes the first read, the value client1 holds now is still tracked
      client1.flushBloomFilterRemovals();

      client0.put(key, "v3").expectNearPreemptiveRemove(key, client1);
   }

   /**
    * Clearing a near cache has to reset the filter the server keeps for it, otherwise the server would keep sending
    * invalidations for entries the client no longer has.
    */
   public void testClearNearCacheResetsServerFilter() throws InterruptedException {
      int key = 0;

      client0.put(key, "v1").expectNearPreemptiveRemove(key);
      client1.get(key, "v1").expectNearGetMissWithValue(key, "v1");

      int bloomFilterVersion = client1.bloomFilterVersion();
      client1.clearNearCache();
      // The whole filter is replaced, which the bloom filter version keeps track of
      assertEquals(bloomFilterVersion + 2, client1.bloomFilterVersion());
      client1.expectNearClearInClient(client1);

      client0.put(key, "v2").expectNearPreemptiveRemove(key);
      client1.expectNoNearEvents(50, TimeUnit.MILLISECONDS);
   }
}
