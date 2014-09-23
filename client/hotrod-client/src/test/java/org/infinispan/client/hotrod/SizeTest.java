package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "client.hotrod.SizeTest")
public class SizeTest extends MultiHotRodServersTest {

   static final int NUM_SERVERS = 3;
   static final int SIZE = 20;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, new ConfigurationBuilder());
   }

   public void testLocalCacheSize() {
      String cacheName = "local-size";
      defineInAll(cacheName, new ConfigurationBuilder());
      // Create a brand new client so that as a local cache it does not do load balancing
      // This is important for size assertions since there's data is not clustered
      RemoteCache<Integer, Integer> remote = createClient(0).getCache(cacheName);
      populateCache(remote);
      assertEquals(SIZE, remote.size());
   }

   public void testReplicatedCacheSize() {
      String cacheName = "replicated-size";
      defineInAll(cacheName, getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      populateCache(cacheName);
      assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
   }

   public void testDistributeCacheSize() {
      String cacheName = "distributed-size";
      defineInAll(cacheName, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      populateCache(cacheName);
      assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
   }

   public void testPersistentDistributedCacheSize() {
      String cacheName = "persistent-distributed-size";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.eviction().maxEntries(1);
      builder.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .shared(true);
      defineInAll(cacheName, builder);
      populateCache(cacheName);
      assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
   }

   private void populateCache(String cacheName) {
      for (int i = 0; i < SIZE; i++)
         clients.get(i % NUM_SERVERS).getCache(cacheName).put(i, i);
   }

   private void populateCache(RemoteCache<Integer, Integer> remote) {
      for (int i = 0; i < SIZE; i++)
         remote.put(i, i);
   }

}
