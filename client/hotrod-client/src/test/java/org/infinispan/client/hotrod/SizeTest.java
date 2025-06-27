package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

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
      RemoteCacheManager newClient = createClient(0);
      try {
         RemoteCache<Integer, Integer> newRemoteCache = newClient.getCache(cacheName);
         populateCache(newRemoteCache);
         assertEquals(SIZE, newRemoteCache.size());
      } finally {
         HotRodClientTestingUtil.killRemoteCacheManager(newClient);
      }
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
      builder.memory().maxCount(1);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .purgeOnStartup(true);
      defineInAll(cacheName, builder);
      assertEquals(0, clients.get(0).getCache(cacheName).size());
      populateCache(cacheName);
      assertEquals(SIZE, server(0).getCacheManager().getCache(cacheName).size());
      assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
   }

   public void testPersistentSizeWithFlag() {
      String cacheName = "persistent-size-with-flag";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      int evictionSize = 2;
      builder.memory().maxCount(evictionSize);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .purgeOnStartup(true);
      defineInAll(cacheName, builder);
      RemoteCache<Integer, Integer> remoteCache = clients.get(0).getCache(cacheName);
      assertEquals(0, remoteCache.size());
      populateCache(remoteCache);
      assertEquals(SIZE, remoteCache.size());

      assertEquals(evictionSize, remoteCache.withFlags(Flag.SKIP_CACHE_LOAD).size());
   }

   private void populateCache(String cacheName) {
      for (int i = 0; i < SIZE; i++) {
         clients.get(i % NUM_SERVERS).getCache(cacheName).put(i, i);
      }
   }

   private void populateCache(RemoteCache<Integer, Integer> remote) {
      for (int i = 0; i < SIZE; i++)
         remote.put(i, i);
   }

}
