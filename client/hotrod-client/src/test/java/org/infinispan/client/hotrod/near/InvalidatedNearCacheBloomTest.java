package org.infinispan.client.hotrod.near;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.BloomFilter;
import org.infinispan.commons.util.MurmurHash3BloomFilter;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.InvalidatedNearCacheBloomTest")
public class InvalidatedNearCacheBloomTest extends SingleHotRodServerTest {

   private static final int NEAR_CACHE_SIZE = 4;

   private StorageType storageType;
   private AssertsNearCache<Integer, String> assertClient;

   private final BloomFilter<byte[]> bloomFilter = MurmurHash3BloomFilter.createFilter(NEAR_CACHE_SIZE << 2);

   private InvalidatedNearCacheBloomTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new InvalidatedNearCacheBloomTest().storageType(StorageType.OBJECT),
            new InvalidatedNearCacheBloomTest().storageType(StorageType.BINARY),
            new InvalidatedNearCacheBloomTest().storageType(StorageType.OFF_HEAP),
      };
   }

   @BeforeMethod
   void beforeMethod() {
      assertClient.expectNoNearEvents();
      // All tests rely upon having the bits set for the key 1
      bloomFilter.addToFilter(assertClient.remote.keyToBytes(1));
   }

   @AfterMethod
   void resetBloomFilter() throws InterruptedException {
      assertClient.expectNoNearEvents(50, TimeUnit.MILLISECONDS);

      ((InvalidatedNearRemoteCache) assertClient.remote).clearNearCache();
      CompletionStages.join(((InvalidatedNearRemoteCache) assertClient.remote).updateBloomFilter());
      // Don't let the clear leak
      assertClient.events.clear();
   }

   @Override
   protected String parameters() {
      return "[storageType-" + storageType + "]";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder cb = hotRodCacheConfiguration();
      cb.memory().storageType(storageType);
      return TestCacheManagerFactory.createCacheManager(cb);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      assertClient = createAssertClient();
      return assertClient.manager;
   }

   private <K, V> AssertsNearCache<K, V> createAssertClient() {
      ConfigurationBuilder builder = clientConfiguration();
      return AssertsNearCache.create(this.cache(), builder);
   }

   private ConfigurationBuilder clientConfiguration() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(NEAR_CACHE_SIZE)
            .nearCacheUseBloomFilter(true);
      return builder;
   }

   public void testSingleKeyFilter() {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.put(1, "v2").expectNearPreemptiveRemove(1);
      assertClient.get(1, "v2").expectNearGetMissWithValue(1, "v2");
      assertClient.get(1, "v2").expectNearGetValue(1, "v2");
      assertClient.remove(1).expectNearRemove(1);
      assertClient.get(1, null).expectNearGetMiss(1);
   }

   public void testMultipleKeyFilterConflictButNoRead() {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);

      int conflictKey = findNextKey(bloomFilter, 1, true);
      assertClient.put(conflictKey, "v1").expectNearPreemptiveRemove(conflictKey);
      assertClient.put(conflictKey, "v2").expectNearPreemptiveRemove(conflictKey);
   }

   public void testMultipleKeyFilterConflict() {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.get(1, "v1").expectNearGetMissWithValue(1, "v1");

      int conflictKey = findNextKey(bloomFilter, 1, true);
      // This is a create thus no remove is sent
      assertClient.put(conflictKey, "v1").expectNearPreemptiveRemove(conflictKey);
      // This conflicts with our original key thus it will send a remove despite nothing being removed
      assertClient.put(conflictKey, "v2").expectNearRemove(conflictKey);
      assertClient.get(1, "v1").expectNearGetValue(1, "v1");
   }

   public void testMultipleKeyFilterNoConflict() {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.get(1, "v1").expectNearGetMissWithValue(1, "v1");

      int nonConflictKey = findNextKey(bloomFilter, 1, false);
      // Both of the following never send a remove event back as the key wasn't present in bloom filter
      assertClient.put(nonConflictKey, "v1").expectNearPreemptiveRemove(nonConflictKey);
      assertClient.put(nonConflictKey, "v2").expectNearPreemptiveRemove(nonConflictKey);
   }

   public void testServerBloomFilterUpdate() throws InterruptedException {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.get(1, "v1").expectNearGetMissWithValue(1, "v1");

      int nonConflictKey = findNextKey(bloomFilter, 1, false);

      assertClient.put(nonConflictKey, "v1").expectNearPreemptiveRemove(nonConflictKey);
      assertClient.get(nonConflictKey, "v1").expectNearGetMissWithValue(nonConflictKey, "v1");

      boolean serverBloomFilterUpdated = false;
      for (int i = 0; i < 10; ++i) {
         assertClient.put(nonConflictKey, "v1");
         // The cache will always preemptively invalidate the value if necessary
         MockNearCacheService.MockEvent event = assertClient.events.poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         assertTrue(event instanceof MockNearCacheService.MockRemoveEvent);
         assertEquals(nonConflictKey, ((MockNearCacheService.MockRemoveEvent<?>) event).key);

         // Eventually this be null - which means our cache has been updated on the server side
         event = assertClient.events.poll(100, TimeUnit.MILLISECONDS);
         if (event == null) {
            serverBloomFilterUpdated = true;
            break;
         }
         assertTrue(event instanceof MockNearCacheService.MockRemoveEvent);
         assertEquals(nonConflictKey, ((MockNearCacheService.MockRemoveEvent<?>) event).key);

         Thread.sleep(10);
      }

      assertTrue("The server bloom filter was never updated and we got remove events every time",
            serverBloomFilterUpdated);
   }

   int findNextKey(BloomFilter<byte[]> filter, int originalValue, boolean present) {
      while (true) {
         byte[] testKey = assertClient.remote.keyToBytes(++originalValue);
         if (present == filter.possiblyPresent(testKey)) {
            return originalValue;
         }
      }
   }
}
