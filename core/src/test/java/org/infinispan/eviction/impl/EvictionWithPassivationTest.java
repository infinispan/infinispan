package org.infinispan.eviction.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.AbstractInternalDataContainer;
import org.infinispan.container.offheap.SegmentedBoundedOffHeapDataContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.Mocks;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.EvictionWithPassivationTest")
public class EvictionWithPassivationTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "testCache";
   private final int EVICTION_MAX_ENTRIES = 2;
   private StorageType storage;
   private EvictionListener evictionListener;

   public EvictionWithPassivationTest() {
      // Cleanup needs to be after method, else LIRS can cause failures due to it not caching values due to hot
      // size being equal to full container size
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new EvictionWithPassivationTest().withStorage(StorageType.BINARY),
            new EvictionWithPassivationTest().withStorage(StorageType.HEAP),
            new EvictionWithPassivationTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected String parameters() {
      return "[" + storage + "]";
   }

   private ConfigurationBuilder buildCfg() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg
            .persistence()
               .passivation(true)
               .addStore(DummyInMemoryStoreConfigurationBuilder.class).purgeOnStartup(true)
            .invocationBatching().enable()
            .memory().storage(storage);
      cfg.memory().maxCount(EVICTION_MAX_ENTRIES);
      return cfg;
   }

   public EvictionWithPassivationTest withStorage(StorageType storage) {
      this.storage = storage;
      return this;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(true));
      cacheManager.defineConfiguration(CACHE_NAME, buildCfg().build());
      evictionListener = new EvictionListener();
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);
      testCache.addListener(evictionListener);
      return cacheManager;
   }

   public void testBasicStore() {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);
      testCache.clear();
      testCache.put("X", "4567");
      testCache.put("Y", "4568");
      testCache.put("Z", "4569");

      assertEquals("4567", testCache.get("X"));
      assertEquals("4568", testCache.get("Y"));
      assertEquals("4569", testCache.get("Z"));

      for (int i = 0; i < 10; i++) {
         testCache.getAdvancedCache().startBatch();
         String k = "A" + i;
         testCache.put(k, k);
         k = "B" + i;
         testCache.put(k, k);
         testCache.getAdvancedCache().endBatch(true);
      }

      for (int i = 0; i < 10; i++) {
         String k = "A" + i;
         assertEquals(k, testCache.get(k));
         k = "B" + i;
         assertEquals(k, testCache.get(k));
      }
   }

   public void testActivationInBatchRolledBack() {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);

      final String key = "X";
      final String value = "4567";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      assertEquals(value, testCache.get(key));
      testCache.endBatch(false);

      // The data should still be present even if a rollback occurred
      assertEquals(value, testCache.get(key));
   }


   public void testActivationWithAnotherConcurrentRequest() throws Exception {
      final Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);

      final String key = "Y";
      final String value = "4568";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      assertEquals(value, testCache.get(key));

      // Another thread should be able to see the data as well!
      Future<String> future = testCache.getAsync(key);

      assertEquals(value, future.get(10, TimeUnit.SECONDS));

      assertEquals(value, testCache.get(key));

      testCache.endBatch(true);

      // Lastly try the retrieval after batch was committed
      assertEquals(value, testCache.get(key));
   }

   public void testActivationPendingTransactionDoesNotAffectOthers() throws Throwable {
      final String previousValue = "prev-value";
      final Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);

      testCache.clear();

      final String key = "Y";
      final String value;

      if (previousValue != null) {
         testCache.put(key, previousValue);
         value = previousValue + "4568";
      } else {
         value = "4568";
      }

      // evict so it is in the loader but not in data container
      testCache.evict(key);

      testCache.startBatch();

      try {
         if (previousValue != null) {
            assertEquals(previousValue, testCache.put(key, value));
         } else {
            assertNull(testCache.put(key, value));
         }

         // In tx we should see new value
         assertEquals(value, testCache.get(key));

         // The spawned thread shouldn't see the new value yet, should see the old one still
         Future<String> future = fork(() -> testCache.get(key));

         if (previousValue != null) {
            assertEquals(previousValue, future.get(10000, TimeUnit.SECONDS));
         } else {
            assertNull(future.get(10, TimeUnit.SECONDS));
         }
      } catch (Throwable e) {
         testCache.endBatch(false);
         throw e;
      }

      testCache.endBatch(true);

      assertEquals(value, testCache.get(key));
   }

   public void testActivationPutAllInBatchRolledBack() throws Exception {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);

      final String key = "X";
      final String value = "4567";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      testCache.putAll(Collections.singletonMap(key, value + "-putall"));
      testCache.endBatch(false);

      // The data should still be present even if a rollback occurred
      assertEquals(value, testCache.get(key));
   }

   public void testRemovalOfEvictedEntry() throws Exception {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);
      int phase = evictionListener.phaser.getPhase();
      for (int i = 0; i < EVICTION_MAX_ENTRIES + 1; i++) {
         testCache.put("key" + i, "value" + i);
      }

      // Eviction notification can be non blocking async in certain configs - so wait for notification to complete
      evictionListener.phaser.awaitAdvanceInterruptibly(phase, 10, TimeUnit.SECONDS);
      String evictedKey = evictionListener.getEvictedKey();
      assertEntryInStore(evictedKey, true);
      testCache.remove(evictedKey);
      assertFalse(testCache.containsKey(evictedKey));
      assertNull(testCache.get(evictedKey));
   }

   public void testComputeOnEvictedEntry() throws Exception {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);
      int phase = evictionListener.phaser.getPhase();
      for (int i = 0; i < EVICTION_MAX_ENTRIES + 1; i++) {
         testCache.put("key" + i, "value" + i);
      }

      // Eviction notification can be non blocking async in certain configs - so wait for notification to complete
      evictionListener.phaser.awaitAdvanceInterruptibly(phase, 10, TimeUnit.SECONDS);
      String evictedKey = evictionListener.getEvictedKey();
      assertEntryInStore(evictedKey, true);
      testCache.compute(evictedKey, (k ,v) -> v + "-modfied");
      assertEntryInStore(evictedKey, true);
   }

   public void testRemoveViaComputeOnEvictedEntry() throws Exception {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);
      int phase = evictionListener.phaser.getPhase();
      for (int i = 0; i < EVICTION_MAX_ENTRIES + 1; i++) {
         testCache.put("key" + i, "value" + i);
      }

      // Eviction notification can be non blocking async in certain configs - so wait for notification to complete
      evictionListener.phaser.awaitAdvanceInterruptibly(phase, 10, TimeUnit.SECONDS);
      String evictedKey = evictionListener.getEvictedKey();
      assertEntryInStore(evictedKey, true);
      testCache.compute(evictedKey, (k ,v) -> null);
      assertFalse(testCache.containsKey(evictedKey));
      assertEntryInStore(evictedKey, false);
   }

   public void testCleanStoreOnPut() throws Exception {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);
      testCache.clear();
      putIntoStore("key", "oldValue");
      testCache.put("key", "value");
      assertEntryInStore("key", true);
   }

   public void testConcurrentWriteWithEviction() throws InterruptedException, TimeoutException, ExecutionException {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);
      testCache.clear();

      DataContainer<String, String> dc = testCache.getAdvancedCache().getDataContainer();
      Class dcClass = storage == StorageType.OFF_HEAP ? SegmentedBoundedOffHeapDataContainer.class : AbstractInternalDataContainer.class;
      CheckPoint checkPoint = new CheckPoint();
      // Need to capture the evicting key to register a concurrent operation before eviction
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      DataOperationOrderer doo = Mocks.blockingFieldMock(checkPoint, DataOperationOrderer.class, dc, dcClass, "orderer", (stub, mock) ->
         stub.when(mock).orderOn(argumentCaptor.capture(), Mockito.any()));
      try {
         Future<Void> future = fork(() -> {
            for (int i = 0; i < 10; ++i) {
               testCache.put("k" + i, "v" + i);
            }
         });
         checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 1000, TimeUnit.SECONDS);

         CompletableFuture<DataOperationOrderer.Operation> stage = new CompletableFuture<>();
         Object keyEvicting = argumentCaptor.getValue();
         doo.orderOn(keyEvicting, stage);

         checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

         // Wait for the orderer to respond but don't release it until we complete it
         checkPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);

         doo.completeOperation(keyEvicting, stage, DataOperationOrderer.Operation.WRITE);

         checkPoint.triggerForever(Mocks.AFTER_RELEASE);

         future.get(10, TimeUnit.SECONDS);
      } finally {
         TestingUtil.replaceField(doo, "orderer", dc, dcClass);
      }
   }

   private void assertEntryInStore(String key, boolean expectPresent) throws Exception {
      assertNotNull(key);
      AdvancedCache<String, String> testCache = cacheManager.<String, String>getCache(CACHE_NAME).getAdvancedCache();

      Object loaderKey = testCache.getKeyDataConversion().toStorage(key);

      CompletionStage<MarshallableEntry<String, String>> stage = TestingUtil.extractComponent(testCache, PersistenceManager.class)
            .loadFromAllStores(loaderKey, true, true);
      MarshallableEntry<String, String> entry = CompletionStages.join(stage);
      if (expectPresent) {
         assertNotNull(entry);
      } else {
         assertNull(entry);
      }
      DummyInMemoryStore loader = TestingUtil.getFirstStore(testCache);
      if (expectPresent) {
         eventuallyEquals(entry, () -> loader.loadEntry(loaderKey));
      } else {
         assertFalse(loader.contains(loaderKey));
      }
   }

   private void putIntoStore(String key, String value) {
      AdvancedCache<String, String> testCache = cacheManager.<String, String>getCache(CACHE_NAME).getAdvancedCache();
      DummyInMemoryStore writer = TestingUtil.getFirstStore(testCache);
      Object writerKey = testCache.getKeyDataConversion().toStorage(key);
      Object writerValue = testCache.getValueDataConversion().toStorage(value);
      MarshallableEntry entry;
      if (Configurations.isTxVersioned(testCache.getCacheConfiguration())) {
         entry = MarshalledEntryUtil.createWithVersion(writerKey, writerValue, testCache);
      } else {
         entry = MarshalledEntryUtil.create(writerKey, writerValue, testCache);
      }
      writer.write(entry);
   }

   @Listener
   public static class EvictionListener {
      private String evictedKey;
      private final Phaser phaser = new Phaser(1);

      @CacheEntriesEvicted
      public void entryEvicted(CacheEntriesEvictedEvent e) {
         evictedKey = (String) e.getEntries().keySet().iterator().next();
         // Notify main thread we have evicted something
         phaser.arrive();
      }

      public String getEvictedKey() {
         return evictedKey;
      }
   }

}
