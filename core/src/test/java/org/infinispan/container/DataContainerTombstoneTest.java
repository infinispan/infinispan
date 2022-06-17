package org.infinispan.container;

import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Basic functional tests for tombstones stored in DataContainer.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "container.DataContainerTombstoneTest")
public class DataContainerTombstoneTest extends SingleCacheManagerTest {

   private StorageType storageType = StorageType.HEAP;

   private DataContainerTombstoneTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   public void testStateTransfer() throws Exception {
      String cacheName = "state-transfer-test";
      ConfigurationBuilder builder = defaultConfiguration();
      Cache<Object, String> cache1 = defineCache(cacheManager, cacheName, builder);

      Map<Object, PrivateMetadata> tombstones = createAndStoreTombstones(cache1, 2);

      try (EmbeddedCacheManager manager2 = createCacheManager()) {
         Cache<Object, String> cache2 = manager2.getCache(cacheName);
         TestingUtil.waitForNoRebalance(cache1, cache2);
         for (Map.Entry<Object, PrivateMetadata> entry : tombstones.entrySet()) {
            assertTombstoneInDataContainer(cache2, entry.getKey(), entry.getValue());
         }
      }
   }

   public void testStream() {
      String cacheName = "stream";
      ConfigurationBuilder builder = defaultConfiguration();
      Cache<Object, String> cache1 = defineCache(cacheManager, cacheName, builder);

      Map<Object, String> data = createAndStoreData(cache1, 20);
      Map<Object, PrivateMetadata> tombstones = createAndStoreTombstones(cache1, 20);

      // by default, only data is returned
      cache1.forEach((key, value) -> {
         assertEquals(data.get(key), value);
         assertFalse(tombstones.containsKey(key));
      });

      // STREAM_TOMBSTONES (internal usage) return tombstones too
      cache1.getAdvancedCache().withFlags(Flag.STREAM_TOMBSTONES).forEach((key, value) -> {
         if (data.containsKey(key)) {
            assertEquals(data.get(key), value);
            assertFalse(tombstones.containsKey(key));
         } else {
            assertTrue(tombstones.containsKey(key));
         }
      });

      cache1.keySet().forEach(k -> {
         assertTrue(data.containsKey(k));
         assertFalse(tombstones.containsKey(k));
      });

      cache1.getAdvancedCache().withFlags(Flag.STREAM_TOMBSTONES).keySet().forEach(k -> assertTrue(data.containsKey(k) || tombstones.containsKey(k)));
   }

   public void testDataAccess() {
      String cacheName = "data-access";
      ConfigurationBuilder builder = defaultConfiguration();
      Cache<Object, String> cache1 = defineCache(cacheManager, cacheName, builder);

      Map<Object, PrivateMetadata> tombstones = createAndStoreTombstones(cache1, 5);

      // tombstones should not count
      assertEquals(0, cache1.size());
      assertTrue(cache1.isEmpty());

      Map<Object, String> data = createAndStoreData(cache1, 15);

      // data should be there
      data.forEach((k, v) -> assertEquals(v, cache1.get(k)));

      // tombstones don't
      tombstones.forEach((k, __) -> {
         assertNull(cache1.get(k));
         assertFalse(cache1.containsKey(k));
      });

      assertEquals(data.size(), cache1.size());
   }

   @Override
   protected String parameters() {
      return defaultParametersString(parameterNames(), parameterValues());
   }

   private static String[] parameterNames() {
      return new String[]{null};
   }

   private Object[] parameterValues() {
      return new Object[]{storageType};
   }

   @Factory
   public Object[] defaultFactory() {
      return new Object[]{
            new DataContainerTombstoneTest().storageType(StorageType.HEAP),
            new DataContainerTombstoneTest().storageType(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return createClusteredCacheManager(true, GlobalConfigurationBuilder.defaultClusteredBuilder(), null, new TransportFlags());
   }

   private static PrivateMetadata createTombstoneMetadata(Cache<?, ?> cache, int version) {
      VersionGenerator generator = TestingUtil.extractComponent(cache, VersionGenerator.class);
      IncrementableEntryVersion entryVersion = generator.generateNew();
      while (version > 0) {
         entryVersion = generator.increment(entryVersion);
         version--;
      }
      return new PrivateMetadata.Builder().entryVersion(entryVersion).tombstone(true).build();
   }

   private static <K, V> Cache<K, V> defineCache(EmbeddedCacheManager cacheManager, String name, ConfigurationBuilder builder) {
      return cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(name, builder.build());
   }

   private ConfigurationBuilder defaultConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      builder.clustering().hash().numSegments(32);
      // optimistic transaction forced since we need to interact with PrivateMetadata (or cross-site)
      // other configuration just ignore the PrivateMetadata (during state transfer at least)
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.OPTIMISTIC);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.memory().storage(storageType);
      return builder;
   }

   private static void putTombstone(Cache<Object, ?> cache, Object key, PrivateMetadata metadata) {
      assertTrue(metadata.isTombstone()); //make sure it is correct
      Object storageKey = cache.getAdvancedCache().getKeyDataConversion().toStorage(key);
      int segment = TestingUtil.getSegmentForKey(storageKey, cache);
      TestingUtil.internalDataContainer(cache).putTombstone(segment, storageKey, metadata);
   }

   private static void assertTombstoneInDataContainer(Cache<Object, ?> cache, Object key, PrivateMetadata metadata) {
      Object storageKey = cache.getAdvancedCache().getKeyDataConversion().toStorage(key);
      int segment = TestingUtil.getSegmentForKey(storageKey, cache);
      InternalCacheEntry<Object, ?> entry = TestingUtil.internalDataContainer(cache).peek(segment, storageKey);
      assertNotNull(entry);
      assertTrue(entry.isTombstone());
      assertEquals(metadata, entry.getInternalMetadata());
   }

   private static Map<Object, PrivateMetadata> createAndStoreTombstones(Cache<Object, ?> cache, int nTombstones) {
      Map<Object, PrivateMetadata> tombstones = new HashMap<>(nTombstones);
      for (int i = 0; i < nTombstones; ++i) {
         String key = "tomb-" + i;
         PrivateMetadata metadata = createTombstoneMetadata(cache, i);
         tombstones.put(key, metadata);
         putTombstone(cache, key, metadata);
      }
      return tombstones;
   }

   private static Map<Object, String> createAndStoreData(Cache<Object, String> cache, int nData) {
      Map<Object, String> data = new HashMap<>(nData);
      for (int i = 0; i < nData; ++i) {
         String key = "key-" + i;
         String value = "value-" + i;
         cache.put(key, value);
         data.put(key, value);
      }
      return data;
   }
}
