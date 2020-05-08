package org.infinispan.anchored;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.anchored.configuration.AnchoredKeysConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stationary.ScaleUpWithoutStateTransferTest")
public class ScaleUpWithoutStateTransferTest extends MultipleCacheManagersTest {
   public static final String CACHE_NAME = "testCache";
   public static final String KEY_1 = "key1";
   public static final String KEY_2 = "key2";
   public static final String KEY_3 = "key3";
   public static final String VALUE = "value";

   private StorageType storageType;

   @Override
   public Object[] factory() {
      return new Object[] {
            new ScaleUpWithoutStateTransferTest().storageType(StorageType.OBJECT),
//            new ScaleUpWithoutStateTransferTest().storageType(StorageType.BINARY),
//            new ScaleUpWithoutStateTransferTest().storageType(StorageType.OFF_HEAP),
            };
   }

   public ScaleUpWithoutStateTransferTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      addNode();
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"storageType"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{storageType};
   }

   private void addNode() {
      GlobalConfigurationBuilder managerBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      managerBuilder.defaultCacheName(CACHE_NAME);

      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.REPL_SYNC).hash().numSegments(1);
      cacheBuilder.memory().storageType(storageType);
      cacheBuilder.addModule(AnchoredKeysConfigurationBuilder.class).enabled(true);

      addClusterEnabledCacheManager(managerBuilder, cacheBuilder);
   }

   public void testEntriesAreAddedToNewestNode() {
      cache(0).put(KEY_1, VALUE);
      DataContainer<Object, Object> dataContainer = advancedCache(0).getDataContainer();

      assertValue(KEY_1, VALUE);
      assertNoValue(KEY_2);

      assertLocation(KEY_1, 0, VALUE);
      assertNoLocation(KEY_2);

      addNode();

      assertValue(KEY_1, VALUE);
      assertNoValue(KEY_2);

      cache(0).put(KEY_2, VALUE);
      cache(0).put(KEY_3, VALUE);

      assertValue(KEY_1, VALUE);
      assertValue(KEY_2, VALUE);
      assertValue(KEY_3, VALUE);

      TestingUtil.waitForNoRebalance(caches());
      assertLocation(KEY_1, 0, VALUE);
      assertLocation(KEY_2, 1, VALUE);

      addNode();

      assertValue(KEY_1, VALUE);
      assertValue(KEY_2, VALUE);
      assertValue(KEY_3, VALUE);

      TestingUtil.waitForNoRebalance(caches());
      assertLocation(KEY_1, 0, VALUE);
      assertLocation(KEY_2, 1, VALUE);
      assertLocation(KEY_3, 1, VALUE);
   }

   private void assertValue(Object key, String expectedValue) {
      for (Cache<Object, Object> cache : caches()) {
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         Object value = cache.get(key);
         assertEquals("Wrong value for " + key + " on " + address, expectedValue, value);
      }
   }

   private void assertNoValue(Object key) {
      for (Cache<Object, Object> cache : caches()) {
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         Object value = cache.get(key);
         assertNull("Extra value for " + key + " on " + address, value);
      }
   }

   private void assertLocation(Object key, int ownerIndex, String expectedValue) {
      List<Cache<Object, Object>> caches = caches();
      for (int i = 0; i < caches.size(); i++) {
         Cache<Object, Object> cache = caches.get(i);
         InternalCacheEntry<Object, Object> entry = cache.getAdvancedCache().getDataContainer().peek(key);
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         if (i == ownerIndex) {
            assertEquals("Wrong value for " + key + " on " + address, expectedValue, entry.getValue());
         } else {
            assertEquals("Wrong location for " + key + " on " + address, address(ownerIndex), entry.getValue());
         }
      }
   }

   private void assertNoLocation(Object key) {
      List<Cache<Object, Object>> caches = caches();
      for (Cache<Object, Object> cache : caches) {
         InternalCacheEntry<Object, Object> entry = cache.getAdvancedCache().getDataContainer().peek(key);
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         assertNull("Expected no location on " + address, entry);
      }

   }
}
