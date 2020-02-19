package org.infinispan.anchored;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.anchored.configuration.AnchoredKeysConfigurationBuilder;
import org.infinispan.anchored.impl.AnchoredKeysInterceptor;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stationary.ScalingUpWithoutStateTransferTest")
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
            new ScaleUpWithoutStateTransferTest().storageType(StorageType.BINARY),
            new ScaleUpWithoutStateTransferTest().storageType(StorageType.OFF_HEAP),
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
      cacheBuilder.clustering().cacheMode(CacheMode.INVALIDATION_SYNC).hash().numSegments(1);
      cacheBuilder.memory().storageType(storageType);
      cacheBuilder.addModule(AnchoredKeysConfigurationBuilder.class).enabled(true);

      addClusterEnabledCacheManager(managerBuilder, cacheBuilder);
   }

   public void testEntriesAreAddedToNewestNode() {
      cache(0).put(KEY_1, VALUE);

      assertEquals(VALUE, cache(0).get(KEY_1));
      assertNull(cache(0).get(KEY_2));

      Cache<Object, Object> anchorCache = cache(0, AnchoredKeysInterceptor.ANCHOR_CACHE_PREFIX + CACHE_NAME);
      assertEquals(address(0), anchorCache.get(KEY_1));
      assertNull(anchorCache.get(KEY_2));

      addNode();

      assertEquals(VALUE, cache(0).get(KEY_1));
      assertEquals(VALUE, cache(1).get(KEY_1));
      assertNull(cache(0).get(KEY_2));
      assertNull(cache(0).get(KEY_2));

      cache(0).put(KEY_2, VALUE);
      cache(0).put(KEY_3, VALUE);

      assertEquals(VALUE, cache(0).get(KEY_1));
      assertEquals(VALUE, cache(1).get(KEY_1));
      assertEquals(VALUE, cache(0).get(KEY_2));
      assertEquals(VALUE, cache(1).get(KEY_2));
      assertEquals(VALUE, cache(0).get(KEY_3));
      assertEquals(VALUE, cache(1).get(KEY_3));

      assertEquals(address(0), anchorCache.get(KEY_1));
      assertEquals(address(1), anchorCache.get(KEY_2));
      assertEquals(address(1), anchorCache.get(KEY_3));

      addNode();

      assertEquals(VALUE, cache(2).get(KEY_1));
      assertEquals(VALUE, cache(2).get(KEY_2));
      assertEquals(VALUE, cache(2).get(KEY_3));
   }
}
