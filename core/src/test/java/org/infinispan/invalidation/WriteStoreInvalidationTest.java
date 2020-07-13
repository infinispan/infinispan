package org.infinispan.invalidation;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tests to ensure that invalidation caches operate properly with a shared cache store with various operations.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "invalidation.WriteStoreInvalidationTest")
public class WriteStoreInvalidationTest extends MultipleCacheManagersTest {
   private static final String key = "key";
   private static final String value = "value";
   private static final String changedValue = "changed-value";

   private static final String cacheName = "inval-write-cache-store";

   private boolean transactional;

   public WriteStoreInvalidationTest transactional(boolean transactional) {
      this.transactional = transactional;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new WriteStoreInvalidationTest().transactional(true),
            new WriteStoreInvalidationTest().transactional(false),
      };
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "transactional");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), transactional);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, transactional);
      cb.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true)
            .storeName(getClass().getSimpleName());
      createClusteredCaches(2, cacheName, cb);
   }

   @Test
   public void testSharedCacheStoreAfterInvalidation() {
      // Because the store is shared, statistics are also shared
      DummyInMemoryStore store0 = TestingUtil.getFirstStore(cache(0, cacheName));

      store0.clearStats();

      assertNull(cache(0, cacheName).get(key));
      assertStoreStats(store0, 1, 0, 0);

      // Put either loads the previous value or doesn't based on whether the originator is the primary owner
      // So we use IGNORE_RETURN_VALUES to keep the number of loads stable
      advancedCache(0, cacheName).withFlags(Flag.IGNORE_RETURN_VALUES).put(key, value);
      assertStoreStats(store0, 1, 1, 0);

      assertEquals(value, cache(1, cacheName).get(key));
      assertStoreStats(store0, 2, 1, 0);

      advancedCache(1, cacheName).withFlags(Flag.IGNORE_RETURN_VALUES).put(key, changedValue);
      assertStoreStats(store0, 2, 2, 0);

      assertFalse(cache(0, cacheName).getAdvancedCache().getDataContainer().containsKey(key));

      assertEquals(changedValue, cache(0, cacheName).get(key));
      assertStoreStats(store0, 3, 2, 0);
   }

   public void testWriteAndRemoveOnVariousNodes() {
      Cache<String, String> cache0 = cache(0, cacheName);
      Cache<String, String> cache1 = cache(0, cacheName);

      assertNull(cache0.put(key, "foo"));

      assertEquals("foo", cache1.get(key));

      assertEquals("foo", cache1.put(key, "bar"));

      assertEquals("bar", cache0.get(key));
   }

   private void assertStoreStats(DummyInMemoryStore store, int loads, int writes, int deletes) {
      assertEquals(loads, store.stats().get("load").intValue());
      assertEquals(writes, store.stats().get("write").intValue());
      assertEquals(deletes, store.stats().get("delete").intValue());
   }
}
