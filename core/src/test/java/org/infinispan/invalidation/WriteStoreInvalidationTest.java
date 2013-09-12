package org.infinispan.invalidation;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests to ensure that invalidation caches operate properly with a shared cache store with various operations.
 *
 * @author William Burns
 * @since 6.0
 */
public class WriteStoreInvalidationTest extends MultipleCacheManagersTest {
   private static final String key = "key";
   private static final String value = "value";
   private static final String changedValue = "changed-value";

   private static final String cacheName = "inval-write-cache-store";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false);
      cb.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true)
            .storeName(getClass().getSimpleName());
      createClusteredCaches(2, cacheName, cb);
   }

   @Test
   public void testSharedCacheStoreAfterInvalidation() {
      assertNull(cache(0, cacheName).get(key));

      cache(0, cacheName).put(key, value);

      assertEquals(value, cache(1, cacheName).get(key));

      cache(1, cacheName).put(key, changedValue);

      assertFalse(cache(0, cacheName).getAdvancedCache().getDataContainer().containsKey(key));

      assertEquals(changedValue, cache(0, cacheName).get(key));
   }
}
