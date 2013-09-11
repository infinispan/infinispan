package org.infinispan.invalidation;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests to ensure that invalidation caches with a cluster cache loader properly retrieve the value from the remote
 * cache
 *
 * @author William burns
 * @since 6.0
 */
public class ClusteredCacheLoaderInvalidationTest extends MultipleCacheManagersTest {
   private static final String key = "key";
   private static final String value = "value";
   private static final String changedValue = "changed-value";

   private static final String cacheName = "inval-write-cache-store";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false);
      cb.persistence().addStore(ClusterLoaderConfigurationBuilder.class);
      createClusteredCaches(2, cacheName, cb);
   }

   @Test
   public void testCacheLoaderBeforeAfterInvalidation() {
      assertNull(cache(0, cacheName).get(key));

      cache(0, cacheName).put(key, value);

      assertFalse("Invalidation should not contain the value in memory",
                  cache(1, cacheName).getAdvancedCache().getDataContainer().containsKey(key));

      assertEquals(value, cache(1, cacheName).get(key));

      cache(1, cacheName).put(key, changedValue);

      assertFalse("Invalidation should not contain the value in memory after other node put",
                  cache(0, cacheName).getAdvancedCache().getDataContainer().containsKey(key));

      assertEquals(changedValue, cache(0, cacheName).get(key));
   }
}
