package org.infinispan.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.testng.annotations.Test;

/**
 * https://issues.jboss.org/browse/ISPN-3141
 */
@Test(groups = "functional", testName = "api.IgnoreReturnValueForConditionalOperationsTest")
public class IgnoreReturnValueForConditionalOperationsTest extends MultipleCacheManagersTest {

   @Override
   public Object[] factory() {
      return new Object[] {
         new IgnoreReturnValueForConditionalOperationsTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
         new IgnoreReturnValueForConditionalOperationsTest().cacheMode(CacheMode.DIST_SYNC).transactional(true),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(cacheMode, transactional);
      createCluster(TestDataSCI.INSTANCE, dcc, 2);
      waitForClusterToForm();
   }


   public void testConditionalReplace() {
      Object k = init();
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object put = cache.put("kx", "vx");
      put = cache.put("kx", "vx");
      assertTrue(advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).replace(k, "v0", "v1"));
      assertEquals("v1", cache(0).get(k));
      assertEquals("v1", cache(1).get(k));
   }

   public void testConditionalRemove() {
      Object k = init();
      assertTrue(advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).remove(k, "v0"));
      assertNull(cache(0).get(k));
      assertNull(cache(1).get(k));
   }

   private Object init() {
      Object k = getKeyForCache(1);
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));
      assertEquals("v0", cache(1).get(k));
      return k;
   }
}
