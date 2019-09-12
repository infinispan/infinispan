package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "api.GetOnRemovedKeyTest")
@InCacheMode({ CacheMode.REPL_SYNC, CacheMode.DIST_SYNC })
public class GetOnRemovedKeyTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(TestDataSCI.INSTANCE, getDefaultClusteredCacheConfig(cacheMode, true), 2);
      waitForClusterToForm();
   }

   public void testRemoveSeenCorrectly1() throws Throwable {
      Object k = getKey();
      cache(0).put(k, "v");
      tm(0).begin();
      cache(0).remove(k);
      assertNull(cache(0).get(k));
      tm(0).commit();
      assertNull(cache(0).get(k));
   }

   public void testRemoveSeenCorrectly2() throws Throwable {
      Object k = getKey();
      cache(0).put(k, "v");
      tm(0).begin();
      cache(0).remove(k);
      assertNull(cache(0).get(k));
      tm(0).rollback();
      assertEquals("v", cache(0).get(k));
   }

   protected Object getKey() {
      return cacheMode.isDistributed() ? getKeyForCache(0) : "k";
   }
}
