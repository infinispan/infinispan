package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

@Test (groups = "functional", testName = "api.GetOnRemovedKeyTest")
public class GetOnRemovedKeyTest extends MultipleCacheManagersTest {

   protected CacheMode mode = CacheMode.REPL_SYNC;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(mode, true), 2);
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
      return "k";
   }
}
