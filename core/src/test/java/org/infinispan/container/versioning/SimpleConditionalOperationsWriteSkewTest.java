package org.infinispan.container.versioning;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "container.versioning.SimpleConditionalOperationsWriteSkewTest")
public class SimpleConditionalOperationsWriteSkewTest extends MultipleCacheManagersTest {

   protected ConfigurationBuilder getConfig() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().locking().writeSkewCheck(true);
      dcc.transaction().locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      dcc.transaction().versioning().enable().scheme(VersioningScheme.SIMPLE);
      return dcc;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getConfig();
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testReplaceFromMainOwner() throws Throwable {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      tm(0).begin();
      cache(0).put("kkk", "vvv");
      cache(0).replace(k, "v1", "v2");
      tm(0).commit();

      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "0");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "0");

      log.trace("here is the interesting replace.");
      cache(0).replace(k, "0", "1");
      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "1");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "1");
   }

   public void testRemoveFromMainOwner() {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      cache(0).remove(k, "1");

      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "0");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "0");

      cache(0).remove(k, "0");
      assertNull(advancedCache(0).getDataContainer().get(k));
      assertNull(advancedCache(1).getDataContainer().get(k));
   }

   public void testPutIfAbsentFromMainOwner() {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      cache(0).putIfAbsent(k, "1");

      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "0");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "0");

      cache(0).remove(k);

      cache(0).putIfAbsent(k, "1");
      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "1");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "1");
   }
}
